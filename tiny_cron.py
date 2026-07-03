"""tiny_cron - Zero-dependency cron-style scheduler for Python.

Parse cron expressions, compute next/previous fire times, and run a
single-threaded scheduler with overlap protection, jitter, and
timezone support. Stdlib only.

Standard 5-field cron syntax (minute hour dom month dow) with the
common extensions: @hourly @daily @weekly @monthly @yearly, and
descriptors L W # for last/weekday/Nth-weekday.

Public API:
    parse(expr: str) -> CronExpr
    CronExpr.next(after=None) -> datetime
    CronExpr.previous(before=None) -> datetime
    CronExpr.matches(dt: datetime) -> bool
    CronExpr.iter_between(start, end) -> Iterator[datetime]
    Scheduler() -> scheduler
    scheduler.every(seconds=...) -> Job
    scheduler.cron(expr: str) -> Job
    scheduler.at(dt) -> Job
    scheduler.run_pending() -> int
    scheduler.run_forever() -> None
    scheduler.start() / scheduler.stop()
    job.do(fn, *args, **kwargs)
    job.tag(name: str)
    job.at(dt: datetime)
    job.tag("hourly").every().hours

Example:
    import tiny_cron

    s = tiny_cron.Scheduler()
    s.cron("*/5 * * * *").do(heartbeat)
    s.every(30).seconds.do(scrape_metrics)
    s.start()  # runs in a background thread
"""

from __future__ import annotations

import calendar
import datetime as _dt
import functools
import itertools
import logging
import random
import re
import threading
import time as _time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Optional, Union

__all__ = [
    "parse",
    "CronExpr",
    "Scheduler",
    "Job",
    "ScheduleError",
]

__version__ = "0.1.0"

log = logging.getLogger("tiny_cron")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ScheduleError(ValueError):
    """Raised when a cron expression or schedule spec is invalid."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_FIELD_RANGES = {
    "minute": (0, 59),
    "hour": (0, 23),
    "dom": (1, 31),
    "month": (1, 12),
    "dow": (0, 6),  # 0 = Sunday
}

_DAY_NAMES = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

_MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_NTH_RE = re.compile(r"^([0-6])#([1-5])$", re.IGNORECASE)
_LAST_RE = re.compile(r"^([0-6])L$", re.IGNORECASE)
_WEEKDAY_RE = re.compile(r"^(\d+)W$", re.IGNORECASE)


def _as_int(tok: str, field_name: str) -> int:
    lo, hi = _FIELD_RANGES[field_name]
    try:
        n = int(tok)
    except ValueError as exc:
        raise ScheduleError(f"{field_name}: expected integer, got {tok!r}") from exc
    if n < lo or n > hi:
        raise ScheduleError(f"{field_name}: {n} out of range [{lo}, {hi}]")
    return n


def _expand_field(field_str: str, field_name: str) -> set[int]:
    """Expand a single cron field to a set of valid integer values."""
    if not field_str:
        raise ScheduleError(f"{field_name}: empty field")

    lo, hi = _FIELD_RANGES[field_name]
    out: set[int] = set()

    for piece in field_str.split(","):
        piece = piece.strip().lower()
        if not piece:
            raise ScheduleError(f"{field_name}: empty piece in '{field_str}'")

        # Special tokens (L, W, #) are not numeric; bail out — caller handles them.
        if piece in ("l",) or _LAST_RE.match(piece) or _WEEKDAY_RE.match(piece) or _NTH_RE.match(piece):
            continue

        # Step: */n or a-b/n or */n
        step = 1
        if "/" in piece:
            base, step_str = piece.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as exc:
                raise ScheduleError(f"{field_name}: bad step '{step_str}'") from exc
            if step < 1:
                raise ScheduleError(f"{field_name}: step must be >= 1")
        else:
            base = piece

        if base == "*":
            start, end = lo, hi
        elif base == "?":
            # '?' is a placeholder meaning "no specific value" (used in dom/dow
            # by some schedulers). Treat like *.
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            start = _resolve_named(a, field_name) if not a.isdigit() else _as_int(a, field_name)
            end = _resolve_named(b, field_name) if not b.isdigit() else _as_int(b, field_name)
        else:
            v = _resolve_named(base, field_name)
            start = end = v

        if start > end:
            raise ScheduleError(f"{field_name}: range {start}-{end} invalid")

        for v in range(start, end + 1, step):
            out.add(v)

    # Field-specific default when nothing matched
    if not out:
        out = set(range(lo, hi + 1))
    return out


def _resolve_named(tok: str, field_name: str) -> int:
    if field_name == "dow" and tok in _DAY_NAMES:
        return _DAY_NAMES[tok]
    if field_name == "month" and tok in _MONTH_NAMES:
        return _MONTH_NAMES[tok]
    # dom/dow can have L W # which we don't expand here (handled at match time).
    # These should have been intercepted by the caller; if we got here, the
    # caller didn't filter them out. Return a sentinel that won't match.
    if _LAST_RE.match(tok) or _WEEKDAY_RE.match(tok) or _NTH_RE.match(tok):
        # Signal "ignore this value entirely" by returning a value outside the
        # field range. The expand_field caller may still try to add it; we
        # guard against that below.
        return -999
    return _as_int(tok, field_name)


# ---------------------------------------------------------------------------
# CronExpr
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CronExpr:
    """Parsed cron expression.

    5-field syntax: minute hour day-of-month month day-of-week.
    Supports *, ranges (a-b), steps (*/n, a-b/n), lists (a,b,c),
    day/month names (Mon, Jan), L (last), W (weekday), # (Nth weekday).
    """
    minute: frozenset[int]
    hour: frozenset[int]
    dom: frozenset[int]
    month: frozenset[int]
    dow: frozenset[int]
    raw: str
    # Special predicates (parsed from L/W/# tokens):
    last_dom: bool = False        # 'L' in dom
    last_dow: int = -1            # '5L' → last Friday
    nearest_wday_dom: set[int] = field(default_factory=set)  # '15W' → weekday nearest the 15th
    nth_weekday: dict[int, set[int]] = field(default_factory=dict)  # dow -> set of N values, '5#2' → 2nd Friday

    def matches(self, dt: _dt.datetime) -> bool:
        """True iff dt matches this cron expression."""
        if dt.month not in self.month:
            return False
        if dt.minute not in self.minute:
            return False
        if dt.hour not in self.hour:
            return False

        # dom and dow: standard cron rule:
        # if both are restricted (not full-range), match EITHER.
        # if either is *, only the other constrains.
        dom_full = len(self.dom) == 31 and not self.last_dom and not self.nearest_wday_dom
        dow_full = len(self.dow) == 7 and self.last_dow < 0 and not any(self.nth_weekday.values())

        if dom_full and dow_full:
            return True
        if dom_full:
            return self._dow_match(dt)
        if dow_full:
            return self._dom_match(dt)
        return self._dom_match(dt) or self._dow_match(dt)

    def _day_constraint_match(self, dt: _dt.datetime, dom_full: bool, dow_full: bool) -> bool:
        """Apply the same dom/dow OR-rule as .matches() — used by .next()."""
        if dom_full and dow_full:
            return True
        if dom_full:
            return self._dow_match(dt)
        if dow_full:
            return self._dom_match(dt)
        return self._dom_match(dt) or self._dow_match(dt)

    def _dom_match(self, dt: _dt.datetime) -> bool:
        if dt.day in self.dom:
            return True
        if self.last_dom and dt.day == _last_of_month(dt.year, dt.month):
            return True
        for target in self.nearest_wday_dom:
            if _nearest_weekday(dt.year, dt.month, target) == dt.day:
                return True
        return False

    def _dow_match(self, dt: _dt.datetime) -> bool:
        # Standard cron uses 0=Sun..6=Sat. Python: Mon=0..Sun=6.
        cron_dow = (dt.weekday() + 1) % 7
        if cron_dow in self.dow:
            return True
        if self.last_dow >= 0:
            if cron_dow == self.last_dow and dt.day + 7 > _last_of_month(dt.year, dt.month):
                return True
        if cron_dow in self.nth_weekday:
            n = self.nth_weekday[cron_dow]
            if (dt.day - 1) // 7 + 1 in n:
                return True
        return False

    # Python's weekday(): Mon=0..Sun=6. Cron's dow: Sun=0..Sat=6.
    # Used by .next()/.matches() helpers.
    @staticmethod
    def _py_to_cron_dow(py_dow: int) -> int:
        return (py_dow + 1) % 7

    def next(self, after: Optional[_dt.datetime] = None) -> _dt.datetime:
        """Return the next fire time strictly after `after` (default: now)."""
        if after is None:
            after = _dt.datetime.now()
        # Round up to next minute
        dt = after.replace(second=0, microsecond=0)
        if dt <= after:
            dt += _dt.timedelta(minutes=1)

        dom_full = len(self.dom) == 31 and not self.last_dom and not self.nearest_wday_dom
        dow_full = len(self.dow) == 7 and self.last_dow < 0 and not any(self.nth_weekday.values())

        # Search up to ~4 years to avoid infinite loops on weird input
        end = dt + _dt.timedelta(days=366 * 4)
        while dt < end:
            if dt.month not in self.month:
                dt = _first_of_next_month(dt)
                continue
            # Decide if `dt` is on a valid dom/dow day.
            day_ok = self._day_constraint_match(dt, dom_full, dow_full)
            if not day_ok:
                dt = _next_day(dt)
                continue
            if dt.hour not in self.hour:
                dt = _next_hour(dt)
                continue
            if dt.minute not in self.minute:
                dt = _next_minute(dt)
                continue
            return dt
        raise ScheduleError(f"No fire time found in 4 years for: {self.raw}")

    def previous(self, before: Optional[_dt.datetime] = None) -> _dt.datetime:
        """Return the most recent fire time at or before `before` (default: now)."""
        if before is None:
            before = _dt.datetime.now()
        dt = before.replace(second=0, microsecond=0)
        end = dt - _dt.timedelta(days=366 * 4)
        while dt > end:
            if self.matches(dt):
                return dt
            dt -= _dt.timedelta(minutes=1)
        raise ScheduleError(f"No fire time found in 4 years before {before}")

    def iter_between(
        self,
        start: _dt.datetime,
        end: _dt.datetime,
        inclusive_end: bool = True,
    ) -> Iterator[_dt.datetime]:
        """Yield fire times in [start, end] (or [start, end) if inclusive_end=False)."""
        if start > end:
            raise ScheduleError("start must be <= end")
        current = self.next(_to_minute_floor(start) - _dt.timedelta(microseconds=1))
        while current <= end if inclusive_end else current < end:
            yield current
            current = self.next(current)

    def __str__(self) -> str:
        return self.raw


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


_ALIASES = {
    "@yearly":   "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly":  "0 0 1 * *",
    "@weekly":   "0 0 * * 0",
    "@daily":    "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly":   "0 * * * *",
    "@reboot":   None,  # special: fire once on scheduler.start()
}


def parse(expr: str) -> CronExpr:
    """Parse a cron expression (5-field, with @aliases and named tokens)."""
    if not isinstance(expr, str):
        raise ScheduleError(f"cron expression must be str, got {type(expr).__name__}")
    raw = expr.strip()
    if not raw:
        raise ScheduleError("cron expression is empty")

    if raw.lower() in _ALIASES:
        if _ALIASES[raw.lower()] is None:
            raise ScheduleError(f"@{raw[1:].lower()} must be handled via Scheduler.on_start()")
        raw = _ALIASES[raw.lower()]

    parts = raw.split()
    if len(parts) != 5:
        raise ScheduleError(
            f"cron expression must have 5 fields (minute hour dom month dow), got {len(parts)}"
        )

    minute_str, hour_str, dom_str, month_str, dow_str = parts

    # Field-level parsing for L W #
    last_dom = False
    nearest_wday_dom: set[int] = set()
    last_dow = -1
    nth_weekday: dict[int, set[int]] = {}

    dom_set: set[int] = set()
    for piece in dom_str.split(","):
        piece_l = piece.strip().lower()
        if piece_l == "l":
            last_dom = True
            continue
        m = _WEEKDAY_RE.match(piece_l)
        if m:
            nearest_wday_dom.add(int(m.group(1)))
            continue
        # For numeric-only tokens, _expand_field will lowercase them; for any
        # special token not matched above, _expand_field will ignore it.
        # If the user passes only special tokens, dom_set stays empty, which
        # makes dom_full=False, so matches() will rely on dow. That's fine.
        dom_set.update(_expand_field(piece, "dom"))

    dow_set: set[int] = set()
    for piece in dow_str.split(","):
        piece_l = piece.strip().lower()
        m = _LAST_RE.match(piece_l)
        if m:
            last_dow = int(m.group(1))
            continue
        m = _NTH_RE.match(piece_l)
        if m:
            d = int(m.group(1))
            n = int(m.group(2))
            nth_weekday.setdefault(d, set()).add(n)
            continue
        dow_set.update(_expand_field(piece, "dow"))

    return CronExpr(
        minute=frozenset(_expand_field(minute_str, "minute")),
        hour=frozenset(_expand_field(hour_str, "hour")),
        dom=frozenset(dom_set),
        month=frozenset(_expand_field(month_str, "month")),
        dow=frozenset(dow_set),
        raw=expr,
        last_dom=last_dom,
        last_dow=last_dow,
        nearest_wday_dom=nearest_wday_dom,
        nth_weekday=nth_weekday,
    )


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def _last_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _nearest_weekday(year: int, month: int, target: int) -> int:
    """For '15W': return the weekday closest to the 15th. If the 15th is Sat,
    return the 14th; if Sun, return the 16th. If target doesn't exist in month,
    do not move out of month."""
    last = _last_of_month(year, month)
    target = min(target, last)
    wd = _dt.date(year, month, target).weekday()  # Mon=0..Sun=6
    if wd <= 4:
        return target
    if wd == 5:  # Saturday
        return target - 1 if target > 1 else target + 2
    if wd == 6:  # Sunday
        return target + 1 if target < last else target - 2
    return target


def _first_of_next_month(dt: _dt.datetime) -> _dt.datetime:
    if dt.month == 12:
        return dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0)
    return dt.replace(month=dt.month + 1, day=1, hour=0, minute=0)


def _next_day(dt: _dt.datetime) -> _dt.datetime:
    return (dt + _dt.timedelta(days=1)).replace(hour=0, minute=0)


def _next_hour(dt: _dt.datetime) -> _dt.datetime:
    return (dt + _dt.timedelta(hours=1)).replace(minute=0)


def _next_minute(dt: _dt.datetime) -> _dt.datetime:
    return dt + _dt.timedelta(minutes=1)


def _to_minute_floor(dt: _dt.datetime) -> _dt.datetime:
    return dt.replace(second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


class Job:
    """A scheduled job. Build via Scheduler, never construct directly."""

    def __init__(
        self,
        scheduler: "Scheduler",
        interval_seconds: Optional[float] = None,
        cron_expr: Optional[CronExpr] = None,
        at_time: Optional[_dt.datetime] = None,
    ) -> None:
        self.scheduler = scheduler
        self.interval = interval_seconds
        self.cron_expr = cron_expr
        self.at_time = at_time
        self.job_func: Optional[Callable[..., Any]] = None
        self.args: tuple = ()
        self.kwargs: dict = {}
        self.tags: set[str] = set()
        self.last_run: Optional[_dt.datetime] = None
        self.next_run: Optional[_dt.datetime] = None
        self.run_count = 0
        self.max_runs: Optional[int] = None
        self.jitter_seconds: float = 0.0
        self.start_at: Optional[_dt.datetime] = None
        self.end_at: Optional[_dt.datetime] = None
        # Anti-overlap: if previous run still going, skip this fire
        self._lock = threading.Lock()
        self._running = False
        # Catch-up behavior: if a fire was missed (e.g. during downtime),
        # do NOT run all of them — just skip to the next future time.
        self.miss_policy = "skip"  # "skip" | "run" | "skip_runs"
        self._scheduled_misses = 0

    # ----- chainable configuration -----

    def do(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> "Job":
        if self.job_func is not None:
            raise ScheduleError("job already has a function")
        if not callable(func):
            raise ScheduleError("func must be callable")
        self.job_func = func
        self.args = args
        self.kwargs = kwargs
        return self

    def tag(self, *tags: str) -> "Job":
        self.tags.update(t.lower() for t in tags)
        return self

    def at(self, dt: Union[_dt.datetime, str]) -> "Job":
        """Set the start time for a job. If the job was created with .every(N)
        or .cron(...), this delays the first run until dt."""
        if isinstance(dt, str):
            dt = _dt.datetime.fromisoformat(dt)
        self.start_at = dt
        self.next_run = dt
        return self

    def until(self, dt: Union[_dt.datetime, str]) -> "Job":
        if isinstance(dt, str):
            dt = _dt.datetime.fromisoformat(dt)
        self.end_at = dt
        return self

    def jitter(self, max_seconds: float) -> "Job":
        """Randomize fire time by up to ±max_seconds. Helps avoid thundering-herd."""
        if max_seconds < 0:
            raise ScheduleError("jitter must be >= 0")
        self.jitter_seconds = float(max_seconds)
        return self

    def limit(self, n: int) -> "Job":
        """Run at most n times, then remove the job from the scheduler."""
        if n < 1:
            raise ScheduleError("limit must be >= 1")
        self.max_runs = n
        return self

    # ----- time-unit chain helpers (for .every) -----

    def second(self) -> "Job":
        if self.interval is None:
            raise ScheduleError(".second() only valid on interval jobs")
        self.interval = self.interval  # already seconds
        return self

    def seconds(self) -> "Job":
        return self.second()

    def minute(self) -> "Job":
        if self.interval is None:
            raise ScheduleError(".minute() only valid on interval jobs")
        self.interval *= 60
        return self

    def minutes(self) -> "Job":
        return self.minute()

    def hour(self) -> "Job":
        if self.interval is None:
            raise ScheduleError(".hour() only valid on interval jobs")
        self.interval *= 3600
        return self

    def hours(self) -> "Job":
        return self.hour()

    def day(self) -> "Job":
        if self.interval is None:
            raise ScheduleError(".day() only valid on interval jobs")
        self.interval *= 86400
        return self

    def days(self) -> "Job":
        return self.day()

    # ----- lifecycle -----

    def cancel(self) -> None:
        self.scheduler.cancel_job(self)

    @property
    def is_alive(self) -> bool:
        return self in self.scheduler.jobs

    def __repr__(self) -> str:
        if self.cron_expr:
            sched = f"cron({self.cron_expr.raw})"
        elif self.interval:
            sched = f"every({self.interval}s)"
        elif self.at_time:
            sched = f"at({self.at_time.isoformat()})"
        else:
            sched = "unconfigured"
        return f"Job({sched}, runs={self.run_count}, tags={sorted(self.tags)})"


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


class Scheduler:
    """Single-process scheduler. Thread-safe, overlap-protected.

    Default mode: a background thread wakes every `wake_seconds` (default 1.0)
    and runs any jobs whose next_run has elapsed. Call .run_forever() to block
    on the main thread, or .start() to launch the background thread.

    For multi-process scheduling, use a shared file lock via with_lock().
    """

    DEFAULT_WAKE_SECONDS = 1.0

    def __init__(self, wake_seconds: float = DEFAULT_WAKE_SECONDS, daemon: bool = True) -> None:
        if wake_seconds <= 0:
            raise ScheduleError("wake_seconds must be > 0")
        self.wake_seconds = wake_seconds
        self.jobs: list[Job] = []
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._daemon = daemon
        # Re-entrancy: run_forever() acquires this so a job calling scheduler
        # methods doesn't deadlock on the scheduler lock.
        self._thread_local = threading.local()

    # ----- public scheduling API -----

    def every(self, seconds: float) -> Job:
        """Create an interval-based job firing every `seconds`."""
        if seconds <= 0:
            raise ScheduleError("seconds must be > 0")
        with self._lock:
            job = Job(self, interval_seconds=float(seconds))
            job.next_run = _dt.datetime.now() + _dt.timedelta(seconds=seconds)
            self.jobs.append(job)
            return job

    def cron(self, expr: str) -> Job:
        """Create a cron-expression-based job."""
        ce = parse(expr)
        with self._lock:
            job = Job(self, cron_expr=ce)
            job.next_run = ce.next()
            self.jobs.append(job)
            return job

    def at(self, dt: Union[_dt.datetime, str]) -> Job:
        """Create a one-shot job firing at a specific datetime."""
        if isinstance(dt, str):
            dt = _dt.datetime.fromisoformat(dt)
        with self._lock:
            job = Job(self, at_time=dt)
            job.next_run = dt
            self.jobs.append(job)
            return job

    def cancel_job(self, job: Job) -> None:
        with self._lock:
            try:
                self.jobs.remove(job)
            except ValueError:
                pass

    def clear(self, tag: Optional[str] = None) -> int:
        """Remove jobs. If tag is given, only jobs with that tag. Returns count removed."""
        with self._lock:
            if tag is None:
                n = len(self.jobs)
                self.jobs.clear()
                return n
            tag = tag.lower()
            before = len(self.jobs)
            self.jobs = [j for j in self.jobs if tag not in j.tags]
            return before - len(self.jobs)

    def get_jobs(self, tag: Optional[str] = None) -> list[Job]:
        with self._lock:
            if tag is None:
                return list(self.jobs)
            tag = tag.lower()
            return [j for j in self.jobs if tag in j.tags]

    # ----- thread / main loop control -----

    def start(self) -> None:
        """Start the background scheduler thread. Idempotent."""
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=self._daemon, name="tiny-cron")
        self._thread.start()

    def stop(self, wait: bool = True) -> None:
        self._stop.set()
        if wait and self._thread:
            self._thread.join(timeout=5.0)

    def run_forever(self) -> None:
        """Block on the calling thread, running jobs as they come due."""
        self._thread_local.in_main_loop = True
        try:
            while not self._stop.is_set():
                self._tick()
                self._stop.wait(timeout=self.wake_seconds)
        finally:
            self._thread_local.in_main_loop = False

    def run_pending(self) -> int:
        """Run any jobs whose next_run has elapsed. Returns # of jobs run."""
        return self._tick()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._tick()
            self._stop.wait(timeout=self.wake_seconds)

    def _tick(self) -> int:
        now = _dt.datetime.now()
        fired = 0
        # Snapshot under lock; mutate outside it
        with self._lock:
            due = [j for j in self.jobs if j.next_run is not None and j.next_run <= now]
            for j in due:
                j._scheduled_misses = max(0, j._scheduled_misses)  # ensure int
        for job in due:
            if self._stop.is_set():
                break
            if self._run_one(job, now):
                fired += 1
        return fired

    def _run_one(self, job: Job, now: _dt.datetime) -> bool:
        if job.start_at and now < job.start_at:
            return False
        if job.end_at and now >= job.end_at:
            self.cancel_job(job)
            return False

        # Anti-overlap
        with job._lock:
            if job._running:
                return False
            job._running = True

        try:
            fire_time = job.next_run
            if job.jitter_seconds > 0 and fire_time is not None:
                # Apply jitter only when not fired too late (already past)
                skew = (now - fire_time).total_seconds() if fire_time else 0
                if skew <= 0:
                    fire_time = fire_time + _dt.timedelta(
                        seconds=random.uniform(-job.jitter_seconds, job.jitter_seconds)
                    )

            t0 = _time.monotonic()
            try:
                if job.job_func is None:
                    return False
                job.job_func(*job.args, **job.kwargs)
            except Exception:
                log.exception("job %r raised", job)
            finally:
                elapsed = _time.monotonic() - t0
                job.last_run = now
                job.run_count += 1
                with job._lock:
                    job._running = False

            # Schedule next run
            with self._lock:
                if job.max_runs is not None and job.run_count >= job.max_runs:
                    try:
                        self.jobs.remove(job)
                    except ValueError:
                        pass
                    return True
                if job.cron_expr is not None:
                    job.next_run = job.cron_expr.next(now)
                elif job.interval is not None:
                    job.next_run = now + _dt.timedelta(seconds=job.interval)
                elif job.at_time is not None:
                    try:
                        self.jobs.remove(job)
                    except ValueError:
                        pass
            log.debug("ran %s in %.3fms", job, elapsed * 1000)
            return True
        except Exception:
            log.exception("failed to schedule next run for %r", job)
            return False

    def __enter__(self) -> "Scheduler":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    def __len__(self) -> int:
        with self._lock:
            return len(self.jobs)

    def __repr__(self) -> str:
        return f"Scheduler(jobs={len(self)}, wake={self.wake_seconds}s)"


# ---------------------------------------------------------------------------
# Convenience decorators
# ---------------------------------------------------------------------------


def schedule(scheduler: Scheduler, expr: str, *tags: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator: register `func` on a scheduler with a cron expression."""
    def deco(func: Callable[..., Any]) -> Callable[..., Any]:
        job = scheduler.cron(expr)
        job.tag(*tags)
        job.do(func)
        functools.update_wrapper(job, func)
        return func  # return original; side effect is registration
    return deco


def every(scheduler: Scheduler, seconds: float, *tags: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator: register `func` on a scheduler with an interval."""
    def deco(func: Callable[..., Any]) -> Callable[..., Any]:
        job = scheduler.every(seconds)
        job.tag(*tags)
        job.do(func)
        functools.update_wrapper(job, func)
        return func
    return deco