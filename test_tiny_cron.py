"""Tests for tiny_cron. Stdlib unittest only."""

from __future__ import annotations

import datetime as dt
import re
import threading
import time
import unittest

import tiny_cron as tc
from tiny_cron import (
    CronExpr,
    Job,
    Scheduler,
    ScheduleError,
    parse,
)


class TestParse(unittest.TestCase):
    def test_simple(self):
        c = parse("0 0 * * *")
        self.assertEqual(c.minute, {0})
        self.assertEqual(c.hour, {0})
        self.assertEqual(len(c.dom), 31)
        self.assertEqual(len(c.month), 12)
        self.assertEqual(len(c.dow), 7)

    def test_step(self):
        c = parse("*/15 * * * *")
        self.assertEqual(c.minute, {0, 15, 30, 45})

    def test_range(self):
        c = parse("0 9-17 * * *")
        self.assertEqual(c.hour, set(range(9, 18)))

    def test_list(self):
        c = parse("0,30 * * * *")
        self.assertEqual(c.minute, {0, 30})

    def test_combined(self):
        c = parse("0,15,30,45 9-17 * * 1-5")
        self.assertEqual(c.minute, {0, 15, 30, 45})
        self.assertEqual(c.hour, set(range(9, 18)))
        # dow 1-5: cron uses 0=Sun, Python 0=Mon. so 1..5 = Mon..Fri.
        self.assertEqual(c.dow, {1, 2, 3, 4, 5})

    def test_named_month(self):
        c = parse("0 0 1 jan *")
        self.assertEqual(c.month, {1})
        c = parse("0 0 1 JAN *")
        self.assertEqual(c.month, {1})

    def test_named_day(self):
        c = parse("0 0 * * mon")
        self.assertEqual(c.dow, {1})

    def test_aliases(self):
        self.assertEqual(parse("@hourly").minute, {0})
        self.assertEqual(parse("@hourly").hour, set(range(0, 24)))
        self.assertEqual(parse("@daily").hour, {0})
        self.assertEqual(parse("@daily").minute, {0})
        self.assertEqual(parse("@weekly").dow, {0})
        self.assertEqual(parse("@monthly").dom, {1})
        self.assertEqual(parse("@yearly").month, {1})

    def test_last_dom(self):
        c = parse("0 0 L * *")
        self.assertTrue(c.last_dom)
        # last day of Feb 2024 (leap) = 29
        self.assertTrue(c.matches(dt.datetime(2024, 2, 29, 0, 0)))
        self.assertFalse(c.matches(dt.datetime(2024, 2, 28, 0, 0)))
        # last day of Feb 2023 (non-leap) = 28
        self.assertTrue(c.matches(dt.datetime(2023, 2, 28, 0, 0)))

    def test_last_weekday(self):
        # '5L' = last Friday
        c = parse("0 0 * * 5L")
        self.assertEqual(c.last_dow, 5)
        # Last Friday of Jan 2026 = 30
        self.assertTrue(c.matches(dt.datetime(2026, 1, 30, 0, 0)))
        self.assertFalse(c.matches(dt.datetime(2026, 1, 23, 0, 0)))

    def test_nearest_weekday_dom(self):
        # '15W' = nearest weekday to the 15th
        c = parse("0 0 15W * *")
        self.assertEqual(c.nearest_wday_dom, {15})
        # Jan 15, 2026 is a Thursday → fires on the 15th itself
        self.assertTrue(c.matches(dt.datetime(2026, 1, 15, 0, 0)))
        self.assertFalse(c.matches(dt.datetime(2026, 1, 16, 0, 0)))

    def test_nth_weekday(self):
        # '5#2' = 2nd Friday
        c = parse("0 0 * * 5#2")
        self.assertIn(5, c.nth_weekday)
        self.assertIn(2, c.nth_weekday[5])
        # 2nd Friday of Jan 2026 = 9
        self.assertTrue(c.matches(dt.datetime(2026, 1, 9, 0, 0)))
        self.assertFalse(c.matches(dt.datetime(2026, 1, 16, 0, 0)))

    def test_invalid_field_count(self):
        with self.assertRaises(ScheduleError):
            parse("0 0 * *")
        with self.assertRaises(ScheduleError):
            parse("0 0 * * * * *")

    def test_invalid_range(self):
        with self.assertRaises(ScheduleError):
            parse("99 * * * *")  # minute out of range
        with self.assertRaises(ScheduleError):
            parse("0 24 * * *")  # hour out of range

    def test_empty(self):
        with self.assertRaises(ScheduleError):
            parse("")


class TestMatches(unittest.TestCase):
    def test_basic_match(self):
        c = parse("30 14 1 1 *")
        self.assertTrue(c.matches(dt.datetime(2026, 1, 1, 14, 30)))
        self.assertFalse(c.matches(dt.datetime(2026, 1, 1, 14, 31)))
        self.assertFalse(c.matches(dt.datetime(2026, 2, 1, 14, 30)))
        # Year not constrained by cron; matches every year on Jan 1 14:30
        self.assertTrue(c.matches(dt.datetime(2027, 1, 1, 14, 30)))
        self.assertFalse(c.matches(dt.datetime(2026, 1, 2, 14, 30)))

    def test_dow_dom_or_logic(self):
        # Standard cron: dom and dow are OR'd when both restricted.
        # 0 12 1,15 * 1 means: noon on the 1st, 15th, OR any Monday.
        c = parse("0 12 1,15 * 1")
        # Jan 1, 2026 is a Thursday — matches dom
        self.assertTrue(c.matches(dt.datetime(2026, 1, 1, 12, 0)))
        # Jan 5, 2026 is a Monday — matches dow
        self.assertTrue(c.matches(dt.datetime(2026, 1, 5, 12, 0)))
        # Jan 2, 2026 is a Friday — neither
        self.assertFalse(c.matches(dt.datetime(2026, 1, 2, 12, 0)))


class TestNextPrevious(unittest.TestCase):
    def test_next_basic(self):
        c = parse("0 12 * * *")
        n = c.next(dt.datetime(2026, 1, 1, 0, 0))
        self.assertEqual(n, dt.datetime(2026, 1, 1, 12, 0))

    def test_next_skips(self):
        c = parse("0 12 * * *")
        n = c.next(dt.datetime(2026, 1, 1, 13, 0))
        self.assertEqual(n, dt.datetime(2026, 1, 2, 12, 0))

    def test_next_complex(self):
        c = parse("*/5 * * * *")
        n = c.next(dt.datetime(2026, 1, 1, 0, 3))
        self.assertEqual(n, dt.datetime(2026, 1, 1, 0, 5))

    def test_previous(self):
        c = parse("0 12 * * *")
        p = c.previous(dt.datetime(2026, 1, 1, 13, 0))
        self.assertEqual(p, dt.datetime(2026, 1, 1, 12, 0))

    def test_iter_between(self):
        c = parse("0 12 * * *")
        fires = list(c.iter_between(
            dt.datetime(2026, 1, 1, 0, 0),
            dt.datetime(2026, 1, 4, 0, 0),
            inclusive_end=False,
        ))
        self.assertEqual(len(fires), 3)
        self.assertEqual(fires[0], dt.datetime(2026, 1, 1, 12, 0))
        self.assertEqual(fires[-1], dt.datetime(2026, 1, 3, 12, 0))

    def test_next_after_dow(self):
        c = parse("0 9 * * 1")  # Mondays at 9am
        # Friday Jan 2, 2026 → next Monday is Jan 5
        n = c.next(dt.datetime(2026, 1, 2, 17, 0))
        self.assertEqual(n, dt.datetime(2026, 1, 5, 9, 0))


class TestScheduler(unittest.TestCase):
    def test_every_basic(self):
        s = Scheduler()
        s.every(60).do(lambda: None).tag("test")
        self.assertEqual(len(s), 1)
        s.clear()

    def test_cron(self):
        s = Scheduler()
        s.cron("@hourly").do(lambda: None)
        self.assertEqual(len(s), 1)

    def test_at(self):
        s = Scheduler()
        when = dt.datetime.now() + dt.timedelta(seconds=2)
        s.at(when).do(lambda: None)
        self.assertEqual(len(s), 1)

    def test_run_pending_fires_due(self):
        s = Scheduler()
        counter = {"n": 0}
        def inc():
            counter["n"] += 1
        job = s.every(0.05)
        job.do(inc)
        # Force the job to be due immediately
        job.next_run = dt.datetime.now() - dt.timedelta(seconds=1)
        n = s.run_pending()
        self.assertEqual(n, 1)
        self.assertEqual(counter["n"], 1)
        s.clear()

    def test_anti_overlap(self):
        s = Scheduler()
        blocker = threading.Event()
        started = threading.Event()
        def slow():
            started.set()
            blocker.wait(timeout=5)
        job = s.every(0.05).do(slow)
        job.next_run = dt.datetime.now() - dt.timedelta(seconds=1)
        # Run the first tick on a background thread (it's blocking).
        first = threading.Thread(target=s.run_pending)
        first.start()
        self.assertTrue(started.wait(timeout=1))
        # Second tick from main thread — must NOT fire again (overlap protection).
        job.next_run = dt.datetime.now() - dt.timedelta(seconds=1)
        n = s.run_pending()
        self.assertEqual(n, 0)
        blocker.set()
        first.join(timeout=5)

    def test_background_thread_runs(self):
        s = Scheduler(wake_seconds=0.05)
        counter = {"n": 0}
        s.every(0.05).do(lambda: counter.__setitem__("n", counter["n"] + 1))
        s.start()
        time.sleep(0.5)
        s.stop()
        self.assertGreater(counter["n"], 1)

    def test_tags(self):
        s = Scheduler()
        s.every(60).do(lambda: None).tag("metrics", "hourly")
        s.every(120).do(lambda: None).tag("cleanup")
        self.assertEqual(len(s.get_jobs("metrics")), 1)
        self.assertEqual(len(s.get_jobs("hourly")), 1)
        self.assertEqual(len(s.get_jobs()), 2)
        n = s.clear("metrics")
        self.assertEqual(n, 1)
        self.assertEqual(len(s), 1)

    def test_limit_removes(self):
        s = Scheduler()
        job = s.every(0.05).do(lambda: None).limit(2)
        job.next_run = dt.datetime.now() - dt.timedelta(seconds=1)
        s.run_pending()
        # After first run, next_run is +0.05s in the future; force it due again.
        job.next_run = dt.datetime.now() - dt.timedelta(seconds=1)
        s.run_pending()
        self.assertEqual(len(s), 0)

    def test_jitter_setter(self):
        s = Scheduler()
        job = s.every(60).jitter(5)
        self.assertEqual(job.jitter_seconds, 5.0)

    def test_at_parses_string(self):
        s = Scheduler()
        when = (dt.datetime.now() + dt.timedelta(seconds=10)).isoformat()
        s.at(when).do(lambda: None)
        self.assertEqual(len(s), 1)

    def test_cancel_job(self):
        s = Scheduler()
        job = s.every(60).do(lambda: None)
        self.assertTrue(job.is_alive)
        s.cancel_job(job)
        self.assertFalse(job.is_alive)

    def test_do_twice_raises(self):
        s = Scheduler()
        job = s.every(60)
        job.do(lambda: None)
        with self.assertRaises(ScheduleError):
            job.do(lambda: None)

    def test_run_pending_after_exception(self):
        s = Scheduler()
        def boom():
            raise RuntimeError("intentional")
        job = s.every(0.05).do(boom)
        job.next_run = dt.datetime.now() - dt.timedelta(seconds=1)
        # Must not propagate, must reschedule
        n = s.run_pending()
        self.assertEqual(n, 1)
        self.assertEqual(job.run_count, 1)
        self.assertIsNotNone(job.next_run)

    def test_until_stops(self):
        s = Scheduler()
        past = dt.datetime.now() - dt.timedelta(seconds=1)
        job = s.every(0.05).until(past)
        job.next_run = dt.datetime.now() - dt.timedelta(seconds=1)
        s.run_pending()
        self.assertEqual(len(s), 0)

    def test_decorator(self):
        s = Scheduler()
        @tc.every(s, 60, "decorated")
        def my_task():
            return 42
        self.assertEqual(len(s.get_jobs("decorated")), 1)


class TestSchedulerContext(unittest.TestCase):
    def test_context_manager(self):
        s = Scheduler(wake_seconds=0.05)
        counter = {"n": 0}
        with s:
            s.every(0.05).do(lambda: counter.__setitem__("n", counter["n"] + 1))
            time.sleep(0.3)
        self.assertGreater(counter["n"], 1)


class TestRepr(unittest.TestCase):
    def test_job_repr_cron(self):
        s = Scheduler()
        job = s.cron("@hourly").do(lambda: None)
        r = repr(job)
        self.assertIn("cron", r)

    def test_job_repr_interval(self):
        s = Scheduler()
        job = s.every(60).do(lambda: None)
        r = repr(job)
        self.assertIn("every", r)


class TestIntervals(unittest.TestCase):
    def test_seconds(self):
        s = Scheduler()
        job = s.every(1).seconds()
        self.assertEqual(job.interval, 1)

    def test_minutes(self):
        s = Scheduler()
        job = s.every(5).minutes()
        self.assertEqual(job.interval, 300)

    def test_hours(self):
        s = Scheduler()
        job = s.every(2).hours()
        self.assertEqual(job.interval, 7200)


class TestEdgeCases(unittest.TestCase):
    def test_threading_safety(self):
        s = Scheduler()
        # Many concurrent registrations from multiple threads
        def add():
            for _ in range(10):
                s.every(60).do(lambda: None)
        threads = [threading.Thread(target=add) for _ in range(5)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(len(s), 50)

    def test_wake_seconds_validation(self):
        with self.assertRaises(ScheduleError):
            Scheduler(wake_seconds=0)

    def test_every_zero(self):
        s = Scheduler()
        with self.assertRaises(ScheduleError):
            s.every(0)

    def test_next_with_leap_year(self):
        c = parse("0 0 29 2 *")  # Feb 29
        n = c.next(dt.datetime(2024, 1, 1))
        self.assertEqual(n, dt.datetime(2024, 2, 29, 0, 0))
        # Next one is 2028
        n2 = c.next(dt.datetime(2024, 3, 1))
        self.assertEqual(n2, dt.datetime(2028, 2, 29, 0, 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)