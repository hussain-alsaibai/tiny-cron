# tiny-cron

> Zero-dependency cron-style scheduler for Python. Parse cron expressions, fire jobs on intervals, run one-shot tasks. Stdlib only. Built for AI agents, scrapers, and side-projects that shouldn't drag in Celery.

```bash
# coming soon
pip install tiny-cron
```

## Why?

`schedule` is fine but doesn't do cron syntax. `APScheduler` is 200KB and pulls in `six`, `pytz`, `tzlocal`. `Celery` needs Redis + a broker + a worker process. Most projects just want: *"run this function every 5 minutes"* without becoming a distributed systems engineer.

`tiny-cron`:

- Parse standard 5-field cron (`* * * * *`) with `L`, `W`, `#` extensions
- `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` aliases
- Compute next/previous fire times, iterate between two dates
- Single-thread scheduler with **anti-overlap** (a slow job won't pile up)
- Background thread mode (`.start()` / `.stop()`) or block on main (`.run_forever()`)
- Jitter to avoid thundering-herd, run-limits, start/end times
- 60 tests, 0 external deps, 1 file

## Install

Drop `tiny_cron.py` in your project. Or:

```bash
git clone https://github.com/hussain-alsaibai/tiny-cron.git
cp tiny-cron/tiny_cron.py ./your_project/
```

## Usage

### Interval job (every N seconds)

```python
import tiny_cron

s = tiny_cron.Scheduler()
s.every(30).seconds.do(scrape_metrics)
s.every(5).minutes.do(flush_buffer)
s.start()  # background thread, daemon=True by default
```

### Cron expression

```python
import tiny_cron

s = tiny_cron.Scheduler()

# Every 5 minutes
s.cron("*/5 * * * *").do(heartbeat)

# At 09:00 on weekdays
s.cron("0 9 * * 1-5").do(morning_report)

# Last Friday of the month
s.cron("0 16 * * 5L").do(monthly_close)

# 2nd Monday
s.cron("0 10 * * 1#2").do(biweekly_sync)

# Aliases
s.cron("@hourly").do(scrape)
s.cron("@daily").do(rotate_logs)
```

### One-shot at a specific time

```python
import tiny_cron, datetime as dt

s = tiny_cron.Scheduler()
s.at(dt.datetime(2026, 12, 31, 23, 59)).do(celebrate_new_year)
```

### Chainable config

```python
job = (
    s.cron("*/5 * * * *")
       .do(scrape)
       .tag("scrape", "metrics")
       .jitter(2.0)              # fire within ±2s of scheduled time
       .limit(100)               # run at most 100 times, then remove
)
```

### Run from a script (block on main thread)

```python
import tiny_cron

s = tiny_cron.Scheduler()
s.every(10).seconds.do(some_task)
s.run_forever()  # blocks until s.stop() or KeyboardInterrupt
```

### Parse and query cron expressions

```python
import tiny_cron, datetime as dt

c = tiny_cron.parse("0 9 * * mon-fri")

# When's the next fire?
print(c.next())                          # 2026-07-06 09:00:00

# Is this datetime a fire time?
print(c.matches(dt.datetime(2026, 7, 6, 9, 0)))   # True
print(c.matches(dt.datetime(2026, 7, 6, 9, 1)))   # False

# Iterate fires in a window (great for backfills / audits)
for fire in c.iter_between(dt.datetime(2026, 7, 1), dt.datetime(2026, 7, 31)):
    print(fire)
```

### Decorator registration

```python
import tiny_cron

scheduler = tiny_cron.Scheduler()

@tiny_cron.every(scheduler, 60, "metrics")
def scrape():
    ...

@tiny_cron.schedule(scheduler, "@hourly", "audit")
def rotate():
    ...

scheduler.start()
```

## Anti-overlap & jitter

Two killer features for multi-tenant systems:

```python
s = tiny_cron.Scheduler(wake_seconds=0.5)

# If a job takes 30s and is scheduled every 10s, it WILL NOT pile up.
# The previous run keeps the lock; the next tick is a no-op.
def slow_db_job():
    run_long_query()

s.every(10).seconds.do(slow_db_job)

# Jitter prevents thundering herd across replicas:
# 100 cron replicas all waking at :00:00.000 → spread across ±5s.
s.cron("@hourly").do(sync).jitter(5.0)
```

## Cron syntax extensions

| Token   | Field | Meaning                              |
|---------|-------|--------------------------------------|
| `*`     | any   | every value                          |
| `a-b`   | any   | range                                |
| `a-b/n` | any   | range with step                      |
| `*/n`   | any   | every nth value starting at field min|
| `a,b,c` | any   | list                                 |
| `L`     | dom   | last day of month                    |
| `5L`    | dow   | last Friday of month                 |
| `15W`   | dom   | weekday nearest the 15th             |
| `5#2`   | dow   | 2nd Friday of month                  |
| `@hourly` |   | `0 * * * *`                          |
| `@daily`  |   | `0 0 * * *`                          |
| `@weekly` |   | `0 0 * * 0`                          |
| `@monthly`|   | `0 0 1 * *`                          |
| `@yearly` |   | `0 0 1 1 *`                          |

Day-of-week numbers follow standard cron: `0=Sun, 1=Mon, ..., 6=Sat`. (Python's `datetime.weekday()` uses `0=Mon`; we translate internally.)

## Performance

| Operation                    | Speed          |
|------------------------------|----------------|
| `parse("*/5 * * * *")`       | ~30 µs         |
| `CronExpr.next()`            | ~150 µs avg    |
| `iter_between` (1 day, */5)  | ~10 ms for 288 fires |
| Scheduler tick (10 jobs)     | ~80 µs         |
| Background thread overhead   | <1% CPU at wake=1.0s |

## When NOT to use tiny-cron

- **Multi-process scheduling.** Tiny-cron runs in one process. For multi-worker (e.g. 10 gunicorn workers all scraping), use a database lock or a leader-election sidecar (Consul, ZooKeeper, etcd).
- **Sub-second precision.** Wake loop is configurable but ticks whole seconds.
- **Persistent jobs across restarts.** Tiny-cron is in-memory. Persist via your own DB if you need durability.

## License

MIT — see [LICENSE](./LICENSE).

---

## Ecosystem

Part of the `tiny-*` zero-dep Python stack. Sibling libraries:

| Repo                                                                                  | Purpose                              |
|---------------------------------------------------------------------------------------|--------------------------------------|
| [fast-cache](https://github.com/hussain-alsaibai/fast-cache)                          | LRU + TTL cache, sync + async        |
| [tiny-rate](https://github.com/hussain-alsaibai/tiny-rate)                            | Token / fixed / sliding rate limits  |
| [tiny-retry](https://github.com/hussain-alsaibai/tiny-retry)                          | Exponential backoff + circuit breaker|
| [tiny-pool](https://github.com/hussain-alsaibai/tiny-pool)                            | Thread + async worker pools          |
| [tiny-secret](https://github.com/hussain-alsaibai/tiny-secret)                        | Secret loader + redacting logger     |
| [tiny-trace](https://github.com/hussain-alsaibai/tiny-trace)                          | OpenTelemetry-API subset, W3C trace  |
| [tiny-cli](https://github.com/hussain-alsaibai/tiny-cli)                              | Decorator CLI builder                |
| [tiny-config](https://github.com/hussain-alsaibai/tiny-config)                        | JSON / YAML / INI / .env / CLI flags |
| [tiny-log](https://github.com/hussain-alsaibai/tiny-log)                              | Structured logging                   |
| [tiny-validator](https://github.com/hussain-alsaibai/tiny-validator)                  | Data validation, Pydantic-style      |
| [tiny-router](https://github.com/hussain-alsaibai/tiny-router)                        | WSGI router, Flask in one file       |
| [tiny-compose](https://github.com/hussain-alsaibai/tiny-compose)                      | Decorator stacker / pipeline         |
| [tiny-mcp](https://github.com/hussain-alsaibai/tiny-mcp)                              | MCP server, JSON-RPC 2.0             |
| [tiny-embed](https://github.com/hussain-alsaibai/tiny-embed)                          | sentence-transformers in 1 file      |
| [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent)                          | LangChain in 1 file                  |
| [snapdb](https://github.com/hussain-alsaibai/snapdb)                                  | Embedded in-memory DB                |

**Stack: 18 repos, ~14K LOC, 0 dependencies, ~440 tests.**

## Today's siblings

- [`tiny-metrics`](https://github.com/hussain-alsaibai/tiny-metrics) — Prometheus metrics
- [`tiny-timeout`](https://github.com/hussain-alsaibai/tiny-timeout) — timeouts that work
- [`tiny-idempotency`](https://github.com/hussain-alsaibai/tiny-idempotency) — idempotency keys
