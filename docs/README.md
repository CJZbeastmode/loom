# Loom — Sprint Documentation Index

**Loom** is a streaming ETL engine for API-heavy data pipelines, written in C++20.
Users describe a pipeline as a JSON DAG (e.g. "fetch from Jira at 10 req/sec, then
transform → filter → aggregate → write to S3") and Loom runs it continuously,
respecting each API's rate limits, capping memory via backpressure, and pushing
completed responses downstream immediately — no waiting for the whole batch.

This index maps the three completed sprints. Each sprint doc is written to be
read front-to-back: it builds the **intuition** first, then walks the **code**
in detail, so that by the end you can close the file and reimplement it.

---

## The big picture (read this first)

Every sprint is one layer of the "ingestion pipeline":

```
  JSON DAG file
      │
      ▼
┌─────────────────┐   Sprint 1: Parser
│  DAG IR (C++    │   reads JSON → typed structs
│  structs)       │
└─────────────────┘
      │
      ▼
┌─────────────────┐   Sprint 2: Durable Queue + Rate Limiter
│  RequestQueue   │   stores pending HTTP requests in SQLite;
│  RateLimiter    │   token bucket shapes request rate
└─────────────────┘
      │
      ▼
┌─────────────────┐   Sprint 3: HTTP Client + Source Scheduler
│  HttpClient     │   libuv async HTTP
│  SourceScheduler│   the loop that "dequeue → rate-limit → dispatch → push result"
│  StreamQueue    │   bounded buffer with high/low watermarks (backpressure)
└─────────────────┘
```

**The mental model for the whole engine** — three independent controls per API
source (this appears in every sprint):

| Control | Limits | Meaning |
|---|---|---|
| `rate_limit` | requests **started per second** | time-based token bucket |
| `max_inflight` | requests **open at once** | concurrency cap |
| `backpressure` | **memory** | pauses dispatch when downstream buffers fill |

A pipeline is "streaming" because request #1's response flows into compute the
moment it arrives — request #100 may still be in flight.

---

## Sprints

| # | Doc | Core deliverable | Status |
|---|---|---|---|
| 1 | [Sprint 1 — DAG Parser](sprint1-parser.md) | JSON → typed DAG structs, env-var substitution, 8 step types, both connection formats | Implemented |
| 2 | [Sprint 2 — Durable Request Queue & Rate Limiter](sprint2-queue-rate-limiter.md) | SQLite-backed persistent queue (crash-resume) + lock-free token bucket | Implemented |
| 3 | [Sprint 3 — Async HTTP Client & Source Scheduler](sprint3-http-scheduler.md) | libuv async HTTP client, streaming source scheduler, bounded backpressure queue | Implemented |
| 4 | [Sprint 4 — Work-Stealing Thread Pool & Parallel Compute](sprint4-compute.md) | work-stealing pool + columnar transform/filter, benchmarked parallel speedup | Implemented |
| 5 | [Sprint 5 — Aggregate, Sink & Connectors](sprint5-aggregate-sink.md) | streaming group-by, CSV/JSON sink, connector interface + registry | Implemented |

---

## How to build & test

Requires: CMake ≥ 3.20, Ninja, jsoncpp, SQLite3, libuv (all installable via
Homebrew; no GitHub-sourced dependencies).

```bash
cmake -S . -B build -G Ninja \
      -DLOOM_BUILD_TESTS=ON \
      -DLOOM_USE_SQLITE=ON \
      -DLOOM_USE_LIBUV=ON
ninja -C build

# run every test executable
for t in build/test_*; do "$t"; done
```

Test suites and expected counts:

| Executable | Coverage | # Tests |
|---|---|---|
| `test_parser` | parser: errors, steps, connections, params, env vars, real pipeline | 35 |
| `test_request_queue` | SQLite queue CRUD, lifecycle, durability, resume, run isolation | 13 |
| `test_rate_limiter` | token bucket: exhaustion, refill, blocking, throughput, thread-safety | 7 |
| `test_http_client` | async HTTP: GET, concurrency, timeout, invalid URL, fail_on_status | 6 |
| `test_source_scheduler` | end-to-end streaming, max-inflight cap, backpressure, failures | 4 |
| `test_dag_executor` | executor lifecycle | 3 |
| `test_thread_pool` | work-stealing pool: correctness, speedup, nested submit | 5 |
| `test_filter` | columnar filter: all ops, AND, nulls, parallel, benchmark | 11 |
| `test_transform` | registry, apply, parallel apply, row-count guard | 4 |
| `test_aggregate` | group-by: count/sum/avg/min/max/percentile, incremental, nulls | 10 |
| `test_sink` | CSV/JSON serialization, path materialization, connector round-trip | 10 |

> **Note on the test framework:** tests use a tiny self-contained harness
> (`tests/test_utils.h`), not Google Test. This was a deliberate choice so the
> project has zero dependencies that can't be obtained in China. It provides
> `TEST(suite, name)`, `EXPECT_EQ/NE/TRUE/FALSE/THROW/NO_THROW`, and a `main()`
> that runs every registered test and reports pass/fail counts.

---

## Reading order & "aha" map

| Question | Where to get the "aha" |
|---|---|
| How does raw JSON become typed C++ structs? | Sprint 1, "The core insight" |
| Why does the rate limiter have no background thread? | Sprint 2, "Refill is a projection" |
| Why is the token bucket stored as integers? | Sprint 2, "Fixed-point scaling (kScale)" |
| How do 3 threads share the pipeline safely? | Sprint 3, "The threading model" |
| What actually causes `uv_run` to return? | Sprint 3, "The gotchas we fixed" |
| How does backpressure stop the whole upstream? | Sprint 3, "Backpressure, end to end" |
| How does a worker find work when its own queue is empty? | Sprint 4, "The work-stealing pool" |
| Why store each column as its own contiguous array? | Sprint 4, "The columnar data model" |
| How is `avg` computed without storing every row? | Sprint 5, "Group-by = hash map + accumulators" |
| How does the sink not care if output is disk or S3? | Sprint 5, "Connectors" |
