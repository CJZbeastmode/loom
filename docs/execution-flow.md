# Execution Flow — What Each File Does When a Pipeline Runs

This walks through what happens when you run a pipeline, e.g.
`loom run pipelines/jira_scraper.dag.json` (fetch 100 Jira tickets → transform →
filter → aggregate → write to S3), and names the file responsible for each step.

> **Honest status note:** the end-to-end wiring (`engine.cpp` and
> `dag/executor.cpp`) is still a stub — that's Sprint 6. Each component works
> and is tested in isolation, but `loom run` doesn't chain them yet. The flow
> below is the intended execution path.

---

## Phase 1 — Read & interpret the pipeline definition

**`src/main.cpp`** — the front door. Parses `loom run <file>` / `loom validate
<file>`, then hands the path to the engine.

**`src/engine.cpp`** — the conductor. Its `run(dag_path)` is: parse → validate →
build executor → run → report metrics. (Currently a stub.)

**`src/dag/parser.cpp`** — the *compiler*. Turns the JSON text into typed
`dag::DAG` structs (steps, connections, rate limits). Nothing runs yet; this is
purely "understand the spec". Its output is what every later phase reads.

**`src/dag/validator.cpp`** — the *linter*. Checks the parsed DAG for errors
(does `start_at` exist? do `from_step` references resolve? any cycles?).
Currently returns "valid" for everything.

---

## Phase 2 — Materialize the work to do

**`src/queue/request_queue.cpp`** — the *todo list on disk*. The `generate` step
writes one SQLite row per HTTP request ("GET /issue/AA-1", "GET /issue/AA-2",
…). Because it's durable, a crash can resume instead of restarting. Role: **the
source of truth for "what's left to fetch"**.

---

## Phase 3 — Ingest (streaming, rate-limited)

Four files cooperate to pull requests from the queue and push responses
downstream **as they arrive**:

**`src/dag/source_scheduler.cpp`** — the *foreman*. Its loop is the heartbeat of
ingestion:

```
while (rate_limiter says "yes"  &&  inflight < cap  &&  downstream not full)
    request_queue.dequeue()  →  http_client.submit()
```

It's the only thing that decides *when* to dispatch.

**`src/queue/rate_limiter.cpp`** — the *speed governor*. Grants "you may start a
request now" tokens at the Jira rate (10/sec). The foreman asks it on every
dispatch.

**`src/http/client.cpp`** — the *delivery driver*. Does the actual non-blocking
HTTP GET (libuv event loop) and fires the callback when the response body
arrives. Knows nothing about Jira or the DAG — just "fetch this URL".

**`src/queue/stream_queue.h`** — the *conveyor belt with a safety valve*.
Completed responses land here in a bounded buffer. If the belt fills (compute is
slow), it signals "backpressured", which makes the foreman stop dispatching.
Role: **the boundary between ingestion and compute, and the thing that caps
memory**.

---

## Phase 4 — Compute (parallel, columnar)

**`src/compute/column.h`** — the *shared data format*. A `RecordBatch` (columns
of `int64/double/string/bool`) is what flows between all compute operators.
Everything downstream speaks this type.

**`src/compute/thread_pool.cpp`** — the *workers*. A pool of threads that run
tasks; the operators hand it row-chunks to process in parallel. It's the muscle
behind "across 4–8 workers".

**`src/compute/transform.cpp`** — the *reshape* operator. Runs a registered
function per batch (e.g. "extract fields from raw Jira JSON" → a clean
`{key, status, assignee}` batch).

**`src/compute/filter.cpp`** — the *gate* operator. Drops rows not matching
conditions (`status == "Resolved"`), in parallel across the pool.

**`src/compute/aggregate.cpp`** — the *summarize* operator. Group-by with
running accumulators (`count/sum/avg/min/max/percentile`); `accumulate()` ingests
each micro-batch, `finalize()` emits one row per group (e.g. "avg cycle time per
assignee").

**`src/compute/join.cpp` / `unnest.cpp`** — the *combine / explode* operators
(later sprints — stubs).

---

## Phase 5 — Write results out

**`src/compute/sink.cpp`** — the *packager*. Serializes the final batch to the
requested format (CSV/JSON) and materializes the path template
(`outputs/cycle_time/{date}.csv` → today's date). Doesn't care *where* the bytes
land — that's the connector's job.

**`src/connectors/file_connector.cpp`** — the *local disk* connector (working).
Writes/reads files under a root directory. Used for real output and for tests.

**`src/connectors/s3_connector.cpp`** — the *S3/MinIO* connector (stub). Carries
`S3Config` and implements the `Connector` interface, but throws until the S3
transport (SigV4 signing + endpoint) is wired.

**`src/connectors/connector_registry.cpp`** — the *address book*. Maps a
connection name (`"s3_landing"`) to a `Connector` instance so the sink can
resolve it.

---

## The wiring that's still missing

**`src/dag/executor.cpp`** — the *assembly line* (Sprint 6). Its job is to
connect the phases: topological-sort the steps, spawn a `SourceScheduler` for
each `generate`, thread the `StreamQueue`s between steps (`generate → transform
→ filter → aggregate → sink`), and run everything until drained. Until this is
written, the phases exist but aren't chained.

---

## One-line summary of each file's role

| File | Role in the pipeline |
|---|---|
| `main.cpp` | CLI entry point |
| `engine.cpp` | orchestrator (parse→validate→run) — stub |
| `dag/parser.cpp` | compile JSON → typed DAG |
| `dag/validator.cpp` | lint the DAG — stub |
| `dag/executor.cpp` | wire all steps together — stub |
| `dag/source_scheduler.cpp` | decide *when* to fetch |
| `queue/request_queue.cpp` | durable list of "what to fetch" |
| `queue/rate_limiter.cpp` | speed governor |
| `http/client.cpp` | actual network I/O |
| `queue/stream_queue.h` | bounded conveyor + backpressure |
| `compute/column.h` | shared data format |
| `compute/thread_pool.cpp` | parallel workers |
| `compute/transform.cpp` | reshape rows |
| `compute/filter.cpp` | drop rows |
| `compute/aggregate.cpp` | group-by summary |
| `compute/sink.cpp` | serialize + write output |
| `connectors/file_connector.cpp` | local disk connector (working) |
| `connectors/s3_connector.cpp` | S3/MinIO connector (stub) |
| `connectors/connector_registry.cpp` | name → connector lookup |
