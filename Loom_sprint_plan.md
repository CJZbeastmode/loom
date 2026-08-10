# Loom — Sprint Plan

## Sprint 0: Project Setup & Scaffolding (Week 1)

**Goal:** Bootable C++ project with build, Docker, CI, and empty headers for the full architecture.

**What to code:**
- `CMakeLists.txt` with vcpkg/Conan dependencies (nlohmann/json, SQLite, libuv, Arrow, gRPC, pybind11, spdlog, Google Test)
- `include/loom/` — full header skeleton (every `.h` listed in the design, each with class stubs and method signatures)
- `src/main.cpp` — CLI skeleton: `loom run <dag.json>`, `loom validate <dag.json>`, `loom --version`
- `docker/Dockerfile.engine`, `docker/Dockerfile.worker`, `docker-compose.yml` — dev environment with MinIO, SQLite
- `.github/workflows/ci.yml` — CMake build + unit test run on PR

**Deliverables:** Project compiles, Docker services start (engine + MinIO), empty tests pass, CI green.

---

## Sprint 1: DAG Parser & Validator (Week 2)

**Goal:** Read a `.dag.json` file and produce a validated, type-safe internal DAG IR.

**What to code:**
- `include/loom/dag/parser.h` + `src/dag/parser.cpp`
  - JSON → internal structs: `DAG`, `Step`, `Connection`, `Input`, `Condition`, `Aggregate`, `JoinConfig`
  - Parse all step types (`generate`, `transform`, `filter`, `aggregate`, `join`, `sink`, `stub`)
  - Parse all connection types (`http`, `s3`)
  - Environment variable substitution (`${VAR:-default}`)
- `include/loom/dag/validator.h` + `src/dag/validator.cpp`
  - Schema validation: required fields, type checking
  - Structural validation: no cycles in input references, `StartAt` exists, all `from_step`/`from_connection` targets resolve
  - Warnings for stubs

**Deliverables:** The example `jira_scraper.dag.json` parses without error; intentional malformed DAGs are rejected with clear messages.

---

## Sprint 2: Durable Request Queue & Rate Limiter (Week 3)

**Goal:** Persistent request queue (SQLite) and lock-free token-bucket rate limiter working in isolation.

**What to code:**
- `include/loom/queue/request_queue.h` + `src/queue/request_queue.cpp`
  - SQLite schema creation (matching the design's `CREATE TABLE request_queue`)
  - CRUD: `enqueue_batch()`, `dequeue_next()`, `mark_inflight(id)`, `mark_done(id, status, body)`, `mark_failed(id, error)`, `mark_skipped(id)`
  - Status lifecycle as state machine (`pending → inflight → done/failed/skipped`)
  - Durable checkpoint so a process restart can resume pending work
- `include/loom/queue/rate_limiter.h` + `src/queue/rate_limiter.cpp`
  - Thread-safe token bucket with `std::atomic<int64_t>` and periodic refill
  - `try_acquire()` — returns immediately if a token is available (lock-free CAS)
  - `wait_and_acquire()` — blocks until the next refill window
  - Configurable `max_tokens` and `refill_rate_per_sec`
- `tests/test_request_queue.cpp`, `tests/test_rate_limiter.cpp`

**Deliverables:** Queue passes CRUD + durability tests. Rate limiter correctly shapes throughput (e.g., 10/sec with burst=10 over a measured 5-second window).

---

## Sprint 3: Async HTTP Client & Source Scheduler (Week 4–5)

**Goal:** Streaming HTTP ingestion loop: dispatch requests subject to rate/inflight/backpressure limits, push completed responses downstream immediately.

**What to code:**
- `include/loom/http/client.h` + `src/http/client.cpp`
  - libuv event-loop-backed async HTTP/HTTPS client
  - Per-connection pool of inflight requests (abstracted behind a callback interface)
  - HTTP auth: bearer token, basic auth, header injection
  - Timeout per request; automatic abort and error callback on timeout
  - Status-code-aware error handling (e.g., `fail_on_status: [429, 500, 502, 503]`)
- `include/loom/queue/stream_queue.h` + `src/queue/stream_queue.cpp`
  - Bounded MPSC/MCMP queue for Arrow record batches
  - High-water mark (`max_buffered_responses`) — when exceeded, pauses upstream dispatch
  - Low-water mark (`resume_at`) — when capacity drops below, resumes dispatch
  - Atomic watermarks checked on enqueue
- `src/executor/source_scheduler.cpp` (new file, header not explicitly listed — add to `dag/`)
  - Per-connection dispatch loop: `while (rate_limiter.try_acquire() && inflight < max_inflight && !backpressured) { dequeue → dispatch }`
  - Response callback: marks queue row done, converts response body to Arrow batch, pushes into bounded stream queue
  - Termination condition: request queue empty AND all inflight resolved

**Deliverables:** Against a mock HTTP server, 100 requests are dispatched at exactly 10/sec with max 10 inflight; responses stream into the bounded queue as they arrive; backpressure triggers at the high-water mark.

---

## Sprint 4: Work-Stealing Thread Pool & Arrow Compute (Week 6)

**Goal:** Multi-threaded execution of transform and filter operators on Arrow columnar data.

**What to code:**
- `include/loom/compute/thread_pool.h` + `src/compute/thread_pool.cpp`
  - Work-stealing thread pool (bounded number of worker threads)
  - Each thread has its own task queue; idle threads steal from others
  - Task type: `std::function<void()>` representing a micro-batch operation
- `include/loom/compute/transform.h` + `src/compute/transform.cpp`
  - Accept an Arrow record batch and a registered transform function
  - Apply function column-by-column or row-by-row depending on function signature
  - C++ native transforms: inline lambda or function pointer
  - Python transforms: call pybind11-wrapped Python function per batch (see below)
- `include/loom/compute/filter.h` + `src/compute/filter.cpp`
  - SIMD-accelerated column filter on Arrow arrays
  - Support comparison operators (`eq`, `neq`, `gt`, `lt`, `gte`, `lte`, `not_null`, `is_null`)
  - Evaluate multiple conditions with AND logic
  - Produce a filtered Arrow record batch (using Arrow's `Take` or selection vector)
- `pyloom/transform.py` — Python-side registry for user-defined transform functions

**Deliverables:** Given a 1000-row Arrow batch, a Python transform extracts fields and a C++ filter drops rows matching conditions, all in parallel across 4–8 workers. Benchmarked against single-threaded baseline.

---

## Sprint 5: Aggregate, Sink & S3 Connector (Week 7)

**Goal:** Streaming aggregation, S3/MinIO read/write, and the sink step type.

**What to code:**
- `include/loom/compute/aggregate.h` + `src/compute/aggregate.cpp`
  - Incremental group-by state: per-group accumulator stores running counts, sums, min, max
  - Support `avg`, `count`, `sum`, `min`, `max`, `percentile` (t-digest approximation)
  - Each incoming micro-batch updates partial aggregates in a thread-safe hash map
  - `finalize()` called when upstream is exhausted — emits one row per group
- `include/loom/connectors/s3_connector.h` + `src/connectors/s3_connector.cpp`
  - AWS SDK C++ or libcurl-based S3/MinIO client
  - `read_json(path)` → JSON object (input data for generate step)
  - `write_parquet(path, batch)` — Arrow batch → Parquet → upload
  - `write_json_gz(path, data)` — JSON gzip upload (archive format)
  - Endpoint override for MinIO local development
- `include/loom/connectors/connector_registry.h` + `src/connectors/connector_registry.cpp`
  - Factory that resolves connection type string → connector instance
  - Passes connection config (auth, endpoint, etc.) to constructor
- Sink step execution: reads `connection` + `output` config, materializes the path template with date, writes in specified format

**Deliverables:** Aggregate computes running group-by stats as filtered micro-batches arrive. Sink writes Parquet to MinIO and verifies the file exists with correct schema.

---

## Sprint 6: DAG Executor & CLI Integration (Week 8)

**Goal:** End-to-end pipeline execution: wire all components together and ship the `loom run` command.

**What to code:**
- `include/loom/dag/executor.h` + `src/dag/executor.cpp` (streaming DAG runner)
  - Topological sort of steps from `StartAt`
  - For each step, spawn workers based on type:
    - `generate` → instantiate request queue, create source scheduler thread
    - `transform`/`filter`/`aggregate` → create thread pool workers consuming from upstream bounded queue
    - `sink` → create writer consuming from upstream queue
  - Step chaining: each step's output queue is the next step's input queue
  - Backpressure propagates: if a downstream queue hits high-water, the upstream scheduler pauses
  - Run completion: all source queues empty + all inflight resolved + all compute drains → exit
- `include/loom/engine.h` + `src/engine.cpp`
  - `Engine::run(dag_path)` — parse → validate → construct executor → run → report
  - Metrics collection: total requests, successes, failures, retries, elapsed time, bytes transferred
  - Graceful shutdown on SIGINT/SIGTERM
- `src/main.cpp` — wire up spdlog, read config, call engine
- `pipelines/jira_scraper.dag.json` + `pipelines/transforms/jira_parser.py` — working example

**Deliverables:** `loom run pipelines/jira_scraper.dag.json` with 100 fake Jira tickets against a mock HTTP server. Responses stream through transform → filter → aggregate → sink while requests are still in flight. Exit code and summary stats printed.

---

## Sprint 7: Keyed Streaming Joins & Multi-Source Orchestration (Week 9–10)

**Goal:** Join records from multiple independently-rate-limited sources per key, without global barriers.

**What to code:**
- `include/loom/dag/join_state.h` + `src/dag/join_state.cpp`
  - Per-key state map: `key → {source_id → optional<ArrowBatch>}`
  - When all inputs for a key arrive, mark it READY and emit the joined row
  - Per-key configurable timeout: if a dependency doesn't arrive within `timeout`, apply `on_timeout` policy:
    - `dead_letter` → write incomplete key to a dead-letter queue/log
    - `inner` → drop the key entirely (never emit)
    - `left` → emit primary record with null secondary columns
  - Bounded memory: if join state exceeds configurable limit, pause source dispatch (global backpressure)
- `src/dag/executor.cpp` — add join step type:
  - Register multiple upstream source schedulers as input
  - Each upstream pushes into the join state on key
  - Join state emits ready keys to the downstream bounded queue
- `tests/test_join_state.cpp` — unit tests for ready/wait/expire/timeout scenarios

**Deliverables:** Jira + GitHub + PagerDuty with different rate limits (10/30/5 req/sec). Keys become ready and flow downstream as soon as all three sources deliver data for that key. Keys whose dependencies don't arrive within 30s get dead-lettered. Throughput is gated by the slowest source per key, not globally.

---

## Sprint 8: Recovery, Retry & Resilience (Week 11)

**Goal:** Pipeline survives crashes, transient failures, and rate limit responses.

**What to code:**
- Retry scheduling in `src/executor/source_scheduler.cpp`:
  - On `429` or `5xx` response → exponential backoff (configurable `base_ms`, `max_attempts`)
  - Retryable requests re-enter the pending queue with incremented `retry_count`
  - After `max_attempts`, mark as `failed` and optionally dead-letter
  - Respect rate limits during retries (don't flood on recovery)
- Crash resume in `src/queue/request_queue.cpp`:
  - On startup, scan for rows with `status = 'pending'` or `status = 'inflight'` from a prior `dag_run_id`
  - Reset inflight rows to pending (they were never completed before crash)
  - Skip rows with `status = 'done'` (already processed)
- Flush/archive policy engine in `src/queue/request_queue.cpp`:
  - Trigger types: `batch_complete`, `buffer_size:N`, `interval:Ns`
  - On trigger, bulk-copy completed responses to archive connection (S3) in specified format
  - Retention cleanup: delete rows older than `retention_days` after successful archive
- Integration test: kill the process mid-pipeline, restart, verify no duplicate work and all rows completed

**Deliverables:** Pipeline with 20% random failures recovers via retry with backoff. Kill -9 mid-run, restart — pending/inflight work resumes, done work is skipped. Archived responses appear in S3/MinIO archive bucket.

---

## Sprint 9: Distributed Architecture & gRPC Control Plane (Week 12–13)

**Goal:** A coordinator can partition a DAG across multiple executor nodes and orchestrate execution.

**What to code:**
- `include/loom/distributed/coordinator.h` + `src/distributed/coordinator.cpp`
  - Read DAG, partition steps across registered executors
  - Partitioning strategies: round-robin, hash-partition by key column (for joins/aggregates), or entire-step assignment
  - gRPC server: accepts executor registrations, heartbeats
  - gRPC client: sends task assignments, control signals (start/stop/abort)
  - Global rate limiter: coordinator aggregates API limit across executors sharing the same external API connection
- `include/loom/distributed/executor_node.h` + `src/distributed/executor_node.cpp`
  - gRPC client: registers with coordinator, sends periodic heartbeats
  - gRPC server: receives task assignments (step + partition range), reports progress
  - Runs the same single-node executor for its assigned partition of the DAG
- `include/loom/distributed/rpc.h` — `.proto` definitions for:
  - `RegisterExecutor(executor_id, capacity) → accepted`
  - `Heartbeat(executor_id, stats) → ack`
  - `AssignTask(step_id, partition_key_range, dag_snapshot) → ack`
  - `TaskProgress(executor_id, step_id, metrics) → ack`
- Proto compilation in CMakeLists.txt

**Deliverables:** Coordinator + 2 executors (Docker). Coordinator partitions a 4-step DAG across 2 executors; each executor runs its assigned steps and reports progress back. Heartbeat-based liveness detection works.

---

## Sprint 10: Arrow Flight, Shuffle & Failover (Week 14–15)

**Goal:** Zero-copy data transfer between executor nodes and fault-tolerant execution.

**What to code:**
- Arrow Flight integration in `src/distributed/rpc.cpp`:
  - Flight server per executor — streams Arrow record batches between nodes
  - Coordinator instructs executors where to push/pull data (Flight endpoints)
- Hash partitioning + shuffle in `src/distributed/shuffle.cpp` (new file):
  - After a map-phase step completes on executor A, hash-partition its output by key
  - Push each partition to the executor responsible for that key range
  - Receiving executor merges incoming partitions into its local stream for the next step
- Executor failover in `src/distributed/coordinator.cpp`:
  - Heartbeat timeout (configurable) → mark executor as dead
  - Redistribute the dead executor's unfinished tasks to healthy executors
  - Replay the dead executor's pending request queue from the durable SQLite store (Sprint 2)
  - Or, if input data is still available on another executor, re-partition and reassign
- Globally coordinated rate limiting:
  - Coordinator owns the authoritative token bucket for each shared API connection
  - Executors request tokens from coordinator via gRPC (batching to reduce RPC overhead)
  - Coordinator refills and distributes tokens fairly

**Deliverables:** 3-node cluster with hash-partitioned shuffle. Kill one executor mid-pipeline — tasks redistribute, pipeline completes. Arrow Flight transfers a 100K-row batch between executors with measured throughput.

---

## Sprint 11: Polish — Benchmarks, Metrics, Docs & Python API (Week 16)

**Goal:** Production-ready observability, performance baselines, user-facing Python API, and documentation.

**What to code:**
- Prometheus metrics endpoint (`:9090/metrics`):
  - Counters: `loom_requests_total{status}`, `loom_retries_total`, `loom_batches_processed`
  - Gauges: `loom_inflight_requests`, `loom_queue_depth`, `loom_workers_active`
  - Histograms: `loom_request_latency_seconds`, `loom_batch_process_seconds`
  - Exposed via embedded HTTP server in the coordinator
- Benchmark suite (`benchmarks/`):
  - Throughput benchmarks: 1K/10K/100K requests at varying rate limits
  - Latency benchmarks: end-to-end latency from request dispatch to sink write
  - Backpressure benchmarks: measure memory usage under sustained overload
  - Join benchmarks: multi-source join latency with varying skew
  - Compare single-node vs distributed at 2/4/8 executors
- `pyloom/dag.py` — Python DAG builder API:
  - Fluent builder: `DAG("my_pipeline").source("jira").transform(my_func).filter("status", "eq", "done").sink("s3")`
  - Serialize to `.dag.json` (compatible with C++ engine)
  - Validate in Python before serialization
- Documentation:
  - `README.md` — quickstart, architecture overview, config reference
  - `docs/step-types.md` — every step type with examples
  - `docs/connections.md` — every connection type with config examples
  - `docs/distributed.md` — coordinator/executor setup, fault model
  - `docs/writing-transforms.md` — C++ and Python transform authoring guide
- `config.yaml` — global engine defaults (thread pool size, log level, metrics port)

**Deliverables:** Benchmarks published as part of CI. All docs written. Python API builds a DAG programmatically and the resulting JSON is accepted by `loom run`.

---

## Summary

| Sprint | Focus | Cumulative Capability |
|---|---|---|
| 0 | Project scaffolding | Builds, Docker runs |
| 1 | DAG parser + validator | Reads and validates `.dag.json` |
| 2 | Request queue + rate limiter | Durable queues with token bucket |
| 3 | HTTP client + source scheduler | Streaming ingestion with backpressure |
| 4 | Thread pool + Arrow compute | Parallel transform/filter |
| 5 | Aggregate + sink + S3 | Incremental aggregation, writes output |
| 6 | DAG executor + CLI | `loom run` works end-to-end |
| 7 | Keyed streaming joins | Multi-source per-key readiness |
| 8 | Recovery + retry | Crashes and transient failures handled |
| 9 | Distributed control plane | Coordinator + multi-executor gRPC |
| 10 | Arrow Flight + shuffle | Zero-copy data transfer + failover |
| 11 | Polish | Benchmarks, metrics, docs, Python API |

Total: 16 weeks across 12 sprints (matches the design's 8-week scope for a focused MVP of Sprints 0–6, then 8 weeks of multi-source and distributed work).
