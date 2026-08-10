**# Loom — JSON-Defined Distributed ETL Engine**

**\*\*Tagline:\*\*** *\*"A streaming DAG engine for API-heavy ETL: rate-limited ingestion, backpressure, and Arrow-native compute in C++."\**

**\*\*Workflow\*\***: Loom reads the DAG and connection configs, materializes durable HTTP request queues, and starts each source at its own configured rate and concurrency limits. Responses are not collected behind a global batch barrier. As soon as a response (or Arrow micro-batch) is available, Loom pushes it into downstream transform/filter/aggregate operators. If a downstream operator requires multiple sources, Loom buffers partial inputs by key and schedules work immediately when that key's dependencies are satisfied. Bounded queues provide backpressure so slow compute cannot cause unbounded response buffering. A pipeline run ends when all source requests are terminal, all join/dependency buffers are resolved according to their timeout policy, and all downstream work has drained.


**## Overview**

A streaming DAG execution engine for API-heavy ETL workflows. Users define pipelines as JSON DAGs (Step Functions-style). The C++ runtime continuously schedules remote requests up to each API's rate and concurrency limits, pipelines completed responses into Arrow-native transforms, applies bounded backpressure, and can trigger keyed downstream work as soon as its individual dependencies are ready. Optional multi-node execution uses gRPC + Arrow Flight.

\---

**## JSON DAG format (Step Functions-inspired)**

Based on Amazon States Language but stripped to essentials. A DAG is a JSON file with a \`StartAt\` chain, parallel branches, and map/transform/filter/sink steps.

\`\`\`jsonc
// pipelines/jira\_scraper.dag.json
{
  "name": "jira-cycle-time",
  "version": "1.0",
  "start\_at": "generate\_request\_queue",
  "error\_handling": {
    "retry": { "max\_attempts": 3, "backoff": "exponential", "base\_ms": 1000 }
  },
  "connections": {
    "jira\_api": {
      "type": "http",
      "base\_url": "https\://jira.corp.com/rest/api/2",
      "rate\_limit": { "max": 10, "window\_sec": 1 },
      "max\_inflight": 10,
      "backpressure": { "max\_buffered\_responses": 50, "resume\_at": 25 },
      "auth": { "type": "bearer", "token\_from": "env\:JIRA\_TOKEN" },
      "timeout\_ms": 30000,
      "fail\_on\_status": [429, 500, 502, 503]
    },
    "s3\_landing": {
      "type": "s3",
      "endpoint": "${MINIO\_ENDPOINT:-http\://localhost:9000}",
      "bucket": "loom-data",
      "access\_key": "${MINIO\_ACCESS\_KEY:-minioadmin}",
      "secret\_key": "${MINIO\_SECRET\_KEY:-minioadmin}",
      "prefix": "jira-scraper"
    },
    "s3\_archive": {
      "type": "s3",
      "endpoint": "${MINIO\_ENDPOINT:-http\://localhost:9000}",
      "bucket": "loom-archive",
      "access\_key": "${MINIO\_ACCESS\_KEY:-minioadmin}",
      "secret\_key": "${MINIO\_SECRET\_KEY:-minioadmin}",
      "prefix": "jira-raw-archive"
    },
    "redshift": { "type": "stub", "reason": "not yet implemented" },
    "bigquery": { "type": "stub", "reason": "not yet implemented" },
    "snowflake": { "type": "stub", "reason": "not yet implemented" }
  },
  "steps": [
    {
      "id": "generate\_request\_queue",
      "type": "generate",
      "connection": "jira\_api",
      "description": "Build the request queue: one HTTP GET per Jira ticket AA-1 through AA-100",
      "input": {
        "from\_connection": "s3\_landing",
        "path": "inputs/tickets\_to\_scrape.json"
      },
      "template": {
        "method": "GET",
        "path\_template": "/issue/{ticket\_id}",
        "headers": {
          "Accept": "application/json",
          "X-Request-Tag": "loom-scraper-v1"
        }
      },
      "request\_queue": {
        "durable": true,
        "storage": "sqlite:///var/loom/request\_queue.db",
        "max\_pending": 10000,
        "flush\_policy": {
          "trigger": "batch\_complete",
          "archive\_connection": "s3\_archive",
          "archive\_format": "json.gz",
          "retention\_days": 14
        }
      }
    },
    {
      "id": "parse\_jira\_response",
      "type": "transform",
      "description": "Extract fields from raw Jira JSON",
      "input": { "from\_step": "generate\_request\_queue" },
      "parallel": 8,
      "function": {
        "language": "python",
        "entrypoint": "pipelines/transforms/jira\_parser.py\:extract"
      }
    },
    {
      "id": "filter\_resolved",
      "type": "filter",
      "description": "Keep only resolved tickets for cycle-time",
      "input": { "from\_step": "parse\_jira\_response" },
      "parallel": 4,
      "condition": [
        { "field": "status", "op": "eq", "value": "Resolved" },
        { "field": "resolution\_date", "op": "not\_null" }
      ]
    },
    {
      "id": "compute\_cycle\_time",
      "type": "aggregate",
      "description": "Group by assignee, compute avg cycle time",
      "input": { "from\_step": "filter\_resolved" },
      "parallel": 4,
      "group\_by": ["assignee"],
      "aggregates": [
        { "name": "avg\_cycle\_days", "field": "cycle\_time\_days", "func": "avg" },
        { "name": "tickets\_resolved", "field": "id", "func": "count" },
        { "name": "p95\_cycle\_days", "field": "cycle\_time\_days", "func": "percentile", "arg": 95 }
      ]
    },
    {
      "id": "write\_results",
      "type": "sink",
      "description": "Write cycle time report to S3/MinIO",
      "input": { "from\_step": "compute\_cycle\_time" },
      "connection": "s3\_landing",
      "output": {
        "path\_template": "outputs/cycle\_time/{date}.parquet",
        "format": "parquet",
        "partition": "daily"
      }
    },
    {
      "id": "write\_slack\_summary",
      "type": "stub",
      "reason": "not yet implemented",
      "description": "Post summary to Slack webhook — connector pending"
    },
    {
      "id": "write\_snowflake",
      "type": "stub",
      "reason": "not yet implemented",
      "description": "INSERT INTO snowflake reporting table — connector pending"
    }
  ]
}
\`\`\`

\---

**## How streaming HTTP ingestion works**

This is the core of the "solve the API bottleneck without adding pipeline latency" problem. Rate limiting, concurrency limiting, and backpressure are separate controls.

\`\`\`
                       ┌─────────────────────────┐
  generate step ──────▶│ Durable Request Queue   │
                       │ pending / inflight / ...│
                       └────────────┬────────────┘
                                    │
                        source scheduler
                                    │
                  ┌─────────────────┼──────────────────┐
                  │                 │                  │
                  ▼                 ▼                  ▼
             rate limit         max inflight      backpressure
            10 starts/sec       <= 10 open      buffer < 50 rows
                  │                 │                  │
                  └─────────────────┼──────────────────┘
                                    ▼
                              async HTTP I/O
                         req 1 ... req 10 ...
                                    │
                         responses arrive independently
                          ↙          ↓          ↘
                     Arrow batch  Arrow batch  Arrow batch
                          │          │          │
                          └──────────┼──────────┘
                                     ▼
                              bounded stream queue
                                     │
                                     ▼
                         transform → filter → aggregate
                                     │
                                     ▼
                                    sink
\`\`\`

A source may have thousands of pending requests while exposing only a small number concurrently. For example, `rate_limit = 10/sec` and `max_inflight = 10` means Loom never fires all 100 requests at once. It continuously issues requests only when both the time-based rate limiter and the inflight-capacity check allow it.

**\*\*Three independent controls:\*\***

- `rate_limit`: maximum request starts in a time window; tokens refill with time and are **not** returned when a response completes.
- `max_inflight`: maximum number of currently open HTTP requests.
- `backpressure`: pauses new dispatch when downstream buffers exceed a high-water mark, then resumes below a low-water mark.

**\*\*Streaming rule:\*\*** a completed response can enter downstream computation immediately. Request 1 never has to wait for request 100 before parsing starts.

**\*\*Request queue persistence (SQLite by default):\*\***

\`\`\`sql
CREATE TABLE request\_queue (
    request\_id   TEXT PRIMARY KEY,
    dag\_run\_id   TEXT NOT NULL,
    step\_id      TEXT NOT NULL,
    method       TEXT DEFAULT 'GET',
    url          TEXT NOT NULL,
    headers      TEXT DEFAULT '{}',
    body         TEXT,
    status       TEXT DEFAULT 'pending',
    retry\_count  INT DEFAULT 0,
    created\_at   TEXT,
    started\_at   TEXT,
    completed\_at TEXT,
    response\_status INT,
    response\_body  TEXT,
    result\_path   TEXT
);
\`\`\`

**\*\*Status lifecycle:\*\*** \`pending → inflight → done/failed/skipped\`

**\*\*Token bucket algorithm (per connection):\*\***
\`\`\`cpp
class RateLimiter {
    std::atomic\<int64\_t> tokens;
    int64\_t max\_tokens;
    int64\_t refill\_rate\_per\_sec;
    std::chrono::steady\_clock::time\_point last\_refill;

    bool try\_acquire();         // lock-free CAS on time-refilled tokens
    void wait\_and\_acquire();   // waits until the rate window permits a start
};
\`\`\`

**\*\*Flush/archive policies:\*\***
\`\`\`jsonc
"flush\_policy": {
  "trigger": "batch\_complete",    // or "buffer\_size:1000" or "interval:60s"
  "archive\_connection": "s3\_archive",
  "archive\_format": "json.gz",    // or "parquet", "json"
  "retention\_days": 14,           // auto-cleanup after
  "keep\_failed": true             // also archive failed responses
}
\`\`\`

\---

**## Complete Jira workflow example**

**\*\*Command:\*\***
\`\`\`bash
loom run pipelines/jira\_scraper.dag.json
\`\`\`

**\*\*Input file\*\*** (\`s3://loom-data/inputs/tickets\_to\_scrape.json\`):
\`\`\`json
["AA-1", "AA-2", "AA-3", ..., "AA-100"]
\`\`\`

**### Execution trace**

\| # | Step | What happens |
\|---|---|---|
\| 1 | `generate\_request\_queue` | Reads 100 ticket IDs from S3 and creates 100 durable `pending` request rows. |
\| 2 | source scheduler | Jira allows 10 request starts/sec with `max_inflight = 10`. Loom dispatches only while both constraints permit it. |
\| 3 | first response completes | Suppose AA-4 returns at 180 ms. Loom marks it `done`, converts it into an Arrow micro-batch, and immediately makes `parse_jira_response` runnable for that data. AA-1, AA-2, etc. may still be inflight. |
\| 4 | `parse\_jira\_response` | Worker threads parse completed responses continuously rather than waiting for all 100. |
\| 5 | `filter\_resolved` | Parsed micro-batches flow directly into the SIMD filter. |
\| 6 | `compute\_cycle\_time` | Maintains partial per-assignee aggregate state as filtered rows arrive. Finalizes once upstream is exhausted. |
\| 7 | `write\_results` | Writes finalized aggregate output to S3/MinIO. Raw responses can be archived incrementally or when the source reaches terminal completion. |
\| 8 | backpressure | If transform workers fall behind and the response buffer reaches its high-water mark, Loom temporarily stops dispatching new HTTP requests even if rate-limit tokens are available. |
\| 9 | recovery | Failed/429/timeout requests remain durable with retry metadata; a process restart can resume pending work without refetching completed requests. |

**### Example timeline (100 requests, 10 starts/sec, max 10 inflight)**

\`\`\`
t=0.00s: requests 1-10 dispatched
t=0.18s: request 4 completes → parse/filter begins immediately
t=0.31s: request 1 completes → parse/filter begins
t=1.00s: rate limiter permits more starts; free inflight slots are filled
t=1.15s: request 11 completes → downstream processing starts immediately
...
source continues near its allowed rate while compute overlaps with network I/O
final source response arrives → upstream closes → partial aggregates finalize → sink commits
\`\`\`

The API can remain the dominant bottleneck. Loom's goal is not to make the remote API faster; it is to avoid adding unnecessary latency around that bottleneck and to keep downstream resources busy whenever data is available.

\---

**## Keyed streaming joins and dependency readiness**

A downstream task can depend on records from several independently rate-limited sources. Loom should schedule work **per key**, not wait for every source to finish globally.

Example: an engineering-metrics worker needs Jira + GitHub + PagerDuty data for `ABC-123`.

\`\`\`
Jira stream ─────────┐
                     │
GitHub stream ───────┼──▶ keyed join buffer (`ticket_id`)
                     │             │
PagerDuty stream ────┘             ▼
                         ABC-123: J✓ G✓ P✓ ──▶ READY → worker
                         ABC-124: J✓ G✗ P✓ ──▶ wait
                         ABC-125: J✓ G✓ P✓ ──▶ READY → worker
\`\`\`

If Jira for `ABC-123` arrives at 0.2 s, GitHub at 0.8 s, and PagerDuty at 3.1 s, the worker for `ABC-123` becomes runnable at 3.1 s immediately. It does not wait for all Jira, GitHub, or PagerDuty requests in the pipeline to complete.

Example join configuration:

\`\`\`jsonc
{
  "id": "enrich_engineering_item",
  "type": "join",
  "inputs": ["jira", "github", "pagerduty"],
  "key": "ticket_id",
  "join": "inner",
  "timeout": "30s",
  "on_timeout": "dead_letter"
}
\`\`\`

For a left join, Loom can emit the primary record after the timeout with missing secondary inputs represented as null. Join buffers must also be bounded; their memory pressure participates in global backpressure.

**\*\*Scheduling principle:\*\*** source schedulers care about API limits; the DAG scheduler cares about dependency readiness. A task becomes runnable as soon as its own required inputs are ready and a worker slot is available.

\---

**## Real-world end-to-end example: engineering productivity pipeline**

A company wants daily team-level engineering metrics from Jira, GitHub, and PagerDuty. The three APIs have different limits:

\`\`\`
Jira       10 req/sec, 10 inflight
GitHub     30 req/sec, 20 inflight
PagerDuty   5 req/sec,  5 inflight
\`\`\`

Loom keeps three independent source schedulers running, streams completed responses into Arrow batches, joins records by team/ticket/date as soon as their dependencies are ready, and incrementally computes metrics such as cycle time, merged PR count, and incident count.

\`\`\`
Jira API ───────┐
GitHub API ─────┼─▶ normalize ─▶ keyed join ─▶ partial aggregate ─▶ Snowflake/S3
PagerDuty API ──┘
\`\`\`

If PagerDuty is the slowest source, only keys waiting on PagerDuty remain blocked; unrelated ready keys continue through the pipeline. If Python transforms become slower than ingestion, bounded queues push back on the corresponding HTTP sources instead of allowing memory usage to grow without bound.

\---

**## Step types reference**

\| Step type | Purpose | Key config fields |
\|---|---|---|
\| \`generate\` | Build HTTP request queue from list | \`template\`, \`rate\_limit\`, \`request\_queue\` |
\| \`transform\` | Apply Python/C++ function to each row | \`function\`, \`parallel\` |
\| \`filter\` | SIMD-accelerated row filtering | \`condition\`, \`parallel\` |
\| \`aggregate\` | Group-by + aggregate functions | \`group\_by\`, \`aggregates\`, \`parallel\` |
\| \`join\` | Keyed streaming join; fires when per-key dependencies are ready | \`inputs\`, \`key\`, \`join\`, \`timeout\`, \`on_timeout\` |
\| \`sink\` | Write results to a connection | \`connection\`, \`output\` |
\| \`stub\` | Placeholder for future connectors | \`reason\` |

**## Connection types**

\| Connection | Status | Config |
\|---|---|---|
\| \`http\` | **\*\*implemented\*\*** | \`base\_url\`, \`rate\_limit\`, \`max\_inflight\`, \`backpressure\`, \`auth\`, \`timeout\_ms\` |
\| \`s3\` | **\*\*implemented\*\*** | \`endpoint\`, \`bucket\`, \`access\_key\`, \`secret\_key\`, \`prefix\` |
\| \`redshift\` | \`stub\` | — |
\| \`bigquery\` | \`stub\` | — |
\| \`snowflake\` | \`stub\` | — |
\| \`slack\` | \`stub\` | — |
\| \`kafka\` | \`stub\` | — |
\| \`postgres\` | \`stub\` | — |

\---

**## Architecture diagram**

\`\`\`
┌───────────────────────────────────────────────────────────────┐
│                  JSON DAG File (.dag.json)                    │
├───────────────────────────────────────────────────────────────┤
│                   DAG Parser + Validator                      │
├──────────────────────────┬────────────────────────────────────┤
│  Dependency Scheduler    │  Per-Connection Source Schedulers  │
│  - runnable-by-key       │  - time-based rate limiter         │
│  - join readiness        │  - max inflight                    │
│  - timeout policy        │  - retries                         │
│                          │  - backpressure                     │
├──────────────────────────┴────────────────────────────────────┤
│          Bounded Streaming Queues + Join State Store          │
├───────────────────────────────────────────────────────────────┤
│  Work-Stealing Thread Pool + Arrow Columnar Runtime           │
│  transform → filter → keyed join → partial aggregate → sink   │
├───────────────────────────────────────────────────────────────┤
│  Connectors: HTTP, S3/MinIO (others incrementally added)      │
├───────────────────────────────────────────────────────────────┤
│  Distributed Layer (gRPC + Arrow Flight)                      │
│  Coordinator → Executors → partition/shuffle → fault recovery │
└───────────────────────────────────────────────────────────────┘
\`\`\`

\---

**## Project structure**

\`\`\`
loom/
├── CMakeLists.txt
├── README.md
├── config.yaml                     # global engine config
├── include/
│   └── loom/
│       ├── engine.h                # DAG runner entrypoint
│       ├── dag/                    # DAG parser, validator, executor
│       │   ├── parser.h            # JSON → internal DAG IR
│       │   ├── validator.h         # schema validation
│       │   ├── executor.h          # dependency-aware streaming scheduler
│       │   └── join_state.h        # keyed dependency/join readiness state
│       ├── queue/
│       │   ├── request\_queue.h     # SQLite-backed durable queue
│       │   ├── stream\_queue.h      # bounded queue + high/low watermarks
│       │   └── rate\_limiter.h      # time-refilled token bucket (CAS)
│       ├── http/
│       │   └── client.h            # libuv async HTTP client
│       ├── compute/
│       │   ├── transform.h         # Arrow-based transforms
│       │   ├── filter.h            # SIMD column filter
│       │   ├── aggregate.h         # incremental/SIMD group-by + aggregate
│       │   ├── join.h              # keyed streaming join operator
│       │   └── thread\_pool.h       # work-stealing thread pool
│       ├── connectors/
│       │   ├── s3\_connector.h      # MinIO / AWS S3
│       │   └── connector\_registry.h
│       └── distributed/
│           ├── coordinator.h       # DAG partitioner + task assigner
│           ├── executor\_node.h     # Executor-side worker
│           └── rpc.h               # gRPC + Arrow Flight
├── src/                            # C++ implementations
│   ├── main.cpp                    # CLI entrypoint
│   └── ...
├── pyloom/
│   ├── \_\_init\_\_.py
│   ├── dag.py                      # Python DAG builder API
│   └── transform.py                # Python transform registry
├── pipelines/
│   ├── jira\_scraper.dag.json       # example DAG
│   └── transforms/
│       └── jira\_parser.py          # Python transform function
├── tests/
│   ├── test\_parser.cpp
│   ├── test\_rate\_limiter.cpp
│   ├── test\_request\_queue.cpp
│   └── test\_dag\_executor.cpp
└── docker/
    ├── Dockerfile.engine
    ├── Dockerfile.worker
    └── docker-compose.yml
\`\`\`

\---

**## Phase plan**

**### Phase 1 — Single-node streaming MVP (weeks 1–3)**
\- JSON DAG parser + validator
\- SQLite-backed durable request queue
\- Per-connection time-based token bucket rate limiter
\- Separate `max_inflight` accounting
\- libuv async HTTP client
\- Bounded response queues + high/low-watermark backpressure
\- Work-stealing thread pool
\- S3/MinIO connector
\- Python transform execution via pybind11
\- Arrow micro-batch transform/filter
\- Incremental aggregate state
\- CLI: `loom run pipeline.dag.json`
\- Demo: 100 Jira tickets streamed through transform/filter while requests are still in flight

**### Phase 2 — Multi-source readiness + recovery (weeks 4–5)**
\- Keyed streaming join buffer
\- Per-key dependency readiness
\- Join timeout policies (`inner`, `left`, dead-letter)
\- Retry scheduling for 429/5xx/timeouts
\- Crash resume from durable request state
\- Demo: Jira + GitHub + PagerDuty with independent source limits

**### Phase 3 — Distributed execution (weeks 6–8)**
\- Coordinator/executor split
\- gRPC control plane
\- Arrow Flight for data transfer
\- Hash partitioning + shuffle
\- Executor heartbeat + failover
\- Globally coordinated rate limiting for shared external APIs
\- Benchmark suite + Prometheus metrics
\- Documentation + interview narrative

\---

**## Key technical decisions**

\| Decision | Choice | Why |
\|---|---|---|
\| In-memory format | Apache Arrow | Industry standard, SIMD-ready, zero-copy, Spark/DuckDB use it |
\| Async I/O | libuv | Mature, cross-platform, epoll/kqueue backed |
\| Inter-node RPC | gRPC + Arrow Flight | gRPC for control, Arrow Flight for zero-copy data transfer |
\| Rate limiting | Time-refilled token bucket + lock-free atomic counters | Enforces request-start rate independently of request completion |
\| Concurrency | Separate max-inflight counter | Slow requests cannot create unbounded concurrent connections |
\| Backpressure | Bounded queues + high/low watermarks | Prevents fast sources from overwhelming slower downstream operators |
\| Streaming joins | Keyed readiness state + timeout policy | Runs each key as soon as its own dependencies arrive; no global barrier |
\| Thread pool | work-stealing | Handles uneven task durations well |
\| Python bridge | pybind11 | Proven from market-intel, low overhead |
\| Build system | CMake + vcpkg (or conan) | Standard C++ tooling |

\---

**## What makes this interview-worthy**

1\. **\*\*Rate-limited streaming ingestion\*\*** — separates request rate, inflight concurrency, and backpressure; demonstrates real scheduler design rather than just async HTTP.
2\. **\*\*Per-key dependency scheduling\*\*** — keyed joins become runnable as soon as their own Jira/GitHub/PagerDuty inputs arrive, avoiding global barriers.
3\. **\*\*Spark-like DAG execution engine\*\*** — shows understanding of distributed query engines: partitioning, shuffling, lineage, fault tolerance.
4\. **\*\*Apache Arrow + SIMD\*\*** — demonstrates knowledge of columnar data standards and hardware acceleration.
5\. **\*\*End-to-end distributed system\*\*** — coordinator/executor, gRPC, fault tolerance. This is what Databricks/Snowflake engineers build.
6\. **\*\*Multi-language design\*\*** — C++ engine + Python user-facing API. Shows breadth.