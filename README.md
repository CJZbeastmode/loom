# Loom — JSON-Defined Distributed ETL Engine

*A streaming DAG engine for API-heavy ETL: rate-limited ingestion, backpressure, and Arrow-native compute in C++.*

## Quickstart

```bash
# Build
cmake -B build -G Ninja
cmake --build build -j$(nproc)

# Run tests
cd build && ctest --output-on-failure

# Run a pipeline
./build/loom run pipelines/jira_scraper.dag.json

# Validate a DAG without running
./build/loom validate pipelines/jira_scraper.dag.json
```

## Docker

```bash
cd docker
docker compose up -d          # starts engine + MinIO
docker compose run engine run pipelines/jira_scraper.dag.json
```

## Dependencies

| Dependency    | Purpose                    | Required?  |
|---------------|----------------------------|------------|
| nlohmann/json | JSON parsing               | Required   |
| spdlog        | Logging                    | Required   |
| SQLite3       | Durable request queue      | Optional   |
| libuv         | Async HTTP client          | Optional   |
| Apache Arrow  | Columnar compute engine    | Optional   |
| gRPC          | Distributed execution      | Optional   |
| pybind11      | Python transform bridge    | Optional   |
| Google Test   | Unit tests                 | Required   |

## Architecture

```
JSON DAG (.dag.json)
       │
       ▼
DAG Parser + Validator
       │
       ├──▶ Dependency Scheduler (per-key readiness, join timeout)
       │
       ├──▶ Per-Connection Source Schedulers (rate limit, inflight, backpressure)
       │
       ▼
Bounded Streaming Queues + Join State Store
       │
       ▼
Work-Stealing Thread Pool + Arrow Columnar Runtime
 transform → filter → keyed join → partial aggregate → sink
       │
       ▼
Connectors: HTTP, S3/MinIO
       │
       ▼
Distributed Layer (gRPC + Arrow Flight) — coordinator → executors
```

## Project Structure

```
loom/
├── CMakeLists.txt
├── config.yaml
├── include/loom/
│   ├── engine.h
│   ├── dag/        (parser, validator, executor, join_state)
│   ├── queue/      (request_queue, stream_queue, rate_limiter)
│   ├── http/       (client)
│   ├── compute/    (transform, filter, aggregate, join, thread_pool)
│   ├── connectors/ (s3_connector, connector_registry)
│   └── distributed/(coordinator, executor_node, rpc)
├── src/            (C++ implementations)
├── pyloom/         (Python DAG builder API)
├── pipelines/      (example DAGs and transforms)
├── tests/          (Google Test unit tests)
├── benchmarks/
└── docker/
```
# loom
