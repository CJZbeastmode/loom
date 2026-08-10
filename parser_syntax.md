# Loom DAG Parser Syntax

The `parser.h` defines the domain model for a Loom pipeline — a directed acyclic graph (DAG) of data processing steps. This is the in-memory representation parsed from a JSON configuration file.

---

## Top Level: `DAG`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Pipeline name |
| `version` | `string` | Pipeline version |
| `start_at` | `string` | ID of the step to begin execution at |
| `error_handling` | `ErrorHandling` | Global retry/backoff policy |
| `connections` | `vector<Connection>` | External data sources and sinks |
| `steps` | `vector<Step>` | Processing nodes in the DAG |

---

## Enums

### `StepType`
The type of processing a step performs:
| Value | Purpose |
|-------|---------|
| `Generate` | Produces initial data (calls an API, reads from S3, etc.) |
| `Transform` | Modifies each record (Python function or native callback) |
| `Filter` | Removes or keeps rows based on conditions |
| `Aggregate` | Groups rows and computes summary statistics |
| `Join` | Combines records from two+ upstream steps on a key |
| `Unnest` | Expands list/struct columns into multiple rows (one per element) |
| `Sink` | Writes final output (Parquet, JSON.gz, CSV) |
| `Stub` | Placeholder step — not yet implemented |

### `ConnectionType`
| Value | Purpose |
|-------|---------|
| `Http` | REST API connection with auth, rate limiting, backpressure |
| `S3` | Object storage connection (S3-compatible) |

### `FlushTrigger`
When to flush buffered requests/results to persistent storage:
| Value | Behavior |
|-------|----------|
| `BatchComplete` | Flush after each batch finishes |
| `BufferSize` | Flush when buffer reaches `buffer_size` count |
| `Interval` | Flush on a time interval (`interval_sec`) |

### `JoinType`
| Value | Behavior |
|-------|----------|
| `Inner` | Only emit records present in all joined sources |
| `Left` | Emit all records from the primary source, null-fill missing matches |

### `OnTimeout`
What to do when a join record doesn't arrive before the timeout:
| Value | Behavior |
|-------|----------|
| `DeadLetter` | Send unmatched partial records to a dead-letter queue |
| `Drop` | Silently discard unmatched records |
| `EmitNull` | Emit the record with null fields for the missing side |

### `ParameterSourceKind`
Where a Generate-step parameter gets its values during fan-out:
| Value | Behavior |
|-------|----------|
| `Literal` | Single fixed value |
| `List` | Iterates over an explicit list of values |
| `Range` | Iterates over an integer range (start, end, step) |
| `Column` | Iterates over distinct values from an upstream RecordBatch column |

---

## Structs

### `ErrorHandling`
Global retry configuration for the pipeline.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_attempts` | `int` | `3` | Maximum retry attempts per request |
| `backoff` | `string` | `"exponential"` | Backoff strategy |
| `base_ms` | `int` | `1000` | Base delay in milliseconds |

### `RateLimit`
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max` | `int` | `10` | Max requests per window |
| `window_sec` | `double` | `1.0` | Time window in seconds |

### `Backpressure`
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_buffered_responses` | `int` | `1000` | Pause submitting new requests above this threshold |
| `resume_at` | `int` | `500` | Resume submitting when buffered count drops to this |

### `Auth`
| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | `"bearer"` or `"basic"` |
| `token_from` | `string` | Env var reference, e.g. `"env:API_TOKEN"` |
| `username` | `string` | Username for basic auth |
| `password` | `string` | Password for basic auth |

### `Parameter`
Defines a variable used for fan-out iteration in Generate steps. The `path_template` can reference parameters with curly braces (e.g. `{ticket_number}`). The engine substitutes the value and produces one request per parameter combination.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `string` | | Placeholder name, referenced in `path_template` |
| `source` | `ParameterSourceKind` | `Literal` | Where parameter values come from |
| `value` | `string` | | Fixed value (for `Literal` source) |
| `values` | `vector<string>` | | Explicit list (for `List` source) |
| `range_start` | `int64` | `0` | Start of range, inclusive |
| `range_end` | `int64` | `0` | End of range, exclusive |
| `range_step` | `int64` | `1` | Step increment |
| `from_step` | `optional<string>` | | Upstream step ID (for `Column` source) |
| `field` | `optional<string>` | | Column name in upstream batch (for `Column` source) |

### `Connection`
Describes an external system the pipeline reads from or writes to.

| Field | Type | Relevant for | Description |
|-------|------|-------------|-------------|
| `name` | `string` | _all_ | Unique identifier, referenced by steps |
| `type` | `ConnectionType` | _all_ | `Http` or `S3` |
| `base_url` | `string` | HTTP | Root URL for API calls |
| `rate_limit` | `RateLimit` | HTTP | Request rate throttling |
| `max_inflight` | `int` | HTTP | Max concurrent in-flight requests (`10`) |
| `backpressure` | `Backpressure` | HTTP | Buffer limits before pausing |
| `auth` | `Auth` | HTTP | Authentication credentials |
| `timeout_ms` | `int` | HTTP | Request timeout in ms (`30000`) |
| `fail_on_status` | `vector<int>` | HTTP | HTTP status codes that count as failures |
| `endpoint` | `string` | S3 | S3-compatible endpoint URL |
| `bucket` | `string` | S3 | Bucket name |
| `access_key` | `string` | S3 | Access key |
| `secret_key` | `string` | S3 | Secret key |
| `prefix` | `string` | S3 | Key prefix for objects |
| `reason` | `string` | Stub | Explanation for stub connections |

### `StepInput`
Specifies where a step's data comes from.

| Field | Type | Description |
|-------|------|-------------|
| `from_step` | `optional<string>` | Upstream step ID (single source) |
| `from_connection` | `optional<string>` | Direct connection as source |
| `path` | `optional<string>` | Filesystem path |
| `inputs` | `vector<string>` | Multiple source step IDs (for Join steps) |
| `field` | `optional<string>` | Dot-path projection: selects a sub-field from upstream output (e.g. `"result.items"`) |

### `RequestTemplate`
Defines the shape of HTTP requests emitted by a Generate step.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `method` | `string` | `"GET"` | HTTP method |
| `path_template` | `string` | | URL path, may contain placeholders |
| `headers.Accept` | `string` | `"application/json"` | Accept header |
| `headers.XRequestTag` | `string` | | Custom request tag header |

### `FlushPolicy`
Controls when buffered data is written to persistent storage.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `trigger` | `FlushTrigger` | `BatchComplete` | What event triggers a flush |
| `buffer_size` | `int` | `1000` | Record count threshold (for `BufferSize` trigger) |
| `interval_sec` | `int` | `60` | Time threshold (for `Interval` trigger) |
| `archive_connection` | `string` | | Connection name for archive storage |
| `archive_format` | `string` | `"json.gz"` | Archive file format |
| `retention_days` | `int` | `14` | How long to keep archived data |
| `keep_failed` | `bool` | `true` | Whether to retain failed request data |

### `RequestQueueConfig`
Configuration for the durable request queue used by Generate steps.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `durable` | `bool` | `true` | Persist queue to disk |
| `storage` | `string` | `"sqlite://var/loom/request_queue.db"` | SQLite DB path |
| `max_pending` | `int` | `10000` | Maximum queued requests |
| `flush_policy` | `FlushPolicy` | | When to flush queued data |

### `TransformFunction`
Defines a Python-based transformation.

| Field | Type | Description |
|-------|------|-------------|
| `language` | `string` | `"python"` |
| `entrypoint` | `string` | Path to function, e.g. `"pipelines/transforms/parser.py:extract"` |

### `FilterCondition`
A row-level filter predicate.

| Field | Type | Description |
|-------|------|-------------|
| `field` | `string` | Column name to filter on |
| `op` | `string` | Operator: `eq`, `neq`, `gt`, `lt`, `gte`, `lte`, `not_null`, `is_null` |
| `value` | `string` | Value to compare against |

### `AggregateDef`
Defines a single aggregation.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `string` | | Output column name |
| `field` | `string` | | Input column to aggregate |
| `func` | `string` | | Function: `avg`, `count`, `sum`, `min`, `max`, `percentile` |
| `arg` | `double` | `0.0` | Argument for parameterized functions (e.g. `95` for p95) |

### `JoinConfig`
Configuration for a Join step.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `key` | `string` | | Column to join on |
| `join` | `JoinType` | `Inner` | Join type |
| `timeout` | `string` | `"30s"` | Max wait for all sides to deliver |
| `on_timeout` | `OnTimeout` | `DeadLetter` | Behavior when timeout expires |

### `SinkOutput`
Defines how output is written.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path_template` | `string` | | Output path, e.g. `"outputs/cycle_time/{date}.parquet"` |
| `format` | `string` | `"parquet"` | Output format: `parquet`, `json.gz`, `csv` |
| `partition` | `string` | | Partitioning scheme: `"daily"`, `"hourly"` |

### `UnnestConfig`
Configuration for an Unnest step — explodes a list or struct column into individual rows.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `field` | `string` | | The list/struct column to explode into rows |
| `array_column` | `string` | | Name of the output column holding each exploded element |
| `drop_null` | `bool` | `true` | Drop rows where the list is null or empty |

### `Step`
The central building block — a single node in the DAG.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Unique step identifier |
| `type` | `StepType` | What this step does |
| `description` | `string` | Human-readable description |
| `connection` | `string` | Connection name (for **Generate** and **Sink** steps) |
| `input` | `StepInput` | Where input data comes from |
| `parameters` | `vector<Parameter>` | **(Generate only)** Variables for fan-out request iteration |
| `parallel` | `int` | Parallelism level (`1`) |
| `template_` | `RequestTemplate` | **(Generate only)** HTTP request shape |
| `request_queue` | `RequestQueueConfig` | **(Generate only)** Durable queue config |
| `function` | `TransformFunction` | **(Transform only)** Python function to apply |
| `conditions` | `vector<FilterCondition>` | **(Filter only)** Filter predicates |
| `group_by` | `vector<string>` | **(Aggregate only)** Columns to group by |
| `aggregates` | `vector<AggregateDef>` | **(Aggregate only)** Aggregation functions |
| `join_config` | `JoinConfig` | **(Join only)** Join parameters |
| `unnest` | `UnnestConfig` | **(Unnest only)** Explode column into rows |
| `output` | `SinkOutput` | **(Sink only)** Output destination config |
| `reason` | `string` | **(Stub only)** Why this step is not yet implemented |

---

## Example JSON

```json
{
  "name": "jira_cycle_time",
  "version": "1.0",
  "start_at": "fetch_issues",
  "error_handling": {
    "max_attempts": 5,
    "backoff": "exponential",
    "base_ms": 2000
  },
  "connections": [
    {
      "name": "jira_api",
      "type": "Http",
      "base_url": "https://company.atlassian.net/rest/api/3",
      "rate_limit": { "max": 10, "window_sec": 1.0 },
      "auth": { "type": "bearer", "token_from": "env:JIRA_TOKEN" },
      "fail_on_status": [429, 500, 502, 503]
    }
  ],
  "steps": [
    {
      "id": "fetch_issues",
      "type": "Generate",
      "connection": "jira_api",
      "template_": {
        "method": "GET",
        "path_template": "/search?jql=project=PROJ&maxResults=100",
        "headers": { "Accept": "application/json", "XRequestTag": "jira-export" }
      },
      "request_queue": {
        "durable": true,
        "max_pending": 5000
      }
    },
    {
      "id": "parse_fields",
      "type": "Transform",
      "input": { "from_step": "fetch_issues" },
      "function": {
        "language": "python",
        "entrypoint": "pipelines/transforms/jira_parser.py:extract"
      }
    },
    {
      "id": "filter_bugs",
      "type": "Filter",
      "input": { "from_step": "parse_fields" },
      "conditions": [
        { "field": "issue_type", "op": "eq", "value": "Bug" }
      ]
    },
    {
      "id": "cycle_time_agg",
      "type": "Aggregate",
      "input": { "from_step": "filter_bugs" },
      "group_by": ["assignee", "priority"],
      "aggregates": [
        { "name": "avg_cycle_time", "field": "cycle_time_days", "func": "avg" },
        { "name": "p95_cycle_time", "field": "cycle_time_days", "func": "percentile", "arg": 95 }
      ]
    },
    {
      "id": "write_output",
      "type": "Sink",
      "input": { "from_step": "cycle_time_agg" },
      "output": {
        "path_template": "outputs/cycle_time/{date}.parquet",
        "format": "parquet",
        "partition": "daily"
      }
    }
  ]
}
```

---

## Example: Parameterized Iteration + Unnest

```json
{
  "name": "ticket_export",
  "version": "1.0",
  "start_at": "fetch_by_ticket",
  "connections": [
    {
      "name": "jira_api",
      "type": "Http",
      "base_url": "https://company.atlassian.net/rest/api/3",
      "auth": { "type": "bearer", "token_from": "env:JIRA_TOKEN" }
    },
    {
      "name": "s3_out",
      "type": "S3",
      "endpoint": "https://s3.amazonaws.com",
      "bucket": "exports",
      "access_key": "env:AWS_ACCESS_KEY",
      "secret_key": "env:AWS_SECRET_KEY"
    }
  ],
  "steps": [
    {
      "id": "fetch_by_ticket",
      "type": "Generate",
      "connection": "jira_api",
      "parameters": [
        {
          "name": "ticket",
          "source": "List",
          "values": ["PROJ-100", "PROJ-200", "PROJ-300"]
        }
      ],
      "template_": {
        "method": "GET",
        "path_template": "/issue/{ticket}",
        "headers": { "Accept": "application/json" }
      }
    },
    {
      "id": "range_fetch",
      "type": "Generate",
      "connection": "jira_api",
      "parameters": [
        {
          "name": "start_at",
          "source": "Range",
          "range_start": 0,
          "range_end": 1000,
          "range_step": 100
        }
      ],
      "template_": {
        "method": "GET",
        "path_template": "/search?jql=project=PROJ&startAt={start_at}&maxResults=100"
      }
    },
    {
      "id": "parse_tickets",
      "type": "Transform",
      "input": { "from_step": "fetch_by_ticket" },
      "function": {
        "language": "python",
        "entrypoint": "pipelines/transforms/ticket_parser.py:extract"
      }
    },
    {
      "id": "explode_comments",
      "type": "Unnest",
      "input": { "from_step": "parse_tickets", "field": "comments" },
      "unnest": {
        "field": "comments",
        "array_column": "comment"
      }
    },
    {
      "id": "write_output",
      "type": "Sink",
      "connection": "s3_out",
      "input": { "from_step": "explode_comments" },
      "output": {
        "path_template": "exports/comments/{date}.parquet",
        "format": "parquet",
        "partition": "daily"
      }
    }
  ]
}
```

This pipeline demonstrates:

1. **List iteration** — `fetch_by_ticket` iterates over three explicit ticket keys, substituting `{ticket}` in the path template to generate three separate HTTP requests.
2. **Range iteration** — `range_fetch` paginates through a search API using `start_at` from 0 to 999 in steps of 100, generating 10 requests.
3. **Transform + structured output** — `parse_tickets` receives each response and returns rows with a `comments` list column.
4. **Unnest** — `explode_comments` uses `input.field: "comments"` to select just that column from the upstream output, then explodes each comment list into individual rows (one row per comment).
5. **Sink** — writes the exploded comments to partitioned Parquet in S3.

---

## Example: Python List Output → Parameterized API Calls

```json
{
  "name": "enrich_tickets",
  "version": "1.0",
  "start_at": "search_project",
  "connections": [
    {
      "name": "jira_api",
      "type": "Http",
      "base_url": "https://company.atlassian.net/rest/api/3",
      "rate_limit": { "max": 10, "window_sec": 1.0 },
      "auth": { "type": "bearer", "token_from": "env:JIRA_TOKEN" },
      "fail_on_status": [429, 500, 502, 503]
    }
  ],
  "steps": [
    {
      "id": "search_project",
      "type": "Generate",
      "connection": "jira_api",
      "parameters": [
        {
          "name": "project",
          "source": "List",
          "values": ["PROJ", "DEVOPS"]
        }
      ],
      "template_": {
        "method": "GET",
        "path_template": "/search?jql=project={project}&maxResults=200",
        "headers": { "Accept": "application/json" }
      }
    },
    {
      "id": "extract_ticket_ids",
      "type": "Transform",
      "input": { "from_step": "search_project" },
      "function": {
        "language": "python",
        "entrypoint": "pipelines/extract.py:collect_ticket_ids"
      }
    },
    {
      "id": "expand_tickets",
      "type": "Unnest",
      "input": { "from_step": "extract_ticket_ids", "field": "ticket_numbers" },
      "unnest": {
        "field": "ticket_numbers",
        "array_column": "ticket"
      }
    },
    {
      "id": "fetch_details",
      "type": "Generate",
      "connection": "jira_api",
      "input": { "from_step": "expand_tickets" },
      "parameters": [
        {
          "name": "ticketnumber",
          "source": "Column",
          "from_step": "expand_tickets",
          "field": "ticket"
        }
      ],
      "template_": {
        "method": "GET",
        "path_template": "/issue/{ticketnumber}?fields=summary,status,assignee&expand=changelog",
        "headers": { "Accept": "application/json" }
      }
    },
    {
      "id": "write_output",
      "type": "Sink",
      "input": { "from_step": "fetch_details" },
      "output": {
        "path_template": "outputs/ticket_details/{date}.parquet",
        "format": "parquet",
        "partition": "daily"
      }
    }
  ]
}
```

**Flow:**

1. **`search_project`** — Generate step iterates over project keys `["PROJ", "DEVOPS"]`, hitting Jira's search API once per project. Each call returns up to 200 issues.

2. **`extract_ticket_ids`** — Python function `collect_ticket_ids` runs per response batch. It parses the JSON, extracts issue keys, and returns rows with a column like:
   ```python
   # pipelines/extract.py
   def collect_ticket_ids(batch):
       rows = []
       for issue in batch.get("issues", []):
           related = issue.get("fields", {}).get("issuelinks", [])
           rows.append({
               "ticket_numbers": [link["outwardIssue"]["key"] for link in related]
           })
       return rows
   ```

3. **`expand_tickets`** — Unnest explodes the `ticket_numbers` list column into individual rows. A row with `ticket_numbers: ["PROJ-100", "PROJ-200"]` becomes two rows, each with a `ticket` column containing one key.

4. **`fetch_details`** — Another Generate step, this time using `ParameterSourceKind::Column`. It reads the `ticket` column from the upstream `expand_tickets` step and substitutes `{ticketnumber}` in the path template. Each exploded ticket key triggers its own GET request:
   ```
   GET /issue/PROJ-100?fields=summary,status,assignee&expand=changelog
   GET /issue/PROJ-200?fields=summary,status,assignee&expand=changelog
   ```

5. **`write_output`** — Writes the enriched ticket details to daily-partitioned Parquet.
