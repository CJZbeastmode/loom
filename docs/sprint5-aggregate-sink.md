# Sprint 5 — Aggregate, Sink & Connectors

**Files:**
`include/loom/compute/aggregate.h` + `src/compute/aggregate.cpp` (implemented),
`include/loom/connectors/connector.h` (new interface),
`include/loom/connectors/file_connector.h` + `src/connectors/file_connector.cpp` (new),
`include/loom/connectors/s3_connector.h` + `src/connectors/s3_connector.cpp` (stub),
`include/loom/connectors/connector_registry.h` + `src/connectors/connector_registry.cpp` (rewritten),
`include/loom/compute/sink.h` + `src/compute/sink.cpp` (new),
`tests/test_aggregate.cpp`, `tests/test_sink.cpp`
**Status:** Implemented (aggregate + sink + local connector work; S3 transport stubbed).

> **Design note:** real S3/MinIO write requires AWS SigV4 request signing and an
> endpoint, neither available in this environment. So the *working* connector is
> a local filesystem one; `S3Connector` carries the config and throws until the
> transport is wired. Everything else (aggregation, serialization, path
> materialization, the registry) is fully functional and tested.

---

## Objective

Streaming **aggregation** and the **sink** step: turn filtered micro-batches
into per-group summary rows, then write them out to a connector.

**Deliverable (adapted):** aggregate computes running group-by stats as batches
arrive and emits one row per group on `finalize()`; the sink materializes a path
template and writes the result (CSV/JSON) through a connector, then the file is
verified to exist with the correct contents.

---

## PART A — `Aggregate`: incremental group-by

### The mental model ("the aha")

> **Group-by is a hash map keyed by the group, with a running accumulator as the
> value.** `accumulate()` walks each incoming row, computes its group key from
> the group-by columns, and updates that group's accumulators. Nothing is
> "final" until `finalize()`, which emits one row per group.
>
> Because it's incremental, batches can arrive one at a time (streaming) — you
> don't need all the data up front. `avg` is just `sum` + a count, so it never
> stores every row; only `percentile` buffers values (exact here, a t-digest
> later).

### The state

```cpp
struct AggState {
    int64_t count = 0;    // rows seen (count)
    double sum = 0.0;     // sum / avg
    int64_t n = 0;        // non-null numeric values (avg denominator)
    double min, max;      // numeric min/max
    bool numeric_seen;
    std::vector<double> values;  // percentile buffer
};
```

The Impl holds `group_by` (column names), `aggs` (defs), and:

```cpp
std::unordered_map<std::string, std::vector<AggState>> groups;  // key → per-agg state
std::vector<std::string> order;                                 // first-seen order
```

The map key is the group-by columns' values joined with a `\x1f` separator (a
character that won't appear in data). `order` preserves insertion order so
`finalize()` output is deterministic.

### `accumulate(batch)`

```cpp
// resolve key columns + each agg's field column ONCE
// for each row:
//   key = join(keys[i].as_string(row) for each key column)
//   states = groups[key]  (create empty if first time, push to order)
//   for each aggregate def:
//       count       → states.count++
//       sum / avg   → states.sum += v; states.n++
//       percentile  → states.values.push_back(v)
//       min / max   → states.min/max = min/max(...)
```

**The "aha"s:**

1. **Fields resolve once, then per-row work is just index reads** — no map
   lookups per value.
2. **Nulls are skipped** via the validity bitmap (`is_null(i)`), so `avg` is
   `sum / non-null-count`, while `count` still counts every row.
3. **`count` ignores the field entirely** (it counts rows in the group) — that's
   why the parser example uses `{"field": "id", "func": "count"}` with a
   throwaway field.

### `finalize()`

Builds the output batch: one **String** column per group-by key, then one column
per aggregate (`count` → Int64, everything else → Double), and fills it by
walking `order`:

```cpp
avg         → s.n ? s.sum / s.n : 0.0
percentile  → percentile(s.values, def.arg)     // sort + interpolate
min / max   → s.numeric_seen ? s.min / s.max : 0.0
```

The percentile helper uses linear interpolation:
`rank = p/100 * (n-1)` → interpolate between the two surrounding sorted values.

---

## PART B — Connectors

### The mental model ("the aha")

> **A connector is a "where do the bytes go?" abstraction.** The engine's sink
> logic ("serialize this batch to CSV, then write it to
> `outputs/cycle_time/{date}.csv`") shouldn't care whether the destination is
> the local disk, S3, or a database. So it writes through a tiny interface and
> a registry maps a *name* (from the DAG, e.g. `"s3_landing"`) to a concrete
> connector.

```cpp
class Connector {
public:
    virtual ~Connector() = default;
    virtual void write(const std::string& path, const std::string& data) = 0;
    virtual std::string read(const std::string& path) = 0;
    virtual bool exists(const std::string& path) const = 0;
};
```

### `FileConnector` — the working one

Roots all paths under a base directory, creates parent dirs on write:

```cpp
FileConnector conn("/tmp/out");
conn.write("results/a.csv", "id\n1\n");   // → /tmp/out/results/a.csv
conn.read("results/a.csv");               // "id\n1\n"
conn.exists("results/a.csv");             // true
```

Implementation is `std::filesystem` + `std::ofstream`/`std::ifstream` — no
dependencies.

### `S3Connector` — the stub

Holds `S3Config {endpoint, bucket, access_key, secret_key, prefix}` and
implements the interface, but `write`/`read` throw
`"S3Connector::write not implemented"`. It's here so the registry, the sink, and
the type system all work today, and only the transport needs adding later.

### `ConnectorRegistry` — name → connector

```cpp
ConnectorRegistry reg;
reg.add("s3_landing", std::make_unique<FileConnector>("/tmp/out"));
reg.get("s3_landing")->write(...);   // or throws on unknown name
reg.has("s3_landing");               // true
```

---

## PART C — `Sink`: serialize + write

### The mental model ("the aha")

> **A sink is two transformations: path materialization, then serialization.**
> `"outputs/cycle_time/{date}.csv"` → replace `{date}` with today's date (and
> `{ts}` with a timestamp) → the actual path. Then turn the batch into text in
> the requested format and hand it to the connector.

```cpp
Sink::Config cfg;
cfg.path_template = "results/{date}.csv";
cfg.format = "csv";           // or "json"
cfg.partition = "daily";      // daily → YYYY-MM-DD ; hourly → YYYY-MM-DD-HH

Sink sink(&conn, cfg);
std::string path = sink.write(batch);   // returns the materialized path
```

- `materialize_path` — substitutes `{date}` and `{ts}` (pure function, tested
  independently).
- `to_csv` — header row + comma-separated rows, with quoting for values
  containing commas/quotes/newlines.
- `to_json` — a JSON array of objects, with proper type-aware values and string
  escaping.
- `write` — pick the serializer by `format`, then `connector->write(path, data)`.
  `parquet`/`json.gz` throw (no Parquet/gzip support yet).

---

## How it fits the pipeline

This completes the **compute tail** of the DAG. A sink step consumes the output
of the last compute operator and writes it out:

```
SourceScheduler ─▶ StreamQueue ─▶ Transform ─▶ Filter ─▶ Aggregate ─▶ Sink ─▶ Connector
                                          (parallel)     (group-by)    (serialize)   (disk/S3)
```

The `Aggregate` here is what turns the jira example's "filter_resolved → 
compute_cycle_time (group by assignee, avg cycle time)" into one row per
assignee; the `Sink` is what writes it to `outputs/cycle_time/{date}.csv`.

---

## The "aha" patterns

1. **Group-by = hash map + running accumulators**; `avg` is `sum` + count, so
   it's O(1) per row and streams.
2. **Nulls skip via the validity bitmap** — `avg` divides by non-null count.
3. **Percentile buffers values** (exact); a t-digest can replace it without
   changing the API.
4. **Connectors hide "where bytes go"** behind a 3-method interface; the sink
   doesn't care if it's disk or S3.
5. **Path templates materialize at write time** (`{date}` / `{ts}`), so the same
   DAG works for daily and hourly runs.

## Replicate it yourself (recipe)

1. `Aggregate`: `set_group_by`/`set_aggregates`; a map from joined-key →
   `vector<AggState>`; `accumulate` updates per-row; `finalize` emits
   String-per-key + typed-per-agg columns.
2. `Connector` interface (write/read/exists) + `FileConnector` (filesystem) +
   `S3Connector` (throws) + `ConnectorRegistry` (name → unique_ptr).
3. `Sink`: `materialize_path` (substitute `{date}`/`{ts}`), `to_csv`, `to_json`,
   `write` (dispatch by format → connector).
4. Tests: every agg func, incremental-vs-single equivalence, nulls, multi-column
   groups, unknown-column/non-numeric errors, CSV/JSON exact strings, file
   round-trip, registry resolution, S3 not-implemented.
