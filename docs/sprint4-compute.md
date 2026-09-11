# Sprint 4 — Work-Stealing Thread Pool & Parallel Compute

**Files:**
`include/loom/compute/column.h` (new),
`include/loom/compute/thread_pool.h` + `src/compute/thread_pool.cpp` (rewritten),
`include/loom/compute/filter.h` + `src/compute/filter.cpp` (rewritten),
`include/loom/compute/transform.h` + `src/compute/transform.cpp` (rewritten),
`tests/test_thread_pool.cpp`, `tests/test_filter.cpp`, `tests/test_transform.cpp`
**Status:** Implemented.

> **Design note:** Apache Arrow is not installed (and isn't fetchable in China),
> so this sprint uses a **minimal self-contained columnar model** that keeps
> Arrow's core idea — each column is a contiguous, typed array — without the
> dependency. The concepts (columnar layout, parallel transform/filter,
> work-stealing) are identical; swapping in real Arrow later is mechanical.

---

## Objective

Multi-threaded execution of **transform** and **filter** over columnar data.

**Deliverable:** given a large batch, a transform computes a new column and a
filter drops rows matching conditions, in parallel across 4–8 workers, with a
measurable speedup over the single-threaded baseline (we measured **3.7x** on
an 8-worker filter over 2M rows).

---

## PART A — The columnar data model (`column.h`)

### The mental model ("the aha")

> **A columnar batch stores each field as its own contiguous array** — all the
> `id`s in one `vector<int64_t>`, all the `status` strings in one
> `vector<string>`. This is the opposite of "row objects". It's what makes
> filters/aggregates fast: scanning one column means walking one contiguous
> buffer, not chasing pointers through thousands of row objects.

Two types:

```cpp
enum class ColumnType { Int64, Double, String, Bool };

struct Column {
    ColumnType type;
    std::vector<int64_t> i64;      // used when type == Int64
    std::vector<double> f64;       // Double
    std::vector<std::string> str;  // String
    std::vector<uint8_t> boolean;  // Bool (0/1)
    std::vector<uint8_t> valid;    // validity bitmap: 1 present, 0 null (empty = all present)
    ...
};

struct RecordBatch {
    std::vector<std::string> names;   // schema (column names)
    std::vector<Column> columns;      // one per name
    int64_t num_rows();
    RecordBatch slice(start, end);    // row range of every column
};
```

Key helpers:
- `Column::as_string(i)` — human-readable value of a cell (for display/filter).
- `Column::is_null(i)` — checks the validity bitmap.
- `RecordBatch::slice(start, end)` — a row-range view (copy) of all columns.
- `concat(batches)` — merge chunk results back into one batch.

A **validity bitmap** (a parallel array of 0/1) supports the `is_null` /
`not_null` filter ops without a full "optional<T>" per cell. Empty bitmap means
"no nulls".

---

## PART B — The work-stealing thread pool

### The mental model ("the aha")

> **A naive pool has one shared queue** — every worker locks it, pops a task,
> runs it. Under load that single queue becomes a contention point, and if one
> worker gets a pile of slow tasks, others idle.
>
> **A work-stealing pool gives every worker its OWN queue.** A worker runs its
> own newest task **LIFO** (last-in-first-out — good cache locality, and nested
> submits stay hot). When idle, it **steals the OLDEST task** from a victim
> queue **FIFO** (first-in-first-out — the victim's "cold" work). Owner-LIFO /
> thief-FIFO is the classic Chase-Lev policy; here we use a plain mutex per
> worker instead of a lock-free deque.

### The structure

```cpp
struct ThreadPool::Impl {
    struct Worker {
        int id;
        std::thread thread;
        std::deque<std::function<void()>> queue;  // OWN queue
        std::mutex mutex;
        std::condition_variable cv;
        bool stop = false;
        int steal_cursor = 0;                     // rotating victim start
    };
    std::vector<std::unique_ptr<Worker>> workers;
    std::atomic<int> next_submit{0};   // round-robin target for submit
    std::atomic<int> pending{0};       // submitted-not-yet-completed
    std::mutex done_mutex; std::condition_variable done_cv;
};
```

### The three core routines

**Pop own (LIFO):**
```cpp
bool pop_own(Worker* w, Task& out) {
    lock(w->mutex);
    if (w->queue.empty()) return false;
    out = std::move(w->queue.back());   // newest
    w->queue.pop_back();
    return true;
}
```

**Steal (FIFO, rotating victim):**
```cpp
bool steal(Worker* thief, Task& out) {
    for (int k = 0; k < n; ++k) {
        Worker* victim = workers[(thief->steal_cursor + k) % n].get();
        if (victim == thief) continue;
        lock(victim->mutex);
        if (victim->queue.empty()) continue;
        out = std::move(victim->queue.front());   // oldest
        victim->queue.pop_front();
        thief->steal_cursor = (idx + 1) % n;      // don't always hit the same victim
        return true;
    }
    return false;
}
```

**Worker loop:**
```cpp
void worker_loop(Worker* w) {
    while (true) {
        Task task;
        if (pop_own(w, task) || steal(w, task)) {
            task();
            if (pending.fetch_sub(1) == 1) done_cv.notify_all();  // last task → wake waiters
            continue;
        }
        unique_lock lock(w->mutex);
        if (w->stop) return;
        w->cv.wait_for(lock, 1ms);   // park briefly; re-check (self-correcting)
    }
}
```

**The three "aha"s:**

1. **`wait_for(1ms)`, not `wait()`** — a timed wait makes the pool
   self-correcting: even if a notify is missed, an idle worker re-scans for
   stealable work every millisecond. Correctness no longer depends on perfect
   wakeup ordering.
2. **`pending` counts *submitted minus completed*, not queue depth.** A task is
   "pending" from submit until its `task()` returns (even while running). That's
   exactly what `wait_all()` needs: "everything I handed you has finished".
   Decrement happens in the worker loop right after `task()` returns.
3. **Round-robin `submit`** spreads initial work, but **stealing** is what
   smooths out *imbalance* (one worker's queue draining faster than another's,
   or tasks spawned dynamically from within other tasks).

### The public API (unchanged header)

`submit(task)`, `wait_all()`, `worker_count()`, `pending_tasks()`, `shutdown()`.

`wait_all` blocks on `done_cv` until `pending == 0`; `shutdown` sets `stop` on
every worker, notifies, and joins.

---

## PART C — `Filter`: columnar, parallel row selection

### The mental model ("the aha")

> **Filtering is two independent steps:** (1) *evaluate* each row → a yes/no
> mask; (2) *materialize* the "yes" rows into a new batch (Arrow calls this
> `Take`). Step 1 is **embarrassingly parallel** — row `i` doesn't care about
> row `j`. So we split rows across workers, each fills a slice of a shared
> mask, then we `Take` once.

### The comparison engine (type-aware)

`FilterCondition { field, op, value }` with ops
`eq, neq, gt, lt, gte, lte, not_null, is_null`. One `compare()` switch handles
every column type:

```cpp
bool compare(const Column& col, size_t i, const std::string& op, const std::string& value) {
    if (op == "not_null") return !col.is_null(i);
    if (op == "is_null")  return col.is_null(i);
    switch (col.type) {
        case Int64:  { int64_t v = std::stoll(value); int64_t x = col.i64[i]; ... x > v ... }
        case Double: { double v = std::stod(value);  ... }
        case String: { const std::string& x = col.str[i]; ... x == value ... }
        case Bool:   { bool v = (value=="true"); bool x = col.boolean[i]; ... }
    }
}
```

`value` is a string and is parsed into the column's type — `{"id", "gt", "5"}`
parses `"5"` to `int64_t`. Conditions are ANDed: a row survives only if *all*
conditions match.

### `apply` (single-threaded reference)

```cpp
RecordBatch apply(const RecordBatch& batch) {
    resolve columns;                            // validate fields once
    mask = vector<uint8_t>(num_rows, 0);
    for i in rows: mask[i] = row_matches(i);
    return take(batch, mask);                   // materialize
}
```

### `apply_parallel` — split rows, share a mask

```cpp
RecordBatch apply_parallel(const RecordBatch& batch, ThreadPool& pool) {
    resolve columns;  copy conditions (for the lambdas);
    mask = vector<uint8_t>(num_rows, 0);
    chunk = (rows + nworkers - 1) / nworkers;
    for w in nworkers:
        pool.submit([&, start, end] {
            for i in [start, end): mask[i] = row_matches(i);
        });
    pool.wait_all();
    return take(batch, mask);
}
```

Each worker writes a **disjoint slice** of the shared `mask` vector — no
contention, no locks. Then one `take()` materializes. The heavy, CPU-bound part
(row evaluation) is what parallelizes; `take` is memory-bandwidth-bound and kept
sequential (it could also be parallelized per-column later).

---

## PART D — `Transform`: registered functions, parallel chunks

### The mental model ("the aha")

> **A transform is just a function `batch → batch`.** The engine keeps a
> *registry* of named functions. To run one in parallel you split the batch into
> row chunks, apply the function to each chunk independently, and concatenate
> the results. The only requirement is that the function be **row-preserving**
> (same number of rows out as in) so chunks can be concatenated.

```cpp
using TransformFunc = std::function<RecordBatch(const RecordBatch&)>;

class Transform {
    void register_function(name, func);
    RecordBatch apply(name, batch);                          // whole batch
    RecordBatch apply_parallel(name, batch, ThreadPool&);    // chunk + concat
};
```

### `apply_parallel`

```cpp
int64_t chunk = (rows + nworkers - 1) / nworkers;
for w in nworkers:
    pool.submit([&, w, start, end] {
        results[w] = func(batch.slice(start, end));
        if (results[w].num_rows() != end - start)
            throw "must preserve row count";
    });
pool.wait_all();
return concat(results);
```

Each chunk is a `RecordBatch::slice(start, end)`; results are collected in a
pre-sized `vector<RecordBatch>` (index `w`, so no lock needed); `concat`
reassembles them in order. A per-worker `exception_ptr` propagates errors from
worker threads back to the caller after `wait_all`.

The test's example transform doubles the `id` column into a new `id2` column —
a stand-in for "compute `cycle_time_days` from `resolved_at - created_at`".

---

## How it fits the pipeline

This is the **compute** layer. In Sprint 3 the source scheduler pushed
`Response` items into a `StreamQueue`. In the full engine, those responses
become `RecordBatch`es that flow into `Transform`/`Filter` workers (consuming
from the bounded queue), then into `Aggregate`/`Sink` (Sprint 5). The thread
pool is the executor those workers run on.

```
SourceScheduler ──▶ StreamQueue<RecordBatch> ──▶ Transform (parallel)
                                                      │
                                                      ▼
                                                Filter (parallel)
                                                      │
                                                      ▼
                                               Aggregate / Sink   (next sprint)
```

---

## The "aha" patterns to internalize

1. **Columnar layout** = one contiguous vector per field; enables cache-friendly
   scans.
2. **Work-stealing = per-worker deques + owner-LIFO / thief-FIFO.** Kills the
   single-queue contention point and smooths load imbalance.
3. **`wait_for` + a `pending` counter** makes the pool self-correcting and makes
   `wait_all` trivial.
4. **Filter = parallel "evaluate" + one "materialize".** The embarrassingly
   parallel part is the mask; the copy is separate.
5. **Parallel transform = slice → map → concat.** Only works for
   row-preserving functions.

## Replicate it yourself (recipe)

1. `column.h`: `ColumnType`, `Column` (typed vectors + validity bitmap +
   `as_string`/`is_null`), `RecordBatch` (names + columns + `slice`/`concat`).
2. `ThreadPool`: per-worker deque + mutex + cv; `pop_own` (LIFO), `steal`
   (FIFO, rotating victim); worker loop pops/steals → runs → `pending--` →
   notify; `submit` round-robins; `wait_all` waits on `pending==0`;
   `shutdown` flags + joins.
3. `Filter`: `set_conditions`; `compare()` type switch; `apply` builds a mask
   then `take`; `apply_parallel` splits rows across the pool into disjoint
   mask slices.
4. `Transform`: registry; `apply` looks up + calls; `apply_parallel` slices,
   maps, concatenates, enforces row-preservation.
5. Tests: many-task correctness, speedup timing, nested-submit liveness,
   every comparison op, parallel==serial, row-count rejection; a benchmark
   printing single vs parallel timings.
