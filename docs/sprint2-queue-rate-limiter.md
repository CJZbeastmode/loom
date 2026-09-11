# Sprint 2 — Durable Request Queue & Lock-Free Rate Limiter

**Files:**
`include/loom/queue/request_queue.h`, `src/queue/request_queue.cpp`,
`include/loom/queue/rate_limiter.h`, `src/queue/rate_limiter.cpp`,
`tests/test_request_queue.cpp`, `tests/test_rate_limiter.cpp`
**Status:** Implemented.

---

## The two problems

A source scheduler (Sprint 3) needs two primitives:

1. **A queue of "work to do"** — all the HTTP requests a pipeline wants to send.
   It must be **durable**: if the process dies mid-run, restarting must resume
   the leftover work, not re-fetch what already finished.
2. **A rate limiter** — the API says "10 requests per second". We need a cheap,
   thread-safe device that refuses to grant more than that.

Sprint 2 delivers both, in isolation, with tests.

---

## PART A — The token-bucket rate limiter

### The mental model ("the aha")

> **A token bucket is a picture of a time budget, not a real bucket.**
> Imagine a bucket that holds at most `max_tokens` coins. Each HTTP request
> costs one coin. Time is the mint: the bucket earns coins continuously at
> `rate` coins-per-second, up to the bucket's capacity.
>
> **Burst vs rate** — the capacity is the *burst* (how many you can fire
> instantly), the refill is the *sustained rate* (how many per second forever).
> `RateLimiter(10, 10.0)` = fire 10 immediately, then 10/sec forever.

**The key design decision:** there is **no background timer** filling the
bucket. The bucket is a *projection* — a calculation from the clock. The only
real stored state is:

```
tokens          = how many tokens there were the last time we looked
last_refill_ns  = the clock time when we looked
```

Every call recomputes: `tokens_now = tokens_at_last_look + rate × elapsed`.
Nothing "runs" between calls. This is what makes it lock-free: there is no
shared state to keep updated continuously.

### Why integers? (the fixed-point trick)

`rate` can be fractional (10.5/sec) and tokens can be partial (0.3 coins
accrued after 30ms). `std::atomic<double>` is awkward, so we **scale**:

```
1 token == 1,000,000 "micro-tokens"   (kScale = 1'000'000)
```

- capacity `max_tokens = 10`  → stored `10 * kScale = 10,000,000`
- rate `10.5/sec`             → stored `10.5 * kScale = 10,500,000` micro/sec
- "consume one token"         → subtract `kScale`

Everything stays an `int64_t`, which fits `std::atomic` cleanly.

### The struct

```cpp
struct RateLimiter::Impl {
    std::atomic<int64_t> tokens{0};          // micro-tokens available
    int64_t max_tokens = 0;                  // capacity (micro-tokens)
    std::atomic<int64_t> refill_per_sec{0};  // refill (micro-tokens/sec)
    std::atomic<int64_t> last_refill_ns{0};  // steady-clock of last banking
};
```

### `try_acquire()` — the lock-free CAS heart

```cpp
bool RateLimiter::try_acquire() {
    const int64_t max  = impl_->max_tokens;
    const int64_t rate = impl_->refill_per_sec.load(relaxed);
    int64_t cur = impl_->tokens.load(relaxed);
    while (true) {                                   // CAS retry loop
        int64_t now     = now_ns();                  // current clock
        int64_t last    = impl_->last_refill_ns.load(relaxed);
        int64_t elapsed = (now - last < 0) ? 0 : now - last;
        int64_t refill  = (elapsed * rate) / 1'000'000'000;  // micro-tokens accrued

        int64_t banked = cur + refill;               // add the projection
        if (banked > max) banked = max;              // cap at capacity

        int64_t after = banked - kScale;             // subtract ONE token
        if (after < 0) {
            // Not enough credit: bank the refill, fail.
            if (tokens.compare_exchange_weak(cur, banked, release, relaxed)) {
                last_refill_ns.compare_exchange_strong(last, now, relaxed, relaxed);
                return false;
            }
        } else {
            // Enough credit: consume one token, keep the remainder.
            if (tokens.compare_exchange_weak(cur, after, release, relaxed)) {
                last_refill_ns.compare_exchange_strong(last, now, relaxed, relaxed);
                return true;
            }
        }
        // CAS failed ⇒ someone else changed `tokens`; loop with the fresh value.
    }
}
```

**The three "aha"s of this function:**

1. **Refill is computed, then either banked (failure) or banked-then-consumed
   (success).** In the failure branch we *still write `banked` back* — if we
   discarded it we'd be throwing away accrued time. `banked` and the
   `last_refill_ns` advance are a **matched pair**: advance the clock, save the
   coins it produced.
2. **`compare_exchange_weak(cur, new)` is the atomic compare-and-swap.**
   It writes `new` only if `tokens` still equals `cur`; otherwise it updates
   `cur` to the current value and returns false. The `while(true)` loop retries
   until the CAS wins. This is the "lock-free" part: concurrent threads race on
   the atomic, and the token count can never go negative or exceed capacity.
3. **Why the second `compare_exchange_strong`?** Only ever *advance* the
   `last_refill_ns` clock forward, so two racing threads don't re-bank the same
   elapsed window.

### `wait_and_acquire()` — block until a token exists

```cpp
void RateLimiter::wait_and_acquire() {
    while (!try_acquire()) {
        int64_t rate = impl_->refill_per_sec.load(relaxed);
        if (rate <= 0) { sleep(1ms); continue; }
        int64_t period_ns = (kScale * 1'000'000'000LL) / rate;  // ns per token
        if (period_ns <= 0) period_ns = 1'000'000;
        std::this_thread::sleep_for(std::chrono::nanoseconds(period_ns));
    }
}
```

Sleeps roughly one token-period, then retries. `set_rate()` just stores the new
rate; `available_tokens()` is a read-only projection (no CAS, no writes).

### The tests that prove it

- `TryAcquireExhaustsTokens` — burst of 3 grants exactly 3, 4th denied.
- `RefillOverTime` — drain 1 token, sleep 250ms at 5/sec, a token is back.
- `ThroughputShaping` — over 2s at burst-10/10-per-sec, roughly
  10 + 20 = ~30 acquisitions (asserted between 15 and 40 for jitter).
- `ThreadSafety` — 8 threads racing on one bucket never exceed the burst.

---

## PART B — The durable request queue

### The mental model ("the aha")

> **A durable queue is a relational table wearing a queue costume.**
> Every request is a **row** in SQLite. "Enqueue" is an INSERT. "Dequeue" is a
> SELECT-of-the-next-pending plus an UPDATE-to-inflight. The status column is a
> **state machine**: `pending → inflight → done / failed / skipped`.
>
> Durability falls out for free: the table lives on disk. Crash → inflight rows
> are simply *reset back to pending* on restart (that's `resume()`).

### The schema (matches the design doc exactly)

```sql
CREATE TABLE request_queue (
    request_id      TEXT PRIMARY KEY,
    dag_run_id      TEXT NOT NULL,      -- scopes a run
    step_id         TEXT NOT NULL,
    method          TEXT DEFAULT 'GET',
    url             TEXT NOT NULL,
    headers         TEXT DEFAULT '{}',
    body            TEXT,
    status          TEXT DEFAULT 'pending',   -- the state machine column
    retry_count     INT DEFAULT 0,
    created_at      TEXT, started_at TEXT, completed_at TEXT,
    response_status INT, response_body TEXT, result_path TEXT
);
CREATE INDEX idx_request_queue_status ON request_queue(dag_run_id, status);
```

**`dag_run_id` is the isolation key.** Every query filters by it, so two runs
sharing one database file never see each other's work. (The test
`RunIsolation` proves it.)

### Why a single `UPDATE` handles every transition (the elegant bit)

Instead of 5 different SQL statements, one prepared statement with `COALESCE`
only *overwrites the fields it's given*:

```sql
UPDATE request_queue
SET status = ?,
    started_at     = COALESCE(?, started_at),
    completed_at   = COALESCE(?, completed_at),
    response_status= COALESCE(?, response_status),
    response_body  = COALESCE(?, response_body),
    retry_count    = retry_count + ?
WHERE request_id = ? AND dag_run_id = ?;
```

- `mark_inflight` → bind `'inflight'`, a timestamp for `started_at`, NULLs for
  everything else (they're *preserved*).
- `mark_done`     → bind `'done'`, timestamp + status + body.
- `mark_failed`   → bind `'failed'`, timestamp + error string into body.

`COALESCE(x, y)` = "use x unless it's NULL, then use y". So NULL means "don't
touch this column". One statement, four callers.

### `dequeue()` — atomic "pop"

Wrapped in a transaction so the SELECT-then-UPDATE can't race:

```cpp
std::optional<RequestRecord> RequestQueue::dequeue() {
    std::lock_guard<std::mutex> lock(impl_->mutex);   // (added in Sprint 3)
    exec(db, "BEGIN IMMEDIATE;");                     // take the write lock

    // SELECT the oldest 'pending' row for this run...
    //   (stmt_select_pending, ORDER BY rowid LIMIT 1)
    // if found: bind it into a RequestRecord struct
    // then UPDATE that row to 'inflight' (same transaction)

    exec(db, "COMMIT;");
    return record_or_nullopt;
}
```

### `resume()` — crash recovery

```cpp
UPDATE request_queue SET status='pending', started_at=NULL
WHERE status='inflight' AND dag_run_id=?;
```

Called from the constructor. On restart, any request that was in flight when the
process died is "un-dispatched" — exactly the durability promise. Test
`DurabilityInflightResetsOnReopen` writes to a real file, dequeues (leaving it
inflight), destroys the queue, reopens it, and asserts it's pending again.

### Thread safety (the fix that Sprint 3 forced)

The queue is used from two threads at once (the scheduler's worker thread calls
`dequeue`/counts; the HTTP event-loop thread calls `mark_done`/`mark_failed`).
SQLite itself is thread-safe, but **prepared statements are not** — they carry
per-statement state. So a single `mutable std::mutex` wraps every public method.
The rule: **one mutex, lock at the top of every method, no nested locks.**
(`is_exhausted` calls the two count methods, which each lock separately — fine,
because the locks aren't nested.)

### The in-memory fallback

The whole SQLite body sits behind `#ifdef LOOM_HAS_SQLITE`. Without it, the
same public API is backed by a `std::vector<RequestRecord>` + the same mutex.
Same interface, two backends — hidden behind the `Impl` (PImpl) pattern.

---

## How the two parts fit together

The rate limiter answers "may I start a request **now**?" The queue answers
"what should I work on, and is the work **durable**?". In Sprint 3 the scheduler
asks both questions on every dispatch tick:

```
while (rate_limiter.try_acquire()  &&  inflight < max  &&  !backpressured)
    record = queue.dequeue()          // durable take
    client.submit(record, callback)   // network I/O
```

## Replicate it yourself (recipe)

**Rate limiter**
1. Store tokens/rate as `int64_t` micro-tokens; keep `last_refill_ns`.
2. In `try_acquire`, recompute refill from elapsed time, cap at capacity,
   CAS `tokens` → success path subtracts one token, failure path banks.
3. Retry loop on CAS failure; advance the timestamp only forward.
4. `wait_and_acquire` sleeps one token-period between attempts.

**Queue**
1. Open SQLite, `CREATE TABLE IF NOT EXISTS` with the schema above.
2. Prepare statements once (insert / select-pending / transition / count).
3. `dequeue` = transaction of SELECT-pending + UPDATE-inflight.
4. One `COALESCE`-based UPDATE for all status transitions.
5. `resume` = UPDATE inflight→pending; call it in the constructor.
6. Wrap every method in one mutex.
7. Gate the SQLite code behind `LOOM_HAS_SQLITE`; provide a vector fallback.
