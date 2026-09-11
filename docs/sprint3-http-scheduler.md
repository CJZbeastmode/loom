# Sprint 3 — Async HTTP Client & Source Scheduler

**Files:**
`include/loom/http/client.h`, `src/http/client.cpp`,
`include/loom/dag/source_scheduler.h`, `src/dag/source_scheduler.cpp`,
`include/loom/queue/stream_queue.h` (converted to header-only),
`tests/mock_http_server.h`, `tests/test_http_client.cpp`, `tests/test_source_scheduler.cpp`
**Status:** Implemented (plain HTTP; HTTPS/TLS is a noted future step).

---

## The objective

The streaming ingestion loop. Wire together Sprint 2's queue + rate limiter with
a real async HTTP client and a bounded output queue, so that:

> "100 requests are dispatched at ~10/sec with max 10 in flight; each completed
> response streams into a bounded queue immediately; backpressure pauses
> dispatch when the downstream buffer fills; the run terminates cleanly."

Three new moving pieces:

| Piece | Job | Runs on which thread |
|---|---|---|
| `HttpClient` | do the actual network I/O, asynchronously | its own event-loop thread |
| `StreamQueue` | bounded buffer with high/low watermarks (backpressure) | shared (mutex + cv) |
| `SourceScheduler` | the dispatch loop that ties queue + limiter + client + output together | its own worker thread |

---

## PART A — `StreamQueue`: bounded buffer + backpressure

### The mental model ("the aha")

> **A pipe with two marks painted on it.** Downstream (a consumer) can drain the
> pipe. Upstream (a producer) fills it. The pipe has a HIGH watermark and a LOW
> watermark. Once the water crosses the HIGH mark, the pipe is
> **backpressured** — producers must stop pushing. Once a consumer drains it
> below the LOW mark, producers may resume.
>
> Hysteresis matters: if resume == high, a jittery consumer would flap
> pause/resume constantly. High and Low being different (e.g. 1000 / 500) gives
> a quiet band.

Converted to a **header-only template** so any item type (`Response`,
`std::string`, an Arrow batch in a later sprint) can be queued without touching
the `.cpp`.

Key API:

```cpp
bool    enqueue(T item);      // non-blocking: FAILS (returns false) if backpressured
void    enqueue_wait(T item); // blocking: waits until not backpressured (producers that can't drop data)
bool    dequeue(T& out);      // non-blocking pop, false if empty
void    dequeue_wait(T& out); // blocking pop (consumers)
bool    is_backpressured();
int     size();
```

**The bug that mattered:** `enqueue_wait` blocks on a condition variable waiting
for `!backpressured`. If `dequeue` released backpressure but never called
`cv.notify_all()`, a blocked producer waited forever. The fix is in every
release path:

```cpp
if (impl_->backpressured && size <= resume_at) {
    impl_->backpressured = false;
    if (low_water_cb) low_water_cb();
    impl_->cv.notify_all();   // ← wake any blocked enqueue_wait producers
}
```

**Backpressure, end to end:** when the scheduler's output queue is full, a
response callback's `enqueue_wait` *blocks the HTTP event-loop thread*. New
responses stop being processed; the scheduler's `tick()` sees
`is_backpressured()` and stops dispatching new requests. The whole pipeline
slows down as one — that is the design goal (bounded memory), not a bug.

---

## PART B — `HttpClient`: the libuv async client

### The mental model ("the aha")

> **libuv is an event loop: one thread, a loop, and a set of callbacks.**
> You register interest ("connect this socket", "start a timer", "wake me on
> async signal"), then call `uv_run()`, which blocks forever dispatching events.
> Your code is never "busy" — it's a series of callbacks chained together.
>
> For one HTTP request the chain is:
>
> ```
> submit() ──▶ uv_async_send ──▶ on_async_wakeup
>                                     │ (start timeout timer, start DNS)
>                                     ▼
>                                 on_resolved  (DNS done)
>                                     │ (init tcp, connect)
>                                     ▼
>                                 on_connect  (connected)
>                                     │ (build request text, uv_write)
>                                     ▼
>                                 on_write  (request sent)
>                                     │ (uv_read_start)
>                                     ▼
>                                 on_read  (response data) ──▶ full body? ──▶ finalize()
>                                     │
>                                     └─ EOF / error ───────────────▶ finalize()
>
> any stage can also jump to finalize() via on_timeout  (the timer)
> ```

### `submit()` — the only method called from another thread

```cpp
void HttpClient::submit(const HttpRequest& request, ResponseCallback callback) {
    Url url;
    if (!parse_url(request.url, url)) { callback(error); return; }   // sync reject
    if (url.scheme == "https")        { callback(error); return; }   // TLS not wired yet

    auto req = std::make_shared<Request>();   // per-request state
    req->http = request; req->callback = std::move(callback);
    req->url = url; req->impl = impl_.get();

    impl_->inflight.fetch_add(1);             // concurrency counter
    { lock(active_mutex);  impl_->active.insert(req); }  // anchor shared_ptr
    { lock(pending_mutex); impl_->pending.push_back(req); }
    uv_async_send(&impl_->async);             // thread-safe: wake the loop
}
```

Three collections matter:
- `pending` — submitted, not yet started (mutex-protected deque).
- `active` — **anchor set**: holds a `shared_ptr<Request>` per live request so
  it can't be freed while its handles exist.
- `inflight` — `std::atomic<int>`, the counter `max_inflight` compares against.

### `on_async_wakeup` — the loop thread's "start working" signal

```cpp
void HttpClient::Impl::on_async_wakeup(uv_async_t* handle) {
    auto* impl = static_cast<Impl*>(handle->data);
    std::deque<std::shared_ptr<Request>> batch;
    { lock(pending_mutex); batch.swap(impl->pending); }

    for (auto& req : batch) {
        req->start = steady_clock::now();
        uv_timer_init(&impl->loop, &req->timer);         // start the timeout clock
        req->timer.data = req.get();
        uv_timer_start(&req->timer, on_timeout, req->http.timeout.count(), 0);

        req->service = std::to_string(req->url.port);    // stable string!
        req->resolver.data = new std::shared_ptr<Request>(req);
        uv_getaddrinfo(&impl->loop, &req->resolver, on_resolved,
                       req->url.host.c_str(), req->service.c_str(), &hints);
    }
}
```

**Two things that must be exactly right here** (both were real bugs):

1. **libuv's `uv_getaddrinfo` stores the raw `char*` pointers you pass it — it
   does NOT copy them.** Passing `std::to_string(port).c_str()` gives a dangling
   pointer the moment the temporary dies. Hence `req->service`, a string member
   that outlives the DNS call.
2. Each async operation's `.data` field carries a **heap-allocated
   `shared_ptr<Request>`** so the request stays alive until that callback fires.

### The callbacks (each advances one stage)

```cpp
// DNS finished
on_resolved:  take shared_ptr out of resolver.data, delete the holder
              if req->finalized → uv_freeaddrinfo, return          (timeout already fired)
              if status < 0     → finalize("DNS resolution failed")
              uv_tcp_init; socket.data = req; uv_tcp_connect(...)

// TCP connected
on_connect:   take shared_ptr out of connect.data, delete holder + connect
              if req->finalized → return
              if status < 0     → finalize("connect error")
              build request text:
                "GET /path HTTP/1.1\r\n"
                "Host: host\r\nAccept: ...\r\n[Authorization]\r\n[X-Request-Tag]\r\n"
                "[Content-Length: n]\r\nConnection: close\r\n\r\n[body]"
              req->write_buf = that text          // member string stays alive for uv_write
              uv_write(req->writer, socket, buf, 1, on_write)

// request bytes sent
on_write:     if status < 0 → finalize("write error")
              uv_read_start(socket, alloc_cb, on_read)     // start reading response

// response bytes arrive
on_read:      append to req->read_buf
              parse_status → finds "\r\n\r\n", extracts the "200" from "HTTP/1.1 200 OK"
              read Content-Length
              if body complete (or HEAD, or EOF) → finalize("", status, body)
              if nread < 0 && != UV_EOF          → finalize("read error")

// the timer fired
on_timeout:   finalize("request timed out")
```

### `finalize()` — the single exit door

Every path ends here; the `finalized` flag guarantees it runs once:

```cpp
static void finalize(Request* req, error, status, body) {
    if (req->finalized) return;
    req->finalized = true;
    if (error.empty() && status != 0)
        for (int c : req->impl->config.fail_on_status)
            if (c == status) { error = "HTTP status ... is in fail_on_status"; break; }
    req->response = {error, status, body, latency};
    uv_timer_stop; if (socket) uv_read_stop;
    close_all_handles(req);            // uv_close socket+timer; counts closes_pending
    auto cb = std::move(req->callback);       // take the callback OUT
    auto resp = req->response;                // copy the response OUT
    req->impl->decrement_inflight();
    cb(std::move(resp));                      // invoke user code
}
```

### Lifetime management (the subtle "aha")

A `Request` struct owns embedded libuv handles (`socket`, `timer` are fields,
not pointers). You cannot free it while a handle exists, but callbacks only hold
a raw `Request*`. The resolution:

- The `active` set holds the last `shared_ptr`.
- `close_all_handles` calls `uv_close` on socket + timer, each bumping
  `closes_pending`.
- `on_handle_closed` decrements; when it hits **0**, `erase()` drops the anchor
  `shared_ptr` → the `Request` is freed.

So a request's lifetime is: *submit → DNS → connect → write → read → finalize →
last handle closes → freed.* No use-after-free, no leak.

### Event-loop lifecycle (another real bug we hit)

```cpp
void run_event_loop() { uv_run(&impl_->loop, UV_RUN_DEFAULT); }   // blocks
void stop_event_loop() {
    uv_stop(&impl_->loop);
    uv_async_send(&impl_->async);   // ← REQUIRED
}
```

`uv_stop()` alone sets a flag but **does not wake a loop blocked in `poll`** —
the loop stays asleep forever. `uv_async_send` writes to the loop's wakeup
descriptor, so `uv_run` wakes up, sees the stop flag, and returns. Verified in
isolation before fixing.

---

## PART C — `SourceScheduler`: the dispatch loop

### The mental model ("the aha")

> **The scheduler is a traffic light at the intersection of three lanes.**
> The green light requires ALL THREE to be green:
>
> ```
> while ( rate_limiter.try_acquire()            // lane 1: do I have a token?
>      && client.inflight_count() < max_inflight // lane 2: is there a concurrency slot?
>      && !output->is_backpressured() ) {        // lane 3: is downstream taking data?
>     record = queue.dequeue();                  // take durable work
>     client.submit(record, callback);           // hand it to the async client
> }
> ```
>
> When any lane is red, dispatch pauses. Nothing else in the scheduler is
> clever — the queue, the limiter, and the client already did the hard work.

### `tick()` — one dispatch sweep

```cpp
bool SourceScheduler::tick() {
    while (limiter->try_acquire()
           && client->inflight_count() < client->max_inflight()
           && !output->is_backpressured()) {
        auto record = queue->dequeue();
        if (!record) break;                     // no more work → stop trying

        http::HttpRequest req;
        req.method = record->method; req.url = record->url; req.body = record->body;

        std::string request_id = record->request_id;
        std::string step_id    = record->step_id;

        client->submit(req, [this, request_id, step_id](http::HttpResponse resp) {
            if (resp.error.empty()) {
                queue->mark_done(request_id, resp.status_code, resp.body);  // durable
                output->enqueue_wait(Response{...});                        // stream downstream
                completed++;
            } else {
                queue->mark_failed(request_id, resp.error);
                failed++;
            }
        });
        dispatched++;
    }
    return !is_done();     // keep looping unless done
}
```

**Notice the callback runs on the client's event-loop thread** — this is the
bridge between the two worlds. It updates the durable queue (Sprint 2), and
pushes a `dag::Response` into the bounded `StreamQueue` for downstream compute.

### `is_done()` — the termination condition

```cpp
bool SourceScheduler::is_done() const {
    return queue->is_exhausted() && client->inflight_count() == 0;
}
```

Done = **no pending work** AND **nothing in flight**. This is exactly the design
doc's rule: "all source queues empty + all inflight resolved".

### `run()` / `start()` / `stop()`

```cpp
void run() {
    running = true;
    while (running) {
        if (!tick()) break;              // done
        sleep_for(poll_interval);        // 2ms default
    }
    running = false;
}
void start() { worker = thread([this]{ run(); }); }
void stop()  { running = false; if (worker.joinable()) worker.join(); }
```

---

## The threading model (put it all together)

```
        main / caller thread                     libuv event-loop thread
   ┌───────────────────────────┐            ┌──────────────────────────────┐
   │ client.submit()           │            │  uv_run → callbacks           │
   │   push pending + async_send│ ──────▶   │   on_resolved → on_connect    │
   │                           │            │   → on_write → on_read        │
   │                           │            │   → response callback:        │
   │                           │            │      queue->mark_done/failed  │
   │                           │            │      output->enqueue_wait(...) │
   └───────────────────────────┘            └──────────────────────────────┘
                                                            │
                         scheduler worker thread            │ (Response)
   ┌───────────────────────────────┐                        ▼
   │  run(): tick() loop           │   ┌───────────────────────────────┐
   │  queue.dequeue()              │   │   StreamQueue<Response>       │
   │  client.submit()              │   │   (bounded, high/low water)   │
   └───────────────────────────────┘   └───────────────────────────────┘
                                                            │
                                                consumer thread (downstream)
                                                output.dequeue() → transform/filter/...
```

- **Scheduler worker thread** calls `dequeue` (SQLite) and `submit` (queue+signal).
- **Event-loop thread** runs network I/O and the response callbacks, which call
  `mark_done` (SQLite) and `enqueue_wait` (blocking push).
- Both touch the **same SQLite connection and prepared statements** → that's why
  the RequestQueue mutex from Sprint 2 is essential.
- The **bounded queue** is the only connection between ingestion and compute,
  and its watermarks are what bound memory.

### The three tests that prove the design

1. `EndToEndStreaming` — 100 requests against a mock server: dispatched == 100,
   completed == 100, output drained == 100, no failures.
2. `RespectsMaxInflight` — server has a 20ms delay; polling never observes
   `inflight_count() > max_inflight`.
3. `BackpressurePausesDispatch` — output capped at 5; once full, the dispatch
   counter stops growing; after draining, all 100 complete.

`tests/mock_http_server.h` is a tiny raw-socket TCP server (accept → read
headers → send a canned `HTTP/1.1` response → close) used to test without any
real external API.

---

## The gotchas we fixed (worth re-reading before you reimplement)

1. **`uv_getaddrinfo` does not copy its string arguments.** Keep them as stable
   members (`req->service`).
2. **`uv_stop` alone never wakes a blocked loop.** Pair it with `uv_async_send`.
3. **Prepared SQLite statements are not thread-safe.** Serialize the whole queue
   with one mutex.
4. **A producer blocked in `enqueue_wait` needs `cv.notify_all()` on the
   release path** — otherwise backpressure deadlocks.
5. **You cannot `delete` a libuv-handle-owning struct until every handle's close
   callback ran** — hence the `active` anchor set + `closes_pending`.

## Replicate it yourself (recipe)

1. `StreamQueue<T>` header-only: mutex + cv + `std::queue<T>` + `backpressured`
   flag; set high-water on push, release + `notify_all` on drain below low-water.
2. `HttpClient`: Impl holds loop + async handle + pending/active sets + atomic
   inflight. `submit` queues + `uv_async_send`. Chain DNS→connect→write→read
   callbacks, each carrying a `shared_ptr<Request>` in `.data`. All exits funnel
   through `finalize`. Free requests only after all handles close.
3. `SourceScheduler`: constructor takes 4 pointers; `tick()` is the three-gate
   while loop; response callback marks the durable queue and pushes the output;
   `run/start/stop`; `is_done` = exhausted && inflight == 0.
4. Test with a `MockHttpServer` (raw sockets) — never a real API.
