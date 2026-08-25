#include "test_utils.h"
#include "loom/queue/request_queue.h"
#include <cstdio>

using namespace loom;

namespace {
std::string temp_db_path() {
    return "/var/folders/hj/0g8xmk093_d0chf2nqjgcn7m0000gn/T/opencode/loom_test_queue.db";
}
}

TEST(RequestQueueTest, NewQueueIsExhausted) {
    queue::RequestQueue queue(":memory:", "test_run_001");
    EXPECT_TRUE(queue.is_exhausted());
}

TEST(RequestQueueTest, CountsInitialized) {
    queue::RequestQueue queue(":memory:", "test_run_001");
    EXPECT_EQ(queue.pending_count(), 0);
    EXPECT_EQ(queue.inflight_count(), 0);
    EXPECT_EQ(queue.done_count(), 0);
}

TEST(RequestQueueTest, EnqueueThenDequeue) {
    queue::RequestQueue queue(":memory:", "run_1");
    queue::RequestRecord r;
    r.request_id = "req-1";
    r.step_id = "gen";
    r.method = "GET";
    r.url = "https://api.example.com/items/1";
    queue.enqueue({r});

    EXPECT_EQ(queue.pending_count(), 1);
    EXPECT_FALSE(queue.is_exhausted());

    auto d = queue.dequeue();
    EXPECT_TRUE(d.has_value());
    EXPECT_EQ(d->request_id, "req-1");
    EXPECT_EQ(d->url, "https://api.example.com/items/1");
    EXPECT_EQ(static_cast<int>(d->status), static_cast<int>(queue::RequestStatus::Inflight));

    EXPECT_EQ(queue.pending_count(), 0);
    EXPECT_EQ(queue.inflight_count(), 1);
}

TEST(RequestQueueTest, DequeueFifoOrder) {
    queue::RequestQueue queue(":memory:", "run_1");
    std::vector<queue::RequestRecord> batch;
    for (int i = 0; i < 5; i++) {
        queue::RequestRecord r;
        r.request_id = "req-" + std::to_string(i);
        r.step_id = "gen";
        r.url = "https://api/x/" + std::to_string(i);
        batch.push_back(r);
    }
    queue.enqueue(batch);
    EXPECT_EQ(queue.pending_count(), 5);

    for (int i = 0; i < 5; i++) {
        auto d = queue.dequeue();
        EXPECT_TRUE(d.has_value());
        EXPECT_EQ(d->request_id, "req-" + std::to_string(i));
    }
    // All dequeued (now inflight), so nothing left to dequeue.
    EXPECT_EQ(queue.pending_count(), 0);
    EXPECT_FALSE(queue.dequeue().has_value());
}

TEST(RequestQueueTest, EmptyDequeueReturnsNullopt) {
    queue::RequestQueue queue(":memory:", "run_1");
    EXPECT_FALSE(queue.dequeue().has_value());
}

TEST(RequestQueueTest, MarkDoneLifecycle) {
    queue::RequestQueue queue(":memory:", "run_1");
    queue::RequestRecord r;
    r.request_id = "req-1";
    r.step_id = "gen";
    r.url = "https://api/x";
    queue.enqueue({r});

    auto d = queue.dequeue();
    EXPECT_TRUE(d.has_value());
    queue.mark_done("req-1", 200, "{\"ok\":true}");

    EXPECT_EQ(queue.done_count(), 1);
    EXPECT_EQ(queue.inflight_count(), 0);
    EXPECT_TRUE(queue.is_exhausted());
}

TEST(RequestQueueTest, MarkFailedStoresError) {
    queue::RequestQueue queue(":memory:", "run_1");
    queue::RequestRecord r;
    r.request_id = "req-1";
    r.step_id = "gen";
    r.url = "https://api/x";
    queue.enqueue({r});

    queue.dequeue();
    queue.mark_failed("req-1", "connection reset");

    EXPECT_EQ(queue.done_count(), 0);
    EXPECT_EQ(queue.inflight_count(), 0);
    EXPECT_TRUE(queue.is_exhausted());
}

TEST(RequestQueueTest, MarkSkipped) {
    queue::RequestQueue queue(":memory:", "run_1");
    queue::RequestRecord r;
    r.request_id = "req-1";
    r.step_id = "gen";
    r.url = "https://api/x";
    queue.enqueue({r});

    queue.dequeue();
    queue.mark_skipped("req-1");
    EXPECT_TRUE(queue.is_exhausted());
}

TEST(RequestQueueTest, ResumeResetsInflightToPending) {
    queue::RequestQueue queue(":memory:", "run_1");
    queue::RequestRecord r;
    r.request_id = "req-1";
    r.step_id = "gen";
    r.url = "https://api/x";
    queue.enqueue({r});

    queue.dequeue();  // now inflight
    EXPECT_EQ(queue.inflight_count(), 1);

    queue.resume();   // simulate crash recovery
    EXPECT_EQ(queue.inflight_count(), 0);
    EXPECT_EQ(queue.pending_count(), 1);

    // It should be dequeuable again.
    auto d = queue.dequeue();
    EXPECT_TRUE(d.has_value());
    EXPECT_EQ(d->request_id, "req-1");
}

TEST(RequestQueueTest, DurabilityAcrossReopen) {
    std::string path = temp_db_path();
    std::remove(path.c_str());

    {
        queue::RequestQueue queue(path, "durable_run");
        queue::RequestRecord r;
        r.request_id = "durable-1";
        r.step_id = "gen";
        r.url = "https://api/x/1";
        queue.enqueue({r});
        EXPECT_EQ(queue.pending_count(), 1);
    }  // queue destroyed, DB closed

    {
        queue::RequestQueue queue(path, "durable_run");  // reopen same dag_run_id
        EXPECT_EQ(queue.pending_count(), 1);
        auto d = queue.dequeue();
        EXPECT_TRUE(d.has_value());
        EXPECT_EQ(d->request_id, "durable-1");
        EXPECT_EQ(d->url, "https://api/x/1");
    }

    std::remove(path.c_str());
}

TEST(RequestQueueTest, DurabilityInflightResetsOnReopen) {
    std::string path = temp_db_path();
    std::remove(path.c_str());

    {
        queue::RequestQueue queue(path, "durable_run");
        queue::RequestRecord r;
        r.request_id = "crash-1";
        r.step_id = "gen";
        r.url = "https://api/x/1";
        queue.enqueue({r});
        queue.dequeue();  // left inflight, simulating a crash mid-flight
        EXPECT_EQ(queue.inflight_count(), 1);
    }

    {
        // Reopen: resume() should have moved inflight -> pending.
        queue::RequestQueue queue(path, "durable_run");
        EXPECT_EQ(queue.pending_count(), 1);
        EXPECT_EQ(queue.inflight_count(), 0);
    }

    std::remove(path.c_str());
}

TEST(RequestQueueTest, RunIsolation) {
    // Two dag_run_ids sharing the same database file must not see each other.
    std::string path = temp_db_path();
    std::remove(path.c_str());

    {
        queue::RequestQueue queue(path, "run_A");
        queue::RequestRecord r;
        r.request_id = "a-1";
        r.step_id = "gen";
        r.url = "https://api/a";
        queue.enqueue({r});
        EXPECT_EQ(queue.pending_count(), 1);
    }

    {
        queue::RequestQueue other(path, "run_B");
        EXPECT_TRUE(other.is_exhausted());
        EXPECT_EQ(other.pending_count(), 0);
        EXPECT_FALSE(other.dequeue().has_value());
    }

    std::remove(path.c_str());
}

TEST(RequestQueueTest, EmptyRequestIdGetsGenerated) {
    queue::RequestQueue queue(":memory:", "run_1");
    queue::RequestRecord r;
    r.step_id = "gen";
    r.url = "https://api/x";
    queue.enqueue({r});

    auto d = queue.dequeue();
    EXPECT_TRUE(d.has_value());
    EXPECT_FALSE(d->request_id.empty());
}
