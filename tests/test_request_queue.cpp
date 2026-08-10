#include <gtest/gtest.h>
#include "loom/queue/request_queue.h"

TEST(RequestQueueTest, NewQueueIsExhausted) {
    loom::queue::RequestQueue queue(":memory:", "test_run_001");
    EXPECT_TRUE(queue.is_exhausted());
}

TEST(RequestQueueTest, CountsInitialized) {
    loom::queue::RequestQueue queue(":memory:", "test_run_001");
    EXPECT_EQ(queue.pending_count(), 0);
    EXPECT_EQ(queue.inflight_count(), 0);
    EXPECT_EQ(queue.done_count(), 0);
}
