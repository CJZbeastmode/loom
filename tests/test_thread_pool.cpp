#include "test_utils.h"
#include "loom/compute/thread_pool.h"

#include <atomic>
#include <chrono>
#include <thread>

using namespace loom;

TEST(ThreadPoolTest, ManyTasksAllComplete) {
    compute::ThreadPool pool(4);
    std::atomic<int> counter{0};
    const int N = 1000;
    for (int i = 0; i < N; ++i) {
        pool.submit([&] { counter.fetch_add(1); });
    }
    pool.wait_all();
    EXPECT_EQ(counter.load(), N);
}

TEST(ThreadPoolTest, ParallelSpeedup) {
    compute::ThreadPool pool(4);
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 4; ++i) {
        pool.submit([] { std::this_thread::sleep_for(std::chrono::milliseconds(100)); });
    }
    pool.wait_all();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start).count();
    // 4 tasks × 100ms = 400ms serial. In parallel they overlap, well under 350ms.
    EXPECT_TRUE(elapsed < 350);
}

TEST(ThreadPoolTest, NestedSubmitNoDeadlock) {
    compute::ThreadPool pool(2);
    std::atomic<int> count{0};
    // A task that itself submits more tasks — workers must steal these and the
    // pool must not deadlock on its own pending counter.
    pool.submit([&] {
        for (int i = 0; i < 10; ++i) {
            pool.submit([&] { count.fetch_add(1); });
        }
    });
    pool.wait_all();
    EXPECT_EQ(count.load(), 10);
}

TEST(ThreadPoolTest, WorkerCountAndPending) {
    compute::ThreadPool pool(3);
    EXPECT_EQ(pool.worker_count(), 3);

    std::atomic<bool> release{false};
    pool.submit([&] { while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(pool.pending_tasks() >= 1);  // the blocking task is still running

    release = true;
    pool.wait_all();
    EXPECT_EQ(pool.pending_tasks(), 0);
}

TEST(ThreadPoolTest, DefaultThreadCount) {
    compute::ThreadPool pool(0);  // 0 → hardware_concurrency
    EXPECT_TRUE(pool.worker_count() >= 1);
}
