#include "loom/compute/thread_pool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace loom {
namespace compute {

// Work-stealing pool: each worker owns a double-ended queue.  A worker runs
// its own tasks last-in-first-out (LIFO, better cache locality for nested
// submits), and when idle steals from the *front* of a victim's queue
// (first-in-first-out).  LIFO-owner / FIFO-thief is the classic Chase-Lev
// policy; here we use a mutex per worker instead of a lock-free deque.
struct ThreadPool::Impl {
    struct Worker {
        int id = 0;
        std::thread thread;
        std::deque<std::function<void()>> queue;
        std::mutex mutex;
        std::condition_variable cv;
        bool stop = false;
        int steal_cursor = 0;  // rotating start index for stealing
    };

    std::vector<std::unique_ptr<Worker>> workers;
    std::atomic<int> next_submit{0};
    std::atomic<int> pending{0};
    std::mutex done_mutex;
    std::condition_variable done_cv;

    // Pop the worker's own newest task (LIFO).
    bool pop_own(Worker* w, std::function<void()>& out) {
        std::lock_guard<std::mutex> lock(w->mutex);
        if (w->queue.empty()) return false;
        out = std::move(w->queue.back());
        w->queue.pop_back();
        return true;
    }

    // Steal the oldest task from another worker (FIFO). Rotates the start
    // victim to spread contention.
    bool steal(Worker* thief, std::function<void()>& out) {
        const int n = static_cast<int>(workers.size());
        for (int k = 0; k < n; ++k) {
            int idx = (thief->steal_cursor + k) % n;
            Worker* victim = workers[idx].get();
            if (victim == thief) continue;
            std::lock_guard<std::mutex> lock(victim->mutex);
            if (victim->queue.empty()) continue;
            out = std::move(victim->queue.front());
            victim->queue.pop_front();
            thief->steal_cursor = (idx + 1) % n;
            return true;
        }
        return false;
    }

    void worker_loop(Worker* w) {
        while (true) {
            std::function<void()> task;
            if (pop_own(w, task) || steal(w, task)) {
                task();
                if (pending.fetch_sub(1) == 1) {
                    std::lock_guard<std::mutex> lock(done_mutex);
                    done_cv.notify_all();
                }
                continue;
            }
            // Nothing to do: park briefly, then re-check (self-correcting even
            // if a notify was missed).
            std::unique_lock<std::mutex> lock(w->mutex);
            if (w->stop) return;
            w->cv.wait_for(lock, std::chrono::milliseconds(1));
        }
    }
};

ThreadPool::ThreadPool(int num_threads) : impl_(std::make_unique<Impl>()) {
    if (num_threads <= 0) {
        num_threads = static_cast<int>(std::thread::hardware_concurrency());
        if (num_threads <= 0) num_threads = 4;
    }
    impl_->workers.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        auto w = std::make_unique<Impl::Worker>();
        w->id = i;
        w->steal_cursor = (i + 1) % num_threads;
        w->thread = std::thread(&Impl::worker_loop, impl_.get(), w.get());
        impl_->workers.push_back(std::move(w));
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::submit(std::function<void()> task) {
    int n = static_cast<int>(impl_->workers.size());
    int idx = impl_->next_submit.fetch_add(1, std::memory_order_relaxed) % n;
    Impl::Worker* w = impl_->workers[idx].get();
    {
        std::lock_guard<std::mutex> lock(w->mutex);
        w->queue.push_back(std::move(task));
    }
    impl_->pending.fetch_add(1, std::memory_order_relaxed);
    w->cv.notify_one();
}

void ThreadPool::wait_all() {
    std::unique_lock<std::mutex> lock(impl_->done_mutex);
    impl_->done_cv.wait(lock, [this] { return impl_->pending.load() == 0; });
}

int ThreadPool::worker_count() const {
    return static_cast<int>(impl_->workers.size());
}

int ThreadPool::pending_tasks() const {
    return impl_->pending.load();
}

void ThreadPool::shutdown() {
    for (auto& w : impl_->workers) {
        {
            std::lock_guard<std::mutex> lock(w->mutex);
            w->stop = true;
        }
        w->cv.notify_all();
    }
    for (auto& w : impl_->workers) {
        if (w->thread.joinable()) w->thread.join();
    }
    impl_->workers.clear();
}

}  // namespace compute
}  // namespace loom
