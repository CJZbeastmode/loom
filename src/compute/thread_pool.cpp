#include "loom/compute/thread_pool.h"
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace loom {
namespace compute {

struct ThreadPool::Impl {
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex mutex;
    std::condition_variable cv;
    bool stopped = false;

    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex);                       // lock acquired
                cv.wait(lock, [this] { return stopped || !tasks.empty(); });    // wait
                if (stopped && tasks.empty()) return;                           // return if stopped and no tasks
                task = std::move(tasks.front());                                // move task
                tasks.pop();                                                    // pop task
                // lock release
            }
            task();
        }
    }
};

// Constructor and destructor
ThreadPool::ThreadPool(int num_threads) : impl_(std::make_unique<Impl>()) {
    if (num_threads <= 0) {
        num_threads = static_cast<int>(std::thread::hardware_concurrency());
        if (num_threads <= 0) num_threads = 4;
    }
    impl_->workers.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        impl_->workers.emplace_back(&Impl::worker_loop, impl_.get());
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::submit(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);     // acquire lock
        impl_->tasks.push(std::move(task));                 // push task
    }
    impl_->cv.notify_one();     // notify one worker
}

void ThreadPool::wait_all() {
    // naive: spin until queue drains
    while (pending_tasks() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

int ThreadPool::worker_count() const {
    return static_cast<int>(impl_->workers.size());
}

int ThreadPool::pending_tasks() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return static_cast<int>(impl_->tasks.size());
}

void ThreadPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->stopped = true;
    }
    impl_->cv.notify_all();
    for (auto& w : impl_->workers) {
        if (w.joinable()) w.join();
    }
    impl_->workers.clear();
}

}  // namespace compute
}  // namespace loom
