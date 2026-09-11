#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>

namespace loom {
namespace queue {

// Bounded FIFO queue with high/low watermarks for backpressure.
//
// enqueue() fails (returns false) once the buffer reaches the high-water mark
// and downstream hasn't drained below the low-water mark yet.  The caller can
// observe the transition via set_high_water_callback / set_low_water_callback.
template <typename T>
class StreamQueue {
public:
    struct Config {
        int max_buffered = 1000;
        int resume_at = 500;
    };

    explicit StreamQueue(const Config& config) : impl_(std::make_unique<Impl>()) {
        impl_->config = config;
    }

    ~StreamQueue() = default;

    StreamQueue(const StreamQueue&) = delete;
    StreamQueue& operator=(const StreamQueue&) = delete;

    // Push one item. Returns false (drops the item) if the queue is
    // backpressured. Returns true on success.
    bool enqueue(T item) {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (impl_->backpressured) return false;
        impl_->buffer.push(std::move(item));
        impl_->cv.notify_one();
        if (!impl_->backpressured &&
            static_cast<int>(impl_->buffer.size()) >= impl_->config.max_buffered) {
            impl_->backpressured = true;
            if (impl_->high_water_cb) impl_->high_water_cb();
        }
        return true;
    }

    // Block until the item can be pushed (waits for downstream to drain below
    // the low-water mark). Used by producers that must not drop data.
    void enqueue_wait(T item) {
        std::unique_lock<std::mutex> lock(impl_->mutex);
        impl_->cv.wait(lock, [this] { return !impl_->backpressured; });
        impl_->buffer.push(std::move(item));
        impl_->cv.notify_one();
        if (!impl_->backpressured &&
            static_cast<int>(impl_->buffer.size()) >= impl_->config.max_buffered) {
            impl_->backpressured = true;
            if (impl_->high_water_cb) impl_->high_water_cb();
        }
    }

    // Pop one item. Returns false if the queue is empty.
    bool dequeue(T& item) {
        std::unique_lock<std::mutex> lock(impl_->mutex);
        if (impl_->buffer.empty()) return false;
        item = std::move(impl_->buffer.front());
        impl_->buffer.pop();
        if (impl_->backpressured &&
            static_cast<int>(impl_->buffer.size()) <= impl_->config.resume_at) {
            impl_->backpressured = false;
            if (impl_->low_water_cb) impl_->low_water_cb();
            impl_->cv.notify_all();  // wake blocked producers
        }
        return true;
    }

    // Block until an item is available, then pop it.
    void dequeue_wait(T& item) {
        std::unique_lock<std::mutex> lock(impl_->mutex);
        impl_->cv.wait(lock, [this] { return !impl_->buffer.empty(); });
        item = std::move(impl_->buffer.front());
        impl_->buffer.pop();
        if (impl_->backpressured &&
            static_cast<int>(impl_->buffer.size()) <= impl_->config.resume_at) {
            impl_->backpressured = false;
            if (impl_->low_water_cb) impl_->low_water_cb();
            impl_->cv.notify_all();  // wake blocked producers
        }
    }

    bool is_backpressured() const { return impl_->backpressured; }

    int size() const {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        return static_cast<int>(impl_->buffer.size());
    }

    void set_high_water_callback(std::function<void()> cb) {
        impl_->high_water_cb = std::move(cb);
    }

    void set_low_water_callback(std::function<void()> cb) {
        impl_->low_water_cb = std::move(cb);
    }

private:
    struct Impl {
        Config config;
        std::queue<T> buffer;
        std::mutex mutex;
        std::condition_variable cv;
        bool backpressured = false;
        std::function<void()> high_water_cb;
        std::function<void()> low_water_cb;
    };

    std::unique_ptr<Impl> impl_;
};

}  // namespace queue
}  // namespace loom
