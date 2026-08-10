#include "loom/queue/stream_queue.h"
#include <queue>
#include <mutex>
#include <condition_variable>

namespace loom {
namespace queue {

template <typename T>
struct StreamQueue<T>::Impl {
    Config config;
    std::queue<T> buffer;
    std::mutex mutex;
    std::condition_variable cv;
    bool backpressured = false;
    std::function<void()> high_water_cb;
    std::function<void()> low_water_cb;
};

template <typename T>
StreamQueue<T>::StreamQueue(const Config& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

template <typename T>
StreamQueue<T>::~StreamQueue() = default;

template <typename T>
bool StreamQueue<T>::enqueue(T item) {
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

template <typename T>
bool StreamQueue<T>::dequeue(T& item) {
    std::unique_lock<std::mutex> lock(impl_->mutex);
    if (impl_->buffer.empty()) return false;
    item = std::move(impl_->buffer.front());
    impl_->buffer.pop();
    if (impl_->backpressured &&
        static_cast<int>(impl_->buffer.size()) <= impl_->config.resume_at) {
        impl_->backpressured = false;
        if (impl_->low_water_cb) impl_->low_water_cb();
    }
    return true;
}

template <typename T>
bool StreamQueue<T>::is_backpressured() const {
    return impl_->backpressured;
}

template <typename T>
int StreamQueue<T>::size() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return static_cast<int>(impl_->buffer.size());
}

template <typename T>
void StreamQueue<T>::set_high_water_callback(std::function<void()> cb) {
    impl_->high_water_cb = std::move(cb);
}

template <typename T>
void StreamQueue<T>::set_low_water_callback(std::function<void()> cb) {
    impl_->low_water_cb = std::move(cb);
}

}  // namespace queue
}  // namespace loom
