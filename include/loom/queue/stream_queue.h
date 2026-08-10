#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>

namespace loom {
namespace queue {

template <typename T>
class StreamQueue {
public:
    struct Config {
        int max_buffered = 1000;
        int resume_at = 500;
    };

    explicit StreamQueue(const Config& config);
    ~StreamQueue();

    bool enqueue(T item);
    bool dequeue(T& item);
    bool is_backpressured() const;
    int size() const;

    void set_high_water_callback(std::function<void()> cb);
    void set_low_water_callback(std::function<void()> cb);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace queue
}  // namespace loom
