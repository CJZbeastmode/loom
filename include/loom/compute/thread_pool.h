#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace loom {
namespace compute {

class ThreadPool {
public:
    explicit ThreadPool(int num_threads = 0);
    ~ThreadPool();

    void submit(std::function<void()> task);
    void wait_all();
    int worker_count() const;
    int pending_tasks() const;
    void shutdown();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
