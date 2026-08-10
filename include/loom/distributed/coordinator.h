#pragma once

#include <memory>
#include <string>
#include <vector>

namespace loom {
namespace distributed {

struct ExecutorInfo {
    std::string executor_id;
    std::string host;
    int port = 0;
    int capacity = 0;
    bool is_healthy = true;
};

struct TaskAssignment {
    std::string step_id;
    int partition_start = 0;
    int partition_end = 0;
    std::string dag_snapshot;
};

class Coordinator {
public:
    explicit Coordinator(int port = 5050);
    ~Coordinator();

    void start();
    void stop();
    void register_executor(const ExecutorInfo& info);
    void unregister_executor(const std::string& executor_id);
    std::vector<ExecutorInfo> healthy_executors() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace distributed
}  // namespace loom
