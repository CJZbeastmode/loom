#pragma once

#include <memory>
#include <string>

namespace loom {
namespace distributed {

struct TaskAssignment;

class ExecutorNode {
public:
    ExecutorNode(const std::string& executor_id, int port = 5051);
    ~ExecutorNode();

    void start();
    void stop();
    void connect_to_coordinator(const std::string& host, int port);
    void heartbeat();
    void on_task_assigned(const TaskAssignment& task);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace distributed
}  // namespace loom
