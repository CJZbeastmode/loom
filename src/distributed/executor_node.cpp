#include "loom/distributed/executor_node.h"

namespace loom {
namespace distributed {

struct ExecutorNode::Impl {
    std::string executor_id;
    int port = 5051;
    bool running = false;
};

ExecutorNode::ExecutorNode(const std::string& executor_id, int port)
    : impl_(std::make_unique<Impl>()) {
    impl_->executor_id = executor_id;
    impl_->port = port;
}

ExecutorNode::~ExecutorNode() = default;

void ExecutorNode::start() { impl_->running = true; }
void ExecutorNode::stop() { impl_->running = false; }

void ExecutorNode::connect_to_coordinator(const std::string& /*host*/, int /*port*/) {}

void ExecutorNode::heartbeat() {}

void ExecutorNode::on_task_assigned(const TaskAssignment& /*task*/) {}

}  // namespace distributed
}  // namespace loom
