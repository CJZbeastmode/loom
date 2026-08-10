#include "loom/distributed/coordinator.h"

namespace loom {
namespace distributed {

struct Coordinator::Impl {
    int port = 5050;
    bool running = false;
};

Coordinator::Coordinator(int port)
    : impl_(std::make_unique<Impl>()) {
    impl_->port = port;
}

Coordinator::~Coordinator() = default;

void Coordinator::start() { impl_->running = true; }
void Coordinator::stop() { impl_->running = false; }

void Coordinator::register_executor(const ExecutorInfo& /*info*/) {}
void Coordinator::unregister_executor(const std::string& /*executor_id*/) {}

std::vector<ExecutorInfo> Coordinator::healthy_executors() const {
    return {};
}

}  // namespace distributed
}  // namespace loom
