#include "loom/dag/executor.h"
#include "loom/dag/parser.h"

namespace loom {
namespace dag {

struct DAGExecutor::Impl {
    const DAG& dag;
    bool running = false;

    explicit Impl(const DAG& dag) : dag(dag) {}
};

DAGExecutor::DAGExecutor(const DAG& dag)
    : impl_(std::make_unique<Impl>(dag)) {}

DAGExecutor::~DAGExecutor() = default;

bool DAGExecutor::run() {
    impl_->running = true;
    return true;
}

void DAGExecutor::shutdown() {
    impl_->running = false;
}

bool DAGExecutor::is_running() const {
    return impl_->running;
}

}  // namespace dag
}  // namespace loom
