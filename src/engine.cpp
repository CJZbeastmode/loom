#include "loom/engine.h"
#include <sstream>

namespace loom {

struct Engine::Impl {
    EngineConfig config;
};

Engine::Engine() : impl_(std::make_unique<Impl>()) {}
Engine::~Engine() = default;

EngineResult Engine::run(const EngineConfig& config) {
    impl_->config = config;
    EngineResult result;
    result.error_message = "not yet implemented";
    return result;
}

EngineResult Engine::validate(const std::string& dag_path) {
    EngineResult result;
    result.error_message = "not yet implemented";
    return result;
}

std::string Engine::version() {
    std::ostringstream oss;
    oss << "loom " << LOOM_VERSION_MAJOR << "." << LOOM_VERSION_MINOR << "." << LOOM_VERSION_PATCH;
    return oss.str();
}

}  // namespace loom
