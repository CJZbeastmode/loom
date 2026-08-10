#include "loom/compute/join.h"

namespace loom {
namespace compute {

struct Join::Impl {
    JoinConfig config;
};

Join::Join(const JoinConfig& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

Join::~Join() = default;

void Join::register_input(const std::string& /*source_id*/) {}

#ifdef LOOM_HAS_ARROW
void Join::set_ready_callback(ReadyCallback /*cb*/) {}
#endif

}  // namespace compute
}  // namespace loom
