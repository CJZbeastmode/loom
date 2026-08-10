#include "loom/compute/unnest.h"

namespace loom {
namespace compute {

struct Unnest::Impl {
    UnnestConfig config;
};

Unnest::Unnest(const UnnestConfig& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

Unnest::~Unnest() = default;

#ifdef LOOM_HAS_ARROW
std::shared_ptr<arrow::RecordBatch> Unnest::apply(
    const std::shared_ptr<arrow::RecordBatch>& /*batch*/) {
    return nullptr;
}
#endif

}  // namespace compute
}  // namespace loom
