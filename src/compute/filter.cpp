#include "loom/compute/filter.h"

namespace loom {
namespace compute {

struct Filter::Impl {};

Filter::Filter() : impl_(std::make_unique<Impl>()) {}
Filter::~Filter() = default;

void Filter::set_conditions(const std::vector<FilterCondition>& /*conditions*/) {}

}  // namespace compute
}  // namespace loom
