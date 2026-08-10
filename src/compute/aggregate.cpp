#include "loom/compute/aggregate.h"

namespace loom {
namespace compute {

struct Aggregate::Impl {};

Aggregate::Aggregate() : impl_(std::make_unique<Impl>()) {}
Aggregate::~Aggregate() = default;

void Aggregate::set_group_by(const std::vector<std::string>& /*columns*/) {}

void Aggregate::set_aggregates(const std::vector<AggregateDef>& /*aggs*/) {}

void Aggregate::reset() {}

}  // namespace compute
}  // namespace loom
