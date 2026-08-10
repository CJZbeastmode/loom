#include "loom/compute/transform.h"

namespace loom {
namespace compute {

struct Transform::Impl {};

Transform::Transform() : impl_(std::make_unique<Impl>()) {}
Transform::~Transform() = default;

void Transform::register_python_function(const std::string& /*name*/,
                                         const std::string& /*module*/,
                                         const std::string& /*func*/) {}

void Transform::register_native_function(const std::string& /*name*/, TransformFunc /*func*/) {}

}  // namespace compute
}  // namespace loom
