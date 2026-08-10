#include "loom/connectors/connector_registry.h"

namespace loom {
namespace connectors {

struct ConnectorRegistry::Impl {};

ConnectorRegistry::ConnectorRegistry() : impl_(std::make_unique<Impl>()) {}
ConnectorRegistry::~ConnectorRegistry() = default;

S3Connector* ConnectorRegistry::get_s3(const std::string& /*name*/, const S3Config& /*config*/) {
    return nullptr;
}

bool ConnectorRegistry::has(const std::string& /*name*/) const {
    return false;
}

}  // namespace connectors
}  // namespace loom
