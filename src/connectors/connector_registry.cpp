#include "loom/connectors/connector_registry.h"

#include <stdexcept>

namespace loom {
namespace connectors {

void ConnectorRegistry::add(const std::string& name, std::unique_ptr<Connector> connector) {
    connectors_[name] = std::move(connector);
}

Connector* ConnectorRegistry::get(const std::string& name) const {
    auto it = connectors_.find(name);
    if (it == connectors_.end()) {
        throw std::runtime_error("unknown connector: " + name);
    }
    return it->second.get();
}

bool ConnectorRegistry::has(const std::string& name) const {
    return connectors_.count(name) != 0;
}

}  // namespace connectors
}  // namespace loom
