#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "loom/connectors/connector.h"

namespace loom {
namespace connectors {

// Holds named connectors so the engine can resolve a connection name
// (e.g. "s3_landing") to a concrete Connector instance.
class ConnectorRegistry {
public:
    ConnectorRegistry() = default;
    ~ConnectorRegistry() = default;

    ConnectorRegistry(const ConnectorRegistry&) = delete;
    ConnectorRegistry& operator=(const ConnectorRegistry&) = delete;

    void add(const std::string& name, std::unique_ptr<Connector> connector);
    Connector* get(const std::string& name) const;
    bool has(const std::string& name) const;

private:
    std::unordered_map<std::string, std::unique_ptr<Connector>> connectors_;
};

}  // namespace connectors
}  // namespace loom
