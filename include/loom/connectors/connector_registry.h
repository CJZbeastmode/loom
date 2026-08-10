#pragma once

#include <memory>
#include <string>

namespace loom {
namespace connectors {

class S3Connector;

class ConnectorRegistry {
public:
    ConnectorRegistry();
    ~ConnectorRegistry();

    S3Connector* get_s3(const std::string& name, const struct S3Config& config);
    bool has(const std::string& name) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace connectors
}  // namespace loom
