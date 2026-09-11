#pragma once

#include <memory>
#include <string>

#include "loom/connectors/connector.h"

namespace loom {
namespace connectors {

struct S3Config {
    std::string endpoint;
    std::string bucket;
    std::string access_key;
    std::string secret_key;
    std::string prefix;
};

// S3-compatible object-storage connector (AWS S3 / MinIO).
//
// NOTE: the actual network write requires S3 request signing (SigV4) and an
// endpoint, neither of which is available in this environment yet.  This class
// carries the config and implements the Connector interface, but write/read
// throw until the transport is wired (a later sprint).
class S3Connector : public Connector {
public:
    explicit S3Connector(const S3Config& config);
    ~S3Connector() override;

    S3Connector(const S3Connector&) = delete;
    S3Connector& operator=(const S3Connector&) = delete;
    S3Connector(S3Connector&&) noexcept;
    S3Connector& operator=(S3Connector&&) noexcept;

    void write(const std::string& path, const std::string& data) override;
    std::string read(const std::string& path) override;
    bool exists(const std::string& path) const override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace connectors
}  // namespace loom
