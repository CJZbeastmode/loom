#include "loom/connectors/s3_connector.h"

#include <stdexcept>

namespace loom {
namespace connectors {

struct S3Connector::Impl {
    S3Config config;
};

S3Connector::S3Connector(const S3Config& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

S3Connector::~S3Connector() = default;
S3Connector::S3Connector(S3Connector&&) noexcept = default;
S3Connector& S3Connector::operator=(S3Connector&&) noexcept = default;

void S3Connector::write(const std::string& /*path*/, const std::string& /*data*/) {
    throw std::runtime_error("S3Connector::write not implemented (requires S3/MinIO transport)");
}

std::string S3Connector::read(const std::string& /*path*/) {
    throw std::runtime_error("S3Connector::read not implemented (requires S3/MinIO transport)");
}

bool S3Connector::exists(const std::string& /*path*/) const {
    return false;
}

}  // namespace connectors
}  // namespace loom
