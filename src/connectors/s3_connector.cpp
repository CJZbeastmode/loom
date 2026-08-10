#include "loom/connectors/s3_connector.h"

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

std::string S3Connector::read_json(const std::string& /*path*/) {
    return "{}";
}

void S3Connector::write_json_gz(const std::string& /*path*/, const std::string& /*data*/) {}

bool S3Connector::file_exists(const std::string& /*path*/) {
    return false;
}

void S3Connector::delete_file(const std::string& /*path*/) {}

}  // namespace connectors
}  // namespace loom
