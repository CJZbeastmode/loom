#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>

#ifdef LOOM_HAS_ARROW
#include <arrow/record_batch.h>
#endif

namespace loom {
namespace connectors {

struct S3Config {
    std::string endpoint;
    std::string bucket;
    std::string access_key;
    std::string secret_key;
    std::string prefix;
};

class S3Connector {
public:
    explicit S3Connector(const S3Config& config);
    ~S3Connector();

    std::string read_json(const std::string& path);

#ifdef LOOM_HAS_ARROW
    void write_parquet(const std::string& path,
                       const std::shared_ptr<arrow::RecordBatch>& batch);
#endif

    void write_json_gz(const std::string& path, const std::string& data);
    bool file_exists(const std::string& path);
    void delete_file(const std::string& path);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace connectors
}  // namespace loom
