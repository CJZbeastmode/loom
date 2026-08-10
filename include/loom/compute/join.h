#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#ifdef LOOM_HAS_ARROW
#include <arrow/record_batch.h>
#endif

namespace loom {
namespace compute {

struct JoinConfig {
    std::string key;
    std::string type = "inner";  // "inner", "left"
    std::chrono::seconds timeout{30};
};

class Join {
public:
    explicit Join(const JoinConfig& config);
    ~Join();

    void register_input(const std::string& source_id);

#ifdef LOOM_HAS_ARROW
    void on_batch(const std::string& source_id, const std::shared_ptr<arrow::RecordBatch>& batch);
    std::shared_ptr<arrow::RecordBatch> drain_ready();
    using ReadyCallback = std::function<void(std::shared_ptr<arrow::RecordBatch>)>;
    void set_ready_callback(ReadyCallback cb);
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
