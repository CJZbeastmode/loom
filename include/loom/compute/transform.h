#pragma once

#include <functional>
#include <memory>
#include <string>

#ifdef LOOM_HAS_ARROW
#include <arrow/record_batch.h>
#endif

namespace loom {
namespace compute {

using TransformFunc = std::function<void(void* input_batch, void* output_batch)>;

class Transform {
public:
    Transform();
    ~Transform();

    void register_python_function(const std::string& name, const std::string& module, const std::string& func);
    void register_native_function(const std::string& name, TransformFunc func);

#ifdef LOOM_HAS_ARROW
    std::shared_ptr<arrow::RecordBatch> apply(
        const std::string& func_name,
        const std::shared_ptr<arrow::RecordBatch>& batch);
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
