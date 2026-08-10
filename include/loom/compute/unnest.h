#pragma once

#include <functional>
#include <memory>
#include <string>

#ifdef LOOM_HAS_ARROW
#include <arrow/record_batch.h>
#endif

namespace loom {
namespace compute {

struct UnnestConfig {
    std::string field;          // list/struct column to explode
    std::string array_column;   // output column name for exploded elements
    bool drop_null = true;
};

class Unnest {
public:
    explicit Unnest(const UnnestConfig& config);
    ~Unnest();

#ifdef LOOM_HAS_ARROW
    std::shared_ptr<arrow::RecordBatch> apply(
        const std::shared_ptr<arrow::RecordBatch>& batch);
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
