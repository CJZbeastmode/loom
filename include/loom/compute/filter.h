#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#ifdef LOOM_HAS_ARROW
#include <arrow/record_batch.h>
#endif

namespace loom {
namespace compute {

struct FilterCondition {
    std::string field;
    std::string op;  // eq, neq, gt, lt, gte, lte, not_null, is_null
    std::string value;
};

class Filter {
public:
    Filter();
    ~Filter();

    void set_conditions(const std::vector<FilterCondition>& conditions);

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
