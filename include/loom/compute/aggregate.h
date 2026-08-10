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

struct AggregateDef {
    std::string name;
    std::string field;
    std::string func;  // avg, count, sum, min, max, percentile
    double arg = 0.0;
};

class Aggregate {
public:
    Aggregate();
    ~Aggregate();

    void set_group_by(const std::vector<std::string>& columns);
    void set_aggregates(const std::vector<AggregateDef>& aggs);

#ifdef LOOM_HAS_ARROW
    void accumulate(const std::shared_ptr<arrow::RecordBatch>& batch);
    std::shared_ptr<arrow::RecordBatch> finalize();
#endif

    void reset();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
