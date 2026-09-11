#pragma once

#include <memory>
#include <string>
#include <vector>

#include "loom/compute/column.h"

namespace loom {
namespace compute {

struct AggregateDef {
    std::string name;   // output column name
    std::string field;  // input column to aggregate
    std::string func;   // count, sum, avg, min, max, percentile
    double arg = 0.0;   // e.g. 95 for p95
};

// Streaming (incremental) group-by aggregation over the columnar model.
//
//   accumulate(batch)  — update per-group partial state for each micro-batch
//   finalize()         — emit one row per group (called when upstream is done)
//
// Groups are keyed by the concatenation of the group-by columns' values.
// count/sum/avg/min/max are O(1) per row; percentile buffers values and is
// computed exactly at finalize (a t-digest approximation can replace it later).
class Aggregate {
public:
    Aggregate();
    ~Aggregate();

    Aggregate(const Aggregate&) = delete;
    Aggregate& operator=(const Aggregate&) = delete;
    Aggregate(Aggregate&&) noexcept;
    Aggregate& operator=(Aggregate&&) noexcept;

    void set_group_by(const std::vector<std::string>& columns);
    void set_aggregates(const std::vector<AggregateDef>& aggs);

    void accumulate(const RecordBatch& batch);
    RecordBatch finalize();
    void reset();

    // Number of groups seen so far.
    size_t group_count() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
