#pragma once

#include <memory>
#include <string>
#include <vector>

#include "loom/compute/column.h"

namespace loom {
namespace compute {

class ThreadPool;

struct FilterCondition {
    std::string field;
    std::string op;  // eq, neq, gt, lt, gte, lte, not_null, is_null
    std::string value;
};

// Row filter over a columnar batch.  Conditions are combined with AND.
// Evaluation is embarrassingly parallel: rows are split across the thread pool,
// each worker fills a slice of a shared selection mask, then the kept rows are
// materialized into a new batch (the columnar equivalent of Arrow's Take).
class Filter {
public:
    Filter();
    ~Filter();

    Filter(const Filter&) = delete;
    Filter& operator=(const Filter&) = delete;
    Filter(Filter&&) noexcept;
    Filter& operator=(Filter&&) noexcept;

    void set_conditions(const std::vector<FilterCondition>& conditions);

    // Single-threaded reference implementation.
    RecordBatch apply(const RecordBatch& batch) const;

    // Parallel: split rows across `pool`, then merge.
    RecordBatch apply_parallel(const RecordBatch& batch, ThreadPool& pool) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
