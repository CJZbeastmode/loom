#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "loom/compute/column.h"

namespace loom {
namespace compute {

class ThreadPool;

// A batch transform: input RecordBatch -> output RecordBatch.
// Row-preserving functions (same row count in and out) can be run in parallel
// via apply_parallel, which chunks the rows across the thread pool.
using TransformFunc = std::function<RecordBatch(const RecordBatch&)>;

class Transform {
public:
    Transform();
    ~Transform();

    Transform(const Transform&) = delete;
    Transform& operator=(const Transform&) = delete;
    Transform(Transform&&) noexcept;
    Transform& operator=(Transform&&) noexcept;

    void register_function(const std::string& name, TransformFunc func);
    bool has_function(const std::string& name) const;

    // Apply a registered function to the whole batch (single-threaded).
    RecordBatch apply(const std::string& name, const RecordBatch& batch) const;

    // Apply a registered row-preserving function to row chunks in parallel,
    // then concatenate. Throws if the function changes the row count.
    RecordBatch apply_parallel(const std::string& name, const RecordBatch& batch,
                               ThreadPool& pool) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
