#include "loom/compute/transform.h"

#include <stdexcept>
#include <vector>

#include "loom/compute/thread_pool.h"

namespace loom {
namespace compute {

struct Transform::Impl {
    std::unordered_map<std::string, TransformFunc> funcs;
};

Transform::Transform() : impl_(std::make_unique<Impl>()) {}
Transform::~Transform() = default;
Transform::Transform(Transform&&) noexcept = default;
Transform& Transform::operator=(Transform&&) noexcept = default;

void Transform::register_function(const std::string& name, TransformFunc func) {
    impl_->funcs[name] = std::move(func);
}

bool Transform::has_function(const std::string& name) const {
    return impl_->funcs.count(name) != 0;
}

RecordBatch Transform::apply(const std::string& name, const RecordBatch& batch) const {
    auto it = impl_->funcs.find(name);
    if (it == impl_->funcs.end()) {
        throw std::runtime_error("unknown transform function: " + name);
    }
    return it->second(batch);
}

RecordBatch Transform::apply_parallel(const std::string& name, const RecordBatch& batch,
                                      ThreadPool& pool) const {
    auto it = impl_->funcs.find(name);
    if (it == impl_->funcs.end()) {
        throw std::runtime_error("unknown transform function: " + name);
    }
    const TransformFunc& func = it->second;

    int64_t rows = batch.num_rows();
    if (rows == 0) return batch;

    int nworkers = std::max(1, pool.worker_count());
    int64_t chunk = (rows + nworkers - 1) / nworkers;

    std::vector<RecordBatch> results(nworkers);
    std::vector<std::exception_ptr> errors(nworkers, nullptr);

    int submitted = 0;
    for (int w = 0; w < nworkers; ++w) {
        int64_t start = w * chunk;
        int64_t end = std::min(rows, start + chunk);
        if (start >= end) break;
        ++submitted;
        pool.submit([&, w, start, end] {
            try {
                results[w] = func(batch.slice(start, end));
                if (results[w].num_rows() != (end - start)) {
                    throw std::runtime_error(
                        "transform function '" + name + "' must preserve row count");
                }
            } catch (...) {
                errors[w] = std::current_exception();
            }
        });
    }
    pool.wait_all();

    for (int w = 0; w < submitted; ++w) {
        if (errors[w]) std::rethrow_exception(errors[w]);
    }

    std::vector<RecordBatch> parts;
    parts.reserve(submitted);
    for (int w = 0; w < submitted; ++w) parts.push_back(std::move(results[w]));
    return concat(parts);
}

}  // namespace compute
}  // namespace loom
