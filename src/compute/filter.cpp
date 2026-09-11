#include "loom/compute/filter.h"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>

#include "loom/compute/thread_pool.h"

namespace loom {
namespace compute {

struct Filter::Impl {
    std::vector<FilterCondition> conditions;
};

namespace {

bool parse_bool(const std::string& s) {
    return s == "true" || s == "1";
}

// Evaluate `op` on a single column cell against a string literal `value`.
bool compare(const Column& col, size_t i, const std::string& op, const std::string& value) {
    // null-aware ops short-circuit on validity.
    if (op == "not_null") return !col.is_null(i);
    if (op == "is_null")  return col.is_null(i);

    switch (col.type) {
        case ColumnType::Int64: {
            int64_t v = std::stoll(value);
            int64_t x = col.i64[i];
            if (op == "eq")  return x == v;
            if (op == "neq") return x != v;
            if (op == "gt")  return x > v;
            if (op == "lt")  return x < v;
            if (op == "gte") return x >= v;
            if (op == "lte") return x <= v;
            break;
        }
        case ColumnType::Double: {
            double v = std::stod(value);
            double x = col.f64[i];
            if (op == "eq")  return x == v;
            if (op == "neq") return x != v;
            if (op == "gt")  return x > v;
            if (op == "lt")  return x < v;
            if (op == "gte") return x >= v;
            if (op == "lte") return x <= v;
            break;
        }
        case ColumnType::String: {
            const std::string& x = col.str[i];
            if (op == "eq")  return x == value;
            if (op == "neq") return x != value;
            if (op == "gt")  return x > value;
            if (op == "lt")  return x < value;
            if (op == "gte") return x >= value;
            if (op == "lte") return x <= value;
            break;
        }
        case ColumnType::Bool: {
            bool v = parse_bool(value);
            bool x = col.boolean[i] != 0;
            if (op == "eq")  return x == v;
            if (op == "neq") return x != v;
            break;
        }
    }
    throw std::runtime_error("unsupported op '" + op + "' for column type");
}

bool row_matches(const RecordBatch& batch, const std::vector<FilterCondition>& conds,
                 const std::vector<const Column*>& cols, size_t i) {
    for (size_t c = 0; c < conds.size(); ++c) {
        if (!compare(*cols[c], i, conds[c].op, conds[c].value)) return false;
    }
    (void)batch;
    return true;
}

// Materialize the rows whose mask[i] != 0 into a new batch.
RecordBatch take(const RecordBatch& batch, const std::vector<uint8_t>& mask) {
    int64_t kept = 0;
    for (uint8_t m : mask) if (m) kept++;

    RecordBatch out;
    out.names = batch.names;
    out.columns.reserve(batch.columns.size());
    for (const auto& c : batch.columns) {
        Column s;
        s.type = c.type;
        switch (c.type) {
            case ColumnType::Int64: {
                s.i64.reserve(kept);
                for (size_t i = 0; i < c.i64.size(); ++i) if (mask[i]) s.i64.push_back(c.i64[i]);
                break;
            }
            case ColumnType::Double: {
                s.f64.reserve(kept);
                for (size_t i = 0; i < c.f64.size(); ++i) if (mask[i]) s.f64.push_back(c.f64[i]);
                break;
            }
            case ColumnType::String: {
                s.str.reserve(kept);
                for (size_t i = 0; i < c.str.size(); ++i) if (mask[i]) s.str.push_back(c.str[i]);
                break;
            }
            case ColumnType::Bool: {
                s.boolean.reserve(kept);
                for (size_t i = 0; i < c.boolean.size(); ++i) if (mask[i]) s.boolean.push_back(c.boolean[i]);
                break;
            }
        }
        if (!c.valid.empty()) {
            s.valid.reserve(kept);
            for (size_t i = 0; i < c.valid.size(); ++i) if (mask[i]) s.valid.push_back(c.valid[i]);
        }
        out.columns.push_back(std::move(s));
    }
    return out;
}

}  // namespace

Filter::Filter() : impl_(std::make_unique<Impl>()) {}
Filter::~Filter() = default;
Filter::Filter(Filter&&) noexcept = default;
Filter& Filter::operator=(Filter&&) noexcept = default;

void Filter::set_conditions(const std::vector<FilterCondition>& conditions) {
    impl_->conditions = conditions;
}

RecordBatch Filter::apply(const RecordBatch& batch) const {
    // Resolve column pointers up front (validates fields once).
    std::vector<const Column*> cols;
    cols.reserve(impl_->conditions.size());
    for (const auto& c : impl_->conditions) {
        cols.push_back(&batch.column(c.field));
    }

    std::vector<uint8_t> mask(batch.num_rows(), 0);
    for (int64_t i = 0; i < batch.num_rows(); ++i) {
        mask[i] = row_matches(batch, impl_->conditions, cols, i) ? 1 : 0;
    }
    return take(batch, mask);
}

RecordBatch Filter::apply_parallel(const RecordBatch& batch, ThreadPool& pool) const {
    // Resolve columns once; copy the conditions for use in worker lambdas.
    std::vector<const Column*> cols;
    cols.reserve(impl_->conditions.size());
    for (const auto& c : impl_->conditions) {
        cols.push_back(&batch.column(c.field));
    }
    const auto& conds = impl_->conditions;

    int64_t rows = batch.num_rows();
    std::vector<uint8_t> mask(rows, 0);

    int nworkers = std::max(1, pool.worker_count());
    int64_t chunk = (rows + nworkers - 1) / nworkers;

    for (int w = 0; w < nworkers; ++w) {
        int64_t start = w * chunk;
        int64_t end = std::min(rows, start + chunk);
        if (start >= end) break;
        pool.submit([&, start, end] {
            for (int64_t i = start; i < end; ++i) {
                mask[i] = row_matches(batch, conds, cols, i) ? 1 : 0;
            }
        });
    }
    pool.wait_all();
    return take(batch, mask);
}

}  // namespace compute
}  // namespace loom
