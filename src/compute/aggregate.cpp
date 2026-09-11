#include "loom/compute/aggregate.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <unordered_map>

namespace loom {
namespace compute {

namespace {

const char kSep = '\x1f';  // group-key component separator

// Exact p-th percentile over a (to-be-sorted) value buffer, linear interpolation.
double percentile(std::vector<double> vals, double p) {
    if (vals.empty()) return 0.0;
    std::sort(vals.begin(), vals.end());
    double rank = p / 100.0 * static_cast<double>(vals.size() - 1);
    size_t lo = static_cast<size_t>(std::floor(rank));
    size_t hi = static_cast<size_t>(std::ceil(rank));
    double frac = rank - static_cast<double>(lo);
    return vals[lo] * (1.0 - frac) + vals[hi] * frac;
}

// Per-(group, aggregate) partial state.
struct AggState {
    int64_t count = 0;   // rows seen (count)
    double sum = 0.0;    // sum / avg
    int64_t n = 0;       // non-null numeric values (avg)
    double min = std::numeric_limits<double>::infinity();
    double max = -std::numeric_limits<double>::infinity();
    bool numeric_seen = false;
    std::vector<double> values;  // percentile buffer
};

}  // namespace

struct Aggregate::Impl {
    std::vector<std::string> group_by;
    std::vector<AggregateDef> aggs;

    // group key (joined) -> per-aggregate state, in first-seen order.
    std::unordered_map<std::string, std::vector<AggState>> groups;
    std::vector<std::string> order;
};

Aggregate::Aggregate() : impl_(std::make_unique<Impl>()) {}
Aggregate::~Aggregate() = default;
Aggregate::Aggregate(Aggregate&&) noexcept = default;
Aggregate& Aggregate::operator=(Aggregate&&) noexcept = default;

void Aggregate::set_group_by(const std::vector<std::string>& columns) {
    impl_->group_by = columns;
}

void Aggregate::set_aggregates(const std::vector<AggregateDef>& aggs) {
    impl_->aggs = aggs;
}

void Aggregate::reset() {
    impl_->groups.clear();
    impl_->order.clear();
}

size_t Aggregate::group_count() const {
    return impl_->groups.size();
}

void Aggregate::accumulate(const RecordBatch& batch) {
    // Resolve group-by columns and each aggregate's field column once.
    std::vector<const Column*> keys;
    keys.reserve(impl_->group_by.size());
    for (const auto& g : impl_->group_by) keys.push_back(&batch.column(g));

    std::vector<const Column*> fields;
    fields.reserve(impl_->aggs.size());
    for (const auto& a : impl_->aggs) {
        if (a.func == "count") {
            fields.push_back(nullptr);  // count ignores the field
            continue;
        }
        const Column* c = &batch.column(a.field);
        if (c->type != ColumnType::Int64 && c->type != ColumnType::Double) {
            throw std::runtime_error("aggregate '" + a.func + "' requires a numeric field (got: " +
                                     a.field + ")");
        }
        fields.push_back(c);
    }

    const int64_t rows = batch.num_rows();
    for (int64_t i = 0; i < rows; ++i) {
        std::string key;
        for (const auto* k : keys) {
            key += k->as_string(i);
            key += kSep;
        }

        auto& states = impl_->groups[key];
        if (states.empty()) {
            states.resize(impl_->aggs.size());
            impl_->order.push_back(key);
        }

        for (size_t a = 0; a < impl_->aggs.size(); ++a) {
            const AggregateDef& def = impl_->aggs[a];
            AggState& s = states[a];

            if (def.func == "count") {
                s.count++;
                continue;
            }

            const Column* col = fields[a];
            if (col->is_null(i)) continue;  // skip nulls

            double v = (col->type == ColumnType::Int64) ? static_cast<double>(col->i64[i])
                                                        : col->f64[i];
            if (def.func == "sum") {
                s.sum += v;
                s.n++;
            } else if (def.func == "avg") {
                s.sum += v;
                s.n++;
            } else if (def.func == "percentile") {
                s.values.push_back(v);
            } else if (def.func == "min") {
                s.min = std::min(s.min, v);
                s.numeric_seen = true;
            } else if (def.func == "max") {
                s.max = std::max(s.max, v);
                s.numeric_seen = true;
            } else {
                throw std::runtime_error("unsupported aggregate func: " + def.func);
            }
        }
    }
}

RecordBatch Aggregate::finalize() {
    const size_t n_agg = impl_->aggs.size();
    const size_t n_key = impl_->group_by.size();

    RecordBatch out;
    out.names.reserve(n_key + n_agg);
    for (const auto& g : impl_->group_by) out.names.push_back(g);
    for (const auto& a : impl_->aggs) out.names.push_back(a.name);

    out.columns.resize(n_key + n_agg);
    for (size_t k = 0; k < n_key; ++k) out.columns[k].type = ColumnType::String;
    for (size_t a = 0; a < n_agg; ++a) {
        out.columns[n_key + a].type =
            (impl_->aggs[a].func == "count") ? ColumnType::Int64 : ColumnType::Double;
    }

    for (const auto& key : impl_->order) {
        const auto& states = impl_->groups[key];

        std::vector<std::string> parts;
        size_t start = 0;
        for (size_t k = 0; k < n_key; ++k) {
            size_t end = key.find(kSep, start);
            parts.push_back(key.substr(start, end - start));
            start = end + 1;
        }
        for (size_t k = 0; k < n_key; ++k) {
            out.columns[k].str.push_back(parts[k]);
        }

        for (size_t a = 0; a < n_agg; ++a) {
            const AggregateDef& def = impl_->aggs[a];
            const AggState& s = states[a];
            Column& dst = out.columns[n_key + a];

            if (def.func == "count") {
                dst.i64.push_back(s.count);
            } else if (def.func == "sum") {
                dst.f64.push_back(s.sum);
            } else if (def.func == "avg") {
                dst.f64.push_back(s.n > 0 ? s.sum / static_cast<double>(s.n) : 0.0);
            } else if (def.func == "percentile") {
                dst.f64.push_back(percentile(s.values, def.arg));
            } else if (def.func == "min") {
                dst.f64.push_back(s.numeric_seen ? s.min : 0.0);
            } else if (def.func == "max") {
                dst.f64.push_back(s.numeric_seen ? s.max : 0.0);
            }
        }
    }

    return out;
}

}  // namespace compute
}  // namespace loom
