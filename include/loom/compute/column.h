#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace loom {
namespace compute {

// Minimal columnar data model.  A column stores a single typed, contiguous
// array (columnar layout, like Apache Arrow but self-contained).  One of the
// typed vectors is populated depending on `type`.
enum class ColumnType {
    Int64,
    Double,
    String,
    Bool,
};

struct Column {
    ColumnType type = ColumnType::Int64;

    std::vector<int64_t> i64;      // Int64
    std::vector<double> f64;       // Double
    std::vector<std::string> str;  // String
    std::vector<uint8_t> boolean;  // Bool (0/1)

    // Validity bitmap: 1 = present, 0 = null. Empty == all present.
    std::vector<uint8_t> valid;

    size_t size() const {
        switch (type) {
            case ColumnType::Int64:  return i64.size();
            case ColumnType::Double: return f64.size();
            case ColumnType::String: return str.size();
            case ColumnType::Bool:   return boolean.size();
        }
        return 0;
    }

    bool is_null(size_t i) const {
        return !valid.empty() && !valid[i];
    }

    // Human-readable value of row i (used for display / string comparison).
    std::string as_string(size_t i) const {
        switch (type) {
            case ColumnType::Int64:  return std::to_string(i64[i]);
            case ColumnType::Double: return std::to_string(f64[i]);
            case ColumnType::String: return str[i];
            case ColumnType::Bool:   return boolean[i] ? "true" : "false";
        }
        return "";
    }

    // ---- builders (keep tests concise) ----
    static Column make_int64(std::vector<int64_t> v) {
        Column c; c.type = ColumnType::Int64; c.i64 = std::move(v); return c;
    }
    static Column make_double(std::vector<double> v) {
        Column c; c.type = ColumnType::Double; c.f64 = std::move(v); return c;
    }
    static Column make_string(std::vector<std::string> v) {
        Column c; c.type = ColumnType::String; c.str = std::move(v); return c;
    }
    static Column make_bool(std::vector<uint8_t> v) {
        Column c; c.type = ColumnType::Bool; c.boolean = std::move(v); return c;
    }
};

// A batch of columns sharing one row count.  Equivalent to Arrow's RecordBatch.
struct RecordBatch {
    std::vector<std::string> names;
    std::vector<Column> columns;

    int64_t num_rows() const {
        return columns.empty() ? 0 : static_cast<int64_t>(columns[0].size());
    }

    int num_columns() const { return static_cast<int>(columns.size()); }

    // Index of the column with `name`, or -1 if absent.
    int column_index(const std::string& name) const {
        for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == name) return static_cast<int>(i);
        }
        return -1;
    }

    const Column& column(const std::string& name) const {
        int idx = column_index(name);
        if (idx < 0) throw std::runtime_error("no such column: " + name);
        return columns[idx];
    }

    // A row slice [start, end) of every column.
    RecordBatch slice(int64_t start, int64_t end) const {
        RecordBatch out;
        out.names = names;
        out.columns.reserve(columns.size());
        for (const auto& c : columns) {
            Column s;
            s.type = c.type;
            auto take = [&](const auto& vec) {
                using T = typename std::decay<decltype(vec)>::type::value_type;
                std::vector<T> v;
                v.reserve(end - start);
                for (int64_t i = start; i < end; ++i) v.push_back(vec[i]);
                return v;
            };
            switch (c.type) {
                case ColumnType::Int64:  s.i64 = take(c.i64); break;
                case ColumnType::Double: s.f64 = take(c.f64); break;
                case ColumnType::String: s.str = take(c.str); break;
                case ColumnType::Bool:   s.boolean = take(c.boolean); break;
            }
            if (!c.valid.empty()) {
                s.valid.assign(c.valid.begin() + start, c.valid.begin() + end);
            }
            out.columns.push_back(std::move(s));
        }
        return out;
    }
};

// Concatenate a list of batches (must share identical schemas).
inline RecordBatch concat(const std::vector<RecordBatch>& batches) {
    if (batches.empty()) return RecordBatch{};
    RecordBatch out;
    out.names = batches[0].names;
    size_t ncols = batches[0].columns.size();
    out.columns.resize(ncols);
    for (size_t j = 0; j < ncols; ++j) {
        out.columns[j].type = batches[0].columns[j].type;
    }
    for (const auto& b : batches) {
        for (size_t j = 0; j < ncols; ++j) {
            const auto& src = b.columns[j];
            auto& dst = out.columns[j];
            switch (src.type) {
                case ColumnType::Int64:  dst.i64.insert(dst.i64.end(), src.i64.begin(), src.i64.end()); break;
                case ColumnType::Double: dst.f64.insert(dst.f64.end(), src.f64.begin(), src.f64.end()); break;
                case ColumnType::String: dst.str.insert(dst.str.end(), src.str.begin(), src.str.end()); break;
                case ColumnType::Bool:   dst.boolean.insert(dst.boolean.end(), src.boolean.begin(), src.boolean.end()); break;
            }
            if (!src.valid.empty()) {
                dst.valid.insert(dst.valid.end(), src.valid.begin(), src.valid.end());
            } else if (!dst.valid.empty()) {
                dst.valid.insert(dst.valid.end(), src.size(), 1);
            }
        }
    }
    return out;
}

}  // namespace compute
}  // namespace loom
