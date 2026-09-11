#include "loom/compute/sink.h"

#include <chrono>
#include <ctime>
#include <sstream>
#include <stdexcept>

#include "loom/connectors/connector.h"

namespace loom {
namespace compute {

namespace {

std::string now_date(const std::string& partition) {
    std::time_t t = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    if (partition == "hourly") {
        std::strftime(buf, sizeof(buf), "%Y-%m-%d-%H", &tm);
    } else {
        std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm);
    }
    return buf;
}

std::string csv_escape(const std::string& v) {
    bool needs_quote = v.find(',') != std::string::npos ||
                       v.find('"') != std::string::npos ||
                       v.find('\n') != std::string::npos;
    if (!needs_quote) return v;
    std::string out = "\"";
    for (char c : v) {
        if (c == '"') out += "\"\"";
        else out += c;
    }
    out += "\"";
    return out;
}

std::string json_escape(const std::string& v) {
    std::string out = "\"";
    for (char c : v) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\t': out += "\\t"; break;
            case '\r': out += "\\r"; break;
            default:   out += c;
        }
    }
    out += "\"";
    return out;
}

}  // namespace

struct Sink::Impl {
    connectors::Connector* connector;
    Config config;
};

Sink::Sink(connectors::Connector* connector, Config config)
    : impl_(std::make_unique<Impl>()) {
    impl_->connector = connector;
    impl_->config = config;
}

Sink::~Sink() = default;

std::string Sink::materialize_path(const std::string& path_template,
                                   const std::string& partition) {
    std::string out = path_template;
    auto replace = [&](const std::string& token, const std::string& value) {
        size_t pos = 0;
        while ((pos = out.find(token, pos)) != std::string::npos) {
            out.replace(pos, token.size(), value);
            pos += value.size();
        }
    };
    replace("{date}", now_date(partition));
    replace("{ts}", std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count()));
    return out;
}

std::string Sink::to_csv(const RecordBatch& batch) {
    std::ostringstream oss;
    int64_t rows = batch.num_rows();

    for (int c = 0; c < batch.num_columns(); ++c) {
        if (c) oss << ',';
        oss << batch.names[c];
    }
    oss << '\n';

    for (int64_t r = 0; r < rows; ++r) {
        for (int c = 0; c < batch.num_columns(); ++c) {
            if (c) oss << ',';
            if (batch.columns[c].is_null(r)) continue;  // empty = null
            oss << csv_escape(batch.columns[c].as_string(r));
        }
        oss << '\n';
    }
    return oss.str();
}

std::string Sink::to_json(const RecordBatch& batch) {
    std::ostringstream oss;
    int64_t rows = batch.num_rows();
    oss << '[';
    for (int64_t r = 0; r < rows; ++r) {
        if (r) oss << ',';
        oss << '{';
        for (int c = 0; c < batch.num_columns(); ++c) {
            if (c) oss << ',';
            oss << '"' << batch.names[c] << "\":";
            const Column& col = batch.columns[c];
            if (col.is_null(r)) {
                oss << "null";
            } else if (col.type == ColumnType::Int64) {
                oss << col.i64[r];
            } else if (col.type == ColumnType::Double) {
                oss << col.f64[r];
            } else if (col.type == ColumnType::Bool) {
                oss << (col.boolean[r] ? "true" : "false");
            } else {
                oss << json_escape(col.str[r]);
            }
        }
        oss << '}';
    }
    oss << ']';
    return oss.str();
}

std::string Sink::write(const RecordBatch& batch) {
    std::string data;
    if (impl_->config.format == "csv") {
        data = to_csv(batch);
    } else if (impl_->config.format == "json") {
        data = to_json(batch);
    } else {
        throw std::runtime_error("unsupported sink format: " + impl_->config.format +
                                 " (supported: csv, json)");
    }

    std::string path = materialize_path(impl_->config.path_template, impl_->config.partition);
    impl_->connector->write(path, data);
    return path;
}

}  // namespace compute
}  // namespace loom
