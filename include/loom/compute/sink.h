#pragma once

#include <memory>
#include <string>

#include "loom/compute/column.h"

namespace loom {
namespace connectors {
class Connector;
}

namespace compute {

// Writes a RecordBatch to a connector, materializing a path template and
// serializing to the requested format.  This is the "sink" step type's engine.
//
//   path_template: "outputs/cycle_time/{date}.csv"  ({date} / {ts} substituted)
//   format:        "csv" or "json"  (parquet/json.gz → later sprint)
//   partition:     "daily" (YYYY-MM-DD) or "hourly" (YYYY-MM-DD-HH)
class Sink {
public:
    struct Config {
        std::string path_template;
        std::string format = "csv";
        std::string partition = "daily";
    };

    Sink(connectors::Connector* connector, Config config);
    ~Sink();

    Sink(const Sink&) = delete;
    Sink& operator=(const Sink&) = delete;

    // Serialize and write the batch; returns the materialized path.
    std::string write(const RecordBatch& batch);

    // Serialization helpers (exposed for reuse/tests).
    static std::string to_csv(const RecordBatch& batch);
    static std::string to_json(const RecordBatch& batch);

    static std::string materialize_path(const std::string& path_template,
                                        const std::string& partition);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace compute
}  // namespace loom
