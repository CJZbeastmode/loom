#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <memory>

namespace loom {
namespace dag {

enum class StepType {
    Generate,
    Transform,
    Filter,
    Aggregate,
    Join,
    Unnest,
    Sink,
    Stub,
};

enum class ConnectionType {
    Http,
    S3,
};

enum class FlushTrigger {
    BatchComplete,
    BufferSize,
    Interval,
};

enum class JoinType {
    Inner,
    Left,
};

enum class OnTimeout {
    DeadLetter,
    Drop,
    EmitNull,
};

struct RateLimit {
    int max = 10;
    double window_sec = 1.0;
};

struct Backpressure {
    int max_buffered_responses = 1000;
    int resume_at = 500;
};

struct Auth {
    std::string type;  // "bearer", "basic"
    std::string token_from;  // "env:VAR_NAME"
    std::string username;
    std::string password;
};

enum class ParameterSourceKind {
    Literal,   // single fixed value
    List,      // explicit list of values
    Range,     // integer range: start..end by step
    Column,    // values taken from an upstream RecordBatch column
};

struct Parameter {
    std::string name;
    ParameterSourceKind source = ParameterSourceKind::Literal;
    // Literal
    std::string value;
    // List
    std::vector<std::string> values;
    // Range
    int64_t range_start = 0;
    int64_t range_end = 0;
    int64_t range_step = 1;
    // Column
    std::optional<std::string> from_step;
    std::optional<std::string> field;
};

struct Connection {
    std::string name;
    ConnectionType type = ConnectionType::Http;
    std::string base_url;
    RateLimit rate_limit;
    int max_inflight = 10;
    Backpressure backpressure;
    Auth auth;
    int timeout_ms = 30000;
    std::vector<int> fail_on_status;
    std::string endpoint;
    std::string bucket;
    std::string access_key;
    std::string secret_key;
    std::string prefix;
    std::string reason;  // for stub connections
};

struct StepInput {
    std::optional<std::string> from_step;
    std::optional<std::string> from_connection;
    std::optional<std::string> path;
    std::vector<std::string> inputs;  // for joins: multiple source step IDs
    std::optional<std::string> field; // dot-path projection: "result.items" or "output.list"
};

struct RequestTemplate {
    std::string method = "GET";
    std::string path_template;
    struct {
        std::string Accept = "application/json";
        std::string XRequestTag;
    } headers;
};

struct FlushPolicy {
    FlushTrigger trigger = FlushTrigger::BatchComplete;
    int buffer_size = 1000;
    int interval_sec = 60;
    std::string archive_connection;
    std::string archive_format = "json.gz";
    int retention_days = 14;
    bool keep_failed = true;
};

struct RequestQueueConfig {
    bool durable = true;
    std::string storage = "sqlite://var/loom/request_queue.db";
    int max_pending = 10000;
    FlushPolicy flush_policy;
};

struct TransformFunction {
    std::string language = "python";
    std::string entrypoint;  // "pipelines/transforms/jira_parser.py:extract"
};

struct FilterCondition {
    std::string field;
    std::string op;  // eq, neq, gt, lt, gte, lte, not_null, is_null
    std::string value;
};

struct AggregateDef {
    std::string name;
    std::string field;
    std::string func;  // avg, count, sum, min, max, percentile
    double arg = 0.0;  // e.g. 95 for p95
};

struct JoinConfig {
    std::string key;
    JoinType join = JoinType::Inner;
    std::string timeout = "30s";
    OnTimeout on_timeout = OnTimeout::DeadLetter;
};

struct SinkOutput {
    std::string path_template;  // "outputs/cycle_time/{date}.parquet"
    std::string format = "parquet";  // parquet, json.gz, csv
    std::string partition;  // "daily", "hourly"
};

struct UnnestConfig {
    std::string field;          // list/struct column to explode into rows
    std::string array_column;   // name of the output column holding the exploded element
    bool drop_null = true;      // drop rows where the list is null or empty
};

struct Step {
    std::string id;
    StepType type = StepType::Stub;
    std::string description;
    std::string connection;  // for generate and sink steps
    StepInput input;
    std::vector<Parameter> parameters;  // for Generate: fan-out iteration vars
    int parallel = 1;
    RequestTemplate template_;       // generate
    RequestQueueConfig request_queue;  // generate
    TransformFunction function;      // transform
    std::vector<FilterCondition> conditions;  // filter
    std::vector<std::string> group_by;  // aggregate
    std::vector<AggregateDef> aggregates; // aggregate
    JoinConfig join_config;             // join
    UnnestConfig unnest;                // unnest
    SinkOutput output;               // sink
    std::string reason;               // stub
};

struct ErrorHandling {
    int max_attempts = 3;
    std::string backoff = "exponential";
    int base_ms = 1000;
};

struct DAG {
    std::string name;
    std::string version;
    std::string start_at;
    ErrorHandling error_handling;
    std::vector<Connection> connections;
    std::vector<Step> steps;
};

}  // namespace dag

class DAGParser {
public:
    static dag::DAG parse_file(const std::string& path);
    static dag::DAG parse_string(const std::string& json);

private:
    DAGParser() = delete;
};

}  // namespace loom
