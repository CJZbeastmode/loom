#include "loom/dag/parser.h"

#include <json/json.h>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <regex>
#include <cstdlib>

namespace loom {

// ── Env-var substitution ────────────────────────────────────────────────────
// Replaces ${VAR} with the environment value and ${VAR:-default} with the
// value or fallback.  Applied to every string read from JSON.

static std::string substitute_env_vars(const std::string& input) {
    static std::regex env_re(R"(\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\})"); // find ${VAR_NAME} or ${VAR_NAME:-default_value}
    std::string result;
    auto it = input.cbegin();
    auto end = input.cend();
    std::smatch m;
    while (std::regex_search(it, end, m, env_re)) {
        result += m.prefix().str();
        const char* val = std::getenv(m[1].str().c_str());
        if (val && val[0] != '\0') {
            result += val;
        } else if (m[2].matched) {
            result += m[2].str();
        }
        it = m.suffix().first;
    }
    result.append(it, end);
    return result;
}

// ── JSON value extractors ───────────────────────────────────────────────────
// Read a key from a Json::Value, return the C++ equivalent or a default.
// Strings automatically go through substitute_env_vars.

static std::string get_str(const Json::Value& obj, const std::string& key,
                            const std::string& def = "") {
    if (!obj.isMember(key)) return substitute_env_vars(def);
    const auto& v = obj[key];
    if (v.isString()) return substitute_env_vars(v.asString());
    if (v.isInt() || v.isInt64() || v.isUInt64()) return std::to_string(v.asInt64());
    if (v.isDouble()) return std::to_string(v.asDouble());
    if (v.isBool()) return v.asBool() ? "true" : "false";
    return def;
}

static int get_int(const Json::Value& obj, const std::string& key, int def = 0) {
    if (!obj.isMember(key)) return def;
    const auto& v = obj[key];
    if (v.isInt()) return v.asInt();
    if (v.isDouble()) return static_cast<int>(v.asDouble());
    if (v.isString()) return std::stoi(v.asString());
    return def;
}

static bool get_bool(const Json::Value& obj, const std::string& key, bool def = false) {
    if (!obj.isMember(key)) return def;
    const auto& v = obj[key];
    if (v.isBool()) return v.asBool();
    if (v.isString()) {
        auto s = v.asString();
        return s == "true" || s == "1";
    }
    if (v.isInt()) return v.asInt() != 0;
    return def;
}

static double get_double(const Json::Value& obj, const std::string& key, double def = 0.0) {
    if (!obj.isMember(key)) return def;
    const auto& v = obj[key];
    if (v.isDouble()) return v.asDouble();
    if (v.isInt()) return static_cast<double>(v.asInt());
    if (v.isString()) return std::stod(v.asString());
    return def;
}

static int64_t get_int64(const Json::Value& obj, const std::string& key, int64_t def = 0) {
    if (!obj.isMember(key)) return def;
    const auto& v = obj[key];
    if (v.isInt64()) return v.asInt64();
    if (v.isInt()) return static_cast<int64_t>(v.asInt());
    if (v.isDouble()) return static_cast<int64_t>(v.asDouble());
    if (v.isString()) return std::stoll(v.asString());
    return def;
}

// ── Enum parsers ────────────────────────────────────────────────────────────
// Map lowercase JSON strings to C++ enum values.

dag::StepType parse_step_type(const std::string& s) {
    if (s == "generate")  return dag::StepType::Generate;
    if (s == "transform") return dag::StepType::Transform;
    if (s == "filter")    return dag::StepType::Filter;
    if (s == "aggregate") return dag::StepType::Aggregate;
    if (s == "join")      return dag::StepType::Join;
    if (s == "unnest")    return dag::StepType::Unnest;
    if (s == "sink")      return dag::StepType::Sink;
    if (s == "stub")      return dag::StepType::Stub;
    throw std::runtime_error("Unknown step type: '" + s + "'");
}

dag::ConnectionType parse_connection_type(const std::string& s) {
    if (s == "http" || s == "Http") return dag::ConnectionType::Http;
    if (s == "s3" || s == "S3")     return dag::ConnectionType::S3;
    if (s == "stub")                return dag::ConnectionType::Http;
    throw std::runtime_error("Unknown connection type: '" + s + "'");
}

dag::FlushTrigger parse_flush_trigger(const std::string& s) {
    if (s == "batch_complete") return dag::FlushTrigger::BatchComplete;
    if (s == "buffer_size")    return dag::FlushTrigger::BufferSize;
    if (s == "interval")       return dag::FlushTrigger::Interval;
    throw std::runtime_error("Unknown flush trigger: '" + s + "'");
}

dag::JoinType parse_join_type(const std::string& s) {
    if (s == "inner") return dag::JoinType::Inner;
    if (s == "left")  return dag::JoinType::Left;
    throw std::runtime_error("Unknown join type: '" + s + "'");
}

dag::OnTimeout parse_on_timeout(const std::string& s) {
    if (s == "dead_letter") return dag::OnTimeout::DeadLetter;
    if (s == "drop")        return dag::OnTimeout::Drop;
    if (s == "emit_null")   return dag::OnTimeout::EmitNull;
    throw std::runtime_error("Unknown on_timeout: '" + s + "'");
}

dag::ParameterSourceKind parse_parameter_source(const std::string& s) {
    if (s == "literal" || s == "Literal") return dag::ParameterSourceKind::Literal;
    if (s == "list"    || s == "List")    return dag::ParameterSourceKind::List;
    if (s == "range"   || s == "Range")   return dag::ParameterSourceKind::Range;
    if (s == "column"  || s == "Column")  return dag::ParameterSourceKind::Column;
    throw std::runtime_error("Unknown parameter source: '" + s + "'");
}

// ── Struct parsers ──────────────────────────────────────────────────────────
// Each function maps one JSON object → one C++ domain struct.

static dag::ErrorHandling parse_error_handling(const Json::Value& obj) {
    dag::ErrorHandling eh;
    if (obj.isNull()) return eh;
    if (obj.isObject()) {
        const Json::Value* source = &obj;
        if (obj.isMember("retry") && obj["retry"].isObject()) {
            source = &obj["retry"];  // support nested {"retry": {...}} format
        }
        eh.max_attempts = get_int(*source, "max_attempts", 3);
        eh.backoff = get_str(*source, "backoff", "exponential");
        eh.base_ms = get_int(*source, "base_ms", 1000);
    }
    return eh;
}

static dag::RateLimit parse_rate_limit(const Json::Value& obj) {
    dag::RateLimit rl;
    rl.max = get_int(obj, "max", 10);
    rl.window_sec = get_double(obj, "window_sec", 1.0);
    return rl;
}

static dag::Backpressure parse_backpressure(const Json::Value& obj) {
    dag::Backpressure bp;
    bp.max_buffered_responses = get_int(obj, "max_buffered_responses", 1000);
    bp.resume_at = get_int(obj, "resume_at", 500);
    return bp;
}

static dag::Auth parse_auth(const Json::Value& obj) {
    dag::Auth auth;
    auth.type = get_str(obj, "type");
    auth.token_from = get_str(obj, "token_from");
    auth.username = get_str(obj, "username");
    auth.password = get_str(obj, "password");
    return auth;
}

static dag::Parameter parse_parameter(const Json::Value& obj) {
    dag::Parameter p;
    p.name = get_str(obj, "name");
    std::string src = get_str(obj, "source", "literal");
    p.source = parse_parameter_source(src);
    p.value = get_str(obj, "value");
    if (obj.isMember("values") && obj["values"].isArray()) {
        for (const auto& v : obj["values"]) {
            p.values.push_back(v.isString() ? substitute_env_vars(v.asString()) : v.toStyledString());
        }
    }
    p.range_start = get_int64(obj, "range_start", 0);
    p.range_end = get_int64(obj, "range_end", 0);
    p.range_step = get_int64(obj, "range_step", 1);
    if (obj.isMember("from_step")) p.from_step = get_str(obj, "from_step");
    if (obj.isMember("field"))     p.field = get_str(obj, "field");
    return p;
}

static dag::StepInput parse_step_input(const Json::Value& obj) {
    dag::StepInput si;
    if (obj.isMember("from_step"))      si.from_step = get_str(obj, "from_step");
    if (obj.isMember("from_connection")) si.from_connection = get_str(obj, "from_connection");
    if (obj.isMember("path"))           si.path = get_str(obj, "path");
    if (obj.isMember("field"))          si.field = get_str(obj, "field");
    if (obj.isMember("inputs") && obj["inputs"].isArray()) {
        for (const auto& v : obj["inputs"]) {
            si.inputs.push_back(v.asString());
        }
    }
    return si;
}

static dag::RequestTemplate parse_request_template(const Json::Value& obj) {
    dag::RequestTemplate tmpl;
    tmpl.method = get_str(obj, "method", "GET");
    tmpl.path_template = get_str(obj, "path_template");
    if (obj.isMember("headers") && obj["headers"].isObject()) {
        const auto& h = obj["headers"];
        tmpl.headers.Accept = get_str(h, "Accept", "application/json");
        if (h.isMember("X-Request-Tag")) tmpl.headers.XRequestTag = get_str(h, "X-Request-Tag");
        if (h.isMember("XRequestTag"))   tmpl.headers.XRequestTag = get_str(h, "XRequestTag");
    }
    return tmpl;
}

static dag::FlushPolicy parse_flush_policy(const Json::Value& obj) {
    dag::FlushPolicy fp;
    std::string trig = get_str(obj, "trigger", "batch_complete");
    fp.trigger = parse_flush_trigger(trig);
    fp.buffer_size = get_int(obj, "buffer_size", 1000);
    fp.interval_sec = get_int(obj, "interval_sec", 60);
    fp.archive_connection = get_str(obj, "archive_connection");
    fp.archive_format = get_str(obj, "archive_format", "json.gz");
    fp.retention_days = get_int(obj, "retention_days", 14);
    fp.keep_failed = get_bool(obj, "keep_failed", true);
    return fp;
}

static dag::RequestQueueConfig parse_request_queue(const Json::Value& obj) {
    dag::RequestQueueConfig rqc;
    rqc.durable = get_bool(obj, "durable", true);
    rqc.storage = get_str(obj, "storage", "sqlite://var/loom/request_queue.db");
    rqc.max_pending = get_int(obj, "max_pending", 10000);
    if (obj.isMember("flush_policy") && obj["flush_policy"].isObject()) {
        rqc.flush_policy = parse_flush_policy(obj["flush_policy"]);
    }
    return rqc;
}

static dag::TransformFunction parse_transform_function(const Json::Value& obj) {
    dag::TransformFunction tf;
    tf.language = get_str(obj, "language", "python");
    tf.entrypoint = get_str(obj, "entrypoint");
    return tf;
}

static dag::FilterCondition parse_filter_condition(const Json::Value& obj) {
    dag::FilterCondition fc;
    fc.field = get_str(obj, "field");
    fc.op = get_str(obj, "op");
    fc.value = get_str(obj, "value");
    return fc;
}

static dag::AggregateDef parse_aggregate_def(const Json::Value& obj) {
    dag::AggregateDef ad;
    ad.name = get_str(obj, "name");
    ad.field = get_str(obj, "field");
    ad.func = get_str(obj, "func");
    ad.arg = get_double(obj, "arg", 0.0);
    return ad;
}

static dag::JoinConfig parse_join_config(const Json::Value& obj) {
    dag::JoinConfig jc;
    jc.key = get_str(obj, "key");
    std::string jt = get_str(obj, "join", "inner");
    jc.join = parse_join_type(jt);
    jc.timeout = get_str(obj, "timeout", "30s");
    std::string ot = get_str(obj, "on_timeout", "dead_letter");
    jc.on_timeout = parse_on_timeout(ot);
    return jc;
}

static dag::SinkOutput parse_sink_output(const Json::Value& obj) {
    dag::SinkOutput so;
    so.path_template = get_str(obj, "path_template");
    so.format = get_str(obj, "format", "parquet");
    so.partition = get_str(obj, "partition");
    return so;
}

static dag::UnnestConfig parse_unnest_config(const Json::Value& obj) {
    dag::UnnestConfig uc;
    uc.field = get_str(obj, "field");
    uc.array_column = get_str(obj, "array_column");
    uc.drop_null = get_bool(obj, "drop_null", true);
    return uc;
}

static dag::Connection parse_connection(const std::string& name, const Json::Value& obj) {
    dag::Connection conn;
    conn.name = name;
    std::string ctype = get_str(obj, "type", "http");
    conn.type = parse_connection_type(ctype);
    conn.base_url = get_str(obj, "base_url");
    if (obj.isMember("rate_limit") && obj["rate_limit"].isObject())
        conn.rate_limit = parse_rate_limit(obj["rate_limit"]);
    conn.max_inflight = get_int(obj, "max_inflight", 10);
    if (obj.isMember("backpressure") && obj["backpressure"].isObject())
        conn.backpressure = parse_backpressure(obj["backpressure"]);
    if (obj.isMember("auth") && obj["auth"].isObject())
        conn.auth = parse_auth(obj["auth"]);
    conn.timeout_ms = get_int(obj, "timeout_ms", 30000);
    if (obj.isMember("fail_on_status") && obj["fail_on_status"].isArray()) {
        for (const auto& v : obj["fail_on_status"]) {
            conn.fail_on_status.push_back(v.asInt());
        }
    }
    conn.endpoint = get_str(obj, "endpoint");
    conn.bucket = get_str(obj, "bucket");
    conn.access_key = get_str(obj, "access_key");
    conn.secret_key = get_str(obj, "secret_key");
    conn.prefix = get_str(obj, "prefix");
    conn.reason = get_str(obj, "reason");
    return conn;
}

// Supports both array format [{"name":"x",...}] and object format {"x":{...}}
static std::vector<dag::Connection> parse_connections(const Json::Value& val) {
    std::vector<dag::Connection> conns;
    if (val.isArray()) {
        for (const auto& c : val) {
            std::string name = get_str(c, "name");
            conns.push_back(parse_connection(name, c));
        }
    } else if (val.isObject()) {
        for (auto it = val.begin(); it != val.end(); ++it) {
            conns.push_back(parse_connection(it.name(), *it));
        }
    }
    return conns;
}

// Reads the step "type" string then dispatches step-type-specific sub-parsers.
// Accepts both "template" and "template_", "condition" and "conditions".
static dag::Step parse_step(const Json::Value& obj) {
    dag::Step step;
    step.id = get_str(obj, "id");
    std::string t = get_str(obj, "type", "stub");
    step.type = parse_step_type(t);
    step.description = get_str(obj, "description");
    step.connection = get_str(obj, "connection");
    if (obj.isMember("input") && obj["input"].isObject())
        step.input = parse_step_input(obj["input"]);
    if (obj.isMember("parameters") && obj["parameters"].isArray()) {
        for (const auto& p : obj["parameters"]) {
            step.parameters.push_back(parse_parameter(p));
        }
    }
    step.parallel = get_int(obj, "parallel", 1);

    if (obj.isMember("template") && obj["template"].isObject())
        step.template_ = parse_request_template(obj["template"]);
    else if (obj.isMember("template_") && obj["template_"].isObject())
        step.template_ = parse_request_template(obj["template_"]);

    if (obj.isMember("request_queue") && obj["request_queue"].isObject())
        step.request_queue = parse_request_queue(obj["request_queue"]);

    if (obj.isMember("function") && obj["function"].isObject())
        step.function = parse_transform_function(obj["function"]);

    if (obj.isMember("condition") && obj["condition"].isArray()) {
        for (const auto& c : obj["condition"]) {
            step.conditions.push_back(parse_filter_condition(c));
        }
    } else if (obj.isMember("conditions") && obj["conditions"].isArray()) {
        for (const auto& c : obj["conditions"]) {
            step.conditions.push_back(parse_filter_condition(c));
        }
    }

    if (obj.isMember("group_by") && obj["group_by"].isArray()) {
        for (const auto& g : obj["group_by"]) {
            step.group_by.push_back(g.asString());
        }
    }

    if (obj.isMember("aggregates") && obj["aggregates"].isArray()) {
        for (const auto& a : obj["aggregates"]) {
            step.aggregates.push_back(parse_aggregate_def(a));
        }
    }

    if (obj.isMember("join_config") && obj["join_config"].isObject())
        step.join_config = parse_join_config(obj["join_config"]);

    if (obj.isMember("unnest") && obj["unnest"].isObject())
        step.unnest = parse_unnest_config(obj["unnest"]);

    if (obj.isMember("output") && obj["output"].isObject())
        step.output = parse_sink_output(obj["output"]);

    step.reason = get_str(obj, "reason");
    return step;
}

// ── Public API ──────────────────────────────────────────────────────────────

dag::DAG DAGParser::parse_string(const std::string& json) {
    // Parse raw text into a JSON tree
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::istringstream stream(json);

    if (!Json::parseFromStream(builder, stream, &root, &errors)) {
        throw std::runtime_error("JSON parse error: " + errors);
    }

    if (!root.isObject()) {
        throw std::runtime_error("DAG must be a JSON object, got: " +
                                 std::string(root.type() == Json::nullValue ? "null" : "non-object"));
    }

    // Extract top-level scalars (env-var substitution happens inside get_str)
    dag::DAG dag;
    dag.name = get_str(root, "name");
    dag.version = get_str(root, "version");
    dag.start_at = get_str(root, "start_at");

    if (dag.name.empty()) {
        throw std::runtime_error("DAG missing required field: 'name'");
    }
    if (dag.start_at.empty()) {
        throw std::runtime_error("DAG missing required field: 'start_at'");
    }

    // Optional: global error handling (supports flat and nested-retry formats)
    if (root.isMember("error_handling")) {
        dag.error_handling = parse_error_handling(root["error_handling"]);
    }

    // Optional: external connections (array or object format)
    if (root.isMember("connections")) {
        dag.connections = parse_connections(root["connections"]);
    }

    // Steps: iterate the array, dispatch each by its "type" field
    if (root.isMember("steps") && root["steps"].isArray()) {
        for (const auto& s : root["steps"]) {
            dag.steps.push_back(parse_step(s));
        }
    }

    if (dag.steps.empty()) {
        throw std::runtime_error("DAG missing required field: 'steps' must be a non-empty array");
    }

    return dag;
}

dag::DAG DAGParser::parse_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + path);
    }
    std::ostringstream buf;
    buf << file.rdbuf();
    if (file.bad()) {
        throw std::runtime_error("Error reading file: " + path);
    }
    return parse_string(buf.str());
}

}  // namespace loom
