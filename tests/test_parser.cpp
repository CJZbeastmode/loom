#include "test_utils.h"
#include "loom/dag/parser.h"
#include <fstream>
#include <cstdlib>

using namespace loom;

TEST(ParserErrors, EmptyStringThrows) {
    EXPECT_THROW(DAGParser::parse_string("{}"), std::runtime_error);
}

TEST(ParserErrors, InvalidJSONThrows) {
    EXPECT_THROW(DAGParser::parse_string("not json{{"), std::runtime_error);
}

TEST(ParserErrors, NonObjectThrows) {
    EXPECT_THROW(DAGParser::parse_string("[]"), std::runtime_error);
    EXPECT_THROW(DAGParser::parse_string("123"), std::runtime_error);
    EXPECT_THROW(DAGParser::parse_string("\"string\""), std::runtime_error);
}

TEST(ParserErrors, MissingNameThrows) {
    std::string json = R"({"version":"1.0","start_at":"foo","steps":[{"id":"foo","type":"stub"}]})";
    EXPECT_THROW(DAGParser::parse_string(json), std::runtime_error);
}

TEST(ParserErrors, MissingStartAtThrows) {
    std::string json = R"({"name":"p","version":"1.0","steps":[{"id":"foo","type":"stub"}]})";
    EXPECT_THROW(DAGParser::parse_string(json), std::runtime_error);
}

TEST(ParserErrors, MissingStepsThrows) {
    std::string json = R"({"name":"p","version":"1.0","start_at":"foo","steps":[]})";
    EXPECT_THROW(DAGParser::parse_string(json), std::runtime_error);
}

TEST(ParserErrors, FileNotFoundThrows) {
    EXPECT_THROW(DAGParser::parse_file("/nonexistent/path/loom_parser_test"), std::runtime_error);
}

TEST(ParserErrors, UnknownStepTypeThrows) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "steps":[{"id":"s1","type":"bogus"}]
    })";
    EXPECT_THROW(DAGParser::parse_string(json), std::runtime_error);
}

TEST(BasicParsing, MinimalDAG) {
    std::string json = R"({
        "name":"test_pipeline","version":"1.0","start_at":"step1",
        "steps":[{"id":"step1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.name, "test_pipeline");
    EXPECT_EQ(dag.version, "1.0");
    EXPECT_EQ(dag.start_at, "step1");
    EXPECT_EQ(static_cast<int>(dag.steps.size()), 1);
    EXPECT_EQ(dag.steps[0].id, "step1");
}

TEST(BasicParsing, DefaultErrorHandling) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.error_handling.max_attempts, 3);
    EXPECT_EQ(dag.error_handling.backoff, "exponential");
    EXPECT_EQ(dag.error_handling.base_ms, 1000);
}

TEST(ErrorHandling, FlatFormat) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "error_handling":{"max_attempts":5,"backoff":"linear","base_ms":500},
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.error_handling.max_attempts, 5);
    EXPECT_EQ(dag.error_handling.backoff, "linear");
    EXPECT_EQ(dag.error_handling.base_ms, 500);
}

TEST(ErrorHandling, RetryNestedFormat) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "error_handling":{"retry":{"max_attempts":10,"backoff":"exponential","base_ms":2000}},
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.error_handling.max_attempts, 10);
    EXPECT_EQ(dag.error_handling.backoff, "exponential");
    EXPECT_EQ(dag.error_handling.base_ms, 2000);
}

TEST(Connections, ArrayFormat_Http) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "connections":[
            {"name":"api","type":"http","base_url":"https://example.com",
             "rate_limit":{"max":5,"window_sec":2.0},
             "max_inflight":20,
             "backpressure":{"max_buffered_responses":500,"resume_at":200},
             "auth":{"type":"bearer","token_from":"env:TOKEN"},
             "timeout_ms":15000,
             "fail_on_status":[429,500,502,503]}
        ],
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(static_cast<int>(dag.connections.size()), 1);
    auto& c = dag.connections[0];
    EXPECT_EQ(c.name, "api");
    EXPECT_EQ(static_cast<int>(c.type), static_cast<int>(dag::ConnectionType::Http));
    EXPECT_EQ(c.base_url, "https://example.com");
    EXPECT_EQ(c.rate_limit.max, 5);
    EXPECT_EQ(c.rate_limit.window_sec, 2.0);
    EXPECT_EQ(c.max_inflight, 20);
    EXPECT_EQ(c.backpressure.max_buffered_responses, 500);
    EXPECT_EQ(c.backpressure.resume_at, 200);
    EXPECT_EQ(c.auth.type, "bearer");
    EXPECT_EQ(c.auth.token_from, "env:TOKEN");
    EXPECT_EQ(c.timeout_ms, 15000);
    EXPECT_EQ(static_cast<int>(c.fail_on_status.size()), 4);
}

TEST(Connections, ObjectFormat) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "connections":{
            "jira_api":{"type":"http","base_url":"https://jira.example.com"},
            "s3_out":{"type":"s3","endpoint":"http://minio:9000","bucket":"data"}
        },
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(static_cast<int>(dag.connections.size()), 2);
    EXPECT_EQ(dag.connections[0].name, "jira_api");
    EXPECT_EQ(dag.connections[1].name, "s3_out");
    EXPECT_EQ(static_cast<int>(dag.connections[1].type), static_cast<int>(dag::ConnectionType::S3));
    EXPECT_EQ(dag.connections[1].bucket, "data");
}

TEST(Connections, S3Connection) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "connections":[
            {"name":"s3","type":"s3","endpoint":"https://s3.amazonaws.com",
             "bucket":"mybucket","access_key":"AKIA...","secret_key":"secret",
             "prefix":"exports/"}
        ],
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& c = dag.connections[0];
    EXPECT_EQ(static_cast<int>(c.type), static_cast<int>(dag::ConnectionType::S3));
    EXPECT_EQ(c.endpoint, "https://s3.amazonaws.com");
    EXPECT_EQ(c.bucket, "mybucket");
    EXPECT_EQ(c.access_key, "AKIA...");
    EXPECT_EQ(c.secret_key, "secret");
    EXPECT_EQ(c.prefix, "exports/");
}

TEST(Steps, GenerateStep) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"gen",
        "connections":[{"name":"api","type":"http","base_url":"https://api.example.com"}],
        "steps":[{
            "id":"gen","type":"generate","connection":"api",
            "description":"Fetch data",
            "parallel":4,
            "template":{"method":"GET","path_template":"/items/{id}"},
            "request_queue":{"durable":false,"max_pending":5000}
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Generate));
    EXPECT_EQ(s.connection, "api");
    EXPECT_EQ(s.description, "Fetch data");
    EXPECT_EQ(s.parallel, 4);
    EXPECT_EQ(s.template_.method, "GET");
    EXPECT_EQ(s.template_.path_template, "/items/{id}");
    EXPECT_FALSE(s.request_queue.durable);
    EXPECT_EQ(s.request_queue.max_pending, 5000);
}

TEST(Steps, TransformStep) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"t1",
        "steps":[{
            "id":"t1","type":"transform",
            "input":{"from_step":"gen","field":"result.items"},
            "function":{"language":"python","entrypoint":"transforms/parse.py:extract"}
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Transform));
    EXPECT_TRUE(s.input.from_step.has_value());
    EXPECT_EQ(*s.input.from_step, "gen");
    EXPECT_TRUE(s.input.field.has_value());
    EXPECT_EQ(*s.input.field, "result.items");
    EXPECT_EQ(s.function.language, "python");
    EXPECT_EQ(s.function.entrypoint, "transforms/parse.py:extract");
}

TEST(Steps, FilterStep_ConditionsArray) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"f1",
        "steps":[{
            "id":"f1","type":"filter",
            "input":{"from_step":"t1"},
            "conditions":[
                {"field":"status","op":"eq","value":"Resolved"},
                {"field":"date","op":"not_null"}
            ]
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Filter));
    EXPECT_EQ(static_cast<int>(s.conditions.size()), 2);
    EXPECT_EQ(s.conditions[0].field, "status");
    EXPECT_EQ(s.conditions[0].op, "eq");
    EXPECT_EQ(s.conditions[0].value, "Resolved");
    EXPECT_EQ(s.conditions[1].field, "date");
    EXPECT_EQ(s.conditions[1].op, "not_null");
}

TEST(Steps, FilterStep_ConditionSingular) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"f1",
        "steps":[{
            "id":"f1","type":"filter","input":{"from_step":"t1"},
            "condition":[
                {"field":"priority","op":"gte","value":"P1"}
            ]
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(static_cast<int>(dag.steps[0].conditions.size()), 1);
    EXPECT_EQ(dag.steps[0].conditions[0].op, "gte");
}

TEST(Steps, AggregateStep) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"a1",
        "steps":[{
            "id":"a1","type":"aggregate",
            "input":{"from_step":"f1"},
            "group_by":["assignee","priority"],
            "aggregates":[
                {"name":"avg_cycles","field":"cycle_time","func":"avg"},
                {"name":"p95","field":"cycle_time","func":"percentile","arg":95},
                {"name":"count","field":"id","func":"count"}
            ]
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Aggregate));
    EXPECT_EQ(static_cast<int>(s.group_by.size()), 2);
    EXPECT_EQ(s.group_by[0], "assignee");
    EXPECT_EQ(s.group_by[1], "priority");
    EXPECT_EQ(static_cast<int>(s.aggregates.size()), 3);
    EXPECT_EQ(s.aggregates[0].name, "avg_cycles");
    EXPECT_EQ(s.aggregates[0].func, "avg");
    EXPECT_EQ(s.aggregates[1].arg, 95.0);
    EXPECT_EQ(s.aggregates[1].func, "percentile");
}

TEST(Steps, JoinStep) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"j1",
        "steps":[{
            "id":"j1","type":"join",
            "input":{"inputs":["left_step","right_step"]},
            "join_config":{"key":"ticket_id","join":"inner","timeout":"60s","on_timeout":"emit_null"}
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Join));
    EXPECT_EQ(static_cast<int>(s.input.inputs.size()), 2);
    EXPECT_EQ(s.input.inputs[0], "left_step");
    EXPECT_EQ(s.input.inputs[1], "right_step");
    EXPECT_EQ(s.join_config.key, "ticket_id");
    EXPECT_EQ(static_cast<int>(s.join_config.join), static_cast<int>(dag::JoinType::Inner));
    EXPECT_EQ(s.join_config.timeout, "60s");
    EXPECT_EQ(static_cast<int>(s.join_config.on_timeout), static_cast<int>(dag::OnTimeout::EmitNull));
}

TEST(Steps, UnnestStep) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"u1",
        "steps":[{
            "id":"u1","type":"unnest",
            "input":{"from_step":"t1","field":"comments"},
            "unnest":{"field":"comments","array_column":"comment","drop_null":false}
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Unnest));
    EXPECT_EQ(s.unnest.field, "comments");
    EXPECT_EQ(s.unnest.array_column, "comment");
    EXPECT_FALSE(s.unnest.drop_null);
}

TEST(Steps, SinkStep) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"sink1",
        "steps":[{
            "id":"sink1","type":"sink",
            "input":{"from_step":"a1"},
            "connection":"s3_out",
            "output":{"path_template":"outputs/{date}.parquet","format":"parquet","partition":"daily"}
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Sink));
    EXPECT_EQ(s.connection, "s3_out");
    EXPECT_EQ(s.output.path_template, "outputs/{date}.parquet");
    EXPECT_EQ(s.output.format, "parquet");
    EXPECT_EQ(s.output.partition, "daily");
}

TEST(Steps, StubStep) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "steps":[{
            "id":"s1","type":"stub","reason":"not implemented yet",
            "description":"Placeholder step"
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& s = dag.steps[0];
    EXPECT_EQ(static_cast<int>(s.type), static_cast<int>(dag::StepType::Stub));
    EXPECT_EQ(s.reason, "not implemented yet");
    EXPECT_EQ(s.description, "Placeholder step");
}

TEST(Parameters, LiteralSource) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"gen",
        "steps":[{
            "id":"gen","type":"generate","connection":"api",
            "parameters":[{"name":"project","source":"literal","value":"PROJ"}]
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& p = dag.steps[0].parameters[0];
    EXPECT_EQ(p.name, "project");
    EXPECT_EQ(static_cast<int>(p.source), static_cast<int>(dag::ParameterSourceKind::Literal));
    EXPECT_EQ(p.value, "PROJ");
}

TEST(Parameters, ListSource) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"gen",
        "steps":[{
            "id":"gen","type":"generate","connection":"api",
            "parameters":[{
                "name":"ticket","source":"List",
                "values":["PROJ-100","PROJ-200","PROJ-300"]
            }]
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& p = dag.steps[0].parameters[0];
    EXPECT_EQ(static_cast<int>(p.source), static_cast<int>(dag::ParameterSourceKind::List));
    EXPECT_EQ(static_cast<int>(p.values.size()), 3);
    EXPECT_EQ(p.values[0], "PROJ-100");
}

TEST(Parameters, RangeSource) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"gen",
        "steps":[{
            "id":"gen","type":"generate","connection":"api",
            "parameters":[{
                "name":"offset","source":"Range",
                "range_start":0,"range_end":1000,"range_step":100
            }]
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& p = dag.steps[0].parameters[0];
    EXPECT_EQ(static_cast<int>(p.source), static_cast<int>(dag::ParameterSourceKind::Range));
    EXPECT_EQ(p.range_start, 0);
    EXPECT_EQ(p.range_end, 1000);
    EXPECT_EQ(p.range_step, 100);
}

TEST(Parameters, ColumnSource) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"gen",
        "steps":[{
            "id":"gen","type":"generate","connection":"api",
            "parameters":[{
                "name":"ticketnumber","source":"column",
                "from_step":"expand","field":"ticket"
            }]
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& p = dag.steps[0].parameters[0];
    EXPECT_EQ(static_cast<int>(p.source), static_cast<int>(dag::ParameterSourceKind::Column));
    EXPECT_TRUE(p.from_step.has_value());
    EXPECT_EQ(*p.from_step, "expand");
    EXPECT_TRUE(p.field.has_value());
    EXPECT_EQ(*p.field, "ticket");
}

TEST(EnvVars, DirectSubstitution) {
    setenv("LOOM_TEST_ENV", "hello-world", 1);
    std::string json = R"({
        "name":"${LOOM_TEST_ENV}","version":"1","start_at":"s1",
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.name, "hello-world");
    unsetenv("LOOM_TEST_ENV");
}

TEST(EnvVars, WithDefault_EnvSet) {
    setenv("LOOM_VAR1", "actual-value", 1);
    std::string json = R"({
        "name":"${LOOM_VAR1:-fallback}","version":"1","start_at":"s1",
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.name, "actual-value");
    unsetenv("LOOM_VAR1");
}

TEST(EnvVars, WithDefault_EnvNotSet) {
    unsetenv("LOOM_VAR2");
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "connections":[
            {"name":"s3","type":"s3","endpoint":"${LOOM_VAR2:-http://localhost:9000}"}
        ],
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.connections[0].endpoint, "http://localhost:9000");
}

TEST(EnvVars, MultipleInString) {
    setenv("HOST", "api.example.com", 1);
    setenv("PORT", "8080", 1);
    std::string json = R"({
        "name":"p","version":"1","start_at":"s1",
        "connections":[
            {"name":"api","type":"http","base_url":"https://${HOST}:${PORT}/v1"}
        ],
        "steps":[{"id":"s1","type":"stub"}]
    })";
    auto dag = DAGParser::parse_string(json);
    EXPECT_EQ(dag.connections[0].base_url, "https://api.example.com:8080/v1");
    unsetenv("HOST");
    unsetenv("PORT");
}

TEST(RequestQueue, FullFlushPolicy) {
    std::string json = R"({
        "name":"p","version":"1","start_at":"gen",
        "connections":[{"name":"api","type":"http"}],
        "steps":[{
            "id":"gen","type":"generate","connection":"api",
            "request_queue":{
                "durable":true,
                "storage":"sqlite:///var/loom/queue.db",
                "max_pending":20000,
                "flush_policy":{
                    "trigger":"buffer_size",
                    "buffer_size":500,
                    "interval_sec":30,
                    "archive_connection":"s3_archive",
                    "archive_format":"json.gz",
                    "retention_days":30,
                    "keep_failed":false
                }
            }
        }]
    })";
    auto dag = DAGParser::parse_string(json);
    auto& rq = dag.steps[0].request_queue;
    EXPECT_TRUE(rq.durable);
    EXPECT_EQ(rq.storage, "sqlite:///var/loom/queue.db");
    EXPECT_EQ(rq.max_pending, 20000);
    EXPECT_EQ(static_cast<int>(rq.flush_policy.trigger), static_cast<int>(dag::FlushTrigger::BufferSize));
    EXPECT_EQ(rq.flush_policy.buffer_size, 500);
    EXPECT_EQ(rq.flush_policy.interval_sec, 30);
    EXPECT_EQ(rq.flush_policy.archive_connection, "s3_archive");
    EXPECT_FALSE(rq.flush_policy.keep_failed);
}

TEST(Pipeline, JiraScraperDAG) {
    std::string json = R"({
      "name": "jira-cycle-time",
      "version": "1.0",
      "start_at": "generate_request_queue",
      "error_handling": {
        "retry": { "max_attempts": 3, "backoff": "exponential", "base_ms": 1000 }
      },
      "connections": {
        "jira_api": {
          "type": "http",
          "base_url": "https://jira.corp.com/rest/api/2",
          "rate_limit": { "max": 10, "window_sec": 1 },
          "max_inflight": 10,
          "backpressure": { "max_buffered_responses": 50, "resume_at": 25 },
          "auth": { "type": "bearer", "token_from": "env:JIRA_TOKEN" },
          "timeout_ms": 30000,
          "fail_on_status": [429, 500, 502, 503]
        },
        "s3_landing": { "type": "s3" }
      },
      "steps": [
        {
          "id": "generate_request_queue",
          "type": "generate",
          "connection": "jira_api",
          "template": { "method": "GET", "path_template": "/issue/{ticket_id}" },
          "request_queue": { "durable": true, "max_pending": 10000 }
        },
        {
          "id": "parse_jira_response",
          "type": "transform",
          "input": { "from_step": "generate_request_queue" },
          "function": { "language": "python", "entrypoint": "pipelines/transforms/jira_parser.py:extract" }
        },
        {
          "id": "filter_resolved",
          "type": "filter",
          "input": { "from_step": "parse_jira_response" },
          "condition": [
            { "field": "status", "op": "eq", "value": "Resolved" }
          ]
        },
        {
          "id": "compute_cycle_time",
          "type": "aggregate",
          "input": { "from_step": "filter_resolved" },
          "group_by": ["assignee"],
          "aggregates": [
            { "name": "avg_cycle_days", "field": "cycle_time_days", "func": "avg" },
            { "name": "tickets_resolved", "field": "id", "func": "count" }
          ]
        },
        {
          "id": "write_results",
          "type": "sink",
          "input": { "from_step": "compute_cycle_time" },
          "connection": "s3_landing",
          "output": { "path_template": "outputs/{date}.parquet", "format": "parquet", "partition": "daily" }
        }
      ]
    })";
    auto dag = DAGParser::parse_string(json);

    EXPECT_EQ(dag.name, "jira-cycle-time");
    EXPECT_EQ(dag.version, "1.0");
    EXPECT_EQ(dag.start_at, "generate_request_queue");
    EXPECT_EQ(dag.error_handling.max_attempts, 3);

    EXPECT_EQ(static_cast<int>(dag.connections.size()), 2);
    EXPECT_EQ(dag.connections[0].name, "jira_api");
    EXPECT_EQ(dag.connections[0].rate_limit.max, 10);
    EXPECT_EQ(dag.connections[0].max_inflight, 10);
    EXPECT_EQ(dag.connections[0].auth.type, "bearer");

    EXPECT_EQ(static_cast<int>(dag.steps.size()), 5);
    EXPECT_EQ(static_cast<int>(dag.steps[0].type), static_cast<int>(dag::StepType::Generate));
    EXPECT_EQ(static_cast<int>(dag.steps[1].type), static_cast<int>(dag::StepType::Transform));
    EXPECT_EQ(static_cast<int>(dag.steps[2].type), static_cast<int>(dag::StepType::Filter));
    EXPECT_EQ(static_cast<int>(dag.steps[3].type), static_cast<int>(dag::StepType::Aggregate));
    EXPECT_EQ(static_cast<int>(dag.steps[4].type), static_cast<int>(dag::StepType::Sink));
}

TEST(Pipeline, ParseFileRoundtrip) {
    auto dag = DAGParser::parse_file("pipelines/jira_scraper.dag.json");
    EXPECT_EQ(dag.name, "jira-cycle-time");
    EXPECT_EQ(dag.start_at, "generate_request_queue");
    EXPECT_NE(static_cast<int>(dag.steps.size()), 0);
    EXPECT_NE(static_cast<int>(dag.connections.size()), 0);
}
