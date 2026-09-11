#include "test_utils.h"
#include "loom/compute/sink.h"
#include "loom/connectors/file_connector.h"
#include "loom/connectors/connector_registry.h"
#include "loom/connectors/s3_connector.h"

#include <cstdio>

using namespace loom;

namespace {
std::string temp_root() {
    return "/var/folders/hj/0g8xmk093_d0chf2nqjgcn7m0000gn/T/opencode/loom_sink_test";
}

compute::RecordBatch sample() {
    compute::RecordBatch b;
    b.names = {"id", "status"};
    b.columns = {
        compute::Column::make_int64({1, 2, 3}),
        compute::Column::make_string({"Open", "Resolved", "Closed"}),
    };
    return b;
}
}

TEST(SinkTest, ToCsv) {
    auto csv = compute::Sink::to_csv(sample());
    EXPECT_EQ(csv, "id,status\n1,Open\n2,Resolved\n3,Closed\n");
}

TEST(SinkTest, ToCsvEscapesCommas) {
    compute::RecordBatch b;
    b.names = {"note"};
    b.columns = {compute::Column::make_string({"has, comma"})};
    EXPECT_EQ(compute::Sink::to_csv(b), "note\n\"has, comma\"\n");
}

TEST(SinkTest, ToJson) {
    auto json = compute::Sink::to_json(sample());
    EXPECT_EQ(json, R"([{"id":1,"status":"Open"},{"id":2,"status":"Resolved"},{"id":3,"status":"Closed"}])");
}

TEST(SinkTest, MaterializePathSubstitutesDateAndTs) {
    std::string p1 = compute::Sink::materialize_path("out/{date}.csv", "daily");
    EXPECT_TRUE(p1.find("{date}") == std::string::npos);
    EXPECT_TRUE(p1.find("{ts}") == std::string::npos);

    std::string p2 = compute::Sink::materialize_path("out/{ts}.csv", "daily");
    // {ts} → all digits
    EXPECT_TRUE(p2.find("{ts}") == std::string::npos);
}

TEST(SinkTest, WriteCsvToFileConnector) {
    connectors::FileConnector conn(temp_root());

    compute::Sink::Config cfg;
    cfg.path_template = "results/{date}.csv";
    cfg.format = "csv";
    compute::Sink sink(&conn, cfg);

    std::string path = sink.write(sample());

    EXPECT_TRUE(conn.exists(path));
    EXPECT_EQ(conn.read(path), "id,status\n1,Open\n2,Resolved\n3,Closed\n");
}

TEST(SinkTest, WriteJsonToFileConnector) {
    connectors::FileConnector conn(temp_root());

    compute::Sink::Config cfg;
    cfg.path_template = "results/out.json";
    cfg.format = "json";
    compute::Sink sink(&conn, cfg);

    std::string path = sink.write(sample());
    EXPECT_TRUE(conn.exists(path));
    EXPECT_EQ(conn.read(path),
              R"([{"id":1,"status":"Open"},{"id":2,"status":"Resolved"},{"id":3,"status":"Closed"}])");
}

TEST(SinkTest, UnsupportedFormatThrows) {
    connectors::FileConnector conn(temp_root());
    compute::Sink::Config cfg;
    cfg.path_template = "out.parquet";
    cfg.format = "parquet";
    compute::Sink sink(&conn, cfg);
    EXPECT_THROW(sink.write(sample()), std::runtime_error);
}

TEST(SinkTest, ConnectorRegistryResolves) {
    connectors::ConnectorRegistry reg;
    reg.add("s3_landing", std::make_unique<connectors::FileConnector>(temp_root()));

    EXPECT_TRUE(reg.has("s3_landing"));
    EXPECT_FALSE(reg.has("missing"));

    connectors::Connector* c = reg.get("s3_landing");
    c->write("hello.txt", "hi");
    EXPECT_EQ(c->read("hello.txt"), "hi");
}

TEST(SinkTest, UnknownConnectorThrows) {
    connectors::ConnectorRegistry reg;
    EXPECT_THROW(reg.get("nope"), std::runtime_error);
}

TEST(SinkTest, S3ConnectorWriteNotImplemented) {
    connectors::S3Config cfg;
    cfg.endpoint = "http://localhost:9000";
    cfg.bucket = "data";
    connectors::S3Connector s3(cfg);
    EXPECT_THROW(s3.write("x.csv", "data"), std::runtime_error);
}
