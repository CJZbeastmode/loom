#include "test_utils.h"
#include "loom/dag/parser.h"
#include "loom/dag/executor.h"

using namespace loom;

TEST(DAGExecutorTest, ConstructorDoesNotThrow) {
    dag::DAG dag;
    dag.name = "test";
    dag.start_at = "step1";

    dag::DAGExecutor executor(dag);
    EXPECT_FALSE(executor.is_running());
}

TEST(DAGExecutorTest, RunSetsRunning) {
    dag::DAG dag;
    dag.name = "test";
    dag.start_at = "step1";

    dag::DAGExecutor executor(dag);
    executor.run();
    EXPECT_TRUE(executor.is_running());
}

TEST(DAGExecutorTest, ShutdownStopsRunning) {
    dag::DAG dag;
    dag.name = "test";
    dag.start_at = "step1";

    dag::DAGExecutor executor(dag);
    executor.run();
    executor.shutdown();
    EXPECT_FALSE(executor.is_running());
}
