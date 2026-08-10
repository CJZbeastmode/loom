#include <gtest/gtest.h>
#include "loom/dag/parser.h"
#include "loom/dag/executor.h"

TEST(DAGExecutorTest, ConstructorDoesNotThrow) {
    loom::dag::DAG dag;
    dag.name = "test";
    dag.start_at = "step1";

    loom::dag::DAGExecutor executor(dag);
    EXPECT_FALSE(executor.is_running());
}

TEST(DAGExecutorTest, RunSetsRunning) {
    loom::dag::DAG dag;
    dag.name = "test";
    dag.start_at = "step1";

    loom::dag::DAGExecutor executor(dag);
    executor.run();
    EXPECT_TRUE(executor.is_running());
}

TEST(DAGExecutorTest, ShutdownStopsRunning) {
    loom::dag::DAG dag;
    dag.name = "test";
    dag.start_at = "step1";

    loom::dag::DAGExecutor executor(dag);
    executor.run();
    executor.shutdown();
    EXPECT_FALSE(executor.is_running());
}
