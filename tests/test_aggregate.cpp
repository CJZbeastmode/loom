#include "test_utils.h"
#include "loom/compute/aggregate.h"

#include <cmath>

using namespace loom;

namespace {
compute::RecordBatch tickets() {
    compute::RecordBatch b;
    b.names = {"assignee", "priority", "cycle_time_days"};
    b.columns = {
        compute::Column::make_string({"Alice", "Bob", "Alice", "Bob", "Alice"}),
        compute::Column::make_string({"P1", "P2", "P1", "P1", "P2"}),
        compute::Column::make_double({3.0, 5.0, 7.0, 1.0, 5.0}),
    };
    return b;
}

compute::Aggregate agg(const std::vector<std::string>& group_by,
                       const std::vector<compute::AggregateDef>& aggs) {
    compute::Aggregate a;
    a.set_group_by(group_by);
    a.set_aggregates(aggs);
    return a;
}
}

TEST(AggregateTest, CountByGroup) {
    auto a = agg({"assignee"}, {{"n", "cycle_time_days", "count"}});
    a.accumulate(tickets());
    auto out = a.finalize();

    EXPECT_EQ(out.num_rows(), 2);  // Alice, Bob
    EXPECT_EQ(out.column("assignee").str[0], "Alice");
    EXPECT_EQ(out.column("assignee").str[1], "Bob");
    // Alice has 3 rows, Bob has 2.
    EXPECT_EQ(out.column("n").i64[0], 3);
    EXPECT_EQ(out.column("n").i64[1], 2);
}

TEST(AggregateTest, AvgByGroup) {
    auto a = agg({"assignee"}, {{"avg_cycle", "cycle_time_days", "avg"}});
    a.accumulate(tickets());
    auto out = a.finalize();

    EXPECT_EQ(out.column("assignee").str[0], "Alice");
    // Alice: (3+7+5)/3 = 5.0 ; Bob: (5+1)/2 = 3.0
    EXPECT_TRUE(std::fabs(out.column("avg_cycle").f64[0] - 5.0) < 1e-9);
    EXPECT_TRUE(std::fabs(out.column("avg_cycle").f64[1] - 3.0) < 1e-9);
}

TEST(AggregateTest, SumMinMax) {
    auto a = agg({"assignee"}, {
        {"total", "cycle_time_days", "sum"},
        {"min", "cycle_time_days", "min"},
        {"max", "cycle_time_days", "max"},
    });
    a.accumulate(tickets());
    auto out = a.finalize();

    // Alice: sum 15, min 3, max 7 ; Bob: sum 6, min 1, max 5
    EXPECT_TRUE(std::fabs(out.column("total").f64[0] - 15.0) < 1e-9);
    EXPECT_TRUE(std::fabs(out.column("total").f64[1] - 6.0) < 1e-9);
    EXPECT_TRUE(std::fabs(out.column("min").f64[0] - 3.0) < 1e-9);
    EXPECT_TRUE(std::fabs(out.column("max").f64[0] - 7.0) < 1e-9);
    EXPECT_TRUE(std::fabs(out.column("min").f64[1] - 1.0) < 1e-9);
    EXPECT_TRUE(std::fabs(out.column("max").f64[1] - 5.0) < 1e-9);
}

TEST(AggregateTest, Percentile) {
    compute::RecordBatch b;
    b.names = {"g", "v"};
    b.columns = {
        compute::Column::make_string({"x", "x", "x", "x"}),
        compute::Column::make_double({10.0, 20.0, 30.0, 40.0}),
    };
    auto a = agg({"g"}, {{"p50", "v", "percentile", 50.0}});
    a.accumulate(b);
    auto out = a.finalize();
    // median of [10,20,30,40] with interpolation = 25
    EXPECT_TRUE(std::fabs(out.column("p50").f64[0] - 25.0) < 1e-9);
}

TEST(AggregateTest, MultipleGroupByColumns) {
    auto a = agg({"assignee", "priority"}, {{"n", "cycle_time_days", "count"}});
    a.accumulate(tickets());
    auto out = a.finalize();

    // Groups: (Alice,P1)=2, (Alice,P2)=1, (Bob,P1)=1, (Bob,P2)=1
    EXPECT_EQ(out.num_rows(), 4);
    // find (Alice,P1)
    int found = -1;
    for (int64_t i = 0; i < out.num_rows(); ++i) {
        if (out.column("assignee").str[i] == "Alice" && out.column("priority").str[i] == "P1")
            found = i;
    }
    EXPECT_TRUE(found >= 0);
    EXPECT_EQ(out.column("n").i64[found], 2);
}

TEST(AggregateTest, IncrementalAccumulateMatchesSingleBatch) {
    // First two rows (Alice P1=3, Bob P2=5), then last three rows.
    auto whole = tickets();
    auto part1 = whole.slice(0, 2);
    auto part2 = whole.slice(2, 5);

    auto a1 = agg({"assignee"}, {{"avg", "cycle_time_days", "avg"}});
    a1.accumulate(part1);
    a1.accumulate(part2);
    auto out1 = a1.finalize();

    auto a2 = agg({"assignee"}, {{"avg", "cycle_time_days", "avg"}});
    a2.accumulate(whole);
    auto out2 = a2.finalize();

    EXPECT_EQ(out1.num_rows(), out2.num_rows());
    for (int64_t i = 0; i < out1.num_rows(); ++i) {
        EXPECT_EQ(out1.column("assignee").str[i], out2.column("assignee").str[i]);
        EXPECT_TRUE(std::fabs(out1.column("avg").f64[i] - out2.column("avg").f64[i]) < 1e-9);
    }
}

TEST(AggregateTest, NullsAreSkipped) {
    compute::RecordBatch b;
    b.names = {"g", "v"};
    b.columns = {
        compute::Column::make_string({"a", "a", "a"}),
        compute::Column::make_double({1.0, 2.0, 3.0}),
    };
    // Mark the middle value null.
    b.columns[1].valid = {1, 0, 1};

    auto a = agg({"g"}, {{"avg", "v", "avg"}, {"n", "v", "count"}});
    a.accumulate(b);
    auto out = a.finalize();
    // avg over non-null {1,3} = 2 ; count is still 3 (rows)
    EXPECT_TRUE(std::fabs(out.column("avg").f64[0] - 2.0) < 1e-9);
    EXPECT_EQ(out.column("n").i64[0], 3);
}

TEST(AggregateTest, UnknownColumnThrows) {
    auto a = agg({"nope"}, {{"n", "v", "count"}});
    EXPECT_THROW(a.accumulate(tickets()), std::runtime_error);
}

TEST(AggregateTest, NonNumericFieldThrows) {
    // sum over a string column must throw.
    auto a = agg({"assignee"}, {{"s", "assignee", "sum"}});
    EXPECT_THROW(a.accumulate(tickets()), std::runtime_error);
}

TEST(AggregateTest, ResetClearsState) {
    auto a = agg({"assignee"}, {{"n", "cycle_time_days", "count"}});
    a.accumulate(tickets());
    EXPECT_EQ(a.group_count(), 2);
    a.reset();
    EXPECT_EQ(a.group_count(), 0);
    auto out = a.finalize();
    EXPECT_EQ(out.num_rows(), 0);
}
