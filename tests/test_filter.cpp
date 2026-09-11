#include "test_utils.h"
#include "loom/compute/filter.h"
#include "loom/compute/thread_pool.h"

#include <chrono>
#include <iostream>

using namespace loom;

namespace {
// 10 rows: id 0..9, a status string, a double score.
compute::RecordBatch make_batch() {
    compute::RecordBatch b;
    b.names = {"id", "status", "score"};
    b.columns = {
        compute::Column::make_int64({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
        compute::Column::make_string(
            {"Open", "Resolved", "Open", "Closed", "Resolved",
             "Open", "Resolved", "Closed", "Open", "Resolved"}),
        compute::Column::make_double({0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5}),
    };
    return b;
}

compute::Filter make_filter(const std::vector<compute::FilterCondition>& conds) {
    compute::Filter f;
    f.set_conditions(conds);
    return f;
}
}

TEST(FilterTest, EqString) {
    auto batch = make_batch();
    auto f = make_filter({{"status", "eq", "Resolved"}});
    auto out = f.apply(batch);
    EXPECT_EQ(out.num_rows(), 4);  // ids 1,4,6,9
    EXPECT_EQ(out.column("id").i64[0], 1);
    EXPECT_EQ(out.column("id").i64[3], 9);
}

TEST(FilterTest, GtInt) {
    auto batch = make_batch();
    auto f = make_filter({{"id", "gt", "5"}});
    auto out = f.apply(batch);
    EXPECT_EQ(out.num_rows(), 4);  // 6,7,8,9
    EXPECT_EQ(out.column("id").i64[0], 6);
    EXPECT_EQ(out.column("id").i64[3], 9);
}

TEST(FilterTest, ComparisonOperators) {
    auto batch = make_batch();
    EXPECT_EQ(make_filter({{"id", "gte", "5"}}).apply(batch).num_rows(), 5);   // 5..9
    EXPECT_EQ(make_filter({{"id", "lt", "3"}}).apply(batch).num_rows(), 3);    // 0..2
    EXPECT_EQ(make_filter({{"id", "lte", "3"}}).apply(batch).num_rows(), 4);   // 0..3
    EXPECT_EQ(make_filter({{"id", "neq", "4"}}).apply(batch).num_rows(), 9);   // all but 4
    EXPECT_EQ(make_filter({{"id", "eq", "4"}}).apply(batch).num_rows(), 1);    // just 4
}

TEST(FilterTest, DoubleComparison) {
    auto batch = make_batch();
    auto f = make_filter({{"score", "gt", "5.0"}});
    auto out = f.apply(batch);
    EXPECT_EQ(out.num_rows(), 5);  // 5.5..9.5 → ids 5..9
}

TEST(FilterTest, AndConditions) {
    auto batch = make_batch();
    // status == Resolved AND id > 3 → ids 4, 6, 9
    auto f = make_filter({{"status", "eq", "Resolved"}, {"id", "gt", "3"}});
    auto out = f.apply(batch);
    EXPECT_EQ(out.num_rows(), 3);
    EXPECT_EQ(out.column("id").i64[0], 4);
    EXPECT_EQ(out.column("id").i64[1], 6);
    EXPECT_EQ(out.column("id").i64[2], 9);
}

TEST(FilterTest, NotNullAndIsNull) {
    compute::RecordBatch b = make_batch();
    // Mark rows 0 and 5 as null in the status column.
    b.columns[1].valid = {0, 1, 1, 1, 1, 0, 1, 1, 1, 1};

    auto not_null = make_filter({{"status", "not_null", ""}}).apply(b);
    EXPECT_EQ(not_null.num_rows(), 8);  // all except 0 and 5

    auto is_null = make_filter({{"status", "is_null", ""}}).apply(b);
    EXPECT_EQ(is_null.num_rows(), 2);  // 0 and 5
}

TEST(FilterTest, EmptyConditionsKeepsAll) {
    auto batch = make_batch();
    auto f = make_filter({});
    EXPECT_EQ(f.apply(batch).num_rows(), 10);
}

TEST(FilterTest, UnknownColumnThrows) {
    auto batch = make_batch();
    auto f = make_filter({{"nope", "eq", "1"}});
    EXPECT_THROW(f.apply(batch), std::runtime_error);
}

TEST(FilterTest, ParallelMatchesSingleThreaded) {
    auto batch = make_batch();
    auto f = make_filter({{"status", "eq", "Resolved"}, {"score", "gt", "1.0"}});

    compute::ThreadPool pool(4);
    auto serial = f.apply(batch);
    auto parallel = f.apply_parallel(batch, pool);

    EXPECT_EQ(serial.num_rows(), parallel.num_rows());
    for (int64_t i = 0; i < serial.num_rows(); ++i) {
        EXPECT_EQ(serial.column("id").i64[i], parallel.column("id").i64[i]);
        EXPECT_EQ(serial.column("status").str[i], parallel.column("status").str[i]);
    }
}

TEST(FilterTest, ParallelLargeBatchCorrect) {
    // 100k rows; filter keeps only evens. Parallel must equal serial.
    const int N = 100000;
    std::vector<int64_t> ids(N);
    for (int i = 0; i < N; ++i) ids[i] = i;
    compute::RecordBatch b;
    b.names = {"id"};
    b.columns = {compute::Column::make_int64(std::move(ids))};

    auto f = make_filter({{"id", "gte", "0"}});  // keep all (stresses the copy path)

    compute::ThreadPool pool(4);
    auto parallel = f.apply_parallel(b, pool);
    EXPECT_EQ(parallel.num_rows(), N);
    EXPECT_EQ(parallel.column("id").i64[N - 1], N - 1);
}

TEST(FilterTest, BenchmarkSingleVsParallel) {
    // Informational: time single-threaded vs 8-worker filter over 2M rows with
    // a string comparison (CPU-bound enough to show a speedup).
    const int N = 2000000;
    std::vector<std::string> tags;
    tags.reserve(N);
    for (int i = 0; i < N; ++i) tags.push_back((i % 2 == 0) ? "keep" : "drop");

    compute::RecordBatch b;
    b.names = {"tag"};
    b.columns = {compute::Column::make_string(std::move(tags))};

    auto f = make_filter({{"tag", "eq", "keep"}});

    auto t0 = std::chrono::steady_clock::now();
    auto serial = f.apply(b);
    auto t1 = std::chrono::steady_clock::now();
    compute::ThreadPool pool(8);
    auto parallel = f.apply_parallel(b, pool);
    auto t2 = std::chrono::steady_clock::now();

    double serial_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double parallel_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();

    EXPECT_EQ(serial.num_rows(), N / 2);
    EXPECT_EQ(parallel.num_rows(), serial.num_rows());

    std::cout << "\n    filter benchmark: serial " << serial_ms << "ms, parallel(8) "
              << parallel_ms << "ms, speedup " << (serial_ms / parallel_ms) << "x\n";
}
