#include "test_utils.h"
#include "loom/compute/transform.h"
#include "loom/compute/thread_pool.h"

#include <vector>

using namespace loom;

namespace {
// A row-preserving transform: doubles the "id" column into a new "id2" column.
compute::TransformFunc double_id = [](const compute::RecordBatch& in) {
    compute::RecordBatch out;
    out.names = {"id", "id2"};
    out.columns.reserve(2);
    out.columns.push_back(in.column("id"));
    const auto& ids = in.column("id").i64;
    std::vector<int64_t> doubled;
    doubled.reserve(ids.size());
    for (auto v : ids) doubled.push_back(v * 2);
    out.columns.push_back(compute::Column::make_int64(std::move(doubled)));
    return out;
};

compute::RecordBatch make_batch(int n) {
    std::vector<int64_t> ids(n);
    for (int i = 0; i < n; ++i) ids[i] = i;
    compute::RecordBatch b;
    b.names = {"id"};
    b.columns = {compute::Column::make_int64(std::move(ids))};
    return b;
}
}

TEST(TransformTest, RegisterAndApply) {
    compute::Transform t;
    t.register_function("double", double_id);
    EXPECT_TRUE(t.has_function("double"));

    auto out = t.apply("double", make_batch(5));
    EXPECT_EQ(out.num_rows(), 5);
    EXPECT_EQ(out.num_columns(), 2);
    EXPECT_EQ(out.column("id2").i64[0], 0);
    EXPECT_EQ(out.column("id2").i64[4], 8);
}

TEST(TransformTest, UnknownFunctionThrows) {
    compute::Transform t;
    EXPECT_THROW(t.apply("missing", make_batch(3)), std::runtime_error);
}

TEST(TransformTest, ParallelMatchesSerial) {
    compute::Transform t;
    t.register_function("double", double_id);

    compute::ThreadPool pool(4);
    auto batch = make_batch(1000);
    auto serial = t.apply("double", batch);
    auto parallel = t.apply_parallel("double", batch, pool);

    EXPECT_EQ(serial.num_rows(), parallel.num_rows());
    for (int64_t i = 0; i < serial.num_rows(); ++i) {
        EXPECT_EQ(serial.column("id2").i64[i], parallel.column("id2").i64[i]);
    }
}

TEST(TransformTest, ParallelRejectsRowCountChange) {
    compute::Transform t;
    // A function that drops rows (not row-preserving).
    t.register_function("halve", [](const compute::RecordBatch& in) {
        return in.slice(0, in.num_rows() / 2);
    });

    compute::ThreadPool pool(2);
    EXPECT_THROW(t.apply_parallel("halve", make_batch(10), pool), std::runtime_error);
}
