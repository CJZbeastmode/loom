#pragma once

#include <memory>
#include <string>

namespace loom {
namespace dag {

struct DAG;
struct Step;

class DAGExecutor {
public:
    explicit DAGExecutor(const DAG& dag);
    ~DAGExecutor();

    bool run();
    void shutdown();
    bool is_running() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace dag
}  // namespace loom
