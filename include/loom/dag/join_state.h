#pragma once

#include <string>
#include <chrono>
#include <functional>
#include <memory>

namespace loom {
namespace dag {

enum class JoinDependencyStatus {
    Waiting,
    Ready,
    Expired,
};

struct JoinStateConfig {
    std::chrono::seconds timeout{30};
    int max_buffered_keys = 10000;
};

template <typename Key, typename Record>
class JoinState {
public:
    explicit JoinState(const JoinStateConfig& config);
    ~JoinState();

    void register_source(const std::string& source_id);
    void on_record(const std::string& source_id, const Key& key, Record record);
    bool is_ready(const Key& key) const;
    void expire_stale_keys();

    using ReadyCallback = std::function<void(const Key&, const std::vector<Record>&)>;
    void set_ready_callback(ReadyCallback cb);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace dag
}  // namespace loom
