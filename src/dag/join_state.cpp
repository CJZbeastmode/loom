#include "loom/dag/join_state.h"

namespace loom {
namespace dag {

template <typename Key, typename Record>
struct JoinState<Key, Record>::Impl {
    JoinStateConfig config;
    ReadyCallback ready_cb;
};

template <typename Key, typename Record>
JoinState<Key, Record>::JoinState(const JoinStateConfig& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

template <typename Key, typename Record>
JoinState<Key, Record>::~JoinState() = default;

template <typename Key, typename Record>
void JoinState<Key, Record>::register_source(const std::string& /*source_id*/) {}

template <typename Key, typename Record>
void JoinState<Key, Record>::on_record(const std::string& /*source_id*/, const Key& /*key*/, Record /*record*/) {}

template <typename Key, typename Record>
bool JoinState<Key, Record>::is_ready(const Key& /*key*/) const {
    return false;
}

template <typename Key, typename Record>
void JoinState<Key, Record>::expire_stale_keys() {}

template <typename Key, typename Record>
void JoinState<Key, Record>::set_ready_callback(ReadyCallback cb) {
    impl_->ready_cb = std::move(cb);
}

}  // namespace dag
}  // namespace loom
