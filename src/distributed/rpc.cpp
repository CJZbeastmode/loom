#include "loom/distributed/rpc.h"

namespace loom {
namespace distributed {

struct RpcService::Impl {};

RpcService::RpcService() : impl_(std::make_unique<Impl>()) {}
RpcService::~RpcService() = default;

void RpcService::start_server(int /*port*/) {}
void RpcService::stop_server() {}
void RpcService::connect_to(const std::string& /*host*/, int /*port*/) {}

}  // namespace distributed
}  // namespace loom
