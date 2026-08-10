#pragma once

#include <memory>
#include <string>

#ifdef LOOM_HAS_ARROW
#include <arrow/record_batch.h>
#endif

namespace loom {
namespace distributed {

class RpcService {
public:
    RpcService();
    ~RpcService();

    void start_server(int port);
    void stop_server();
    void connect_to(const std::string& host, int port);

#ifdef LOOM_HAS_ARROW
    void send_batch(const std::string& peer, const std::shared_ptr<arrow::RecordBatch>& batch);
    std::shared_ptr<arrow::RecordBatch> receive_batch(const std::string& peer);
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace distributed
}  // namespace loom
