#pragma once

#include <memory>
#include <string>

#include "loom/connectors/connector.h"

namespace loom {
namespace connectors {

// A connector that writes to the local filesystem, rooted at `root_dir`.
// Paths passed to write/read/exists are resolved relative to the root.
class FileConnector : public Connector {
public:
    explicit FileConnector(std::string root_dir);
    ~FileConnector() override;

    FileConnector(const FileConnector&) = delete;
    FileConnector& operator=(const FileConnector&) = delete;
    FileConnector(FileConnector&&) noexcept;
    FileConnector& operator=(FileConnector&&) noexcept;

    void write(const std::string& path, const std::string& data) override;
    std::string read(const std::string& path) override;
    bool exists(const std::string& path) const override;

    // The absolute path a relative `path` resolves to.
    std::string resolve(const std::string& path) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace connectors
}  // namespace loom
