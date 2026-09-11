#pragma once

#include <string>

namespace loom {
namespace connectors {

// A generic data sink/source connector.  Concrete connectors decide where the
// bytes actually go (local filesystem, S3/MinIO, a database, ...).
class Connector {
public:
    virtual ~Connector() = default;

    // Write `data` to `path` (creating parent directories as needed).
    virtual void write(const std::string& path, const std::string& data) = 0;

    // Read the full contents of `path` back.
    virtual std::string read(const std::string& path) = 0;

    virtual bool exists(const std::string& path) const = 0;
};

}  // namespace connectors
}  // namespace loom
