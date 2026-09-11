#include "loom/connectors/file_connector.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace loom {
namespace connectors {

struct FileConnector::Impl {
    std::filesystem::path root;
};

FileConnector::FileConnector(std::string root_dir)
    : impl_(std::make_unique<Impl>()) {
    impl_->root = std::filesystem::path(root_dir);
    std::filesystem::create_directories(impl_->root);
}

FileConnector::~FileConnector() = default;
FileConnector::FileConnector(FileConnector&&) noexcept = default;
FileConnector& FileConnector::operator=(FileConnector&&) noexcept = default;

std::string FileConnector::resolve(const std::string& path) const {
    std::filesystem::path p = impl_->root / std::filesystem::path(path);
    return p.string();
}

void FileConnector::write(const std::string& path, const std::string& data) {
    std::filesystem::path p = impl_->root / std::filesystem::path(path);
    std::filesystem::create_directories(p.parent_path());
    std::ofstream out(p, std::ios::binary);
    if (!out.is_open()) {
        throw std::runtime_error("FileConnector: cannot open for write: " + p.string());
    }
    out << data;
}

std::string FileConnector::read(const std::string& path) {
    std::filesystem::path p = impl_->root / std::filesystem::path(path);
    std::ifstream in(p, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("FileConnector: cannot open for read: " + p.string());
    }
    std::ostringstream buf;
    buf << in.rdbuf();
    return buf.str();
}

bool FileConnector::exists(const std::string& path) const {
    std::filesystem::path p = impl_->root / std::filesystem::path(path);
    return std::filesystem::exists(p);
}

}  // namespace connectors
}  // namespace loom
