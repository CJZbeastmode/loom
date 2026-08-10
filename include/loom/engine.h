#pragma once

#include <string>
#include <optional>

namespace loom {

struct EngineConfig {
    std::string dag_path;
    std::string config_path;
    bool dry_run = false;
    int verbosity = 0;
};

struct EngineResult {
    bool success = false;
    int total_requests = 0;
    int successes = 0;
    int failures = 0;
    int retries = 0;
    int skipped = 0;
    double elapsed_sec = 0.0;
    std::string error_message;
};

class Engine {
public:
    Engine();
    ~Engine();

    EngineResult run(const EngineConfig& config);
    EngineResult validate(const std::string& dag_path);
    static std::string version();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace loom
