#include "loom/engine.h"
#include "loom/logging.h"
#include <iostream>
#include <string>
#include <vector>

namespace {

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " <command> [options]\n\n"
              << "Commands:\n"
              << "  run <dag.json>      Run a pipeline DAG\n"
              << "  validate <dag.json> Validate a pipeline DAG without running\n"
              << "  --version, version  Print version and exit\n"
              << "  --help, help        Show this message\n\n"
              << "Options:\n"
              << "  --config <path>     Engine config file (default: config.yaml)\n"
              << "  --dry-run           Parse and validate only, do not execute\n"
              << "  -v, --verbose       Increase verbosity\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    loom::set_log_level(loom::LogLevel::Info);

    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    std::string cmd = argv[1];

    if (cmd == "--version" || cmd == "version") {
        std::cout << loom::Engine::version() << "\n";
        return 0;
    }

    if (cmd == "--help" || cmd == "help") {
        print_usage(argv[0]);
        return 0;
    }

    if (cmd == "run") {
        if (argc < 3) {
            std::cerr << "Error: missing DAG path\n";
            return 1;
        }

        loom::EngineConfig config;
        config.dag_path = argv[2];

        for (int i = 3; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--config" && i + 1 < argc) {
                config.config_path = argv[++i];
            } else if (arg == "--dry-run") {
                config.dry_run = true;
            } else if (arg == "-v" || arg == "--verbose") {
                config.verbosity++;
            }
        }

        loom::Engine engine;
        auto result = engine.run(config);

        LOOM_LOG_INFO("DAG:", config.dag_path);
        LOOM_LOG_INFO("Requests:", result.total_requests, "total,",
                      result.successes, "ok,", result.failures, "failed,",
                      result.retries, "retried,", result.skipped, "skipped");
        LOOM_LOG_INFO("Elapsed:", result.elapsed_sec, "s");

        return result.success ? 0 : 1;
    }

    if (cmd == "validate") {
        if (argc < 3) {
            std::cerr << "Error: missing DAG path\n";
            return 1;
        }

        loom::Engine engine;
        auto result = engine.validate(argv[2]);
        std::cout << (result.success ? "DAG is valid\n" : "DAG is invalid\n");
        if (!result.error_message.empty()) {
            std::cout << result.error_message << "\n";
        }
        return result.success ? 0 : 1;
    }

    std::cerr << "Error: unknown command '" << cmd << "'\n";
    print_usage(argv[0]);
    return 1;
}
