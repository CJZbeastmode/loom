#pragma once

#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace loom {

enum class LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
};

inline LogLevel g_log_level = LogLevel::Info;

inline void set_log_level(LogLevel level) {
    g_log_level = level;
}

namespace log_detail {

inline const char* level_str(LogLevel lvl) {
    switch (lvl) {
        case LogLevel::Trace: return "TRACE";
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO ";
        case LogLevel::Warn:  return "WARN ";
        case LogLevel::Error: return "ERROR";
    }
    return "?????";
}

inline std::string timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    std::tm tm_buf;
    localtime_r(&time_t_now, &tm_buf);
    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}

template <typename... Args>
void log(LogLevel lvl, const char* fmt, Args&&... args) {
    if (lvl < g_log_level) return;
    auto& out = (lvl >= LogLevel::Error) ? std::cerr : std::cout;
    out << "[" << timestamp() << "] [" << level_str(lvl) << "] ";
    // Simple printf-style formatting
    int printed = 0;
    auto printer = [&](const auto& val) {
        if (printed++ > 0) out << " ";
        out << val;
    };
    (printer(std::forward<Args>(args)), ...);
    out << '\n';
}

}  // namespace log_detail

}  // namespace loom

// Convenience macros matching spdlog API
#define LOOM_LOG_TRACE(...) loom::log_detail::log(loom::LogLevel::Trace, "", __VA_ARGS__)
#define LOOM_LOG_DEBUG(...) loom::log_detail::log(loom::LogLevel::Debug, "", __VA_ARGS__)
#define LOOM_LOG_INFO(...)  loom::log_detail::log(loom::LogLevel::Info,  "", __VA_ARGS__)
#define LOOM_LOG_WARN(...)  loom::log_detail::log(loom::LogLevel::Warn,  "", __VA_ARGS__)
#define LOOM_LOG_ERROR(...) loom::log_detail::log(loom::LogLevel::Error, "", __VA_ARGS__)
