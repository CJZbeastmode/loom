#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <stdexcept>
#include <sstream>
#include <chrono>

namespace test_utils {

int passed = 0;
int failed = 0;
int skipped = 0;

struct TestCase {
    std::string suite;
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Registrar {
    Registrar(const std::string& suite, const std::string& name, std::function<void()> fn) {
        registry().push_back({suite, name, std::move(fn)});
    }
};

#define TOKENPASTE2(x, y) x ## y
#define TOKENPASTE(x, y) TOKENPASTE2(x, y)
#define TEST_IMPL(suite, name, id)                                             \
    static void TOKENPASTE(test_fn_, id)();                                    \
    static ::test_utils::Registrar TOKENPASTE(reg_, id)(                       \
        #suite, #name, TOKENPASTE(test_fn_, id));                              \
    static void TOKENPASTE(test_fn_, id)()
#define TEST(suite, name) TEST_IMPL(suite, name, __COUNTER__)

#define COLOR_GREEN  "\033[32m"
#define COLOR_RED    "\033[31m"
#define COLOR_YELLOW "\033[33m"
#define COLOR_RESET  "\033[0m"

inline void check_true(bool cond, const char* file, int line, const char* expr,
                       const std::string& msg = "") {
    if (!cond) {
        std::ostringstream oss;
        oss << file << ":" << line << ": " << COLOR_RED << "FAILED" << COLOR_RESET
            << "  " << expr;
        if (!msg.empty()) oss << " (" << msg << ")";
        throw std::runtime_error(oss.str());
    }
}

inline void check_eq_str(const std::string& a, const std::string& b,
                          const char* file, int line, const char* expr_a, const char* expr_b) {
    if (a != b) {
        std::ostringstream oss;
        oss << file << ":" << line << ": " << COLOR_RED << "FAILED" << COLOR_RESET
            << "  " << expr_a << " == " << expr_b
            << "\n    expected: '" << b << "'\n    actual:   '" << a << "'";
        throw std::runtime_error(oss.str());
    }
}

#define EXPECT_TRUE(cond)  ::test_utils::check_true((cond), __FILE__, __LINE__, #cond)
#define EXPECT_FALSE(cond) ::test_utils::check_true(!(cond), __FILE__, __LINE__, "!" #cond)

#define EXPECT_EQ(a, b) do {                                               \
    auto _va = (a); auto _vb = (b);                                        \
    if (!(_va == _vb)) {                                                   \
        std::ostringstream _oss;                                           \
        _oss << __FILE__ << ":" << __LINE__ << ": " COLOR_RED "FAILED" COLOR_RESET \
             << "  " #a " == " #b                                          \
             << "\n    expected: " << _vb << "\n    actual:   " << _va;    \
        throw std::runtime_error(_oss.str());                              \
    }                                                                      \
} while(0)

#define EXPECT_NE(a, b) do {                                               \
    auto _va = (a); auto _vb = (b);                                        \
    if (_va == _vb) {                                                      \
        std::ostringstream _oss;                                           \
        _oss << __FILE__ << ":" << __LINE__ << ": " COLOR_RED "FAILED" COLOR_RESET \
             << "  " #a " != " #b                                          \
             << "\n    both are: " << _va;                                 \
        throw std::runtime_error(_oss.str());                              \
    }                                                                      \
} while(0)

#define EXPECT_THROW(stmt, ex_type) do {                                   \
    bool _caught = false;                                                  \
    try { stmt; } catch (const ex_type&) { _caught = true; }               \
    catch (...) {}                                                         \
    if (!_caught) {                                                        \
        std::ostringstream _oss;                                           \
        _oss << __FILE__ << ":" << __LINE__ << ": " COLOR_RED "FAILED" COLOR_RESET \
             << "  " #stmt " throws " #ex_type;                            \
        throw std::runtime_error(_oss.str());                              \
    }                                                                      \
} while(0)

#define EXPECT_NO_THROW(stmt) do {                                         \
    try { stmt; } catch (const std::exception& e) {                        \
        std::ostringstream _oss;                                           \
        _oss << __FILE__ << ":" << __LINE__ << ": " COLOR_RED "FAILED" COLOR_RESET \
             << "  " #stmt " should not throw\n    threw: " << e.what();   \
        throw std::runtime_error(_oss.str());                              \
    } catch (...) {                                                        \
        std::ostringstream _oss;                                           \
        _oss << __FILE__ << ":" << __LINE__ << ": " COLOR_RED "FAILED" COLOR_RESET \
             << "  " #stmt " should not throw (unknown exception)";        \
        throw std::runtime_error(_oss.str());                              \
    }                                                                      \
} while(0)

inline int run_all() {
    auto& tests = registry();
    std::string current_suite;
    auto total_start = std::chrono::steady_clock::now();

    for (auto& t : tests) {
        if (t.suite != current_suite) {
            if (!current_suite.empty()) std::cout << std::endl;
            current_suite = t.suite;
            std::cout << COLOR_YELLOW << "[" << current_suite << "]" << COLOR_RESET << std::endl;
        }

        auto start = std::chrono::steady_clock::now();
        try {
            t.fn();
            passed++;
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start).count();
            std::cout << "  " << COLOR_GREEN << "PASS" << COLOR_RESET
                      << "  " << t.name;
            if (ms > 0) std::cout << " (" << ms << "ms)";
            std::cout << std::endl;
        } catch (const std::exception& e) {
            failed++;
            std::cout << "  " << COLOR_RED << "FAIL" << COLOR_RESET
                      << "  " << t.name << "\n    " << e.what() << std::endl;
        } catch (...) {
            failed++;
            std::cout << "  " << COLOR_RED << "FAIL" << COLOR_RESET
                      << "  " << t.name << " (unknown exception)" << std::endl;
        }
    }

    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - total_start).count();

    std::cout << "\n" << COLOR_YELLOW << "=== " << passed << " passed, "
              << failed << " failed (" << total_ms << "ms) ==="
              << COLOR_RESET << std::endl;

    return failed > 0 ? 1 : 0;
}

}  // namespace test_utils

int main() {
    return test_utils::run_all();
}
