cmake_minimum_required(VERSION 3.20)
project(loom VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Options ──────────────────────────────────────────────────────────────
option(LOOM_BUILD_TESTS   "Build unit tests" OFF)
option(LOOM_USE_LIBUV     "Enable libuv async HTTP client" OFF)
option(LOOM_USE_ARROW     "Enable Apache Arrow compute engine" OFF)
option(LOOM_USE_SQLITE    "Enable SQLite request queue" OFF)
option(LOOM_USE_GRPC      "Enable gRPC + Arrow Flight distributed layer" OFF)
option(LOOM_USE_PYBIND11  "Enable Python transform bridge" OFF)

# ── Find system packages ─────────────────────────────────────────────────
find_package(PkgConfig QUIET)

# jsoncpp (system-installed)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)

# libuv (optional)
if(LOOM_USE_LIBUV)
    pkg_check_modules(LIBUV REQUIRED libuv)
endif()

# SQLite3 (optional)
if(LOOM_USE_SQLITE)
    pkg_check_modules(SQLITE3 REQUIRED sqlite3)
endif()

# ── Engine library sources ───────────────────────────────────────────────
set(LOOM_SOURCES
    src/engine.cpp
    src/dag/parser.cpp
    src/dag/validator.cpp
    src/dag/executor.cpp
    src/dag/join_state.cpp
    src/queue/request_queue.cpp
    src/queue/stream_queue.cpp
    src/queue/rate_limiter.cpp
    src/http/client.cpp
    src/compute/transform.cpp
    src/compute/filter.cpp
    src/compute/aggregate.cpp
    src/compute/join.cpp
    src/compute/unnest.cpp
    src/compute/thread_pool.cpp
    src/connectors/s3_connector.cpp
    src/connectors/connector_registry.cpp
)

set(LOOM_LIBS ${JSONCPP_LIBRARIES})

if(LOOM_USE_LIBUV)
    list(APPEND LOOM_LIBS ${LIBUV_LIBRARIES})
endif()

if(LOOM_USE_SQLITE)
    list(APPEND LOOM_LIBS ${SQLITE3_LIBRARIES})
endif()

add_library(loom_engine STATIC ${LOOM_SOURCES})
target_include_directories(loom_engine PUBLIC
    include
    ${JSONCPP_INCLUDE_DIRS}
)
target_link_directories(loom_engine PUBLIC ${JSONCPP_LIBRARY_DIRS})

if(LOOM_USE_LIBUV)
    target_include_directories(loom_engine PUBLIC ${LIBUV_INCLUDE_DIRS})
    target_link_directories(loom_engine PUBLIC ${LIBUV_LIBRARY_DIRS})
    target_compile_definitions(loom_engine PUBLIC LOOM_HAS_LIBUV)
endif()

if(LOOM_USE_SQLITE)
    target_include_directories(loom_engine PUBLIC ${SQLITE3_INCLUDE_DIRS})
    target_link_directories(loom_engine PUBLIC ${SQLITE3_LIBRARY_DIRS})
    target_compile_definitions(loom_engine PUBLIC LOOM_HAS_SQLITE)
endif()

target_link_libraries(loom_engine PUBLIC ${LOOM_LIBS})

target_compile_definitions(loom_engine PUBLIC
    LOOM_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
    LOOM_VERSION_MINOR=${PROJECT_VERSION_MINOR}
    LOOM_VERSION_PATCH=${PROJECT_VERSION_PATCH}
)

# ── CLI executable ──────────────────────────────────────────────────────
add_executable(loom src/main.cpp)
target_link_libraries(loom PRIVATE loom_engine)

# ── Install ──────────────────────────────────────────────────────────────
install(TARGETS loom DESTINATION bin)
install(DIRECTORY include/loom DESTINATION include)
