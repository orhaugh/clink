#pragma once

// clink::logging - the spdlog-backed logging core.
//
// This header is deliberately spdlog-FREE at the API surface: it only
// forward-declares spdlog::logger and hands it out by (smart) pointer, so it
// can be included by clink_core daemon/cluster code AND by the host binaries
// (clink_node) that link clink_core PRIVATE-ly and therefore do not see
// spdlog's include path. The implementation (logging.cpp) is the only
// translation unit that includes <spdlog/...>.
//
// The lightweight clink::log facade (info/warn/error/debug/trace) and the
// op_log operator seam are declared in log_buffer.hpp; their implementations
// live in logging.cpp alongside this module.

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace spdlog {
class logger;
}

namespace clink::logging {

// One-time logging configuration. Defaults make a process behave as it did
// before spdlog adoption: info level, console (stderr) on, no file sink, the
// in-memory ring (LogBufferSink) always present so /api/v1/logs keeps working.
struct LoggingConfig {
    // Level as a lowercase string ("trace".."error" / "off"); parsed by init().
    // String-typed so this header stays spdlog-free for non-core consumers.
    std::string level = "info";
    std::string node_name;  // root logger %n, e.g. "jm" or "tm@<id>"; empty -> "clink"

    // File sink. Empty file_path -> no file sink (console + ring only).
    std::string file_path;
    std::size_t max_file_size_mb = 50;
    std::size_t max_files = 10;
    bool compress_rotated = true;
    int zstd_level = 3;  // 1..19

    bool console = true;  // stderr colour sink

    // Async logging (default on): record formatting + sink writes (including
    // rotation/compression) move onto a background worker pool, off the
    // daemon/operator threads. The flush_every thread + the worker pool are
    // the only logging-owned threads and are joined by shutdown().
    bool async = true;
    std::size_t async_queue_size = 8192;
    std::size_t async_threads = 1;
};

// Idempotent: the first call builds the shared sink set and the root logger;
// later calls are a no-op (and warn). Safe to call before any logging.
void init(const LoggingConfig& cfg);

// Named logger sharing the root's sink set + thread pool, registered so a
// second call with the same name returns the same logger. Use for rich daemon
// subsystems. Before init(), returns a plain stderr logger so callers never
// get a null.
std::shared_ptr<spdlog::logger> logger(std::string_view component);

// The root logger built by init() (nullptr before init()).
std::shared_ptr<spdlog::logger> root_logger();

// Raw non-owning pointer to the root logger, for threading across the plugin
// boundary by data (see RunnerContext::logger). nullptr before init().
spdlog::logger* host_logger() noexcept;

// Flush every logger and join the worker/flush threads via spdlog::shutdown().
// Call once at clean process teardown. NOT safe to call from a signal handler.
void shutdown();

}  // namespace clink::logging
