#include "clink/runtime/logging.hpp"

#include <chrono>
#include <iostream>
#include <mutex>
#include <utility>
#include <vector>

#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "clink/runtime/compressing_rotating_file_sink.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/log_buffer_sink.hpp"

namespace clink::logging {

namespace {

constexpr const char* log_pattern = "[%Y-%m-%dT%H:%M:%S.%e] [%n] [%l] %v";

// Module state behind a function-local static (matches LogBuffer::global()'s
// idiom and avoids static-destruction-order hazards with spdlog's own globals).
struct LoggingState {
    std::mutex mu;
    std::shared_ptr<spdlog::logger> root;
    std::vector<spdlog::sink_ptr> sinks;
    bool async = false;
};

LoggingState& state() {
    static LoggingState s;
    return s;
}

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Ring-level string for a spdlog level. Must agree with LogBuffer::level_rank_
// and LogBufferSink::level_string (trace collapses to "debug").
const char* ring_level(spdlog::level::level_enum lvl) {
    switch (lvl) {
        case spdlog::level::trace:
        case spdlog::level::debug:
            return "debug";
        case spdlog::level::info:
            return "info";
        case spdlog::level::warn:
            return "warn";
        case spdlog::level::err:
        case spdlog::level::critical:
            return "error";
        default:
            return "info";
    }
}

spdlog::level::level_enum parse_level(std::string_view s, spdlog::level::level_enum fallback) {
    if (s == "trace") {
        return spdlog::level::trace;
    }
    if (s == "debug") {
        return spdlog::level::debug;
    }
    if (s == "info") {
        return spdlog::level::info;
    }
    if (s == "warn" || s == "warning") {
        return spdlog::level::warn;
    }
    if (s == "error" || s == "err") {
        return spdlog::level::err;
    }
    if (s == "critical") {
        return spdlog::level::critical;
    }
    if (s == "off") {
        return spdlog::level::off;
    }
    return fallback;
}

spdlog::level::level_enum to_spdlog(LogSeverity sev) {
    switch (sev) {
        case LogSeverity::Debug:
            return spdlog::level::debug;
        case LogSeverity::Warn:
            return spdlog::level::warn;
        case LogSeverity::Error:
            return spdlog::level::err;
        case LogSeverity::Info:
        default:
            return spdlog::level::info;
    }
}

// Legacy fallback path used before init() (and when an operator has no
// host-threaded logger): mirror to stderr and push the four-field record into
// the process ring. Byte-compatible with the pre-spdlog facade so unit tests
// that never call init() keep their exact behaviour.
void legacy_emit(spdlog::level::level_enum lvl, std::string_view source, std::string message) {
    const char* lvl_str = ring_level(lvl);
    std::cerr << '[' << lvl_str << "] " << source << ": " << message << '\n';
    LogRecord rec;
    rec.ts_ms = now_ms();
    rec.level = lvl_str;
    rec.source.assign(source.data(), source.size());
    rec.message = std::move(message);
    LogBuffer::global().push(std::move(rec));
}

// Route a host-side facade call through a source-named logger so the record's
// %n (and therefore LogRecord.source) is the call-site source. Host-only:
// touches the spdlog registry, which is the host's because the facade is only
// called from clink_core daemon/cluster code.
void facade_emit(spdlog::level::level_enum lvl, std::string_view source, std::string message) {
    std::shared_ptr<spdlog::logger> root;
    {
        std::lock_guard lk(state().mu);
        root = state().root;
    }
    if (!root) {
        legacy_emit(lvl, source, std::move(message));
        return;
    }
    logger(source)->log(lvl, message);
}

}  // namespace

void init(const LoggingConfig& cfg) {
    auto& st = state();
    std::lock_guard lk(st.mu);
    if (st.root) {
        st.root->warn("clink::logging::init called more than once; ignoring");
        return;
    }

    std::vector<spdlog::sink_ptr> sinks;
    if (cfg.console) {
        sinks.push_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
    }
    if (!cfg.file_path.empty()) {
        sinks.push_back(
            std::make_shared<CompressingRotatingFileSinkMt>(cfg.file_path,
                                                            cfg.max_file_size_mb * 1024UL * 1024UL,
                                                            cfg.max_files,
                                                            cfg.compress_rotated,
                                                            cfg.zstd_level));
    }
    // Always present so the bounded ring the /api/v1/logs endpoint serves is
    // fed from the same pipeline as console + file.
    sinks.push_back(std::make_shared<LogBufferSinkMt>(LogBuffer::global()));
    st.sinks = sinks;
    st.async = cfg.async;

    const std::string name = cfg.node_name.empty() ? "clink" : cfg.node_name;
    std::shared_ptr<spdlog::logger> root;
    if (cfg.async) {
        spdlog::init_thread_pool(cfg.async_queue_size, cfg.async_threads);
        root = std::make_shared<spdlog::async_logger>(name,
                                                      sinks.begin(),
                                                      sinks.end(),
                                                      spdlog::thread_pool(),
                                                      spdlog::async_overflow_policy::block);
    } else {
        root = std::make_shared<spdlog::logger>(name, sinks.begin(), sinks.end());
    }
    root->set_level(parse_level(cfg.level, spdlog::level::info));
    root->set_pattern(log_pattern);
    root->flush_on(spdlog::level::warn);

    st.root = root;
    spdlog::register_logger(root);
    spdlog::set_default_logger(root);
    spdlog::flush_every(std::chrono::seconds(1));
}

std::shared_ptr<spdlog::logger> logger(std::string_view component) {
    const std::string name(component);
    if (auto existing = spdlog::get(name)) {
        return existing;
    }
    auto& st = state();
    std::lock_guard lk(st.mu);
    // Re-check under the lock: another thread may have created it.
    if (auto existing = spdlog::get(name)) {
        return existing;
    }
    if (!st.root) {
        // Pre-init: hand back a private stderr logger so callers never get a
        // null. Not registered against the (yet unbuilt) sink set.
        auto fallback = std::make_shared<spdlog::logger>(
            name, std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
        fallback->set_pattern(log_pattern);
        return fallback;
    }
    std::shared_ptr<spdlog::logger> lg;
    if (st.async) {
        lg = std::make_shared<spdlog::async_logger>(name,
                                                    st.sinks.begin(),
                                                    st.sinks.end(),
                                                    spdlog::thread_pool(),
                                                    spdlog::async_overflow_policy::block);
    } else {
        lg = std::make_shared<spdlog::logger>(name, st.sinks.begin(), st.sinks.end());
    }
    lg->set_pattern(log_pattern);
    lg->flush_on(spdlog::level::warn);
    lg->set_level(st.root->level());
    spdlog::register_logger(lg);
    return lg;
}

std::shared_ptr<spdlog::logger> root_logger() {
    auto& st = state();
    std::lock_guard lk(st.mu);
    return st.root;
}

spdlog::logger* host_logger() noexcept {
    auto& st = state();
    std::lock_guard lk(st.mu);
    return st.root.get();
}

void shutdown() {
    auto& st = state();
    {
        std::lock_guard lk(st.mu);
        st.root.reset();
        st.sinks.clear();
    }
    spdlog::shutdown();
}

// Operator-scoped logging that crosses the plugin .so boundary. `lg` is a
// host-owned logger threaded in by data (RunnerContext::logger); we log
// through a transient sync logger over the HOST sinks so the record reaches
// the host's console + file + ring (and therefore /api/v1/logs) with %n set to
// the operator's source, regardless of which .so this code runs in. When `lg`
// is null (in-process / pre-init), fall back to the legacy stderr + ring path.
void op_log(spdlog::logger* lg,
            LogSeverity level,
            std::string_view source,
            std::string_view message) {
    const spdlog::level::level_enum lvl = to_spdlog(level);
    if (lg == nullptr) {
        legacy_emit(lvl, source, std::string(message));
        return;
    }
    if (!lg->should_log(lvl)) {
        return;
    }
    spdlog::logger tmp(std::string(source), lg->sinks().begin(), lg->sinks().end());
    tmp.set_level(lg->level());
    tmp.flush_on(spdlog::level::warn);
    tmp.log(lvl, message);
}

}  // namespace clink::logging

namespace clink::log {

void trace(std::string_view source, std::string message) {
    clink::logging::facade_emit(spdlog::level::trace, source, std::move(message));
}
void debug(std::string_view source, std::string message) {
    clink::logging::facade_emit(spdlog::level::debug, source, std::move(message));
}
void info(std::string_view source, std::string message) {
    clink::logging::facade_emit(spdlog::level::info, source, std::move(message));
}
void warn(std::string_view source, std::string message) {
    clink::logging::facade_emit(spdlog::level::warn, source, std::move(message));
}
void error(std::string_view source, std::string message) {
    clink::logging::facade_emit(spdlog::level::err, source, std::move(message));
}

}  // namespace clink::log
