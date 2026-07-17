#pragma once

// Bounded, in-process ring buffer of recent log records.
//
// Purpose is to back the dashboard's /api/v1/logs endpoint: every cluster-
// lifecycle event we'd previously write to stderr is also appended here so
// the SPA can stream it. Stderr writes continue, so existing operator-style
// `journalctl` / container-log workflows aren't broken.
//
// Capacity is fixed at construction (default 1024). When full, the oldest
// record is overwritten. Push and tail both take a single mutex; the rates
// (a handful of records per cluster event) make a finer-grained ring lock
// overkill.

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace spdlog {
class logger;
}

namespace clink {

struct LogRecord {
    std::int64_t ts_ms{};  // milliseconds since unix epoch
    std::string level;     // "debug", "info", "warn", "error"
    std::string source;    // free-form, e.g. "coordinator.register", "worker.run_task"
    std::string message;
};

class LogBuffer {
public:
    explicit LogBuffer(std::size_t capacity = 1024);

    // Append a record. Oldest is overwritten on overflow.
    void push(LogRecord rec);

    // Return the most recent `limit` records, oldest-first. Filters are applied
    // BEFORE the limit trim so the result is the newest `limit` MATCHING
    // records:
    //   - level_filter: if non-empty, only records whose level >= filter
    //     (ordering debug < info < warn < error; unknown treated as "info").
    //   - since_ms: if > 0, only records with ts_ms > since_ms (follow/tail
    //     cursor for incremental polling).
    //   - source_prefix: if non-empty, only records whose source starts with
    //     it (per-component filter).
    std::vector<LogRecord> tail(std::size_t limit,
                                std::string_view level_filter,
                                std::int64_t since_ms = 0,
                                std::string_view source_prefix = {}) const;

    // Distinct source values currently held in the ring, sorted. Backs the
    // /api/v1/logs/components endpoint so a UI can build a component filter
    // without scanning every record.
    std::vector<std::string> distinct_sources() const;

    // Process-wide singleton. Each clink_node owns one.
    static LogBuffer& global();

private:
    static int level_rank_(std::string_view level);

    mutable std::mutex mu_;
    std::vector<LogRecord> buf_;
    std::size_t capacity_;
    std::size_t head_{0};  // next slot to write
    std::size_t size_{0};
};

// Operator-facing log severity, decoupled from spdlog's level enum so this
// (spdlog-free) header can be included by plugin operator code. Ordering and
// the four buckets match LogBuffer::level_rank_.
enum class LogSeverity : std::uint8_t { Debug, Info, Warn, Error };

namespace logging {

// Operator-scoped log seam that crosses the dlopen plugin boundary. `lg` is a
// host-owned spdlog logger threaded in by data (RuntimeContext::logger /
// RunnerContext::logger); logging through it reaches the host's sinks and the
// /api/v1/logs ring even when this call runs inside a plugin .so. When `lg` is
// null (in-process / pre-init) it falls back to stderr + the process ring.
// Declared here (spdlog only forward-declared) so the lightweight
// RuntimeContext seam can route operator logs without pulling spdlog into
// plugin translation units. Defined in logging.cpp.
void op_log(spdlog::logger* lg,
            LogSeverity level,
            std::string_view source,
            std::string_view message);

}  // namespace logging

namespace log {

// Convenience wrappers around the spdlog-backed logging core. Each routes
// through a source-named logger (so the call-site source becomes the record's
// %n and LogRecord.source) and lands in console + file + the /api/v1/logs ring.
// Before clink::logging::init() runs, they fall back to the legacy stderr +
// ring behaviour, so unit tests that never configure logging are unchanged.
//
// HOST-ONLY: do NOT call these from inside a plugin operator (they resolve the
// .so's private logging registry / ring, which the node never reads). Operators
// log via RuntimeContext's log_* helpers instead.
void trace(std::string_view source, std::string message);
void debug(std::string_view source, std::string message);
void info(std::string_view source, std::string message);
void warn(std::string_view source, std::string message);
void error(std::string_view source, std::string message);

}  // namespace log

}  // namespace clink
