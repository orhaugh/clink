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

namespace clink {

struct LogRecord {
    std::int64_t ts_ms{};  // milliseconds since unix epoch
    std::string level;     // "debug", "info", "warn", "error"
    std::string source;    // free-form, e.g. "jm.register", "tm.run_task"
    std::string message;
};

class LogBuffer {
public:
    explicit LogBuffer(std::size_t capacity = 1024);

    // Append a record. Oldest is overwritten on overflow.
    void push(LogRecord rec);

    // Return the most recent `limit` records, oldest-first. If `level_filter`
    // is non-empty, only records whose level >= filter are returned (using
    // the standard ordering debug < info < warn < error). Unknown levels are
    // treated as "info".
    std::vector<LogRecord> tail(std::size_t limit, std::string_view level_filter) const;

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

namespace log {

// Convenience wrappers: append to LogBuffer::global() AND mirror to stderr
// (matching prior behaviour). The mirror keeps existing operator log-grepping
// workflows intact while the buffer powers the HTTP /logs endpoint.
void info(std::string_view source, std::string message);
void warn(std::string_view source, std::string message);
void error(std::string_view source, std::string message);

}  // namespace log

}  // namespace clink
