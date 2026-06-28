#pragma once

// Engine-level dead-letter queue (DLQ).
//
// A connector that hits a record it cannot process - a source decode failure, a
// sink permanent write failure - has historically two choices: drop+count (data
// silently vanishes, you get only a metric) or throw (the whole job crash-loops on
// one poison record). The DLQ is the third option: route the bad record, with its
// raw payload and provenance, to somewhere durable.
//
// It is an AMBIENT engine service, not a graph node: every operator/source/sink
// reaches it through RuntimeContext::report_bad_record(), so there are no
// per-connector side-output tags and no extra edges in the user's dataflow. The
// executor hands one DeadLetterQueue to every subtask (RuntimeContext), the same
// way it hands the logger and metrics registry.
//
// Default = LoggingDeadLetterQueue: bad records are logged (rate-limited, payload
// truncated) via the host logger, so they are at least VISIBLE with zero config.
// A job can swap in a NullDeadLetterQueue (drop silently, the legacy behaviour) or
// a sink-backed implementation (route to a Kafka topic / file / S3 - a follow-on).
//
// DELIVERY: best-effort. The DLQ write is NOT tied to the checkpoint barrier, so a
// record replayed after a failure is re-reported (the BadRecord carries enough
// provenance - connector + location - to dedup downstream). report() MUST NOT
// throw and MUST NOT block the pipeline: a broken DLQ must never take the job down.

#include <chrono>
#include <cstddef>
#include <mutex>
#include <string>
#include <string_view>

#include "clink/runtime/log_buffer.hpp"  // clink::logging::op_log, LogSeverity

namespace clink {

// One record a connector could not process, with enough context to triage it.
struct BadRecord {
    std::string payload;    // the raw record bytes (may be large / binary)
    std::string error;      // why it was rejected (decode error / write failure text)
    std::string connector;  // emitting connector, e.g. "mysql_cdc", "elasticsearch"
    std::string direction;  // "source" | "sink"
    std::string location;   // provenance, e.g. "db.table@binlog.000003:1547", "topic-3@4821"
};

// The DLQ seam. Implementations must be thread-safe (one instance is shared across
// all of a job's subtask threads) and report() must never throw or block.
class DeadLetterQueue {
public:
    virtual ~DeadLetterQueue() = default;
    virtual void report(const BadRecord& rec) = 0;
};

// Drops every bad record silently (a counter elsewhere still tracks volume). The
// explicit "I do not want bad records surfaced" choice; equivalent to the legacy
// drop-and-count behaviour.
class NullDeadLetterQueue final : public DeadLetterQueue {
public:
    void report(const BadRecord& /*rec*/) override {}
};

// Default DLQ: log each bad record (WARN) through the host logger so it reaches the
// node's sinks and the /api/v1/logs ring. Rate-limited so a poison storm cannot
// flood the log, and the payload is truncated in the log line (the full bytes are
// the job of a sink-backed DLQ). Thread-safe.
class LoggingDeadLetterQueue final : public DeadLetterQueue {
public:
    // logger may be null (in-process / pre-init): op_log then falls back to stderr +
    // the process log ring. max_per_window log lines are emitted per window; excess
    // is suppressed and summarised when the window rolls.
    explicit LoggingDeadLetterQueue(spdlog::logger* logger,
                                    std::size_t max_per_window = 100,
                                    std::chrono::milliseconds window = std::chrono::seconds{1},
                                    std::size_t max_payload_preview = 512)
        : logger_(logger),
          max_per_window_(max_per_window),
          window_(window),
          max_payload_preview_(max_payload_preview) {}

    void report(const BadRecord& rec) override {
        std::lock_guard<std::mutex> lk(mu_);
        const auto now = std::chrono::steady_clock::now();
        if (!window_open_ || now - window_start_ >= window_) {
            if (suppressed_ > 0) {
                clink::logging::op_log(
                    logger_,
                    clink::LogSeverity::Warn,
                    "dlq",
                    "suppressed " + std::to_string(suppressed_) +
                        " further bad record(s) in the last window (rate limit)");
            }
            window_start_ = now;
            window_open_ = true;
            count_in_window_ = 0;
            suppressed_ = 0;
        }
        if (count_in_window_ >= max_per_window_) {
            ++suppressed_;
            ++suppressed_total_;
            return;
        }
        ++count_in_window_;
        ++logged_total_;
        clink::logging::op_log(logger_, clink::LogSeverity::Warn, source_for_(rec), format_(rec));
    }

    // Cumulative counters (observability + tests): records actually logged vs
    // suppressed by the rate limit, across all windows.
    std::size_t logged_total() const {
        std::lock_guard<std::mutex> lk(mu_);
        return logged_total_;
    }
    std::size_t suppressed_total() const {
        std::lock_guard<std::mutex> lk(mu_);
        return suppressed_total_;
    }

private:
    std::string source_for_(const BadRecord& rec) const {
        return rec.connector.empty() ? std::string("dlq") : ("dlq:" + rec.connector);
    }

    std::string format_(const BadRecord& rec) const {
        std::string out = "bad record";
        if (!rec.direction.empty()) {
            out += " [" + rec.direction + "]";
        }
        if (!rec.location.empty()) {
            out += " at " + rec.location;
        }
        if (!rec.error.empty()) {
            out += ": " + rec.error;
        }
        if (!rec.payload.empty()) {
            out += " | payload=";
            if (rec.payload.size() > max_payload_preview_) {
                out.append(rec.payload, 0, max_payload_preview_);
                out += "...(" + std::to_string(rec.payload.size()) + " bytes)";
            } else {
                out += rec.payload;
            }
        }
        return out;
    }

    mutable std::mutex mu_;
    spdlog::logger* logger_{nullptr};
    std::size_t max_per_window_;
    std::chrono::milliseconds window_;
    std::size_t max_payload_preview_;
    std::chrono::steady_clock::time_point window_start_{};
    bool window_open_{false};
    std::size_t count_in_window_{0};
    std::size_t suppressed_{0};
    std::size_t logged_total_{0};
    std::size_t suppressed_total_{0};
};

}  // namespace clink
