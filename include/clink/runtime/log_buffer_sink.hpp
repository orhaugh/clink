#pragma once

// spdlog sink that mirrors every formatted record into a clink LogBuffer ring,
// so the bounded in-memory buffer the HTTP /api/v1/logs endpoint serves stays
// fed from the single spdlog pipeline (console + file + ring can never
// diverge).
//
// Build-private: includes <spdlog/...>, consumed only by logging.cpp.
//
// It reads the log_msg fields directly (level, logger name, raw payload, time)
// rather than the pattern-formatted line, so LogRecord keeps the same four
// fields {ts_ms, level, source, message} the endpoint has always emitted:
//   - source  = the spdlog logger name (%n), which the facade sets per call
//   - message = the raw user payload (no timestamp/level prefix)
// The level string mapping is kept consistent with LogBuffer::level_rank_
// (which has no "trace" rank): trace and debug both map to "debug".

#include <chrono>
#include <mutex>
#include <string>

#include <spdlog/details/null_mutex.h>
#include <spdlog/sinks/base_sink.h>

#include "clink/runtime/log_buffer.hpp"

namespace clink::logging {

template <typename Mutex>
class LogBufferSink : public spdlog::sinks::base_sink<Mutex> {
public:
    explicit LogBufferSink(LogBuffer& buffer = LogBuffer::global()) : buffer_(&buffer) {}

protected:
    void sink_it_(const spdlog::details::log_msg& msg) override {
        LogRecord rec;
        rec.ts_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(msg.time.time_since_epoch())
                .count();
        rec.level = level_string(msg.level);
        rec.source.assign(msg.logger_name.data(), msg.logger_name.size());
        rec.message.assign(msg.payload.data(), msg.payload.size());
        buffer_->push(std::move(rec));
    }

    void flush_() override {}

private:
    // Map spdlog levels onto the ring's level vocabulary. Must agree with
    // LogBuffer::level_rank_ (debug < info < warn < error); there is no
    // "trace" rank, so trace collapses to "debug".
    static const char* level_string(spdlog::level::level_enum lvl) {
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

    LogBuffer* buffer_;
};

using LogBufferSinkMt = LogBufferSink<std::mutex>;

}  // namespace clink::logging
