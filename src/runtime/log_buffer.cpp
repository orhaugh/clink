#include "clink/runtime/log_buffer.hpp"

#include <iostream>
#include <utility>

namespace clink {

LogBuffer::LogBuffer(std::size_t capacity) : capacity_(capacity == 0 ? 1 : capacity) {
    buf_.resize(capacity_);
}

void LogBuffer::push(LogRecord rec) {
    std::lock_guard lock(mu_);
    buf_[head_] = std::move(rec);
    head_ = (head_ + 1) % capacity_;
    if (size_ < capacity_) {
        ++size_;
    }
}

int LogBuffer::level_rank_(std::string_view level) {
    if (level == "debug")
        return 0;
    if (level == "warn")
        return 2;
    if (level == "error")
        return 3;
    return 1;  // "info" and unknown
}

std::vector<LogRecord> LogBuffer::tail(std::size_t limit, std::string_view level_filter) const {
    std::lock_guard lock(mu_);
    std::vector<LogRecord> out;
    if (size_ == 0 || limit == 0) {
        return out;
    }
    const int min_rank = level_filter.empty() ? -1 : level_rank_(level_filter);
    out.reserve(std::min(limit, size_));
    // Walk oldest -> newest. Oldest index = (head_ - size_) % capacity_.
    std::size_t start = (head_ + capacity_ - size_) % capacity_;
    for (std::size_t i = 0; i < size_; ++i) {
        const auto& rec = buf_[(start + i) % capacity_];
        if (min_rank >= 0 && level_rank_(rec.level) < min_rank) {
            continue;
        }
        out.push_back(rec);
    }
    // If more than limit matched, keep the most recent `limit` entries.
    if (out.size() > limit) {
        out.erase(out.begin(), out.end() - static_cast<std::ptrdiff_t>(limit));
    }
    return out;
}

LogBuffer& LogBuffer::global() {
    static LogBuffer instance;
    return instance;
}

namespace log {

namespace {

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void emit(std::string level, std::string_view source, std::string message) {
    // Mirror to stderr so existing operator-log tooling keeps seeing events.
    // Format matches the prior std::cerr lines closely enough that grep
    // patterns are preserved.
    std::cerr << '[' << level << "] " << source << ": " << message << '\n';
    LogRecord rec;
    rec.ts_ms = now_ms();
    rec.level = std::move(level);
    rec.source = std::string(source);
    rec.message = std::move(message);
    LogBuffer::global().push(std::move(rec));
}

}  // namespace

void info(std::string_view source, std::string message) {
    emit("info", source, std::move(message));
}

void warn(std::string_view source, std::string message) {
    emit("warn", source, std::move(message));
}

void error(std::string_view source, std::string message) {
    emit("error", source, std::move(message));
}

}  // namespace log

}  // namespace clink
