#include "clink/runtime/log_buffer.hpp"

#include <algorithm>
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

std::vector<LogRecord> LogBuffer::tail(std::size_t limit,
                                       std::string_view level_filter,
                                       std::int64_t since_ms,
                                       std::string_view source_prefix) const {
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
        if (since_ms > 0 && rec.ts_ms <= since_ms) {
            continue;
        }
        if (!source_prefix.empty() && !std::string_view{rec.source}.starts_with(source_prefix)) {
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

std::vector<std::string> LogBuffer::distinct_sources() const {
    std::lock_guard lock(mu_);
    std::vector<std::string> sources;
    const std::size_t start = (head_ + capacity_ - size_) % capacity_;
    for (std::size_t i = 0; i < size_; ++i) {
        const auto& src = buf_[(start + i) % capacity_].source;
        if (std::find(sources.begin(), sources.end(), src) == sources.end()) {
            sources.push_back(src);
        }
    }
    std::sort(sources.begin(), sources.end());
    return sources;
}

LogBuffer& LogBuffer::global() {
    static LogBuffer instance;
    return instance;
}

// The clink::log facade and clink::logging::op_log are implemented in
// logging.cpp (which owns the spdlog pipeline). This translation unit keeps
// only the spdlog-free ring buffer so log_buffer.hpp stays includable by
// plugin operator code.

}  // namespace clink
