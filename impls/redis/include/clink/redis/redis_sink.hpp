#pragma once

// Redis Streams sink: each input record is appended to a stream via XADD. On the
// SQL path the record is a row_to_json_string JSON object; it is stored under a
// single configurable field (default "v") so the redis_source round-trips it back
// verbatim. Optional MAXLEN caps the stream length (approximate trim by default).
//
// Delivery is AT-LEAST-ONCE: XADD is append-only with no producer dedup key, so a
// replay after a failed checkpoint re-appends the buffered batch (the source then
// sees duplicates, which a downstream idempotent consumer or keyed dedup must
// absorb). Records are flushed on a count / byte / linger threshold and on every
// checkpoint barrier, so everything buffered is durable in the stream by the
// barrier.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_client.hpp"

namespace clink::redis {

struct RedisSinkOptions {
    ConnectOptions conn;
    std::string stream;                    // target stream key (required)
    std::string field{"v"};                // field name holding each record's payload
    std::size_t maxlen{0};                 // XADD MAXLEN cap (0 = unbounded)
    bool approx_maxlen{true};              // MAXLEN ~ (approx, cheap) vs = (exact)
    std::size_t batch_records{1000};       // flush after this many buffered records
    std::size_t max_bytes{0};              // flush after this many buffered payload bytes (0 = off)
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    std::string name{"redis_sink"};
};

class RedisSink : public Sink<std::string> {
public:
    explicit RedisSink(RedisSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.stream.empty()) {
            throw std::runtime_error(opts_.name + ": 'stream' is required");
        }
        if (opts_.field.empty()) {
            throw std::runtime_error(opts_.name + ": 'field' must not be empty");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
    }

    void open() override { conn_ = std::make_unique<Connection>(opts_.conn); }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            if (pending_.empty()) {
                first_buffered_at_ = std::chrono::steady_clock::now();  // linger clock
            }
            pending_bytes_ += rec.value().size();
            pending_.push_back(rec.value());
            if (pending_.size() >= opts_.batch_records ||
                (opts_.max_bytes > 0 && pending_bytes_ >= opts_.max_bytes) || linger_elapsed_()) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (pending_.empty()) {
            return;
        }
        if (conn_ == nullptr) {
            throw std::runtime_error(opts_.name + ": flush() before open()");
        }
        const std::size_t n = pending_.size();
        const std::size_t bytes = pending_bytes_;
        const auto t0 = std::chrono::steady_clock::now();
        try {
            // Pipeline every XADD, then drain the replies in order: one network
            // round-trip for the whole batch instead of one per record.
            for (const auto& rec : pending_) {
                conn_->append(xadd_args_(rec));
            }
            for (std::size_t i = 0; i < n; ++i) {
                Reply r = conn_->get_reply();
                if (r.is_error()) {
                    throw std::runtime_error(opts_.name + ": XADD failed: " + r.error_text());
                }
            }
        } catch (...) {
            clink::metrics::connector::error_inc("redis", "sink");
            pending_.clear();
            pending_bytes_ = 0;
            throw;  // job replays from the last checkpoint (at-least-once)
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("redis", n);
        clink::metrics::connector::bytes_out_inc("redis", bytes);
        clink::metrics::connector::commit_latency_observe("redis", static_cast<std::uint64_t>(dt));
        pending_.clear();
        pending_bytes_ = 0;
    }

    void close() override {
        flush();
        conn_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    std::vector<std::string> xadd_args_(const std::string& payload) const {
        std::vector<std::string> args;
        args.reserve(7);
        args.emplace_back("XADD");
        args.push_back(opts_.stream);
        if (opts_.maxlen > 0) {
            args.emplace_back("MAXLEN");
            args.emplace_back(opts_.approx_maxlen ? "~" : "=");
            args.push_back(std::to_string(opts_.maxlen));
        }
        args.emplace_back("*");  // server-assigned id
        args.push_back(opts_.field);
        args.push_back(payload);
        return args;
    }

    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !pending_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    RedisSinkOptions opts_;
    std::unique_ptr<Connection> conn_;
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink::redis
