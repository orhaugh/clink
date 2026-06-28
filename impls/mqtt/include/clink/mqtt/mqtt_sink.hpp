#pragma once

// MQTT sink: each input record is PUBLISH'd to a fixed topic. On the SQL path the
// record is a row_to_json_string JSON object; it is published verbatim as the
// message payload so the mqtt_source round-trips it back. Records are buffered and
// flushed on a count / byte / linger threshold and on every checkpoint barrier.
//
// DELIVERY depends on QoS:
//   - qos 0: fire-and-forget (no broker ack); fastest, lossy on any drop.
//   - qos 1: at-least-once to the broker (PUBACK awaited at flush).
//   - qos 2: exactly-once delivery to the broker (PUBCOMP awaited at flush).
// End-to-end the sink is AT-LEAST-ONCE regardless of QoS: a flush that fails
// after some messages reached the broker is replayed from the last checkpoint, so
// those messages are re-published (a downstream idempotent / keyed-dedup consumer
// must absorb the duplicates). flush() waits until every buffered message of the
// batch is acked (qos > 0) before returning, so everything buffered is durable in
// the broker by the barrier.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/mqtt/mqtt_client.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mqtt {

struct MqttSinkOptions {
    ConnectOptions conn;
    std::string topic;                     // target topic (required)
    int qos{1};                            // publish QoS (0/1/2)
    bool retain{false};                    // set the MQTT retain flag
    std::size_t batch_records{1000};       // flush after this many buffered records
    std::size_t max_bytes{0};              // flush after this many buffered payload bytes (0 = off)
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    std::chrono::milliseconds ack_timeout{30000};  // bound the qos>0 ack wait per flush
    std::string name{"mqtt_sink"};
};

class MqttSink : public Sink<std::string> {
public:
    explicit MqttSink(MqttSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.topic.empty()) {
            throw std::runtime_error(opts_.name + ": 'topic' is required");
        }
        if (opts_.qos < 0 || opts_.qos > 2) {
            throw std::runtime_error(opts_.name + ": 'qos' must be 0, 1 or 2");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
    }

    void open() override {
        conn_ = std::make_unique<MqttConnection>(opts_.conn);
        // Count completed publishes rather than track mids: an MQTT packet id is
        // 16-bit and wraps at 65535, so a per-mid set would collapse duplicate ids
        // within an oversized batch and under-count in-flight messages. A counter
        // is wraparound-immune (every PUBACK/PUBCOMP fires on_publish exactly once).
        conn_->set_publish_cb([this](int /*mid*/) {
            if (outstanding_ > 0) {
                --outstanding_;
            }
        });
    }

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
            outstanding_ = 0;
            for (const auto& rec : pending_) {
                conn_->publish(opts_.topic, rec.data(), rec.size(), opts_.qos, opts_.retain);
                if (opts_.qos > 0) {
                    ++outstanding_;
                }
            }
            drive_until_flushed_();
        } catch (...) {
            clink::metrics::connector::error_inc("mqtt", "sink");
            pending_.clear();
            pending_bytes_ = 0;
            outstanding_ = 0;
            // Drop the connection: a half-flushed pipeline leaves unread acks; a
            // fresh connection on re-open avoids a desynced in-flight window.
            conn_.reset();
            throw;  // job replays from the last checkpoint (at-least-once)
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("mqtt", n);
        clink::metrics::connector::bytes_out_inc("mqtt", bytes);
        clink::metrics::connector::commit_latency_observe("mqtt", static_cast<std::uint64_t>(dt));
        pending_.clear();
        pending_bytes_ = 0;
    }

    void close() override {
        if (conn_ != nullptr) {
            flush();
        }
        conn_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    // Drive the synchronous loop until the batch is durable: for qos > 0 until
    // every PUBLISH is acked (outstanding_ drains via the publish callback); for
    // qos 0 until the socket write buffer is flushed. Bounded by ack_timeout.
    void drive_until_flushed_() {
        const auto deadline = std::chrono::steady_clock::now() + opts_.ack_timeout;
        for (;;) {
            const bool done = opts_.qos > 0 ? outstanding_ == 0 : !conn_->want_write();
            if (done) {
                return;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(opts_.name + ": timed out flushing batch to broker");
            }
            int rc = conn_->loop(100, /*max_packets=*/1);
            if (rc != MOSQ_ERR_SUCCESS) {
                throw std::runtime_error(
                    opts_.name + ": connection error while flushing: " + mosquitto_strerror(rc));
            }
            if (std::string cb_err; conn_->consume_callback_error(cb_err)) {
                throw std::runtime_error(opts_.name + ": publish callback failed: " + cb_err);
            }
        }
    }

    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !pending_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    MqttSinkOptions opts_;
    std::unique_ptr<MqttConnection> conn_;
    std::vector<std::string> pending_;
    std::size_t outstanding_{0};  // un-acked publishes in the current flush (qos > 0)
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink::mqtt
