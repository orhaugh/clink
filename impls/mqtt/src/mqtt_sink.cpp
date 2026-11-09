// MQTT sink. See mqtt_sink.hpp.

#include "clink/mqtt/mqtt_sink.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "fault_points.hpp"

namespace clink::mqtt {

namespace {

constexpr const char* kLabel = "mqtt";

MqttSinkOptions checked(MqttSinkOptions o) {
    if (o.topic.empty()) {
        throw std::runtime_error(o.name + ": 'topic' is required");
    }
    if (o.qos < 0 || o.qos > 2) {
        throw std::runtime_error(o.name + ": 'qos' must be 0, 1 or 2");
    }
    if (o.ack_timeout.count() <= 0) {
        throw std::runtime_error(o.name + ": the acknowledgement timeout must be positive");
    }
    if (o.protocol_version != protocol::kMqtt5 && o.protocol_version != protocol::kMqtt311) {
        throw std::runtime_error(o.name + ": the protocol version must be MQTT 5 or 3.1.1");
    }
    if (o.batch_records == 0) {
        o.batch_records = 1;
    }
    if (o.max_bytes == 0) {
        o.max_bytes = MqttSink::kDefaultMaxBytes;
    }
    if (o.max_record_bytes == 0) {
        o.max_record_bytes = MqttSink::kDefaultMaxRecordBytes;
    }
    return o;
}

}  // namespace

MqttSink::MqttSink(MqttSinkOptions opts) : opts_(checked(std::move(opts))) {}

void MqttSink::open() {
    conn_.reset();
    pending_.clear();
    pending_bytes_ = 0;
    outstanding_ = 0;
    refused_ = 0;
    first_refusal_.clear();
    failure_.clear();
    conn_ = std::make_unique<MqttConnection>(opts_.conn, opts_.protocol_version);
    // Count completed publishes rather than track mids: an MQTT packet id is
    // 16-bit and wraps at 65535, so a per-mid set would collapse duplicate ids
    // within an oversized batch and under-count in-flight messages. A counter
    // is wraparound-immune (every PUBACK/PUBCOMP fires on_publish exactly once).
    // A reason code of 0x80 or above (MQTT 5 only) is the broker refusing the
    // message: that answers it too, and the flush fails once all are answered.
    conn_->set_publish_cb([this](int /*mid*/, int reason_code) {
        if (outstanding_ > 0) {
            --outstanding_;
        }
        if (reason_code >= 0x80 && refused_++ == 0) {
            first_refusal_ = mosquitto_reason_string(reason_code);
        }
    });
}

void MqttSink::require_not_failed_(const std::string& what) const {
    if (!failure_.empty()) {
        throw std::runtime_error(opts_.name + ": " + what + " refused: the sink failed earlier (" +
                                 failure_ +
                                 ") and has not been opened again, so nothing more is published");
    }
}

void MqttSink::require_usable_(const std::string& what) const {
    require_not_failed_(what);
    if (conn_ == nullptr) {
        throw std::runtime_error(opts_.name + ": " + what + " before open()");
    }
}

void MqttSink::fail_(const std::string& why) {
    failure_ = why;
    clink::metrics::connector::error_inc(kLabel, "sink");
    throw std::runtime_error(opts_.name + ": " + why);
}

bool MqttSink::cancel_requested_() const noexcept {
    return runtime() != nullptr && runtime()->cancel_requested();
}

void MqttSink::on_data(const Batch<std::string>& batch) {
    require_usable_("a record");
    for (const auto& rec : batch) {
        if (rec.value().size() > opts_.max_record_bytes) {
            fail_("a record is " + std::to_string(rec.value().size()) +
                  " bytes, more than max_record_bytes (" + std::to_string(opts_.max_record_bytes) +
                  ")");
        }
        if (pending_.empty()) {
            first_buffered_at_ = std::chrono::steady_clock::now();  // linger clock
        }
        pending_bytes_ += rec.value().size();
        pending_.push_back(rec.value());
        if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes ||
            linger_elapsed_()) {
            flush();
        }
    }
}

void MqttSink::on_barrier(CheckpointBarrier /*b*/) {
    require_usable_("a checkpoint barrier");
    const bool writes = !pending_.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterAck);
    }
}

// What became of a flush's messages when it failed.
std::string MqttSink::tally_(std::size_t messages) const {
    if (opts_.qos == 0) {
        return "the flush's " + std::to_string(messages) +
               " messages were handed to the connection, and at qos 0 the broker acknowledges "
               "none, so none is known to have arrived. The job restarts from its last "
               "checkpoint and publishes its records again";
    }
    const std::size_t unknown = std::min(messages, outstanding_);
    const std::size_t refused = std::min(refused_, messages - unknown);
    std::string out = "of the flush's " + std::to_string(messages) + " messages, " +
                      std::to_string(messages - unknown - refused) + " were acknowledged";
    if (refused > 0) {
        out +=
            ", " + std::to_string(refused) + " were refused by the broker (" + first_refusal_ + ")";
    }
    return out + " and the other " + std::to_string(unknown) +
           " are unacknowledged and may or may not have reached the broker. The job restarts "
           "from its last checkpoint and publishes its records again";
}

// A connection that failed while a flush waited, with the broker's reason when
// it ended the connection with one (MQTT 5).
std::string MqttSink::connection_failure_(int rc) const {
    const std::string waited =
        opts_.qos > 0 ? "the broker's acknowledgements" : "the connection to take the flush";
    const std::string reason = conn_->disconnect_reason();
    if (!reason.empty()) {
        return "the broker ended the connection while waiting for " + waited + " (" + reason + ")";
    }
    return "the connection failed while waiting for " + waited + " (" + mosquitto_strerror(rc) +
           ")";
}

void MqttSink::flush() {
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    require_not_failed_("a flush");
    if (pending_.empty()) {
        keep_alive_();
        return;
    }
    if (conn_ == nullptr) {
        throw std::runtime_error(opts_.name + ": flush() before open()");
    }
    if (cancel_requested_()) {
        fail_("cancelled before publishing to '" + opts_.topic + "'");
    }
    const std::size_t n = pending_.size();
    const std::size_t bytes = pending_bytes_;
    const auto t0 = std::chrono::steady_clock::now();
    ensure_connected_();
    outstanding_ = 0;
    refused_ = 0;
    first_refusal_.clear();
    std::size_t handed = 0;
    for (const auto& rec : pending_) {
        try {
            conn_->publish(opts_.topic, rec.data(), rec.size(), opts_.qos, opts_.retain);
        } catch (const std::exception& e) {
            fail_(std::string{"publishing failed ("} + e.what() + ") after " +
                  std::to_string(handed) + " of the flush's " + std::to_string(n) +
                  " messages were handed to the connection; " + tally_(handed));
        }
        ++handed;
        if (opts_.qos > 0) {
            ++outstanding_;
        }
    }
    (void)CLINK_FAULT_POINT(points::kSinkBeforeAck);
    drive_until_flushed_(n);
    if (refused_ > 0) {
        fail_("the broker refused " + std::to_string(refused_) + " of the flush's " +
              std::to_string(n) + " messages (" + first_refusal_ + ") and acknowledged the other " +
              std::to_string(n - refused_) +
              ". The job restarts from its last checkpoint and publishes its records again");
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::records_out_inc(kLabel, n);
    clink::metrics::connector::bytes_out_inc(kLabel, bytes);
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
    pending_.clear();
    pending_bytes_ = 0;
}

void MqttSink::close() {
    // What is buffered or unacknowledged is not waited for: a clean end of input
    // flushes before close(), and a cancelled or failed task must not wait on the
    // broker. Destroying the connection closes its socket at once.
    pending_.clear();
    pending_bytes_ = 0;
    outstanding_ = 0;
    conn_.reset();
}

// libmosquitto sends the keepalive's PINGREQ from its loop, and the sink runs
// the loop to flush a batch, so a sink with nothing to publish for one and a
// half keepalives was disconnected by the broker and failed the job at its next
// flush: a job whose sink publishes rarely, an alert's, restarted every ninety
// seconds of quiet. Every barrier and every empty flush now services the
// connection, without blocking: a ping when one is due, and the broker's reply.
void MqttSink::keep_alive_() {
    if (conn_ != nullptr) {
        (void)conn_->loop(0, 1);
    }
}

// A connection that dropped while the sink was idle (no barrier came within the
// keepalive, or the broker restarted) carried nothing in flight, so it loses
// nothing: it is reconnected before the batch is published on it, and only a
// reconnect that fails fails the job. One service first, so a socket the broker
// closed since the last one is seen as closed.
void MqttSink::ensure_connected_() {
    if (conn_->connected() && conn_->loop(0, 1) == MOSQ_ERR_SUCCESS && conn_->connected()) {
        return;
    }
    const int rc = conn_->reconnect_and_await(opts_.conn.connack_timeout);
    if (rc != MOSQ_ERR_SUCCESS) {
        fail_(std::string{"reconnecting before a flush failed ("} + mosquitto_strerror(rc) +
              "); nothing of the flush was sent");
    }
    ++reconnects_;
}

// Drive the synchronous loop until the batch is durable: for qos > 0 until every
// PUBLISH is acked (outstanding_ drains via the publish callback); for qos 0
// until the socket write buffer is flushed. Bounded by ack_timeout, in slices of
// at most 100 ms, between which the task's cancel is looked at.
void MqttSink::drive_until_flushed_(std::size_t messages) {
    const auto deadline = std::chrono::steady_clock::now() + opts_.ack_timeout;
    for (;;) {
        const bool done = opts_.qos > 0 ? outstanding_ == 0 : !conn_->want_write();
        if (done) {
            return;
        }
        if (cancel_requested_()) {
            fail_(std::string{"the task was cancelled while waiting for "} +
                  (opts_.qos > 0 ? "the broker's acknowledgements"
                                 : "the connection to take the flush") +
                  "; " + tally_(messages));
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            fail_(std::string{opts_.qos > 0 ? "the broker had not acknowledged every message"
                                            : "the connection had not taken every message"} +
                  " within the acknowledgement timeout (" +
                  std::to_string(opts_.ack_timeout.count()) + " ms); " + tally_(messages));
        }
        const int rc = conn_->loop(100, /*max_packets=*/1);
        if (rc != MOSQ_ERR_SUCCESS) {
            fail_(connection_failure_(rc) + "; " + tally_(messages));
        }
        if (std::string cb_err; conn_->consume_callback_error(cb_err)) {
            fail_("the publish callback failed (" + cb_err + "); " + tally_(messages));
        }
    }
}

bool MqttSink::linger_elapsed_() const {
    return opts_.max_age.count() > 0 && !pending_.empty() &&
           std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
}

}  // namespace clink::mqtt
