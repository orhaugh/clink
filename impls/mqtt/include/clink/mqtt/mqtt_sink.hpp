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
// End to end the sink is AT-LEAST-ONCE at qos 1 and 2: flush() waits until every
// buffered message of the batch is acked before returning, so everything buffered
// is durable in the broker by the barrier, and a flush that fails after some
// messages reached the broker is replayed from the last checkpoint, re-publishing
// those messages (a downstream idempotent / keyed-dedup consumer must absorb the
// duplicates). At qos 0 it is AT-MOST-ONCE: the broker acknowledges nothing, so a
// message the flush wrote to a connection that then dropped is lost while the
// checkpoint still completes.
//
// PROTOCOL. MQTT 5 unless protocol_version says 3.1.1. Under MQTT 5 every
// acknowledgement carries a reason code, and a message the broker refused (one
// its ACL denies, one over its size limit) fails the flush once every message
// is answered, naming the broker's reason. MQTT 3.1.1 has no such code: a broker
// that drops a message acknowledges it all the same (Mosquitto does, for a
// publish its ACL denies), and the sink cannot tell.
//
// FAILURES. A flush whose acknowledgements have not all come within ack_timeout,
// a connection that fails while a flush waits, and a publish the client refuses
// fail the flush, saying how many of its messages were acknowledged, refused and
// unacknowledged, whose outcome is unknown. A failure fails the sink: every
// later record, barrier and flush is refused, an empty flush included, until
// open() is called again. close() discards what is buffered and unacknowledged
// rather than waiting for it, and never throws. A wait looks at the task's
// cancel every 100 ms.
//
// BOUNDS. A flush holds at most batch_records records and max_bytes bytes (16
// MiB unless set), and a record longer than max_record_bytes (16 MiB unless set)
// fails the sink before it is published, rather than reaching a broker limit
// that would close the connection with the flush unknown.
//
// CONNECTION: every barrier and every flush with nothing to send services the
// connection, so the keepalive's ping goes out while the sink is idle; and a
// connection that dropped while nothing was in flight on it is reconnected
// before the next batch is published, which loses nothing.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/mqtt/mqtt_client.hpp"
#include "clink/mqtt/mqtt_protocol.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mqtt {

struct MqttSinkOptions {
    ConnectOptions conn;
    std::string topic;                // target topic (required)
    int qos{1};                       // publish QoS (0/1/2)
    bool retain{false};               // set the MQTT retain flag
    std::size_t batch_records{1000};  // flush after this many buffered records
    std::size_t max_bytes{0};         // flush after this many buffered payload bytes (0 = 16 MiB)
    std::size_t max_record_bytes{0};  // refuse a longer record (0 = 16 MiB)
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    std::chrono::milliseconds ack_timeout{30000};     // bound the qos>0 ack wait per flush
    std::uint8_t protocol_version{protocol::kMqtt5};  // kMqtt5 or kMqtt311
    std::string name{"mqtt_sink"};
};

class MqttSink : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxBytes = std::size_t{16} << 20;
    static constexpr std::size_t kDefaultMaxRecordBytes = std::size_t{16} << 20;

    explicit MqttSink(MqttSinkOptions opts);

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    void close() override;
    std::string name() const override { return opts_.name; }

    // How many times the sink reconnected a connection the broker had dropped
    // while nothing was in flight on it.
    [[nodiscard]] std::uint64_t reconnects() const noexcept { return reconnects_; }

private:
    void keep_alive_();
    void ensure_connected_();
    void drive_until_flushed_(std::size_t messages);
    [[nodiscard]] std::string tally_(std::size_t messages) const;
    [[nodiscard]] std::string connection_failure_(int rc) const;
    [[noreturn]] void fail_(const std::string& why);
    void require_not_failed_(const std::string& what) const;
    void require_usable_(const std::string& what) const;
    [[nodiscard]] bool cancel_requested_() const noexcept;
    [[nodiscard]] bool linger_elapsed_() const;

    MqttSinkOptions opts_;
    std::unique_ptr<MqttConnection> conn_;
    std::vector<std::string> pending_;
    std::size_t outstanding_{0};  // un-acked publishes in the current flush (qos > 0)
    std::size_t refused_{0};      // publishes of the current flush the broker refused
    std::string first_refusal_;   // the broker's reason for the first of them
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
    std::uint64_t reconnects_{0};
    std::string failure_;
};

}  // namespace clink::mqtt
