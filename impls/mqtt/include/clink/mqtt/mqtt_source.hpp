#pragma once

// MQTT source: subscribes to a topic (filter) and emits each received message as
// one std::string. By default the message payload is emitted verbatim (the clean
// round-trip with mqtt_sink); with include_topic=true each message is emitted as
// a JSON object {"topic":"...","payload":"..."} so a wildcard subscription can
// carry the per-message topic (the routing key for most MQTT deployments, e.g.
// sensors/+/temperature). include_topic assumes UTF-8 text payloads; for binary
// payloads use the verbatim mode.
//
// DELIVERY: MQTT has no position a source could store, so recovery is the
// broker's: the source holds a persistent session under a stable per-subtask
// client id, and the broker delivers again, when the session reconnects, every
// QoS 1 or 2 message it holds unacknowledged. The source therefore acknowledges
// a message (PUBACK, or PUBREC for QoS 2) only once a completed checkpoint
// covers it: snapshot_offset seals what arrived since the last barrier as that
// checkpoint's, notify_checkpoint_complete makes it ready, and the next
// produce() or close() sends the acknowledgements, in the order the messages
// arrived as MQTT requires. A crash before that leaves them unacknowledged, and
// they are delivered again.
//
// An acknowledgement belongs to the connection its message arrived on: after a
// reconnect the broker sends every unacknowledged message again, under packet
// identifiers it may since have reused, so what is still held from an earlier
// connection is never acknowledged on a later one. An aborted checkpoint
// reconnects for the same reason: MQTT has no negative acknowledgement, and
// only a new connection gets the broker to deliver those messages again. Both
// deliver again messages a later checkpoint may already cover, so the
// guarantee is at least once.
//
// How much one checkpoint interval can read is the session's in-flight window:
// the broker sends at most that many messages unacknowledged. Under MQTT 5 the
// source states it (receive_maximum); under MQTT 3.1.1 the broker's own setting
// decides (mosquitto's max_inflight_messages, 20 unless configured). Meanwhile
// the broker queues what it cannot send, and a broker that bounds that queue
// drops what overflows it.
//
// PARALLELISM: MQTT delivers every matching message to every subscriber, so a
// naive parallel subscription would duplicate. Two modes:
//   - default (shared_group empty): only subtask 0 connects and subscribes; the
//     other subtasks are dormant (idle, never emit). Correct at any parallelism,
//     but only one subtask does the work. Best run at parallelism 1.
//   - shared_group set: each subtask subscribes to "$share/<group>/<topic>" (a
//     shared subscription); the broker load-balances matching messages across the
//     subtasks. Requires a broker that supports shared subscriptions (mosquitto
//     does, for MQTT 3.1.1 clients too; it is an MQTT v5 spec feature).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/mqtt/mqtt_options.hpp"
#include "clink/mqtt/mqtt_protocol.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mqtt {

struct MqttSourceOptions {
    ConnectOptions conn;       // clean_session is not read: the session always persists
    std::string topic;         // topic filter to subscribe to (required)
    int qos{1};                // subscription QoS (0/1/2)
    std::string shared_group;  // non-empty -> "$share/<group>/<topic>" shared subscription
    bool include_topic{
        false};  // true -> emit {"topic":...,"payload":...} JSON, else payload verbatim
    std::chrono::milliseconds block{500};  // longest wait for a message (bounds cancel latency)
    // 5 (MQTT 5) or 4 (MQTT 3.1.1).
    std::uint8_t protocol_version{protocol::kMqtt5};
    // MQTT 5: how many messages the broker may send unacknowledged, which bounds
    // what one checkpoint interval can read; and how long the broker keeps the
    // session once its connection ends.
    std::uint16_t receive_maximum{10000};
    std::uint32_t session_expiry_s{protocol::kSessionNeverExpires};
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"mqtt_source"};
};

class MqttSource : public Source<std::string> {
public:
    explicit MqttSource(MqttSourceOptions opts);
    ~MqttSource() override;

    void open() override;
    bool produce(Emitter<std::string>& out) override;
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;
    void notify_checkpoint_complete(CheckpointId ckpt_id) override;
    void notify_checkpoint_aborted(CheckpointId ckpt_id) override;
    void close() override;

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }
    std::string name() const override;

    // The effective topic this subtask subscribes to.
    [[nodiscard]] std::string subscribe_topic() const;
    [[nodiscard]] bool dormant() const noexcept;
    // Messages delivered and not yet acknowledged, and acknowledgements sent.
    [[nodiscard]] std::size_t unacknowledged() const;
    [[nodiscard]] std::uint64_t acknowledged_total() const noexcept;
    // Connections made after the first: a lost connection, or an aborted checkpoint.
    [[nodiscard]] std::uint64_t reconnects() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::mqtt
