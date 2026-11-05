#pragma once

// The source's MQTT client: a subscriber that leaves acknowledgement to its
// caller. A QoS 1 message stays unacknowledged until acknowledge() sends its
// PUBACK, and a QoS 2 message until acknowledge() sends its PUBREC; the PUBREL
// that follows a PUBREC is answered with PUBCOMP here. A client library that
// acknowledges as soon as a message is handed over, as libmosquitto and Paho
// do, cannot hold one until a checkpoint covers it.
//
// MQTT 5 by default, MQTT 3.1.1 when asked. Only MQTT 5 lets a client say how
// many unacknowledged messages it will take (Receive Maximum); under MQTT 3.1.1
// the broker's own setting decides, which for mosquitto is 20 unless configured.
//
// Single-threaded: every call comes from the thread that owns the source.
// Socket and TLS detail stay behind the pImpl in mqtt_subscriber.cpp; tls=true
// needs the impl built with OpenSSL (CLINK_MQTT_TLS), and without it connect()
// says so.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/mqtt/mqtt_options.hpp"
#include "clink/mqtt/mqtt_protocol.hpp"

namespace clink::mqtt {

struct SubscriberOptions {
    // host, port, client_id, keepalive, credentials, TLS and the CONNACK
    // deadline. clean_session is not read: the session always persists, since
    // redelivery after a reconnect is what recovery stands on.
    ConnectOptions conn;
    std::uint8_t protocol_version{protocol::kMqtt5};
    // MQTT 5 only.
    std::uint16_t receive_maximum{10000};
    std::uint32_t session_expiry_s{protocol::kSessionNeverExpires};
    std::size_t max_packet_bytes{protocol::kMaxRemainingLength};
    std::string name{"mqtt_source"};  // error-message prefix
};

struct Delivery {
    std::string topic;
    std::string payload;
    std::uint16_t packet_id{0};  // QoS 1 and 2: what acknowledge() takes
    std::uint8_t qos{0};
    bool dup{false};
    bool retain{false};
};

class MqttSubscriber {
public:
    explicit MqttSubscriber(SubscriberOptions opts);
    ~MqttSubscriber();

    MqttSubscriber(const MqttSubscriber&) = delete;
    MqttSubscriber& operator=(const MqttSubscriber&) = delete;

    // Connect (TCP, TLS when asked, CONNECT) and wait for the CONNACK, within
    // conn.connack_timeout. A connection already open is dropped first. Returns
    // whether the broker resumed a session it held for this client id. Throws,
    // naming the stage, when the broker cannot be reached or refuses.
    bool connect();

    // Subscribe to one filter and wait for the SUBACK. Under MQTT 5 retained
    // messages come only with a subscription that is new, so a restart does not
    // receive them again. Messages that arrive meanwhile wait for the next
    // poll(). Returns the QoS granted; throws when refused or on a lost connection.
    int subscribe(const std::string& filter, int qos);

    // Read what arrives within `block` and append the messages to `out`,
    // returning as soon as one has. Sends the keepalive's PINGREQ when due and
    // answers each PUBREL. A message the broker sends again while it is still
    // unacknowledged on this connection (MQTT 3.1.1 brokers may) is not handed
    // over twice. Returns false once the connection is gone, with lost_reason()
    // saying why; throws ProtocolError on a malformed packet.
    bool poll(std::chrono::milliseconds block, std::vector<Delivery>& out);

    // PUBACK (QoS 1) or PUBREC (QoS 2) for a message this connection delivered.
    // MQTT requires acknowledgements in the order the messages arrived; that
    // order is the caller's to keep. Returns false when the send fails, and the
    // connection is then gone.
    bool acknowledge(std::uint16_t packet_id, std::uint8_t qos);

    // Send DISCONNECT (normal: the session stays with the broker) and close.
    // Best effort, and idempotent.
    void disconnect() noexcept;

    // Close without DISCONNECT, as a crash does.
    void drop() noexcept;

    [[nodiscard]] bool connected() const noexcept;
    [[nodiscard]] std::string lost_reason() const;
    // Messages delivered on this connection and not yet acknowledged.
    [[nodiscard]] std::size_t unacknowledged() const noexcept;
    // The keepalive in use: the broker's when an MQTT 5 broker sets one.
    [[nodiscard]] std::chrono::seconds keepalive() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::mqtt
