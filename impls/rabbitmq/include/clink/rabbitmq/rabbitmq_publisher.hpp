#pragma once

// What the RabbitMQ sink needs of a channel in confirm mode, behind an
// interface so its failure paths can be tested against a scripted broker.
// librabbitmq stays in the .cpp that implements it.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "clink/rabbitmq/connection_params.hpp"

namespace clink::rabbitmq {

// What the broker said about the publishes, or why it said nothing.
struct PublisherEvent {
    enum class Kind : std::uint8_t {
        Ack,      // basic.ack: the broker took responsibility for the message or messages
        Nack,     // basic.nack or basic.reject: it did not
        Return,   // basic.return: a mandatory message no queue receives (its ack follows)
        Closed,   // the broker closed the channel or the connection
        Timeout,  // nothing came within the wait
        Lost,     // the connection failed, or was shut by cancel()
    };
    Kind kind{Kind::Ack};
    std::uint64_t delivery_tag{0};  // Ack and Nack
    bool multiple{false};           // Ack and Nack: every tag up to delivery_tag
    std::string text;               // the broker's reply code and text, or why nothing came
};

struct PublisherSettings {
    RabbitMqConnParams conn;
    std::string exchange;
    std::string routing_key;
    bool persistent{true};
    bool mandatory{true};
    std::string content_type;
};

class RabbitMqPublisher {
public:
    virtual ~RabbitMqPublisher() = default;
    // Sends one message. Throws when the write fails, after which the message
    // may or may not have reached the broker.
    virtual void publish(std::string_view body) = 0;
    // The next confirm, return or close, waiting at most `timeout`.
    virtual PublisherEvent next(std::chrono::milliseconds timeout) = 0;
    // Ends the read or write in flight; safe to call from another thread.
    virtual void cancel() noexcept = 0;
    // Closes the channel and the connection: with the close handshake, each
    // step bounded, when `orderly`, otherwise by closing the socket. Never
    // throws.
    virtual void close(bool orderly) noexcept = 0;
};

using RabbitMqPublisherFactory =
    std::function<std::unique_ptr<RabbitMqPublisher>(const PublisherSettings&)>;

// A publisher over librabbitmq: connects (bounded by conn.connect_timeout),
// logs in and opens a channel in confirm mode. Throws when it cannot.
[[nodiscard]] std::unique_ptr<RabbitMqPublisher> connect_rabbitmq_publisher(
    const PublisherSettings& s);

}  // namespace clink::rabbitmq
