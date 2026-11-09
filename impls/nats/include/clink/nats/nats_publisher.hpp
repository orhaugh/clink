#pragma once

// What the NATS JetStream sink needs of a JetStream context, behind an
// interface so its failure paths can be tested against a scripted server.
// nats.c stays in the .cpp that implements it.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "clink/nats/connection_params.hpp"

namespace clink::nats {

struct NatsPublisherSettings {
    NatsConnParams conn;
    std::string subject;
    int max_pending{4096};  // the library's own window, a backstop to the sink's
};

// What the server has answered since the publisher opened, cumulatively.
struct NatsAnswers {
    std::uint64_t acked{0};    // a positive PubAck: the stream stored the message
    std::uint64_t refused{0};  // a negative PubAck, or no stream bound to the subject
    std::uint64_t answered_bytes{0};
    std::string first_refusal;  // the server's error text
};

class NatsPublisher {
public:
    virtual ~NatsPublisher() = default;
    // Queues one message for an asynchronous publish. Throws when it cannot,
    // after which the message may or may not reach the server.
    virtual void publish(std::string_view body) = 0;
    // Waits at most `timeout` until the server has answered more than `seen`
    // publishes in all, at once when it already has; returns the answers so
    // far. Throws when the connection is closed and no answer can come.
    virtual NatsAnswers wait(std::uint64_t seen, std::chrono::milliseconds timeout) = 0;
    [[nodiscard]] virtual NatsAnswers answers() const = 0;
    // Closes the context and the connection without waiting. Never throws.
    virtual void close() noexcept = 0;
};

using NatsPublisherFactory =
    std::function<std::unique_ptr<NatsPublisher>(const NatsPublisherSettings&)>;

// A publisher over nats.c: connects (bounded by conn.connect_timeout, writes
// bounded by conn.write_timeout) and opens a JetStream context whose every
// asynchronous publish is answered through a per-message handler. Throws when
// it cannot connect.
[[nodiscard]] std::unique_ptr<NatsPublisher> connect_nats_publisher(const NatsPublisherSettings& s);

}  // namespace clink::nats
