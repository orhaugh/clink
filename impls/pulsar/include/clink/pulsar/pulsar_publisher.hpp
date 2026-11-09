#pragma once

// What the Pulsar sink needs of a producer, behind an interface so its failure
// paths can be tested against a scripted broker. The Pulsar C API stays in the
// .cpp that implements it.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "clink/pulsar/connection_params.hpp"

namespace clink::pulsar {

struct PulsarPublisherSettings {
    PulsarConnParams conn;
    std::string topic;
    bool batching{true};
    std::chrono::milliseconds send_timeout{30000};
    int max_pending{1000};  // the client's own queue, kept above the sink's window
};

// What the broker has answered since the publisher opened, cumulatively.
struct PulsarAnswers {
    std::uint64_t stored{0};  // the broker persisted the message
    std::uint64_t failed{0};  // the send failed: a timeout, or the broker's error
    std::uint64_t answered_bytes{0};
    std::string first_failure;  // the client's result for the first failure
};

class PulsarPublisher {
public:
    virtual ~PulsarPublisher() = default;
    // Hands one message to the producer for an asynchronous send. Throws when
    // it cannot, after which the message may or may not reach the broker.
    virtual void publish(std::string_view body) = 0;
    // Sends whatever the producer holds in a batch, without waiting.
    virtual void push() = 0;
    // Waits at most `timeout` until the broker has answered more than `seen`
    // messages in all, at once when it already has; returns the answers so far.
    virtual PulsarAnswers wait(std::uint64_t seen, std::chrono::milliseconds timeout) = 0;
    [[nodiscard]] virtual PulsarAnswers answers() const = 0;
    // Closes the producer and the client. The client fails what is still
    // unanswered and waits at most its operation timeout for the broker's
    // replies. Never throws.
    virtual void close() noexcept = 0;
};

using PulsarPublisherFactory =
    std::function<std::unique_ptr<PulsarPublisher>(const PulsarPublisherSettings&)>;

// A publisher over the Pulsar C client: connects and creates the producer
// (bounded by conn.operation_timeout_s). Throws when it cannot.
[[nodiscard]] std::unique_ptr<PulsarPublisher> connect_pulsar_publisher(
    const PulsarPublisherSettings& s);

}  // namespace clink::pulsar
