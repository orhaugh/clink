#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include "clink/nats/connection_params.hpp"
#include "clink/nats/nats_publisher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::nats {

// NATS JetStream sink: publishes each std::string record to a subject, which a
// JetStream stream must be bound to (provisioning the stream is an admin task,
// like a RabbitMQ exchange).
//
// Delivery is AT-LEAST-ONCE. Messages are published asynchronously as they
// arrive, and every checkpoint barrier waits until the server has answered each
// one published before it. A positive answer (PubAck) means the stream stored
// the message. JetStream de-duplicates only by Nats-Msg-Id, which the sink does
// not set, so the messages a restored job publishes again are stored again.
//
// Failures. A negative answer, or a publish no stream receives, fails the
// flush once every message is answered; answers that do not all come within
// publish_timeout, a publish the library refuses, and a closed connection fail
// it at once. Each failure says how many of the messages published since the
// last complete flush were stored, refused, and left unanswered, whose outcome
// is unknown. A failure fails the sink: every later record, barrier and flush is
// refused, an empty flush included, until open() is called again. close()
// discards what is unanswered rather than waiting for it, and never throws.
//
// Bounds. At most max_pending messages, and max_pending_bytes of them, await
// answers at once: past either the sink waits for answers, within
// publish_timeout, before it publishes more. The connect waits at most
// conn.connect_timeout and a write to the server at most conn.write_timeout, so
// a server that stops reading cannot hold a publish. Waits are sliced, so a
// cancelled task stops waiting at once, and during a stalled write within
// conn.write_timeout.
//
// nats.c types do not appear here.
class NatsSink final : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxPendingBytes = std::size_t{64} << 20;

    struct Options {
        NatsConnParams conn;
        std::string subject;                               // required
        int max_pending{4096};                             // messages awaiting answers at once
        std::size_t max_pending_bytes{0};                  // their bytes (0 = 64 MiB)
        std::chrono::milliseconds publish_timeout{30000};  // the longest a wait for answers takes
        std::string name{"nats_sink"};
        NatsPublisherFactory publisher_factory;  // unset: a real server connection
    };

    explicit NatsSink(Options opts);
    ~NatsSink() override;

    NatsSink(const NatsSink&) = delete;
    NatsSink& operator=(const NatsSink&) = delete;

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    void close() override;
    [[nodiscard]] std::string name() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::nats
