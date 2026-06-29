#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "clink/nats/connection_params.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::nats {

// NATS JetStream sink: publishes each std::string record to a subject. Delivery is
// AT-LEAST-ONCE. Messages are published asynchronously (js_PublishAsync) for throughput;
// flush()/on_barrier() calls js_PublishAsyncComplete to block until the server has acked every
// pending publish, and THROWS on timeout so the job replays from the last checkpoint rather than
// dropping data. JetStream de-dups within a publish window only via Nats-Msg-Id (not set here),
// so re-delivery on replay can duplicate (at-least-once); the SQL planner rejects exactly_once.
//
// Publishing to a subject requires a JetStream stream bound to that subject to exist (stream
// provisioning is an admin task, like a RabbitMQ exchange). nats.c types do not appear here.
class NatsSink final : public Sink<std::string> {
public:
    struct Options {
        NatsConnParams conn;
        std::string subject;                               // required
        int max_pending{4096};                             // async publish window (backpressure)
        std::chrono::milliseconds publish_timeout{30000};  // wait for pending acks on flush
        std::string name{"nats_sink"};
    };

    explicit NatsSink(Options opts);
    ~NatsSink() override;

    NatsSink(const NatsSink&) = delete;
    NatsSink& operator=(const NatsSink&) = delete;

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    void flush() override;
    void close() override;
    [[nodiscard]] std::string name() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::nats
