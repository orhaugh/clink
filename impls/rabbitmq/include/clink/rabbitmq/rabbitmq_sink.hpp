#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/rabbitmq/connection_params.hpp"

namespace clink::rabbitmq {

// RabbitMQ / AMQP 0-9-1 sink: basic.publish each std::string record to an exchange with a
// routing key. Delivery is AT-LEAST-ONCE: messages are published persistent (delivery_mode=2)
// on a channel in publisher-confirm mode (confirm.select); flush()/on_barrier() block until the
// broker has confirmed (basic.ack) every outstanding publish, and THROW on a nack or timeout so
// the job replays from the last checkpoint rather than dropping data. Publishes are mandatory by
// default, so a message no queue is bound to receive comes back as a basic.return and fails the
// flush too, instead of being confirmed and dropped by the exchange. Re-delivery on replay can
// duplicate (at-least-once); RabbitMQ has no producer dedup key, so exactly-once is rejected by
// the SQL planner.
//
// librabbitmq types do not appear here (amqp.h is confined to the .cpp via a pImpl).
class RabbitMqSink final : public Sink<std::string> {
public:
    struct Options {
        RabbitMqConnParams conn;
        std::string exchange;     // default "" = the default (direct) exchange
        std::string routing_key;  // required: routing key / queue name on the default exchange
        bool persistent{true};    // delivery_mode 2 (survives broker restart) vs 1
        // basic.publish mandatory flag. True: a message no queue is bound to
        // receive is returned and fails the flush. False: the exchange drops
        // it and still confirms it, so it is lost without an error.
        bool mandatory{true};
        std::string content_type{"application/json"};
        std::chrono::milliseconds confirm_timeout{30000};  // wait for broker confirms
        std::string name{"rabbitmq_sink"};
    };

    explicit RabbitMqSink(Options opts);
    ~RabbitMqSink() override;

    RabbitMqSink(const RabbitMqSink&) = delete;
    RabbitMqSink& operator=(const RabbitMqSink&) = delete;

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

}  // namespace clink::rabbitmq
