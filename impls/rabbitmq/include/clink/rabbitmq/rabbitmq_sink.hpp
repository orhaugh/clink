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
// the job replays from the last checkpoint rather than dropping data. Re-delivery on replay can
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
    void flush() override;
    void close() override;
    [[nodiscard]] std::string name() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::rabbitmq
