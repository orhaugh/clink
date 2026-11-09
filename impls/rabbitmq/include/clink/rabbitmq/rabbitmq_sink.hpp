#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/rabbitmq/connection_params.hpp"
#include "clink/rabbitmq/rabbitmq_publisher.hpp"

namespace clink::rabbitmq {

// RabbitMQ / AMQP 0-9-1 sink: basic.publish each std::string record to an
// exchange with a routing key, on a channel in publisher-confirm mode.
//
// Delivery is AT-LEAST-ONCE. Messages are published as they arrive,
// persistent unless set otherwise and mandatory unless set otherwise, and
// every checkpoint barrier waits until the broker has confirmed each one
// published before it. A confirm means the broker took responsibility for the
// message: for a persistent message on a durable queue, written to disk; on a
// quorum queue, accepted by a majority of its replicas. RabbitMQ has no
// producer dedup key, so the messages a restored job publishes again are
// enqueued again.
//
// Failures. A nack (or reject), a mandatory message returned because no queue
// receives it, the broker closing the channel or the connection, a lost
// connection, and confirms that do not all come within confirm_timeout each
// fail the flush, saying how many of the messages published since the last
// complete flush were confirmed, nacked, returned, and left unconfirmed, whose
// outcome is unknown. A failure fails the sink: every later record, barrier
// and flush is refused, an empty flush included, until open() is called again.
// close() discards what is unconfirmed rather than waiting for it, and never
// throws.
//
// Bounds. The connect, and then the handshake and login, each wait at most
// conn.connect_timeout. At most max_unconfirmed messages wait for confirms at
// once: past it the sink waits for confirms before it publishes more. A record
// longer than max_record_bytes fails the sink before it is published. A watch
// shuts the connection's socket when the task's cancel is requested during a
// publish or a wait, so neither outlasts the cancel: a broker that stops
// reading (a memory or disk alarm) would otherwise hold a publish for as long
// as the alarm lasts.
//
// librabbitmq types do not appear here (amqp.h is confined to the .cpp files).
class RabbitMqSink final : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxRecordBytes = std::size_t{16} << 20;

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
        // The longest a flush, or a wait at max_unconfirmed, waits for confirms.
        std::chrono::milliseconds confirm_timeout{30000};
        std::size_t max_unconfirmed{10000};  // messages awaiting confirms at once
        std::size_t max_record_bytes{0};     // 0 = 16 MiB
        std::string name{"rabbitmq_sink"};
        RabbitMqPublisherFactory publisher_factory;  // unset: a real broker connection
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
