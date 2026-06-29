#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/rabbitmq/connection_params.hpp"

namespace clink::rabbitmq {

// RabbitMQ / AMQP 0-9-1 source: basic.consume from a queue, emitting each message body as a
// std::string. Delivery is AT-LEAST-ONCE. Messages are consumed with manual ack (no_ack=false)
// and acked at the checkpoint barrier (snapshot_offset) via basic.ack(multiple=true) up to the
// highest delivery tag emitted so far. On failure the unacked messages are redelivered by the
// broker on reconnect, so nothing is lost (duplicates are the at-least-once trade-off).
//
// HONEST CAVEAT: the Source interface has no post-commit hook, so the ack happens at the barrier
// rather than after the global checkpoint commits. A crash in the narrow window between the
// barrier ack and the checkpoint completing could drop those messages. This matches AMQP's lack
// of seekable offsets - there is no offset to checkpoint+replay the way Kafka does; recovery
// relies on the broker redelivering whatever was left unacked.
//
// librabbitmq types do not appear here (amqp.h is confined to the .cpp via a pImpl).
class RabbitMqSource final : public Source<std::string> {
public:
    struct Options {
        RabbitMqConnParams conn;
        std::string queue;            // required: queue to consume from
        std::string consumer_tag;     // empty -> broker assigns
        bool declare_queue{true};     // queue.declare (durable) on open - idempotent, usable OOTB
        std::uint16_t prefetch{256};  // basic.qos prefetch_count (bounds unacked in flight)
        std::chrono::milliseconds poll_timeout{100};  // consume wait per produce() turn
        std::size_t max_batch_size{256};              // messages emitted per produce() turn
        std::string name{"rabbitmq_source"};
    };

    explicit RabbitMqSource(Options opts);
    ~RabbitMqSource() override;

    RabbitMqSource(const RabbitMqSource&) = delete;
    RabbitMqSource& operator=(const RabbitMqSource&) = delete;

    void open() override;
    bool produce(Emitter<std::string>& out) override;
    void close() override;
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    // At-least-once ack point: ack consumed messages up to the latest emitted delivery tag.
    // Called on the produce() thread (see dag.hpp drain_pending_barriers), so the basic.ack is
    // serialised with basic.consume on the single (non-thread-safe) AMQP connection.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::rabbitmq
