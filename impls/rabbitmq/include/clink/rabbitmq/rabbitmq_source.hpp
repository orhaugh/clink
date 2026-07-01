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
// std::string. Delivery is AT-LEAST-ONCE. Messages are consumed with manual ack (no_ack=false).
//
// The broker ack is deferred to CHECKPOINT COMMIT, not the barrier. snapshot_offset() records
// the highest delivery tag emitted before each barrier against that checkpoint id;
// notify_checkpoint_complete() (driven from the cluster's CommitCheckpoint path) marks every tag
// up to the committed checkpoint safe to ack, and the next produce() turn issues a single
// basic.ack(multiple=true) up to that watermark. This keeps all AMQP calls on the produce()
// thread (the connection is not thread-safe) while ensuring a message is acked only after the
// checkpoint that captured it is globally durable. notify_checkpoint_aborted() drops the pending
// record so an aborted checkpoint never acks - the broker redelivers those messages on reconnect.
// A crash before commit likewise leaves them unacked for redelivery. (AMQP has no seekable
// offset; recovery relies on broker redelivery, so duplicates are the at-least-once trade-off.)
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

    // Record the highest emitted delivery tag against this checkpoint id (no ack yet). Called on
    // the produce() thread (see dag.hpp drain_pending_barriers).
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    // Checkpoint committed/aborted (cluster CommitCheckpoint / AbortCheckpoint dispatch thread).
    // complete() advances the safe-to-ack watermark; the actual basic.ack runs on the produce()
    // thread. aborted() drops the pending record so those messages stay unacked for redelivery.
    void notify_checkpoint_complete(CheckpointId ckpt_id) override;
    void notify_checkpoint_aborted(CheckpointId ckpt_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::rabbitmq
