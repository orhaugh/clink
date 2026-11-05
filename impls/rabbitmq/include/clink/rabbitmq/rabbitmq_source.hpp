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
// The broker ack is deferred to CHECKPOINT COMPLETION, not the barrier. snapshot_offset() seals
// the delivery tags emitted since the last barrier as that checkpoint's (CheckpointAckLedger);
// notify_checkpoint_complete() makes every interval at or below the completed id ready, and the
// next produce() turn, or close(), acknowledges them: one basic.ack(multiple=true) when the ready
// tags follow on from the last acknowledged one, tag by tag when an aborted interval lies below
// them. Every AMQP call stays on the produce() thread (the connection is not thread-safe).
// notify_checkpoint_aborted() releases the interval: it is never acknowledged on this channel, and
// the broker redelivers it when the channel closes. A crash before completion, or between a
// completion and its ack, leaves the messages unacknowledged for redelivery (duplicates are the
// at-least-once trade-off; AMQP has no seekable offset).
//
// The broker holds at most `prefetch` unacknowledged messages for the consumer, and none is
// acknowledged before its checkpoint completes, so prefetch is the most a subtask reads per
// checkpoint: a job that completes no checkpoint reads that many and waits.
//
// librabbitmq types do not appear here (amqp.h is confined to the .cpp via a pImpl).
class RabbitMqSource final : public Source<std::string> {
public:
    struct Options {
        RabbitMqConnParams conn;
        std::string queue;         // required: queue to consume from
        std::string consumer_tag;  // empty -> broker assigns
        bool declare_queue{true};  // queue.declare (durable) on open - idempotent, usable OOTB
        // basic.qos prefetch_count: the most unacknowledged messages held, so the most read per
        // checkpoint. 1..65535 (0 would be unlimited).
        std::uint16_t prefetch{10000};
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

    // Seal the tags emitted since the last barrier as this checkpoint's (no ack yet). Called on the
    // produce() thread (see dag.hpp drain_pending_barriers).
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    // Checkpoint completed/aborted (cluster CommitCheckpoint / AbortCheckpoint dispatch thread).
    // complete() makes the covered intervals ready; the basic.ack runs on the produce() thread.
    // aborted() releases the interval, which is never acknowledged on this channel.
    void notify_checkpoint_complete(CheckpointId ckpt_id) override;
    void notify_checkpoint_aborted(CheckpointId ckpt_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::rabbitmq
