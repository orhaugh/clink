#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/pulsar/connection_params.hpp"

namespace clink::pulsar {

// Apache Pulsar source: subscribes to a topic and emits each message body as a std::string.
// Delivery is AT-LEAST-ONCE, and a restart is replayed in one of two ways:
//
//   * A Shared subscription (the default) lets parallel subtasks share it, each acking only its
//     own messages. The subscription cursor is durable server-side, so after a failure Pulsar
//     redelivers everything left unacked, and restore_offset is a no-op. The broker hands a
//     restarted consumer its unacknowledged messages when it gets to them, which can be after
//     newer ones, so the replay is not in topic order.
//   * An Exclusive subscription (subscription_type "exclusive") has this subtask as its only
//     consumer. Each barrier records the id of the last message emitted before it as operator
//     state, and a restore seeks the subscription back to that message, so the restart replays
//     the topic in order from the message after it; anything at or before the recorded message
//     that still arrives is acknowledged and not emitted again. It needs parallelism 1 and a
//     non-partitioned topic, and the broker must still hold the messages after the recorded one,
//     which a restore from an older checkpoint (a savepoint, say) can reach back for.
//
// The ack is deferred to CHECKPOINT COMMIT, not the barrier. snapshot_offset() buckets the
// messages emitted before each barrier against that checkpoint id; notify_checkpoint_complete()
// (driven from the cluster's CommitCheckpoint path) marks a bucket's messages safe to ack, and
// the next produce() turn issues their pulsar_consumer_acknowledge - so a message is acked only
// after the checkpoint that captured it is globally durable. All pulsar C calls stay on the
// produce() thread (one consumer). notify_checkpoint_aborted() frees the bucket WITHOUT acking,
// so Pulsar redelivers. The receiver-queue size bounds the held-unacked buffer.
//
// libpulsar types do not appear here (the pulsar C API is confined to the .cpp via a pImpl).
class PulsarSource final : public Source<std::string> {
public:
    struct Options {
        PulsarConnParams conn;
        std::string topic;                                // required
        std::string subscription{"clink"};                // shared across subtasks
        int receiver_queue_size{1000};                    // prefetch; bounds held-unacked messages
        std::chrono::milliseconds receive_timeout{1000};  // per produce() turn (cancel latency)
        std::size_t max_batch_size{256};                  // messages emitted per produce() turn
        std::string name{"pulsar_source"};
        // "shared" or "exclusive" (see above).
        std::string subscription_type{"shared"};
        // Exclusive only: how long open() keeps trying a subscription a previous incarnation's
        // consumer still holds, as it can while that incarnation is being torn down.
        std::chrono::milliseconds subscribe_timeout{60000};
    };

    explicit PulsarSource(Options opts);
    ~PulsarSource() override;

    PulsarSource(const PulsarSource&) = delete;
    PulsarSource& operator=(const PulsarSource&) = delete;

    void open() override;
    bool produce(Emitter<std::string>& out) override;
    void close() override;
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    // Bucket the messages emitted before this barrier against the checkpoint id (no ack yet).
    // Runs on the produce() thread (see dag.hpp). An exclusive subscription also records the id of
    // the last message emitted before the barrier; a shared one persists nothing.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    // Exclusive: read back the recorded position, which open() seeks the subscription to. Shared:
    // a no-op, the broker redelivers.
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    // Checkpoint committed/aborted (cluster CommitCheckpoint / AbortCheckpoint dispatch thread).
    // complete() queues the captured messages for ack; aborted() queues them for free without ack
    // (Pulsar redelivers). The actual acknowledge/free runs on the produce() thread.
    void notify_checkpoint_complete(CheckpointId ckpt_id) override;
    void notify_checkpoint_aborted(CheckpointId ckpt_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::pulsar
