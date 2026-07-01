#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/pulsar/connection_params.hpp"

namespace clink::pulsar {

// Apache Pulsar source: subscribes to a topic and emits each message body as a std::string.
// Delivery is AT-LEAST-ONCE. The subscription cursor is durable server-side, so after a failure
// Pulsar redelivers everything left unacked (recovery needs no local state - restore_offset is a
// no-op). A Shared subscription is used by default so parallel subtasks sharing the subscription
// distribute the topic's messages and each acks only its own.
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
    // Runs on the produce() thread (see dag.hpp). Persists nothing - the cursor is server-side.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
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
