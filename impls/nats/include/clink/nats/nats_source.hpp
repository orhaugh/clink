#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/nats/connection_params.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::nats {

// NATS JetStream source: a durable PULL consumer that Fetches batches from a subject and emits
// each message body as a std::string. Delivery is AT-LEAST-ONCE. Messages are consumed with an
// explicit-ack durable consumer; until acked JetStream holds them (bounded by MaxAckPending) and
// redelivers any left unacked after a failure (the durable consumer's ack floor persists
// server-side, so recovery needs no local state - restore_offset is a no-op).
//
// The ack is deferred to CHECKPOINT COMMIT, not the barrier. snapshot_offset() buckets the
// messages emitted before each barrier against that checkpoint id; notify_checkpoint_complete()
// (driven from the cluster's CommitCheckpoint path) marks a bucket's messages safe to ack, and
// the next produce() turn issues their natsMsg_Ack - so a message is acked only after the
// checkpoint that captured it is globally durable. All nats.c calls stay on the produce() thread
// (one subscription, not thread-safe). notify_checkpoint_aborted() releases the bucket WITHOUT
// acking, so AckWait lapses and JetStream redelivers. Keep AckWait larger than the checkpoint
// interval so held (unacked) messages are not redelivered before their checkpoint commits.
//
// Core NATS pub/sub is fire-and-forget (at-most-once); this connector targets JetStream, which
// is the persistent, ack'd streaming layer. nats.c types do not appear here (pImpl).
class NatsSource final : public Source<std::string> {
public:
    struct Options {
        NatsConnParams conn;
        std::string subject;           // required: subject to consume
        std::string stream;            // optional: bind to this stream (else resolved by subject)
        std::string durable{"clink"};  // durable consumer name (shared across subtasks)
        int batch{256};                // messages per Fetch
        std::chrono::milliseconds fetch_timeout{1000};  // Fetch wait (also the cancel latency)
        std::chrono::seconds ack_wait{60};              // consumer AckWait (> checkpoint interval)
        int max_ack_pending{2048};                      // bounds held-unacked messages
        std::string name{"nats_source"};
    };

    explicit NatsSource(Options opts);
    ~NatsSource() override;

    NatsSource(const NatsSource&) = delete;
    NatsSource& operator=(const NatsSource&) = delete;

    void open() override;
    bool produce(Emitter<std::string>& out) override;
    void close() override;
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    // Bucket the messages emitted before this barrier against the checkpoint id (no ack yet).
    // Runs on the produce() thread. Persists nothing - JetStream tracks the durable ack floor.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    // Checkpoint committed/aborted (cluster CommitCheckpoint / AbortCheckpoint dispatch thread).
    // complete() queues the captured messages for ack; aborted() queues them for release without
    // ack (JetStream redelivers). The actual natsMsg_Ack/Destroy runs on the produce() thread.
    void notify_checkpoint_complete(CheckpointId ckpt_id) override;
    void notify_checkpoint_aborted(CheckpointId ckpt_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::nats
