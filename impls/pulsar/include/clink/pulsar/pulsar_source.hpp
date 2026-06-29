#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/pulsar/connection_params.hpp"

namespace clink::pulsar {

// Apache Pulsar source: subscribes to a topic and emits each message body as a std::string.
// Delivery is AT-LEAST-ONCE. Messages are received and held, then individually acknowledged at
// the checkpoint barrier (snapshot_offset); the subscription cursor is durable server-side, so
// after a failure Pulsar redelivers everything left unacked (recovery needs no local state -
// restore_offset is a no-op). A Shared subscription is used by default so parallel subtasks
// sharing the subscription distribute the topic's messages and each acks only its own.
//
// HONEST CAVEAT: like the RabbitMQ/NATS sources, the ack happens at the barrier (the Source
// interface has no post-commit hook), so a crash between the barrier ack and the checkpoint
// completing could drop those messages. The receiver-queue size bounds the held-unacked buffer.
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

    // At-least-once ack point: acknowledge (and free) every message emitted since the last
    // barrier. Runs on the produce() thread (see dag.hpp), serialised with receive().
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::pulsar
