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
// explicit-ack durable consumer and ack'd at the checkpoint barrier (snapshot_offset); until
// then JetStream holds them (bounded by MaxAckPending) and redelivers any left unacked after a
// failure (the durable consumer's ack floor persists server-side, so recovery needs no local
// state - restore_offset is a no-op).
//
// HONEST CAVEAT: like the RabbitMQ source, the ack happens at the barrier (the Source interface
// has no post-commit hook), so a crash between the barrier ack and the checkpoint completing
// could drop those messages. Keep the consumer AckWait larger than the checkpoint interval so
// in-flight (held, unacked) messages are not spuriously redelivered before a barrier.
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

    // At-least-once ack point: ack (and release) every message emitted since the last barrier.
    // Runs on the produce() thread (see dag.hpp), so it is serialised with Fetch on the single
    // nats.c subscription. Persists nothing - JetStream tracks the durable consumer's ack floor.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::nats
