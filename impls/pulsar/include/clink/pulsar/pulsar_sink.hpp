#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/pulsar/connection_params.hpp"
#include "clink/pulsar/pulsar_publisher.hpp"

namespace clink::pulsar {

// Apache Pulsar sink: publishes each std::string record to a topic.
//
// Delivery is AT-LEAST-ONCE. Messages are sent asynchronously as they arrive,
// batched by the producer unless batching is off, and every checkpoint barrier
// sends what is batched and waits until the broker has answered each message
// published before it. A positive answer means the broker persisted the
// message. Broker-side de-duplication needs a producer name and sequence ids,
// which the sink does not set, so the messages a restored job publishes again
// are stored again.
//
// Failures. A send the broker or the client fails (send_timeout_ms passed, the
// topic refused) fails the flush once every message is answered, saying how
// many of the messages published since the last complete flush were stored,
// failed, and left unanswered. A failure fails the sink: every later record,
// barrier and flush is refused, an empty flush included, until open() is
// called again. close() does not wait for answers itself; the client's close
// fails what is unanswered and waits at most its operation timeout for the
// broker. It never throws.
//
// Bounds. At most max_pending messages, and max_pending_bytes of them, await
// answers at once: past either the sink waits for answers before it publishes
// more, so a burst never meets the client's full queue. Each message fails
// after send_timeout_ms unanswered, and waits look at the task's cancel every
// 100 ms.
//
// libpulsar types do not appear here (the Pulsar C API is confined to the .cpp
// files).
class PulsarSink final : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxPendingBytes = std::size_t{64} << 20;

    struct Options {
        PulsarConnParams conn;
        std::string topic;                              // required
        bool batching{true};                            // producer batching (throughput)
        std::chrono::milliseconds send_timeout{30000};  // a message unanswered this long fails
        int max_pending{1000};                          // messages awaiting answers at once
        std::size_t max_pending_bytes{0};               // their bytes (0 = 64 MiB)
        std::string name{"pulsar_sink"};
        PulsarPublisherFactory publisher_factory;  // unset: a real broker connection
    };

    explicit PulsarSink(Options opts);
    ~PulsarSink() override;

    PulsarSink(const PulsarSink&) = delete;
    PulsarSink& operator=(const PulsarSink&) = delete;

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

}  // namespace clink::pulsar
