#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/pulsar/connection_params.hpp"

namespace clink::pulsar {

// Apache Pulsar sink: publishes each std::string record to a topic. Delivery is AT-LEAST-ONCE.
// Messages are published asynchronously (pulsar_producer_send_async) for throughput;
// flush()/on_barrier() calls pulsar_producer_flush to block until the broker has persisted every
// pending publish, and THROWS if any send failed so the job replays from the last checkpoint
// rather than dropping data. Re-delivery on replay can duplicate (at-least-once); the SQL planner
// rejects exactly_once (Pulsar dedup needs a producer name + sequence id, not wired in v1).
//
// libpulsar types do not appear here (the pulsar C API is confined to the .cpp via a pImpl).
class PulsarSink final : public Sink<std::string> {
public:
    struct Options {
        PulsarConnParams conn;
        std::string topic;                              // required
        bool batching{true};                            // producer batching (throughput)
        std::chrono::milliseconds send_timeout{30000};  // producer sendTimeout
        std::string name{"pulsar_sink"};
    };

    explicit PulsarSink(Options opts);
    ~PulsarSink() override;

    PulsarSink(const PulsarSink&) = delete;
    PulsarSink& operator=(const PulsarSink&) = delete;

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    void flush() override;
    void close() override;
    [[nodiscard]] std::string name() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::pulsar
