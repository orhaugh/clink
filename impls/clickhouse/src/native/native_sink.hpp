#pragma once

#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"

#include "native/insert_transport.hpp"
#include "native/sink_options.hpp"

namespace clink::clickhouse::native {

// The native-protocol ClickHouse sink on the Row channel, at-least-once.
//
// open() validates the target before it returns: the barrier mode and the
// memory budget first, with no network work, then the connect, the server
// and table checks and the column plan on a short-lived opener thread, which
// retries a server it cannot reach for up to retry_window_ms and which a
// cancel interrupts. The rows then go to a writer thread of their own, and
// on_barrier returns only once every row received before the barrier has
// been acknowledged by the server.
//
// Nothing here touches the RuntimeContext after open(): the cancel signal,
// the metrics registry, the logger and the memory budget are copied then,
// because the context goes before the sink does.
class NativeSink final : public Sink<sql::Row> {
public:
    // Throws NativeSinkError(option_invalid) when sql_column_types does not
    // parse, or when the options name no endpoint or no table. An empty
    // factory means the process-wide one, current_transport_factory().
    NativeSink(SinkOptions options, TransportFactory factory);
    // Aborts a writer that neither close() nor close_cancelled() stopped,
    // which is the path a task takes when one of the calls below throws.
    ~NativeSink() override;
    NativeSink(const NativeSink&) = delete;
    NativeSink& operator=(const NativeSink&) = delete;
    NativeSink(NativeSink&&) = delete;
    NativeSink& operator=(NativeSink&&) = delete;

    void open() override;
    void on_data(const Batch<sql::Row>& batch) override;
    void on_data(Batch<sql::Row>&& batch) override;
    // Throws rather than returns when the rows cannot be acknowledged (a
    // permanent failure, an exhausted retry window, a cancel), because a
    // normal return lets the checkpoint complete over rows that never reached
    // the table. An unaligned barrier other than the terminal one is refused
    // with clickhouse.barrier_mode_unsupported, before anything is flushed.
    void on_barrier(CheckpointBarrier barrier) override;
    // Clean end of input: every row received is acknowledged before it returns.
    void flush() override;
    void close() override;
    // Never throws.
    void close_cancelled() override;
    // supports_columnar() keeps the default false: a columnar batch is read
    // through its row accessors.
    // The at-least-once guarantee rests on on_barrier preceding the
    // checkpoint's ack, which holds only while this sink owns its chain's
    // checkpoint, so Dag::add_sink keeps it the only sink on its chain.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "clickhouse_native_sink"; }

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::clickhouse::native
