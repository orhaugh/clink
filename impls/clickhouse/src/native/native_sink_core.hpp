#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "native/column_plan.hpp"
#include "native/errors.hpp"
#include "native/insert_transport.hpp"
#include "native/sink_options.hpp"
#include "native/types.hpp"
#include "native/writer.hpp"

namespace clink::clickhouse::native {

// The part of the native sink that does not depend on what the rows arrive as:
// the open (barrier mode, memory cap, the opener thread and its retries, the
// open report), the writer and what reaches it, the barrier, flush and close,
// and the release list. A sink builds Arrow chunks of columns() on the task
// thread and hands them to submit().
//
// Every call is made on the task thread. Nothing here touches the
// RuntimeContext after open(): the cancel signal, the metrics registry, the
// logger and the memory budget are copied then, because the context goes
// before the sink does.
class SinkCore {
public:
    // An empty factory means the process-wide one, current_transport_factory().
    SinkCore(SinkOptions options,
             std::vector<SqlColumn> columns,
             InputKind kind,
             TransportFactory factory);
    // Aborts a writer that neither close() nor close_cancelled() stopped.
    ~SinkCore();
    SinkCore(const SinkCore&) = delete;
    SinkCore& operator=(const SinkCore&) = delete;
    SinkCore(SinkCore&&) = delete;
    SinkCore& operator=(SinkCore&&) = delete;

    // Validates the target before it returns, then starts the writer. A null
    // `ctx` runs the sink unaccounted and names it by the fallbacks. Throws
    // std::logic_error when called twice, and NativeSinkError for a refusal,
    // a failure, an exhausted retry window or a cancel, each logged and a
    // refusal counted. `prepare`, when set, runs once the target has been
    // validated and before the open report and the writer: if it throws, the
    // client is abandoned and the writer never starts, so a later open() tries
    // again.
    void open(const RuntimeContext* ctx,
              OperatorId fallback_id,
              const std::string& fallback_name,
              const std::function<void()>& prepare = {});

    // Throws std::logic_error naming `call` until open() has completed.
    void require_writer(const char* call) const;
    // Drops the input batches the writer has let go of, on the task thread.
    // Never throws.
    void drain_released() noexcept;
    // Charges a built chunk to the budget, queues it and counts it under its
    // carrier. `shares_input` when the chunk reuses arrays of the batch it was
    // built from. Throws as Writer::submit does; the caller then calls abort().
    void submit(std::shared_ptr<arrow::RecordBatch> rows, Carrier carrier, bool shares_input);
    // Stops the writer, which also logs its summary. Never throws.
    void abort() noexcept;

    // The Sink hooks. barrier() and flush() throw rather than return when the
    // rows cannot be acknowledged, after stopping the writer. flush(), close()
    // and close_cancelled() do nothing before open().
    void barrier(CheckpointBarrier barrier);
    void flush();
    void close();
    void close_cancelled() noexcept;

    // Logs a chunk that could not be built from a batch.
    void conversion_failed(const ConversionError& e) const;
    void log(LogSeverity level, const std::string& message) const;
    [[nodiscard]] std::string subtask() const;
    [[nodiscard]] const std::vector<SqlColumn>& columns() const noexcept;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::clickhouse::native
