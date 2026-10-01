#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <arrow/api.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/memory_budget.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "native/arrow_to_block.hpp"
#include "native/error_class.hpp"
#include "native/insert_transport.hpp"
#include "native/sql_text.hpp"
#include "native/target_table.hpp"

namespace spdlog {
class logger;
}

namespace clink::clickhouse::native {

struct Chunk {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::size_t bytes{0};
    MemoryReservation reservation;
};

struct WriterConfig {
    SinkOptions options;  // batch_bytes already capped to the memory budget
    ColumnPlan plan;
    TargetInfo target;
    std::string sink_id;
    std::shared_ptr<MemoryBudget> budget;  // may be null
    CancelSignal cancel;
    MetricsRegistry* metrics{nullptr};  // RuntimeContext::metrics(), host registry
    std::uint64_t op_id{0};
    spdlog::logger* logger{nullptr};  // RuntimeContext::logger()
};

struct WriterStats {
    std::uint64_t rows_acknowledged{0};
    std::uint64_t inserts{0};
    std::uint64_t in_doubt{0};
    std::uint64_t rows_maybe_duplicated{0};
    std::uint64_t rows_resent_with_token{0};
    std::uint64_t abandoned_rows{0};
    std::uint64_t wire_bytes{0};
    std::array<std::uint64_t, kFailureClasses> retries{};
};

class Writer {
public:
    // Takes the transport the opener connected and probed; starts the thread.
    Writer(WriterConfig config, std::unique_ptr<InsertTransport> transport, TokenSource tokens);
    ~Writer();  // abort() if still running
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;
    Writer(Writer&&) = delete;
    Writer& operator=(Writer&&) = delete;

    // Task thread. Blocks while the queue is full. Throws the writer's stored
    // failure, or NativeSinkError(cancelled).
    void submit(Chunk chunk);
    // Task thread. Returns once every INSERT holding rows submitted before
    // the call is acknowledged.
    void flush(std::uint64_t checkpoint_id);
    // Task thread, clean end: flush, stop, join. Throws the stored failure.
    void finish();
    // Any thread: poison, stop, join for up to 5 s, then detach. Never throws.
    void abort() noexcept;

    [[nodiscard]] WriterStats stats() const;
    [[nodiscard]] std::size_t queue_bytes() const noexcept;

    struct Core;  // shared with the thread, so a detach is safe

private:
    std::shared_ptr<Core> core_;
};

}  // namespace clink::clickhouse::native
