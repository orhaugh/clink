#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

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

// The queue between the task thread and the writer holds at most this many
// chunk bytes, and this many chunks, before submit() waits. A chunk larger
// than the cap is still taken by a queue that holds none, so one large batch
// cannot deadlock. Flush and finish requests bypass both caps but keep their
// place behind the chunks already queued.
inline constexpr std::size_t kQueueBytes = 16ULL << 20;
inline constexpr std::size_t kQueueChunks = 256;

// How the batch a chunk was built from reached the sink: as rows, or as a
// columnar sidecar read without building any.
enum class Carrier : std::uint8_t { Row, Columnar };

struct Chunk {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::size_t bytes{0};  // chunk_bytes(*batch)
    MemoryReservation reservation;
    Carrier carrier{Carrier::Row};
};

struct WriterConfig {
    SinkOptions options;  // batch_bytes already capped to the memory budget
    ColumnPlan plan;
    // From the opener's probe. Its squash thresholds are pinned for the
    // writer's life; its identity is the server the given transport is on.
    TargetInfo target;
    std::string sink_id;
    std::shared_ptr<MemoryBudget> budget;  // may be null
    CancelSignal cancel;
    MetricsRegistry* metrics{nullptr};  // RuntimeContext::metrics(), host registry
    std::uint64_t op_id{0};
    spdlog::logger* logger{nullptr};  // RuntimeContext::logger()
    // Called on every new client the writer builds, before its first
    // begin_insert, because a reconnect may reach another replica or a server
    // restarted into another version. The sink binds it to probe_target. A
    // NativeSinkError from it fails the INSERT permanently with that code; a
    // client exception is a metadata-phase failure, retried like any other.
    // The identity used for the token rule is always the transport's own.
    // Empty: the writer keeps the target it has and takes only the new
    // server's identity.
    std::function<TargetInfo(InsertTransport&)> reprobe;
    // The span and the quiet period of the run-time part-rate warning
    // (PartRateMonitor). Tests shorten them to reach the warning quickly.
    std::chrono::milliseconds part_rate_span{std::chrono::seconds{60}};
    std::chrono::milliseconds part_rate_quiet{std::chrono::minutes{10}};
};

struct WriterStats {
    std::uint64_t rows_acknowledged{0};
    std::uint64_t inserts{0};  // INSERTs the server acknowledged, split halves included
    std::uint64_t in_doubt{0};
    // Rows resent after a send under a fresh token, or under their own token
    // where nothing deduplicates them: once per INSERT.
    std::uint64_t rows_maybe_duplicated{0};
    // Rows resent after a send under their own token, to the same server, on
    // a target that keeps a deduplication log: once per INSERT, and never
    // also counted above.
    std::uint64_t rows_resent_with_token{0};
    // Rows submitted to the writer that the server had not acknowledged when
    // it stopped or failed: those of the INSERT in flight, of the chunk it
    // was taking in, and of the chunks still queued. The job's restart
    // replays them.
    std::uint64_t abandoned_rows{0};
    std::uint64_t wire_bytes{0};
    std::array<std::uint64_t, kFailureClasses> retries{};  // by FailureClass
    // Chunks submitted, by the carrier of the batch each was built from.
    std::uint64_t columnar_batches{0};
    std::uint64_t row_batches{0};
};

// The insert thread of one sink subtask. It owns the transport, the block
// builder and every INSERT; the task thread only submits chunks, asks for
// flushes and waits. Every wait on either side is cut into slices of at most
// 50 ms that look at the task's cancel signal.
class Writer {
public:
    // Takes the transport the opener connected and probed; starts the thread.
    // Throws std::invalid_argument for a missing transport or an empty
    // endpoint list.
    Writer(WriterConfig config, std::unique_ptr<InsertTransport> transport, TokenSource tokens);
    ~Writer();  // abort() if still running
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;
    Writer(Writer&&) = delete;
    Writer& operator=(Writer&&) = delete;

    // Task thread. Blocks while the queue is full. Throws the writer's stored
    // failure, or NativeSinkError(cancelled) once the task is cancelled. On a
    // cancel it calls abort() first, so the writer is joined and its
    // cancelled summary logged before the throw, even when the writer saw the
    // cancel first and the failure it stored is what is thrown.
    void submit(Chunk chunk);
    // Task thread. Returns once every INSERT holding rows submitted before
    // the call has been acknowledged by the server. Throws as submit() does,
    // so it never returns over rows that did not reach the table.
    // `checkpoint_id` names the barrier, 0 at the end of input; the writer
    // needs only the order the calls arrive in.
    void flush(std::uint64_t checkpoint_id);
    // Task thread, clean end: close the open INSERT, stop, join. Throws the
    // stored failure, after abort(), so that a writer that did not end
    // cleanly still logs its cancelled summary.
    void finish();
    // Any thread: stop, interrupt the transport, join for up to 5 s, then
    // detach a writer still inside a call that cannot be interrupted. Logs
    // the cancelled summary once, with the rows still queued counted as
    // abandoned; for a detached writer it also counts the rows the writer was
    // holding, since the writer reports nothing more. Idempotent, and a no-op after finish(). Never
    // throws.
    void abort() noexcept;

    [[nodiscard]] WriterStats stats() const;
    [[nodiscard]] std::size_t queue_bytes() const noexcept;

    struct Core;  // shared with the thread, so a detach is safe

private:
    std::shared_ptr<Core> core_;
};

// "clickhouse native sink <outcome>: subtask=K/P rows_acknowledged=... elapsed_ms=E
// columnar_batches=C row_batches=R",
// the fields both exit summaries carry. The writer logs the cancelled one;
// the sink logs the closed one with the same fields.
[[nodiscard]] std::string summary_line(std::string_view outcome,
                                       const SinkOptions& options,
                                       const WriterStats& stats,
                                       std::chrono::milliseconds elapsed);

// The run-time part-rate rule. A writer counts the INSERTs the server
// acknowledged over spans of `span`. At the end of a span in which the
// job-wide rate, this writer's own times the parallelism, was above one a
// second and the INSERTs averaged fewer than 10000 rows, on_insert returns the
// warning to log, at most once per `quiet`. Small and frequent together is
// what makes too many parts; either alone is not. Exposed for tests, which
// pass their own clock.
class PartRateMonitor {
public:
    PartRateMonitor(std::uint32_t parallelism,
                    std::chrono::milliseconds batch_interval,
                    std::chrono::steady_clock::time_point start,
                    std::chrono::milliseconds span = std::chrono::seconds{60},
                    std::chrono::milliseconds quiet = std::chrono::minutes{10});

    [[nodiscard]] std::optional<std::string> on_insert(std::chrono::steady_clock::time_point now,
                                                       std::uint64_t rows);

private:
    std::uint32_t parallelism_;
    std::chrono::milliseconds batch_interval_;
    std::chrono::milliseconds span_;
    std::chrono::milliseconds quiet_;
    std::chrono::steady_clock::time_point span_start_;
    std::uint64_t inserts_{0};
    std::uint64_t rows_{0};
    std::optional<std::chrono::steady_clock::time_point> last_warning_;
};

}  // namespace clink::clickhouse::native
