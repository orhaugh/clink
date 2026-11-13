#include "relate_sink.hpp"

#include <algorithm>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "fault_points.hpp"
#include "metrics.hpp"

namespace clink::relate {

namespace {

using Clock = std::chrono::steady_clock;

constexpr std::string_view kLog = "sink.relate";

// Full jitter: the nth wait is uniform in [0, min(500 ms, 2 ms * 2^n)]. A
// conflict is with another transaction in this process, which ends in
// milliseconds.
class Backoff {
public:
    std::chrono::milliseconds next() {
        const std::uint64_t cap = std::min<std::uint64_t>(500, 2ULL << std::min(n_, 8U));
        ++n_;
        return std::chrono::milliseconds{
            std::uniform_int_distribution<std::uint64_t>(0, cap)(rng_)};
    }

private:
    std::mt19937_64 rng_ = std::mt19937_64(std::random_device{}());
    unsigned n_{0};
};

// Waits `d` in slices of at most 20 ms, returning false as soon as the task is
// cancelled.
bool cancellable_wait(std::chrono::milliseconds d, const CancelSignal& cancel) {
    const auto until = Clock::now() + d;
    while (Clock::now() < until) {
        if (cancel.requested()) {
            return false;
        }
        std::this_thread::sleep_for(
            std::min<Clock::duration>(std::chrono::milliseconds{20}, until - Clock::now()));
    }
    return !cancel.requested();
}

std::string tagged(const char* metric, std::uint64_t op) {
    return std::string(metric) + "{op_id=\"" + std::to_string(op) + "\"}";
}

std::string tagged(const char* metric,
                   std::uint64_t op,
                   std::string_view key,
                   std::string_view value) {
    return std::string(metric) + "{op_id=\"" + std::to_string(op) + "\"," + std::string(key) +
           "=\"" + std::string(value) + "\"}";
}

}  // namespace

RelateSink::RelateSink(SinkOptions options) : opts_(std::move(options)) {}

RelateSink::~RelateSink() {
    release_();
}

// The connection goes before the store it is on.
void RelateSink::release_() {
    conn_.reset();
    store_.reset();
}

std::string RelateSink::where_() const {
    return "store '" + (store_ ? store_->path() : opts_.path) + "'";
}

void RelateSink::fail_(const char* code, const std::string& message) {
    clink::metrics::connector::error_inc(metric::kConnector);
    throw RelateError(code, "relate sink: " + where_() + ": " + message);
}

void RelateSink::open() {
    if (runtime() != nullptr) {
        cancel_ = runtime()->cancel_signal();
    }
    store_ = Store::open(opts_.path);
    conn_ = std::make_unique<Connection>(*store_);
    {
        // One sink at a time on a store: the schema statements and the check
        // are short, and two sinks creating one constraint would conflict.
        std::lock_guard<std::mutex> lock(store_->open_mutex());
        apply_schema_();
        check_statement_();
    }
    opened_ = true;
    publish_buffer_gauges_();
    clink::log::info(kLog,
                     "relate sink open: subtask=" + std::to_string(opts_.subtask) + "/" +
                         std::to_string(opts_.parallelism) + " " + where_() +
                         " schema_statements=" + std::to_string(opts_.schema.size()) +
                         " batch_size=" + std::to_string(opts_.batch_size) +
                         " batch_bytes=" + std::to_string(opts_.batch_bytes) +
                         " retry_window_ms=" + std::to_string(opts_.retry_window_ms));
}

// Each schema statement runs on its own, as Relate runs a schema change. It is
// first explained, which runs nothing, so that a statement that would write
// data is refused before it can: the option is for constraints and indexes.
void RelateSink::apply_schema_() {
    for (std::size_t i = 0; i < opts_.schema.size(); ++i) {
        const auto& statement = opts_.schema[i];
        const std::string which = "schema statement " + std::to_string(i + 1) + " of " +
                                  std::to_string(opts_.schema.size());
        const Reply plan = conn_->run("EXPLAIN " + statement, nullptr);
        if (!plan.ok) {
            fail_(code::kSchemaRefused, which + " is refused: " + plan.error.describe());
        }
        if (plan.query_type != "s") {
            fail_(code::kSchemaRefused,
                  which +
                      " is not a schema statement: the schema option takes CREATE CONSTRAINT "
                      "and CREATE INDEX statements, and rows are written by the statement "
                      "option");
        }
        const Reply applied = conn_->run(statement, nullptr);
        if (!applied.ok) {
            fail_(code::kSchemaRefused,
                  which + " failed: " + applied.error.describe() +
                      ". The sink applies it every time it opens, so write it with IF NOT "
                      "EXISTS");
        }
    }
}

// Runs the statement over no rows inside a transaction that is rolled back.
// Nothing is written, and Relate has parsed it, resolved its parameters and
// said what kind of statement it is.
void RelateSink::check_statement_() {
    RowsParam none;
    const Reply began = conn_->begin();
    if (!began.ok) {
        fail_(code::kStatementRefused,
              "the statement could not be checked: " + began.error.describe());
    }
    const Reply ran = conn_->run(opts_.statement, none.get());
    conn_->rollback();
    if (!ran.ok) {
        fail_(code::kStatementRefused, "the statement is refused: " + ran.error.describe());
    }
    std::vector<std::string> problems;
    if (ran.query_type == "r") {
        problems.emplace_back("it only reads, and a sink's statement must write");
    }
    if (ran.query_type == "s") {
        problems.emplace_back(
            "it changes the schema; constraints and indexes belong in the schema option");
    }
    if (ran.columns > 0) {
        problems.emplace_back(
            "it returns columns, which the sink would discard; remove the "
            "RETURN");
    }
    if (!problems.empty()) {
        std::string all;
        for (const auto& p : problems) {
            all += (all.empty() ? "" : "; ") + p;
        }
        fail_(code::kStatementRefused, "the statement is refused: " + all);
    }
}

void RelateSink::on_data(const Batch<clink::sql::Row>& batch) {
    for (const auto& record : batch) {
        add_row_(record.value());
    }
    if (batch_.rows() > 0 && opts_.flush_interval_ms > 0 &&
        Clock::now() - first_pending_ >= std::chrono::milliseconds{opts_.flush_interval_ms}) {
        commit_pending_("flush_interval_ms");
    }
    publish_buffer_gauges_();
}

void RelateSink::add_row_(const clink::sql::Row& row) {
    const std::uint64_t weight = row_weight(opts_.columns, row);
    if (weight > opts_.batch_bytes) {
        fail_(code::kRowTooLarge,
              "a row weighs " + std::to_string(weight) + " bytes, which passes batch_bytes=" +
                  std::to_string(opts_.batch_bytes) + "; raise batch_bytes");
    }
    if (batch_.rows() > 0 && batch_.bytes() + weight > opts_.batch_bytes) {
        commit_pending_("batch_bytes");
    }
    if (batch_.rows() == 0) {
        first_pending_ = Clock::now();
    }
    try {
        batch_.add(opts_.columns, row, weight);
    } catch (const RelateError&) {
        clink::metrics::connector::error_inc(metric::kConnector);
        throw;
    }
    if (batch_.rows() >= opts_.batch_size) {
        commit_pending_("batch_size");
    }
}

void RelateSink::on_barrier(CheckpointBarrier /*b*/) {
    commit_pending_("checkpoint barrier");
    publish_buffer_gauges_();
}

void RelateSink::flush() {
    commit_pending_("end of input");
    publish_buffer_gauges_();
}

// One batch, one transaction. Returns only once Relate committed it; throws
// when Relate refused it, when the retry window closes, or when the task is
// cancelled. The rows stay buffered until then.
void RelateSink::commit_pending_(const char* why) {
    if (batch_.rows() == 0) {
        return;
    }
    if (!opened_ || closed_) {
        fail_(code::kEngineFailure, "rows reached a sink that is not open");
    }
    const std::uint64_t seq = ++batch_seq_;
    const std::uint64_t rows = batch_.rows();
    const std::uint64_t bytes = batch_.bytes();
    const std::string batch = "batch " + std::to_string(seq) + " (" + std::to_string(rows) +
                              " rows, " + std::to_string(bytes) + " bytes, on " + why + ")";
    MetricsRegistry* reg = runtime() != nullptr ? runtime()->metrics() : nullptr;
    const std::uint64_t op = runtime() != nullptr ? runtime()->operator_id().value() : id().value();
    Backoff backoff;
    std::optional<Clock::time_point> window_end;
    for (std::uint32_t attempt = 1;; ++attempt) {
        if (cancel_.requested()) {
            fail_(code::kCancelled,
                  batch +
                      " was not committed: the task was cancelled; the rows are replayed from "
                      "the last completed checkpoint");
        }
        (void)CLINK_FAULT_POINT(points::kBeforeCommit);
        const auto started = Clock::now();
        Reply r = conn_->begin();
        if (r.ok) {
            r = conn_->run(opts_.statement, batch_.get());
        }
        if (r.ok) {
            r = conn_->commit();
        }
        if (r.ok) {
            const auto ns = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started)
                    .count());
            ++counts_.batches_committed;
            counts_.rows_committed += rows;
            if (!r.bookmark.empty()) {
                counts_.last_bookmark = r.bookmark;
            }
            if (reg != nullptr) {
                reg->counter(tagged(metric::kBatchesCommittedTotal, op)).increment();
                reg->counter(tagged(metric::kRowsCommittedTotal, op)).increment(rows);
                reg->histogram(
                       tagged(metric::kCommitLatencyNs, op),
                       std::vector<double>(metric::kNsBounds.begin(), metric::kNsBounds.end()))
                    .observe(static_cast<double>(ns));
            }
            clink::metrics::connector::records_out_inc(metric::kConnector, rows);
            clink::metrics::connector::bytes_out_inc(metric::kConnector, bytes);
            clink::metrics::connector::commit_latency_observe(metric::kConnector, ns);
            if (attempt > 1) {
                clink::log::info(kLog,
                                 "relate sink: " + where_() + ": " + batch +
                                     " committed on attempt " + std::to_string(attempt));
            }
            batch_.clear();
            (void)CLINK_FAULT_POINT(points::kAfterCommit);
            return;
        }
        // Nothing of the attempt is in the store: the transaction is rolled back.
        conn_->rollback();
        ++counts_.attempts_failed;
        if (reg != nullptr) {
            reg->counter(tagged(metric::kAttemptsFailedTotal,
                                op,
                                "retryable",
                                r.error.retryable ? "true" : "false"))
                .increment();
        }
        if (!r.error.retryable) {
            fail_(code::kBatchRefused,
                  batch + " was refused and rolled back: " + r.error.describe());
        }
        const auto now = Clock::now();
        if (!window_end) {
            window_end = now + std::chrono::milliseconds{opts_.retry_window_ms};
        }
        const auto wait = backoff.next();
        if (opts_.retry_window_ms == 0 || now + wait > *window_end) {
            fail_(code::kRetriesExhausted,
                  batch + " was not committed after " + std::to_string(attempt) +
                      " attempt(s) within retry_window_ms=" +
                      std::to_string(opts_.retry_window_ms) + "; last: " + r.error.describe());
        }
        ++counts_.retries;
        if (reg != nullptr) {
            reg->counter(tagged(metric::kRetriesTotal, op)).increment();
        }
        clink::log::warn(kLog,
                         "relate sink: " + where_() + ": " + batch + " attempt " +
                             std::to_string(attempt) + " was rolled back: " + r.error.describe() +
                             "; retrying in " + std::to_string(wait.count()) + " ms");
        if (!cancellable_wait(wait, cancel_)) {
            fail_(code::kCancelled,
                  batch +
                      " was not committed: the task was cancelled; the rows are replayed from "
                      "the last completed checkpoint");
        }
    }
}

void RelateSink::publish_buffer_gauges_() {
    MetricsRegistry* reg = runtime() != nullptr ? runtime()->metrics() : nullptr;
    if (reg == nullptr) {
        return;
    }
    const std::uint64_t op = runtime()->operator_id().value();
    reg->gauge(tagged(metric::kBufferedRows, op)).set(static_cast<std::int64_t>(batch_.rows()));
    reg->gauge(tagged(metric::kBufferedBytes, op)).set(static_cast<std::int64_t>(batch_.bytes()));
}

void RelateSink::summary_(const char* how) const {
    clink::log::info(
        kLog,
        std::string("relate sink ") + how + ": subtask=" + std::to_string(opts_.subtask) + " " +
            where_() + " batches_committed=" + std::to_string(counts_.batches_committed) +
            " rows_committed=" + std::to_string(counts_.rows_committed) +
            " attempts_failed=" + std::to_string(counts_.attempts_failed) +
            " retries=" + std::to_string(counts_.retries) +
            " rows_not_written=" + std::to_string(batch_.rows()) + " last_bookmark=" +
            (counts_.last_bookmark.empty() ? std::string("none") : counts_.last_bookmark));
}

void RelateSink::close() {
    if (closed_) {
        return;
    }
    closed_ = true;
    summary_("closed");
    release_();
}

void RelateSink::close_cancelled() {
    if (closed_) {
        return;
    }
    closed_ = true;
    // Rows still buffered are not written: the task is being torn down, and a
    // restore replays everything after the last completed checkpoint. A
    // transaction left open is rolled back with the connection.
    summary_("cancelled");
    release_();
}

}  // namespace clink::relate
