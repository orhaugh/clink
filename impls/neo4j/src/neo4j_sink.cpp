#include "neo4j_sink.hpp"

#include <algorithm>
#include <optional>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "fault_points.hpp"
#include "metrics.hpp"
#include "types.hpp"

namespace clink::neo4j {

namespace {

using Clock = std::chrono::steady_clock;
using clink::http_connector::BoundedRequest;
using clink::http_connector::BoundedResponse;
using clink::http_connector::HttpRequest;

constexpr std::string_view kLog = "sink.neo4j";
constexpr std::string_view kBodySuffix = "]}}}";

}  // namespace

// ---- Neo4jSink --------------------------------------------------------------

Neo4jSink::Neo4jSink(SinkOptions options) : opts_(std::move(options)) {
    path_ = opts_.query_path();
    body_prefix_ = "{\"statement\":";
    append_json_string(opts_.statement, body_prefix_);
    body_prefix_ += ",\"parameters\":{\"rows\":{\"$type\":\"List\",\"_value\":[";
    request_.headers["Accept"] = kTypedJson;
    request_.max_response_bytes = opts_.max_response_bytes;
}

Neo4jSink::~Neo4jSink() {
    if (watch_) {
        watch_->stop();
    }
}

std::string Neo4jSink::where_() const {
    return "database '" + opts_.database + "' at " + opts_.endpoint;
}

void Neo4jSink::fail_(const char* code, const std::string& message) {
    clink::metrics::connector::error_inc(metric::kConnector);
    throw Neo4jError(code, "neo4j sink: " + where_() + ": " + message);
}

void Neo4jSink::open() {
    if (runtime() != nullptr) {
        cancel_ = runtime()->cancel_signal();
    }
    http_ = make_client(opts_);
    watch_ = std::make_unique<RequestWatch>(*http_);
    check_statement_();
    opened_ = true;
    publish_buffer_gauges_();
    clink::log::info(kLog,
                     "neo4j sink open: subtask=" + std::to_string(opts_.subtask) + "/" +
                         std::to_string(opts_.parallelism) + " " + where_() + " server=" +
                         (server_version_.empty() ? std::string("unknown") : server_version_) +
                         " batch_size=" + std::to_string(opts_.batch_size) +
                         " batch_bytes=" + std::to_string(opts_.batch_bytes) +
                         " retry_window_ms=" + std::to_string(opts_.retry_window_ms));
}

// EXPLAIN the statement with an empty $rows: nothing runs, and the reply shows
// the credentials, the database, the syntax, the result columns, the query type
// and the plan. A read-only request, so any failure but a refusal is retried.
void Neo4jSink::check_statement_() {
    std::string body = "{\"statement\":";
    append_json_string("EXPLAIN " + opts_.statement, body);
    body += ",\"parameters\":{\"rows\":{\"$type\":\"List\",\"_value\":[]}}}";
    Backoff backoff;
    std::optional<Clock::time_point> window_end;
    for (std::uint32_t attempt = 1;; ++attempt) {
        if (cancel_.requested()) {
            fail_(code::kCancelled, "the task was cancelled while the sink was opening");
        }
        watch_->arm(Clock::now() + std::chrono::milliseconds{opts_.request_timeout_ms}, cancel_);
        const auto r = http_->post_bounded(path_, body, kTypedJson, request_);
        const auto why = watch_->disarm();
        if (why == RequestWatch::Why::Cancelled) {
            fail_(code::kCancelled, "the task was cancelled while the sink was opening");
        }
        Reply reply;
        const Outcome o = judge_check(r, reply);
        if (o.verdict == Verdict::Committed) {
            server_version_ = reply.server_version;
            if (const auto problems = statement_problems(reply); !problems.empty()) {
                std::string all;
                for (const auto& p : problems) {
                    all += (all.empty() ? "" : "; ") + p;
                }
                fail_(code::kStatementRefused, "the statement is refused: " + all);
            }
            return;
        }
        if (o.verdict == Verdict::Refused) {
            fail_(error_code_for(o.error_class), "checking the statement failed: " + o.detail);
        }
        if (o.verdict == Verdict::TlsRefused) {
            fail_(code::kTlsVerify, o.detail);
        }
        if (o.verdict == Verdict::UnknownFinal) {
            fail_(code::kServerRefused,
                  "checking the statement failed: the server's plan for it is larger than "
                  "max_response_bytes=" +
                      std::to_string(opts_.max_response_bytes) + "; raise it");
        }
        const auto now = Clock::now();
        if (!window_end) {
            window_end = now + std::chrono::milliseconds{opts_.retry_window_ms};
        }
        const auto wait = backoff.next();
        if (opts_.retry_window_ms == 0 || now + wait > *window_end) {
            fail_(code::kRetriesExhausted,
                  "checking the statement failed after " + std::to_string(attempt) +
                      " attempt(s) within retry_window_ms=" +
                      std::to_string(opts_.retry_window_ms) + ": " + o.detail);
        }
        clink::log::warn(kLog,
                         "neo4j sink: " + where_() + ": checking the statement, attempt " +
                             std::to_string(attempt) + " failed (" + to_string(o.verdict) + "): " +
                             o.detail + "; retrying in " + std::to_string(wait.count()) + " ms");
        if (!cancellable_wait(wait, cancel_)) {
            fail_(code::kCancelled, "the task was cancelled while the sink was opening");
        }
    }
}

void Neo4jSink::on_data(const Batch<clink::sql::Row>& batch) {
    for (const auto& record : batch) {
        add_row_(record.value());
    }
    if (pending_rows_ > 0 && opts_.flush_interval_ms > 0 &&
        Clock::now() - first_pending_ >= std::chrono::milliseconds{opts_.flush_interval_ms}) {
        commit_pending_("flush_interval_ms");
    }
    publish_buffer_gauges_();
}

void Neo4jSink::add_row_(const clink::sql::Row& row) {
    scratch_.clear();
    encode_row(opts_.columns, row, scratch_);
    const std::size_t fixed = body_prefix_.size() + kBodySuffix.size();
    if (fixed + scratch_.size() > opts_.batch_bytes) {
        fail_(code::kRowTooLarge,
              "a row encodes to " + std::to_string(scratch_.size()) +
                  " bytes, which with the statement passes batch_bytes=" +
                  std::to_string(opts_.batch_bytes) + "; raise batch_bytes");
    }
    if (pending_rows_ > 0 && fixed + rows_.size() + 1 + scratch_.size() > opts_.batch_bytes) {
        commit_pending_("batch_bytes");
    }
    if (pending_rows_ > 0) {
        rows_ += ',';
    } else {
        first_pending_ = Clock::now();
    }
    rows_ += scratch_;
    ++pending_rows_;
    if (pending_rows_ >= opts_.batch_size) {
        commit_pending_("batch_size");
    }
}

std::string Neo4jSink::body_() const {
    std::string body;
    body.reserve(body_prefix_.size() + rows_.size() + kBodySuffix.size());
    body += body_prefix_;
    body += rows_;
    body += kBodySuffix;
    return body;
}

void Neo4jSink::on_barrier(CheckpointBarrier /*b*/) {
    commit_pending_("checkpoint barrier");
    publish_buffer_gauges_();
}

void Neo4jSink::flush() {
    commit_pending_("end of input");
    publish_buffer_gauges_();
}

// One batch, one implicit transaction. Returns only once the server reported
// it committed; throws when it refused it, when the retry window closes, or
// when the task is cancelled. The rows stay buffered until then.
void Neo4jSink::commit_pending_(const char* why) {
    if (pending_rows_ == 0) {
        return;
    }
    if (!opened_ || closed_) {
        fail_(code::kServerRefused, "rows reached a sink that is not open");
    }
    const std::string body = body_();
    const std::uint64_t seq = ++batch_seq_;
    const std::uint64_t rows = pending_rows_;
    const std::string batch = "batch " + std::to_string(seq) + " (" + std::to_string(rows) +
                              " rows, " + std::to_string(body.size()) + " bytes, on " + why + ")";
    MetricsRegistry* reg = runtime() != nullptr ? runtime()->metrics() : nullptr;
    const std::uint64_t op = runtime() != nullptr ? runtime()->operator_id().value() : id().value();
    Backoff backoff;
    std::optional<Clock::time_point> window_end;
    std::uint64_t unknown = 0;
    for (std::uint32_t attempt = 1;; ++attempt) {
        if (cancel_.requested()) {
            fail_(code::kCancelled,
                  batch +
                      " was not committed: the task was cancelled; the rows are replayed "
                      "from the last completed checkpoint");
        }
        (void)CLINK_FAULT_POINT(points::kBeforeRequest);
        const auto sent_at = Clock::now();
        watch_->arm(sent_at + std::chrono::milliseconds{opts_.request_timeout_ms}, cancel_);
        const BoundedResponse r = http_->post_bounded(path_, body, kTypedJson, request_);
        const auto watch = watch_->disarm();
        const auto took = Clock::now() - sent_at;
        if (r.request_written && reg != nullptr) {
            reg->counter(tagged(metric::kRowsSubmittedTotal, op)).increment(rows);
            reg->counter(tagged(metric::kBytesSubmittedTotal, op)).increment(body.size());
        }
        Outcome o = judge_commit(r);
        if (o.verdict == Verdict::Committed) {
            const auto ns = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(took).count());
            ++counts_.batches_committed;
            counts_.rows_committed += rows;
            if (reg != nullptr) {
                reg->counter(tagged(metric::kBatchesCommittedTotal, op)).increment();
                reg->counter(tagged(metric::kRowsCommittedTotal, op)).increment(rows);
                reg->histogram(
                       tagged(metric::kCommitLatencyNs, op),
                       std::vector<double>(metric::kNsBounds.begin(), metric::kNsBounds.end()))
                    .observe(static_cast<double>(ns));
            }
            clink::metrics::connector::records_out_inc(metric::kConnector, rows);
            clink::metrics::connector::bytes_out_inc(metric::kConnector, body.size());
            clink::metrics::connector::commit_latency_observe(metric::kConnector, ns);
            if (attempt > 1) {
                clink::log::info(kLog,
                                 "neo4j sink: " + where_() + ": " + batch +
                                     " committed on attempt " + std::to_string(attempt) +
                                     (unknown > 0 ? "; " + std::to_string(unknown) +
                                                        " earlier attempt(s) had an unknown "
                                                        "outcome, so the batch may have been "
                                                        "applied more than once"
                                                  : std::string{}));
            }
            rows_.clear();
            pending_rows_ = 0;
            (void)CLINK_FAULT_POINT(points::kAfterCommit);
            return;
        }
        if (watch == RequestWatch::Why::Deadline) {
            o.detail = "the attempt passed request_timeout_ms=" +
                       std::to_string(opts_.request_timeout_ms) + "; " + o.detail;
        }
        ++counts_.attempts_failed;
        if (reg != nullptr) {
            reg->counter(tagged(metric::kAttemptsFailedTotal, op, "verdict", to_string(o.verdict)))
                .increment();
        }
        const bool unknown_now =
            o.verdict == Verdict::Unknown || o.verdict == Verdict::UnknownFinal;
        if (unknown_now) {
            ++unknown;
            ++counts_.outcome_unknown;
            counts_.rows_maybe_applied_twice += rows;
            if (reg != nullptr) {
                reg->counter(tagged(metric::kOutcomeUnknownTotal, op)).increment();
            }
        }
        if (watch == RequestWatch::Why::Cancelled) {
            fail_(code::kCancelled,
                  batch + " was not confirmed: the task was cancelled during the request" +
                      (r.request_written ? std::string("; the server may have committed it")
                                         : std::string{}) +
                      "; the rows are replayed from the last completed checkpoint");
        }
        switch (o.verdict) {
            case Verdict::Refused:
                fail_(error_code_for(o.error_class),
                      batch + " was refused and rolled back: " + o.detail);
            case Verdict::TlsRefused:
                fail_(code::kTlsVerify, o.detail);
            case Verdict::UnknownFinal:
                fail_(code::kOutcomeUnknown,
                      batch + ": " + o.detail +
                          "; the server may have committed it, and the replay after this failure "
                          "may apply it again");
            default:
                break;
        }
        const auto now = Clock::now();
        if (!window_end) {
            window_end = now + std::chrono::milliseconds{opts_.retry_window_ms};
        }
        const auto wait = backoff.next();
        if (opts_.retry_window_ms == 0 || now + wait > *window_end) {
            fail_(
                code::kRetriesExhausted,
                batch + " was not committed after " + std::to_string(attempt) +
                    " attempt(s) within retry_window_ms=" + std::to_string(opts_.retry_window_ms) +
                    (unknown > 0 ? " (" + std::to_string(unknown) +
                                       " with an unknown outcome, so it may have been applied)"
                                 : std::string{}) +
                    "; last: " + o.detail);
        }
        ++counts_.retries;
        if (reg != nullptr) {
            reg->counter(tagged(metric::kRetriesTotal, op)).increment();
        }
        clink::log::warn(kLog,
                         "neo4j sink: " + where_() + ": " + batch + " attempt " +
                             std::to_string(attempt) + " failed (" + to_string(o.verdict) +
                             "): " + o.detail +
                             (unknown_now ? "; the server may have committed it, so the retry may "
                                            "apply it again"
                                          : "") +
                             "; retrying in " + std::to_string(wait.count()) + " ms");
        if (!cancellable_wait(wait, cancel_)) {
            fail_(code::kCancelled,
                  batch +
                      " was not committed: the task was cancelled; the rows are replayed "
                      "from the last completed checkpoint");
        }
    }
}

void Neo4jSink::publish_buffer_gauges_() {
    MetricsRegistry* reg = runtime() != nullptr ? runtime()->metrics() : nullptr;
    if (reg == nullptr) {
        return;
    }
    const std::uint64_t op = runtime()->operator_id().value();
    reg->gauge(tagged(metric::kBufferedRows, op)).set(static_cast<std::int64_t>(pending_rows_));
    reg->gauge(tagged(metric::kBufferedBytes, op)).set(static_cast<std::int64_t>(rows_.size()));
}

void Neo4jSink::summary_(const char* how) const {
    clink::log::info(
        kLog,
        std::string("neo4j sink ") + how + ": subtask=" + std::to_string(opts_.subtask) + " " +
            where_() + " batches_committed=" + std::to_string(counts_.batches_committed) +
            " rows_committed=" + std::to_string(counts_.rows_committed) +
            " attempts_failed=" + std::to_string(counts_.attempts_failed) +
            " retries=" + std::to_string(counts_.retries) +
            " outcome_unknown=" + std::to_string(counts_.outcome_unknown) +
            " rows_maybe_applied_twice=" + std::to_string(counts_.rows_maybe_applied_twice) +
            " rows_not_sent=" + std::to_string(pending_rows_));
}

void Neo4jSink::close() {
    if (closed_) {
        return;
    }
    closed_ = true;
    if (watch_) {
        watch_->stop();
    }
    summary_("closed");
}

void Neo4jSink::close_cancelled() {
    if (closed_) {
        return;
    }
    closed_ = true;
    if (watch_) {
        watch_->stop();
    }
    // Rows still buffered are not sent: the task is being torn down, and a
    // restore replays everything after the last completed checkpoint.
    summary_("cancelled");
}

}  // namespace clink::neo4j
