#include "lookup.hpp"

#include <algorithm>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "fault_points.hpp"
#include "metrics.hpp"
#include "reply.hpp"
#include "types.hpp"

namespace clink::neo4j {

namespace {

using Clock = std::chrono::steady_clock;
using clink::config::JsonValue;
using clink::http_connector::BoundedResponse;
using clink::sql::Row;

constexpr std::string_view kLog = "lookup.neo4j";
constexpr std::string_view kSinkPrefix = "neo4j sink: ";
// How often the runner wakes the operator while lookups are in flight.
constexpr std::int64_t kPollMs = 2;
// The open-time EXPLAIN's reply holds a plan, which can be larger than any
// lookup's answer.
constexpr std::size_t kCheckResponseBytes = 4U << 20;

std::int64_t wall_clock_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::uint64_t ns_since(Clock::time_point t) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t).count());
}

// A message a shared helper worded for the sink, without that prefix.
std::string unprefixed(const Neo4jError& e) {
    std::string m = e.what();
    if (m.starts_with(kSinkPrefix)) {
        m.erase(0, kSinkPrefix.size());
    }
    return m;
}

std::string joined(const std::vector<std::string>& v, const char* sep) {
    std::string out;
    for (const auto& s : v) {
        out += (out.empty() ? "" : sep) + s;
    }
    return out;
}

}  // namespace

// ---- LookupPool -------------------------------------------------------------

LookupPool::LookupPool(LookupOptions options,
                       CancelSignal cancel,
                       MetricsRegistry* metrics,
                       std::uint64_t op_id)
    : opts_(std::move(options)), cancel_(std::move(cancel)), metrics_(metrics), op_id_(op_id) {
    path_ = opts_.query_path();
    body_prefix_ = "{\"statement\":";
    append_json_string(opts_.statement, body_prefix_);
    body_prefix_ += ",\"accessMode\":\"READ\"";
    if (opts_.server_timeout_s > 0) {
        body_prefix_ += ",\"maxExecutionTime\":" + std::to_string(opts_.server_timeout_s);
    }
    body_prefix_ += ",\"parameters\":{\"key\":";
    request_.headers["Accept"] = kTypedJson;
    request_.max_response_bytes = opts_.max_response_bytes;
    for (const auto& c : opts_.result_columns) {
        result_names_.push_back(c.name);
    }
}

LookupPool::~LookupPool() {
    stop();
}

std::string LookupPool::where() const {
    return "database '" + opts_.database + "' at " + opts_.endpoint;
}

void LookupPool::fail_(const char* code, const std::string& message) const {
    clink::metrics::connector::error_inc(metric::kConnector);
    throw Neo4jError(code, "neo4j lookup: " + where() + ": " + message);
}

LookupPool::Counts LookupPool::counts() const {
    return Counts{
        requests_.load(), retries_.load(), matches_.load(), no_matches_.load(), failures_.load()};
}

void LookupPool::open() {
    for (std::uint64_t i = 0; i < opts_.max_concurrent_requests; ++i) {
        auto w = std::make_unique<Worker>();
        w->http = make_client(opts_);
        w->watch = std::make_unique<RequestWatch>(*w->http);
        workers_.push_back(std::move(w));
    }
    check_statement_(*workers_.front());
    const std::lock_guard<std::mutex> lock(mu_);
    for (auto& w : workers_) {
        w->exited.store(false);
        Worker* raw = w.get();
        w->thread = std::thread([this, raw] { run_(*raw); });
    }
    started_ = true;
}

// EXPLAIN the statement with $key as Null, read-only: nothing runs, and the
// reply shows the credentials, the database, the syntax, the result columns,
// the query type and the plan. Retried like a lookup, since it reads.
//
// maxExecutionTime goes with it so that a server that refuses the field is
// found here, but not as server_timeout_s, which bounds a read. The server
// applies the limit only once it has planned the statement, and planning can
// take longer than a read may (seconds, on a server that has just started,
// with every subtask's statement planned at once), so a check held to it fails
// a statement that is fine. The check's limit is past the attempt's deadline
// instead: a check that runs out of time ends there, and is retried.
void LookupPool::check_statement_(Worker& w) {
    std::string body = "{\"statement\":";
    append_json_string("EXPLAIN " + opts_.statement, body);
    body += ",\"accessMode\":\"READ\"";
    if (opts_.server_timeout_s > 0) {
        body += ",\"maxExecutionTime\":" + std::to_string(opts_.request_timeout_ms / 1000 + 1);
    }
    body += ",\"parameters\":{\"key\":{\"$type\":\"Null\",\"_value\":null}}}";
    clink::http_connector::BoundedRequest check = request_;
    check.max_response_bytes = std::max<std::size_t>(opts_.max_response_bytes, kCheckResponseBytes);
    Backoff backoff;
    std::optional<Clock::time_point> window_end;
    std::uint64_t retries = 0;
    for (std::uint32_t attempt = 1;; ++attempt) {
        if (cancel_.requested()) {
            fail_(code::kCancelled, "the task was cancelled while the lookup was opening");
        }
        w.watch->arm(Clock::now() + std::chrono::milliseconds{opts_.request_timeout_ms}, cancel_);
        const auto r = w.http->post_bounded(path_, body, kTypedJson, check);
        const auto why = w.watch->disarm();
        if (why == RequestWatch::Why::Cancelled) {
            fail_(code::kCancelled, "the task was cancelled while the lookup was opening");
        }
        Reply reply;
        Outcome o = judge_check(r, reply);
        if (o.verdict == Verdict::Committed) {
            server_version_ = reply.server_version;
            if (const auto problems = lookup_statement_problems(reply, result_names_);
                !problems.empty()) {
                fail_(code::kStatementRefused,
                      "the statement is refused: " + joined(problems, "; "));
            }
            if (const auto scans = scan_operators(reply); !scans.empty()) {
                clink::log::warn(kLog,
                                 "neo4j lookup: " + where() +
                                     ": the statement's plan reads without an index (" +
                                     joined(scans, ", ") +
                                     "), so every lookup scans; index the property the "
                                     "statement matches $key on, or constrain it unique");
            }
            return;
        }
        if (why == RequestWatch::Why::Deadline) {
            o.detail = "the attempt passed request_timeout_ms=" +
                       std::to_string(opts_.request_timeout_ms) + "; " + o.detail;
        }
        switch (o.verdict) {
            case Verdict::Refused:
                if (o.code == "Neo.ClientError.Request.Invalid" && opts_.server_timeout_s > 0) {
                    fail_(code::kServerRefused,
                          "checking the statement failed: " + o.detail +
                              "; server_timeout_s sends maxExecutionTime, which servers before "
                              "Neo4j 2026.04 refuse, so unset it against such a server");
                }
                fail_(error_code_for(o.error_class), "checking the statement failed: " + o.detail);
            case Verdict::TlsRefused:
                fail_(code::kTlsVerify, o.detail);
            case Verdict::UnknownFinal:
                fail_(code::kResponseTooLarge,
                      "checking the statement failed: the server's plan for it is larger than " +
                          std::to_string(check.max_response_bytes) + " bytes");
            default:
                break;
        }
        const auto now = Clock::now();
        if (!window_end) {
            window_end = now + std::chrono::milliseconds{opts_.retry_window_ms};
        }
        const auto wait = backoff.next();
        if (retries >= opts_.max_retries || opts_.retry_window_ms == 0 ||
            now + wait > *window_end) {
            fail_(code::kRetriesExhausted,
                  "checking the statement failed after " + std::to_string(attempt) +
                      " attempt(s) (max_retries=" + std::to_string(opts_.max_retries) +
                      ", retry_window_ms=" + std::to_string(opts_.retry_window_ms) +
                      "): " + o.detail);
        }
        ++retries;
        clink::log::warn(kLog,
                         "neo4j lookup: " + where() + ": checking the statement, attempt " +
                             std::to_string(attempt) + " failed (" + to_string(o.verdict) + "): " +
                             o.detail + "; retrying in " + std::to_string(wait.count()) + " ms");
        if (!cancellable_wait(wait, cancel_, &stopping_)) {
            fail_(code::kCancelled, "the task was cancelled while the lookup was opening");
        }
    }
}

void LookupPool::submit(std::shared_ptr<LookupCall> call) {
    std::size_t queued = 0;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (!started_ || stopping_.load()) {
            call->error = std::make_exception_ptr(Neo4jError(
                code::kCancelled, "neo4j lookup: " + where() + ": the lookup is not open"));
            call->done.store(true, std::memory_order_release);
            return;
        }
        queue_.push_back(std::move(call));
        queued = queue_.size();
    }
    cv_.notify_one();
    publish_queue_gauges_(queued);
}

void LookupPool::publish_queue_gauges_(std::size_t queued) {
    if (metrics_ != nullptr) {
        metrics_->gauge(tagged(metric::kLookupQueued, op_id_))
            .set(static_cast<std::int64_t>(queued));
    }
}

void LookupPool::run_(Worker& w) {
    while (true) {
        std::shared_ptr<LookupCall> call;
        std::size_t queued = 0;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this] { return stopping_.load() || !queue_.empty(); });
            if (queue_.empty()) {
                break;
            }
            call = std::move(queue_.front());
            queue_.pop_front();
            queued = queue_.size();
        }
        publish_queue_gauges_(queued);
        if (stopping_.load() || cancel_.requested()) {
            ++failures_;
            call->error = std::make_exception_ptr(Neo4jError(
                code::kCancelled,
                "neo4j lookup: " + where() + ": the lookup was not run: the task was cancelled"));
            call->done.store(true, std::memory_order_release);
            continue;
        }
        serve_(w, *call);
    }
    w.exited.store(true);
}

void LookupPool::serve_(Worker& w, LookupCall& call) {
    try {
        call.result = read_(w, call.key);
        if (call.result.matched) {
            ++matches_;
        } else {
            ++no_matches_;
        }
        if (metrics_ != nullptr) {
            metrics_
                ->counter(tagged(
                    call.result.matched ? metric::kLookupMatchesTotal : metric::kLookupNoMatchTotal,
                    op_id_))
                .increment();
        }
        (void)CLINK_FAULT_POINT(points::kLookupAfterReply);
    } catch (const Neo4jError& e) {
        ++failures_;
        if (metrics_ != nullptr) {
            metrics_->counter(tagged(metric::kLookupFailuresTotal, op_id_, "code", e.code()))
                .increment();
        }
        call.error = std::current_exception();
    } catch (...) {
        ++failures_;
        call.error = std::current_exception();
    }
    if (metrics_ != nullptr) {
        metrics_
            ->histogram(tagged(metric::kLookupLatencyNs, op_id_),
                        std::vector<double>(metric::kNsBounds.begin(), metric::kNsBounds.end()))
            .observe(static_cast<double>(ns_since(call.queued_at)));
    }
    call.done.store(true, std::memory_order_release);
}

LookupResult LookupPool::read_(Worker& w, const std::string& key) {
    std::string body;
    body.reserve(body_prefix_.size() + key.size() + 2);
    body += body_prefix_;
    body += key;
    body += "}}";
    Backoff backoff;
    std::optional<Clock::time_point> window_end;
    std::uint64_t retries = 0;
    for (std::uint32_t attempt = 1;; ++attempt) {
        if (stopping_.load() || cancel_.requested()) {
            fail_(code::kCancelled,
                  "the lookup was not finished: the task was cancelled, and a restore runs it "
                  "again");
        }
        (void)CLINK_FAULT_POINT(points::kLookupBeforeRequest);
        const auto sent_at = Clock::now();
        w.watch->arm(sent_at + std::chrono::milliseconds{opts_.request_timeout_ms}, cancel_);
        const BoundedResponse r = w.http->post_bounded(path_, body, kTypedJson, request_);
        const auto why = w.watch->disarm();
        const std::int64_t observed_at = wall_clock_ms();
        ++requests_;
        if (metrics_ != nullptr) {
            metrics_->counter(tagged(metric::kLookupRequestsTotal, op_id_)).increment();
        }
        if (why == RequestWatch::Why::Cancelled || stopping_.load()) {
            fail_(code::kCancelled,
                  "the lookup was not finished: the task was cancelled during the request, and "
                  "a restore runs it again");
        }
        Reply reply;
        Outcome o = judge_read(r, reply);
        if (o.verdict == Verdict::Committed) {
            if (metrics_ != nullptr) {
                metrics_
                    ->histogram(
                        tagged(metric::kLookupRequestLatencyNs, op_id_),
                        std::vector<double>(metric::kNsBounds.begin(), metric::kNsBounds.end()))
                    .observe(static_cast<double>(ns_since(sent_at)));
            }
            LookupResult result = answer_(reply);
            result.observed_at_ms = observed_at;
            if (attempt > 1) {
                clink::log::info(kLog,
                                 "neo4j lookup: " + where() + ": answered on attempt " +
                                     std::to_string(attempt));
            }
            return result;
        }
        // The client's own read timeout can end the attempt before the
        // watch does; either way the attempt ran out of time.
        if (why == RequestWatch::Why::Deadline ||
            Clock::now() - sent_at >= std::chrono::milliseconds{opts_.request_timeout_ms}) {
            o.detail = "the attempt passed request_timeout_ms=" +
                       std::to_string(opts_.request_timeout_ms) + "; " + o.detail;
        }
        switch (o.verdict) {
            case Verdict::Refused:
                if (o.code ==
                    "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration") {
                    fail_(code::kTimeout,
                          "the server ended the read at server_timeout_s=" +
                              std::to_string(opts_.server_timeout_s) +
                              "; the statement is too slow for the limit");
                }
                fail_(error_code_for(o.error_class), "the server refused the lookup: " + o.detail);
            case Verdict::TlsRefused:
                fail_(code::kTlsVerify, o.detail);
            case Verdict::UnknownFinal:
                fail_(code::kResponseTooLarge,
                      o.detail +
                          " (max_response_bytes=" + std::to_string(opts_.max_response_bytes) + ")");
            default:
                break;
        }
        const auto now = Clock::now();
        if (!window_end) {
            window_end = now + std::chrono::milliseconds{opts_.retry_window_ms};
        }
        const auto wait = backoff.next();
        if (retries >= opts_.max_retries) {
            fail_(code::kRetriesExhausted,
                  "the lookup failed after " + std::to_string(attempt) +
                      " attempt(s), max_retries=" + std::to_string(opts_.max_retries) +
                      "; last: " + o.detail);
        }
        if (opts_.retry_window_ms == 0 || now + wait > *window_end) {
            fail_(code::kRetriesExhausted,
                  "the lookup failed after " + std::to_string(attempt) +
                      " attempt(s) within retry_window_ms=" +
                      std::to_string(opts_.retry_window_ms) + "; last: " + o.detail);
        }
        ++retries;
        ++retries_;
        if (metrics_ != nullptr) {
            metrics_->counter(tagged(metric::kLookupRetriesTotal, op_id_)).increment();
        }
        clink::log::warn(kLog,
                         "neo4j lookup: " + where() + ": attempt " + std::to_string(attempt) +
                             " failed (" + to_string(o.verdict) + "): " + o.detail +
                             "; retrying in " + std::to_string(wait.count()) + " ms");
        if (!cancellable_wait(wait, cancel_, &stopping_)) {
            fail_(code::kCancelled,
                  "the lookup was not finished: the task was cancelled, and a restore runs it "
                  "again");
        }
    }
}

LookupResult LookupPool::answer_(const Reply& reply) const {
    // The open-time check made the result columns the declared ones; a reply
    // that names others is not an answer to this statement.
    std::vector<std::size_t> index;
    for (const auto& name : result_names_) {
        const auto it = std::find(reply.fields.begin(), reply.fields.end(), name);
        if (it == reply.fields.end() || reply.fields.size() != result_names_.size()) {
            fail_(code::kMalformedReply,
                  "the reply's result columns (" + joined(reply.fields, ", ") +
                      ") are not the declared ones (" + joined(result_names_, ", ") + ")");
        }
        index.push_back(static_cast<std::size_t>(it - reply.fields.begin()));
    }
    if (reply.row_count > 1) {
        fail_(code::kMultipleRows,
              "the statement returned " + std::to_string(reply.row_count) +
                  " rows for one key, and a lookup takes at most one: it does not choose "
                  "between them. Aggregate the rows in the statement");
    }
    LookupResult result;
    result.bookmark = reply.bookmark;
    if (reply.row_count == 0) {
        return result;
    }
    result.matched = true;
    for (std::size_t i = 0; i < opts_.result_columns.size(); ++i) {
        try {
            result.values.push_back(
                decode_result(opts_.result_columns[i], reply.first_row.at(index[i])));
        } catch (const Neo4jError& e) {
            fail_(e.code().c_str(), unprefixed(e));
        }
    }
    return result;
}

void LookupPool::stop() {
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (stopping_.exchange(true) && !started_) {
            return;
        }
    }
    cv_.notify_all();
    // A request in flight ends at the next interrupt. Repeat until every
    // request thread has left, since one interrupt can land between two of
    // the client's own steps.
    const auto running = [this] {
        return std::any_of(
            workers_.begin(), workers_.end(), [](const auto& w) { return !w->exited.load(); });
    };
    while (running()) {
        for (auto& w : workers_) {
            if (!w->exited.load()) {
                w->http->interrupt();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    for (auto& w : workers_) {
        if (w->thread.joinable()) {
            w->thread.join();
        }
        w->watch->stop();
    }
    std::deque<std::shared_ptr<LookupCall>> left;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        left.swap(queue_);
        started_ = false;
    }
    for (auto& call : left) {
        call->error = std::make_exception_ptr(Neo4jError(
            code::kCancelled, "neo4j lookup: " + where() + ": the lookup was not run: it stopped"));
        call->done.store(true, std::memory_order_release);
    }
}

// ---- Neo4jLookupJoin --------------------------------------------------------

Neo4jLookupJoin::Neo4jLookupJoin(LookupOptions options) : opts_(std::move(options)) {
    shape_.probe_alias = opts_.probe_alias;
    shape_.dim_alias = opts_.dim_alias;
    shape_.probe_columns = opts_.probe_columns;
    for (const auto& c : opts_.dim_columns) {
        shape_.dim_columns.push_back(c.name);
    }
}

Neo4jLookupJoin::~Neo4jLookupJoin() {
    // The coroutines first: each holds its call, which outlives the pool.
    inner_.reset();
    if (pool_) {
        pool_->stop();
    }
}

void Neo4jLookupJoin::open() {
    CancelSignal cancel;
    MetricsRegistry* reg = nullptr;
    std::uint64_t op = id().value();
    if (runtime() != nullptr) {
        cancel = runtime()->cancel_signal();
        reg = runtime()->metrics();
        op = runtime()->operator_id().value();
    }
    pool_ = std::make_unique<LookupPool>(opts_, std::move(cancel), reg, op);
    pool_->open();
    inner_ = std::make_unique<AsyncLookupOperator<Row, Row>>(
        [this](const Row& probe) { return lookup_(probe); },
        static_cast<std::size_t>(opts_.max_in_flight),
        /*ordered=*/true,
        "neo4j_lookup_join_row");
    inner_->attach_runtime(runtime());
    inner_->set_id(id());
    clink::log::info(
        kLog,
        "neo4j lookup open: subtask=" + std::to_string(opts_.subtask) + " " + pool_->where() +
            " server=" +
            (pool_->server_version().empty() ? std::string("unknown") : pool_->server_version()) +
            " max_in_flight=" + std::to_string(opts_.max_in_flight) +
            " max_concurrent_requests=" + std::to_string(opts_.max_concurrent_requests) +
            " request_timeout_ms=" + std::to_string(opts_.request_timeout_ms) +
            " retry_window_ms=" + std::to_string(opts_.retry_window_ms) +
            " max_retries=" + std::to_string(opts_.max_retries));
}

clink::async::Task<Row> Neo4jLookupJoin::lookup_(Row probe) {
    Row dim;
    const auto it = probe.values.find(opts_.probe_key_column);
    if (it == probe.values.end() || it->second.is_null()) {
        // SQL equality never holds for NULL, so a NULL key is a no-match,
        // decided without a request.
        ++null_keys_;
        if (runtime() != nullptr && runtime()->metrics() != nullptr) {
            runtime()
                ->metrics()
                ->counter(tagged(metric::kLookupNullKeysTotal, runtime()->operator_id().value()))
                .increment();
        }
        if (!opts_.matched_column.empty()) {
            dim.values[opts_.matched_column] = JsonValue{false};
        }
        co_return clink::sql::merge_lookup_row(shape_, probe, dim);
    }
    const JsonValue key = it->second;
    auto call = std::make_shared<LookupCall>();
    try {
        encode_value_as(opts_.key_type, key, opts_.probe_key_column, call->key);
    } catch (const Neo4jError& e) {
        throw Neo4jError(e.code(), "neo4j lookup: the probe key " + unprefixed(e));
    }
    call->queued_at = Clock::now();
    pool_->submit(call);
    // The operator resumes this coroutine while it waits; nothing else does.
    while (!call->done.load(std::memory_order_acquire)) {
        co_await std::suspend_always{};
    }
    if (call->error) {
        std::rethrow_exception(call->error);
    }
    const LookupResult& result = call->result;
    if (result.matched) {
        dim.values[opts_.dim_key_column] = key;
        for (std::size_t i = 0; i < opts_.result_columns.size(); ++i) {
            dim.values[opts_.result_columns[i].name] = result.values[i];
        }
    }
    if (!opts_.matched_column.empty()) {
        dim.values[opts_.matched_column] = JsonValue{result.matched};
    }
    if (!opts_.observed_at_column.empty()) {
        dim.values[opts_.observed_at_column] = JsonValue{result.observed_at_ms};
    }
    if (!opts_.bookmark_column.empty() && !result.bookmark.empty()) {
        dim.values[opts_.bookmark_column] = JsonValue{result.bookmark};
    }
    co_return clink::sql::merge_lookup_row(shape_, probe, dim);
}

void Neo4jLookupJoin::process(const StreamElement<Row>& element, Emitter<Row>& out) {
    inner_->process(element, out);
    if (element.is_data()) {
        // Emit what has finished since, rather than at the next marker.
        inner_->poll(out);
    }
    publish_in_flight_();
}

void Neo4jLookupJoin::flush(Emitter<Row>& out) {
    inner_->flush(out);
    publish_in_flight_();
}

void Neo4jLookupJoin::fire_due_timers(Emitter<Row>& out, std::int64_t now_ms) {
    Operator::fire_due_timers(out, now_ms);
    if (inner_ && inner_->in_flight_count() > 0) {
        inner_->poll(out);
        publish_in_flight_();
    }
}

std::optional<std::int64_t> Neo4jLookupJoin::next_timer_deadline_ms() const {
    auto next = Operator::next_timer_deadline_ms();
    if (inner_ && inner_->in_flight_count() > 0 && runtime() != nullptr &&
        runtime()->timer_service() != nullptr) {
        const std::int64_t soon = runtime()->timer_service()->now_ms() + kPollMs;
        return next ? std::min(*next, soon) : soon;
    }
    return next;
}

void Neo4jLookupJoin::publish_in_flight_() {
    if (runtime() != nullptr && runtime()->metrics() != nullptr && inner_) {
        runtime()
            ->metrics()
            ->gauge(tagged(metric::kLookupInFlight, runtime()->operator_id().value()))
            .set(static_cast<std::int64_t>(inner_->in_flight_count()));
    }
}

void Neo4jLookupJoin::close() {
    if (closed_) {
        return;
    }
    closed_ = true;
    if (!pool_) {
        return;
    }
    pool_->stop();
    const auto c = pool_->counts();
    clink::log::info(
        kLog,
        "neo4j lookup closed: subtask=" + std::to_string(opts_.subtask) + " " + pool_->where() +
            " requests=" + std::to_string(c.requests) + " matches=" + std::to_string(c.matches) +
            " no_match=" + std::to_string(c.no_matches) +
            " null_keys=" + std::to_string(null_keys_) + " retries=" + std::to_string(c.retries) +
            " failures=" + std::to_string(c.failures) +
            " in_flight=" + std::to_string(in_flight()));
}

}  // namespace clink::neo4j
