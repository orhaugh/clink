#include "native/native_sink_core.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/api.h>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/counter.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/memory_budget.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "native/arrow_to_block.hpp"
#include "native/column_plan.hpp"
#include "native/default_ca.hpp"
#include "native/error_class.hpp"
#include "native/errors.hpp"
#include "native/metrics.hpp"
#include "native/retry.hpp"
#include "native/sql_text.hpp"
#include "native/statements.hpp"
#include "native/target_table.hpp"
#include "native/types.hpp"
#include "native/writer.hpp"

namespace clink::clickhouse::native {

namespace {

namespace mm = clink::metrics;
using Clock = std::chrono::steady_clock;

// The longest the task thread waits on the opener without looking at the
// task's cancel signal.
constexpr std::chrono::milliseconds kSlice{50};
// How long a cancelled open waits for the opener before it detaches it. Only
// DNS, the TCP connect poll and the TLS handshake can hold an opener this long:
// they run before the socket has an fd that interrupt() could shut down.
constexpr std::chrono::seconds kJoinWait{5};
constexpr std::string_view kLogSource = "sink.clickhouse";
// The smallest batch_bytes the options accept, and so the smallest the memory
// cap may leave.
constexpr std::uint64_t kMinBatchBytes = 1ULL << 20;
// Below this every INSERT is small, whatever the rate.
constexpr std::uint64_t kSmallBatchRows = 10000;

// Thrown on the opener thread once the task is cancelled or the open is
// interrupted: the opener drops what it has and reports the cancel.
struct OpenerStopped {};

std::string tagged(const char* metric, std::uint64_t op_id) {
    return std::string(metric) + "{op_id=\"" + std::to_string(op_id) + "\"}";
}

std::string tagged(const char* metric,
                   std::uint64_t op_id,
                   std::string_view key,
                   std::string_view value) {
    std::string out(metric);
    out += "{op_id=\"";
    out += std::to_string(op_id);
    out += "\",";
    out += key;
    out += "=\"";
    out += value;
    out += "\"}";
    return out;
}

std::string endpoint_text(const Endpoint& endpoint) {
    const bool v6 = endpoint.host.find(':') != std::string::npos;
    return (v6 ? "[" + endpoint.host + "]" : endpoint.host) + ":" + std::to_string(endpoint.port);
}

std::string version_text(const ServerIdentity& server) {
    return std::to_string(server.major) + "." + std::to_string(server.minor) + "." +
           std::to_string(server.patch);
}

std::string subtask_text(const SinkOptions& options) {
    return std::to_string(options.subtask_idx) + "/" + std::to_string(options.parallelism);
}

const char* phase_name(Phase phase) {
    switch (phase) {
        case Phase::Connect:
            return "connect";
        case Phase::Metadata:
            return "metadata";
        case Phase::Begin:
            return "begin";
        case Phase::Send:
            return "send";
        case Phase::End:
            return "end";
    }
    return "unknown";
}

const char* compression_name(Compression compression) {
    switch (compression) {
        case Compression::None:
            return "none";
        case Compression::Lz4:
            return "lz4";
        case Compression::Zstd:
            return "zstd";
    }
    return "unknown";
}

std::string one_decimal(double value) {
    std::array<char, 64> buffer{};
    const auto result = std::to_chars(
        buffer.data(), buffer.data() + buffer.size(), value, std::chars_format::fixed, 1);
    return result.ec == std::errc{} ? std::string(buffer.data(), result.ptr) : std::string("?");
}

// The codes that refuse a configuration rather than report a failure to write:
// they count in refusals_total. A run-time failure, an exhausted window and a
// cancel do not.
bool is_refusal(std::string_view code) {
    return code != code::kCancelled && code != code::kRetryWindowExhausted &&
           code != code::kInsertFailed && code != code::kHeaderDrift &&
           code != code::kConversionFailed && code != code::kTooManyPartitions;
}

// What the opener hands the task thread: a client connected to a server that
// passed every check, that server's description, and the column plan.
struct Opened {
    std::unique_ptr<InsertTransport> transport;
    TargetInfo target;
    ColumnPlan plan;
};

// The opener's whole world: what it reads, what it produces, and what the task
// thread uses to interrupt it. Shared with the thread, so an opener the task
// thread had to detach still owns everything it touches.
struct OpenState {
    // Fixed before the thread starts.
    SinkOptions options;
    TransportFactory factory;
    std::vector<SqlColumn> columns;
    InputKind kind{InputKind::SqlTable};
    std::string qualified;
    CancelSignal cancel;
    spdlog::logger* logger{nullptr};
    MetricsRegistry* metrics{nullptr};
    std::uint64_t op_id{0};

    std::atomic<bool> stop{false};

    std::mutex mu;
    std::condition_variable cv;
    // Under mu. The transport the opener is using, so the task thread can
    // interrupt it; the opener clears it before it lets the transport go.
    InsertTransport* live{nullptr};
    bool interrupted{false};
    bool done{false};
    std::unique_ptr<InsertTransport> transport;
    std::optional<TargetInfo> target;
    std::optional<ColumnPlan> plan;
    std::exception_ptr failure;

    // The registry and the logger belong to the host, and a detached opener
    // can outlive the job, so every report goes through report(), which stops
    // for good once the task thread detaches the opener.
    std::mutex report_mu;
    bool detached{false};

    void run() noexcept;
    void attempts(std::unique_ptr<InsertTransport>& t,
                  std::optional<TargetInfo>& found,
                  std::optional<ColumnPlan>& planned);
    void publish(InsertTransport& t);
    void check_stop() const;
    // Task thread: stop the opener and wake a call it is blocked in. Sticky,
    // so a transport the opener has yet to build is interrupted as it is
    // published.
    void interrupt() noexcept;

    template <class F>
    void report(F&& f) noexcept {
        try {
            const std::lock_guard<std::mutex> lock(report_mu);
            if (detached) {
                return;
            }
            std::forward<F>(f)();
        } catch (...) {
            // A report that fails must not take the open down with it.
        }
    }
};

void OpenState::run() noexcept {
    std::unique_ptr<InsertTransport> t;
    std::optional<TargetInfo> found;
    std::optional<ColumnPlan> planned;
    std::exception_ptr failed;
    try {
        attempts(t, found, planned);
    } catch (const OpenerStopped&) {
        failed = std::make_exception_ptr(NativeSinkError(
            code::kCancelled,
            "clickhouse native sink: the task was cancelled while the sink was opening " +
                qualified));
    } catch (...) {
        failed = std::current_exception();
    }
    if (failed && t) {
        // abandon() is the only way the sink disposes of a client.
        t->abandon();
    }
    {
        const std::lock_guard<std::mutex> lock(mu);
        live = nullptr;
        if (failed) {
            failure = failed;
        } else {
            transport = std::move(t);
            target = std::move(found);
            plan = std::move(planned);
        }
        done = true;
    }
    cv.notify_all();
    // A transport that failed goes here, after the task thread can no longer
    // reach it through `live`.
}

void OpenState::attempts(std::unique_ptr<InsertTransport>& t,
                         std::optional<TargetInfo>& found,
                         std::optional<ColumnPlan>& planned) {
    // The window covers the whole open, so an outage shorter than it costs
    // the task nothing.
    RetryWindow window(options.retry_window);
    window.start(Clock::now());
    Backoff backoff;
    std::size_t endpoint = 0;
    std::uint32_t unclassified = 0;
    std::uint32_t validation = 0;
    for (std::uint32_t attempt = 1;; ++attempt) {
        check_stop();
        const Endpoint ep = options.endpoints.at(endpoint);
        Phase phase = Phase::Connect;
        std::optional<Failure> failure_seen;
        try {
            if (!t) {
                t = factory(options);
                if (!t) {
                    throw std::runtime_error("the transport factory made no transport");
                }
                publish(*t);
            }
            check_stop();
            t->connect(ep);
            check_stop();
            if (attempt > 1) {
                // A client built after a failed attempt, which the writer
                // counts the same way once it runs.
                report([this] {
                    if (metrics != nullptr) {
                        metrics->counter(tagged(metric::kReconnectsTotal, op_id)).increment();
                    }
                });
            }
            phase = Phase::Metadata;
            TargetInfo info = probe_target(*t, options);
            ColumnPlan p = compile_or_refuse(columns, info.columns, qualified, kind);
            found = std::move(info);
            planned = std::move(p);
            return;
        } catch (const OpenerStopped&) {
            throw;
        } catch (const NativeSinkError&) {
            // A refusal rests on what a server said or on the options, which
            // no retry changes. It is still a failed attempt, which the
            // writer counts the same way when a reconnect meets one.
            report([] { mm::connector::error_inc(metric::kConnector); });
            throw;
        } catch (...) {
            failure_seen = to_failure(std::current_exception(), phase, false);
        }
        const Failure& f = *failure_seen;
        if (t) {
            // Whatever the client had in flight is dropped before anything
            // else; the next attempt builds a new one.
            t->abandon();
        }
        check_stop();
        report([] { mm::connector::error_inc(metric::kConnector); });

        const FailureClass cls = classify(f, false);
        const Decision decision = action_for(cls, f, AttemptState{0, unclassified, validation});
        if (cls == FailureClass::Unclassified) {
            ++unclassified;
        }
        if (f.signal == Signal::Validation) {
            ++validation;
        }
        if (decision.action == Action::Fail) {
            throw NativeSinkError(
                decision.fail_code != nullptr ? decision.fail_code : code::kInsertFailed,
                "clickhouse native sink: opening " + qualified + " on " + endpoint_text(ep) +
                    " failed at " + phase_name(phase) + ": " + f.message);
        }
        if (phase == Phase::Connect) {
            // Move on from an endpoint that refuses; stay on one that answers.
            endpoint = (endpoint + 1) % options.endpoints.size();
        }
        const std::chrono::milliseconds wait = backoff.next();
        if (!window.allows(Clock::now(), wait)) {
            throw NativeSinkError(
                code::kRetryWindowExhausted,
                "clickhouse native sink: could not open " + qualified + " within retry_window_ms=" +
                    std::to_string(options.retry_window.count()) + " (" + std::to_string(attempt) +
                    " attempts); the last failed at " + phase_name(phase) + " on " +
                    endpoint_text(ep) + " (" + to_string(cls) + "): " + f.message);
        }
        // The same series the writer moves for a retried INSERT, so an outage
        // that holds the open shows in them while it lasts, not only once a
        // client gets through.
        report([&] {
            if (metrics != nullptr) {
                metrics->counter(tagged(metric::kRetriesTotal, op_id, "class", to_string(cls)))
                    .increment();
                if (cls == FailureClass::MergeBackpressure) {
                    metrics->counter(tagged(metric::kPartsBackoffTotal, op_id)).increment();
                }
            }
        });
        report([&] {
            clink::logging::op_log(
                logger,
                LogSeverity::Warn,
                kLogSource,
                "clickhouse native sink: subtask=" + subtask_text(options) + " opening " +
                    qualified + ": attempt " + std::to_string(attempt) + " failed at " +
                    phase_name(phase) + " on " + endpoint_text(ep) + " (" + to_string(cls) +
                    "): " + f.message + "; retrying in " + std::to_string(wait.count()) + " ms");
        });
        const Clock::time_point wait_start = Clock::now();
        if (!cancellable_wait(wait, cancel, stop)) {
            throw OpenerStopped{};
        }
        const auto waited =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - wait_start).count();
        report([&] {
            if (metrics != nullptr) {
                // The writer's bounds: whichever registers the series first
                // sets them.
                metrics
                    ->histogram(tagged(metric::kRetryWaitNs, op_id),
                                {metric::kNsBounds.begin(), metric::kNsBounds.end()})
                    .observe(static_cast<double>(waited));
            }
        });
    }
}

void OpenState::publish(InsertTransport& t) {
    const std::lock_guard<std::mutex> lock(mu);
    live = &t;
    if (interrupted) {
        t.interrupt();
    }
}

void OpenState::check_stop() const {
    if (stop.load(std::memory_order_acquire) || cancel.requested()) {
        throw OpenerStopped{};
    }
}

void OpenState::interrupt() noexcept {
    stop.store(true, std::memory_order_release);
    const std::lock_guard<std::mutex> lock(mu);
    interrupted = true;
    if (live != nullptr) {
        live->interrupt();
    }
}

// The opener thread, joined or left behind on every way out of open(), so
// that no path can destroy it while it still runs.
class OpenerThread {
public:
    explicit OpenerThread(std::shared_ptr<OpenState> state)
        : state_(std::move(state)), thread_([s = state_] { s->run(); }) {}
    ~OpenerThread() { leave(); }
    OpenerThread(const OpenerThread&) = delete;
    OpenerThread& operator=(const OpenerThread&) = delete;
    OpenerThread(OpenerThread&&) = delete;
    OpenerThread& operator=(OpenerThread&&) = delete;

    void join() { thread_.join(); }

    // Interrupts it, silences it and lets it go. It owns its state through
    // its own share, and its interrupted transport cannot be used when the
    // call it is blocked in returns.
    void leave() noexcept {
        if (!thread_.joinable()) {
            return;
        }
        state_->interrupt();
        {
            const std::lock_guard<std::mutex> lock(state_->report_mu);
            state_->detached = true;
        }
        try {
            thread_.detach();
        } catch (...) {
            // detach throws only for a thread that cannot be joined, which
            // joinable() has ruled out.
        }
    }

private:
    std::shared_ptr<OpenState> state_;
    std::thread thread_;
};

}  // namespace

struct SinkCore::Impl {
    Impl(SinkOptions o, std::vector<SqlColumn> c, InputKind k, TransportFactory f)
        : options(std::move(o)),
          factory(f ? std::move(f) : current_transport_factory()),
          columns(std::move(c)),
          kind(k),
          qualified(qualified_table(options.database, options.table)) {}

    void cache(const RuntimeContext* ctx, std::uint64_t fallback_id, const std::string& fallback);
    void apply_memory_cap();
    [[nodiscard]] Opened open_target();
    void start(Opened opened, const std::function<void()>& prepare);
    void report_open(const TargetInfo& target, const ColumnPlan& plan) const;
    void open_failed(const NativeSinkError& e) const;
    void require_writer(const char* call) const;
    // The tail every intake shares: charge a built chunk to the budget, queue
    // it and count it under its carrier. `shares_input` when the chunk reuses
    // arrays of the batch it was built from.
    void submit(std::shared_ptr<arrow::RecordBatch> rows, Carrier carrier, bool shares_input);
    void conversion_failed(const ConversionError& e) const;
    void count_refusal(const std::string& code) const;
    void log(LogSeverity level, const std::string& message) const;
    [[nodiscard]] std::string subtask() const { return subtask_text(options); }

    SinkOptions options;  // batch_bytes capped to the memory budget at open
    const TransportFactory factory;
    const std::vector<SqlColumn> columns;
    const InputKind kind;
    const std::string qualified;

    // Copied from the RuntimeContext at open: the context goes before the
    // sink does, so nothing reads it afterwards.
    CancelSignal cancel;
    MetricsRegistry* metrics{nullptr};
    spdlog::logger* logger{nullptr};
    std::shared_ptr<MemoryBudget> budget;
    std::uint64_t op_id{0};
    std::string sink_id;

    // What batch_bytes was before the memory cap reduced it, when it did.
    std::optional<std::uint64_t> reduced_from;
    Clock::time_point opened_at{};

    // Registered at open, so every series shows from the start. Null without
    // a context.
    Counter* columnar_batches{nullptr};
    Counter* row_batches{nullptr};

    // Declared last, so it goes first: its destructor aborts a writer that
    // was never closed.
    std::unique_ptr<Writer> writer;
};

void SinkCore::Impl::cache(const RuntimeContext* ctx,
                           std::uint64_t fallback_id,
                           const std::string& fallback) {
    if (ctx == nullptr) {
        op_id = fallback_id;
        sink_id = sanitise_sink_id(fallback);
        return;
    }
    cancel = ctx->cancel_signal();
    metrics = ctx->metrics();
    logger = ctx->logger();
    budget = ctx->memory_budget();
    op_id = ctx->operator_id().value();
    // The same id the INSERTs carry in log_comment, so the open report and
    // system.query_log can be matched.
    sink_id = sanitise_sink_id(ctx->operator_name());
    if (metrics != nullptr) {
        columnar_batches = &metrics->counter(
            tagged(metric::kInputBatchesTotal, op_id, "carrier", metric::kCarrierColumnar));
        row_batches = &metrics->counter(
            tagged(metric::kInputBatchesTotal, op_id, "carrier", metric::kCarrierRow));
    }
}

void SinkCore::Impl::apply_memory_cap() {
    if (!budget || budget->limit() == 0) {
        // Unaccounted: there is no limit to plan against.
        return;
    }
    // The sink plans for at most half of the budget's own limit: the queue
    // plus up to twice batch_bytes held for one INSERT. The sum is compared
    // without being formed, because a batch_bytes near 2^63 would wrap it.
    const std::uint64_t limit = budget->limit();
    const std::uint64_t half = limit / 2;
    const std::uint64_t queue = kQueueBytes;
    if (half >= queue && options.batch_bytes <= (half - queue) / 2) {
        return;
    }
    const std::uint64_t reduced = half > queue ? (half - queue) / 2 : 0;
    if (reduced < kMinBatchBytes) {
        throw NativeSinkError(
            code::kMemoryBudgetTooSmall,
            "clickhouse native sink: the memory budget's limit of " + std::to_string(limit) +
                " bytes is too small: the sink plans its buffers into half of it, the writer's "
                "queue takes " +
                std::to_string(queue) +
                " bytes of that, and what is left gives each INSERT less than the 1 MiB that "
                "batch_bytes needs at least. Give the job a budget of at least " +
                std::to_string(2 * (queue + 2 * kMinBatchBytes)) + " bytes, or none.");
    }
    reduced_from = options.batch_bytes;
    options.batch_bytes = reduced;
}

Opened SinkCore::Impl::open_target() {
    if (cancel.requested()) {
        throw NativeSinkError(
            code::kCancelled,
            "clickhouse native sink: the task was cancelled before the sink opened " + qualified);
    }
    auto state = std::make_shared<OpenState>();
    state->options = options;
    state->factory = factory;
    state->columns = columns;
    state->kind = kind;
    state->qualified = qualified;
    state->cancel = cancel;
    state->logger = logger;
    state->metrics = metrics;
    state->op_id = op_id;
    // The opener's own share of the state is what makes a detach safe.
    OpenerThread opener(state);

    bool cancelled = false;
    {
        std::unique_lock<std::mutex> lock(state->mu);
        while (!state->done) {
            if (cancel.requested()) {
                cancelled = true;
                break;
            }
            state->cv.wait_for(lock, kSlice);
        }
    }
    if (!cancelled) {
        // The join is the happens-before edge that hands the client to this
        // thread, and from here to the writer's.
        opener.join();
        if (state->failure) {
            std::rethrow_exception(state->failure);
        }
        return Opened{
            std::move(state->transport), std::move(*state->target), std::move(*state->plan)};
    }

    // A cancel must not wait on a blocked socket call: interrupt it, give the
    // opener a moment to see that, then leave it behind.
    state->interrupt();
    bool exited = false;
    {
        std::unique_lock<std::mutex> lock(state->mu);
        exited = state->cv.wait_for(lock, kJoinWait, [&state] { return state->done; });
    }
    if (exited) {
        opener.join();
        if (state->transport) {
            // It finished as the cancel arrived: drop the client unused.
            state->transport->abandon();
            state->transport.reset();
        }
    } else {
        // Still inside DNS, the connect poll or the TLS handshake, where no
        // interrupt reaches.
        opener.leave();
    }
    throw NativeSinkError(
        code::kCancelled,
        "clickhouse native sink: the task was cancelled while the sink was opening " + qualified);
}

void SinkCore::Impl::start(Opened opened, const std::function<void()>& prepare) {
    try {
        if (prepare) {
            prepare();
        }
        report_open(opened.target, opened.plan);
    } catch (...) {
        // abandon() is the only way the sink disposes of a client.
        opened.transport->abandon();
        throw;
    }

    WriterConfig config;
    config.options = options;
    config.plan = std::move(opened.plan);
    config.target = std::move(opened.target);
    config.sink_id = sink_id;
    config.budget = budget;
    config.cancel = cancel;
    config.metrics = metrics;
    config.op_id = op_id;
    config.logger = logger;
    // Runs on the writer thread, which may outlive this sink once detached, so
    // it holds its own copy of the options and nothing else.
    config.reprobe = [opts = options](InsertTransport& t) { return probe_target(t, opts); };
    writer = std::make_unique<Writer>(
        std::move(config), std::move(opened.transport), TokenSource::random());
}

void SinkCore::Impl::report_open(const TargetInfo& target, const ColumnPlan& plan) const {
    const std::string sub = subtask();
    std::string line = "clickhouse native sink open: subtask=" + sub;
    line += kind == InputKind::SqlTable ? " factory=clickhouse_native_sink"
                                        : " factory=make_clickhouse_native_sink input=typed";
    line += " mode=append delivery=at_least_once";
    line += " table=" + qualified;
    line += " engine=" + target.engine;
    line += " server=" + version_text(target.server) +
            (target.tested_line ? " (tested)" : " (accepted, untested)");
    line += " endpoint=" + endpoint_text(target.server.endpoint);
    line += " tls=";
    line += !options.tls.enabled ? "off" : (options.tls.verify ? "on" : "on (verify off)");
    if (const auto fallback = fallback_ca_location(options.tls)) {
        // The linked OpenSSL's own CA locations do not exist on this system.
        line += " (system CAs from " + fallback->path + ")";
    }
    line += " compression=";
    line += compression_name(options.compression);
    line += " dedup_setting=";
    line += target.caps.deduplicate_insert ? "deduplicate_insert" : "insert_deduplicate";
    line += " strict_limits=";
    line += target.caps.use_strict_insert_block_limits ? "sent" : "not_sent";
    line += " token_on_resend=";
    line += target.keep_token_on_resend ? "kept" : "fresh";
    line += " dedup_window=" + target.dedup_report;
    line += " " + target.async_report;
    line += " columns=" + std::to_string(plan.columns.size());
    line += " omitted=" + std::to_string(plan.omitted.size());
    line += " batch_rows=" + std::to_string(options.batch_rows);
    line += " batch_bytes=" + std::to_string(options.batch_bytes);
    if (reduced_from) {
        line += " (reduced from " + std::to_string(*reduced_from) + " for the memory budget)";
    }
    line += " batch_interval_ms=" + std::to_string(options.batch_interval.count());
    line += " retry_window_ms=" + std::to_string(options.retry_window.count());
    // A floor for a steady stream: a fill of batch_rows or batch_bytes before
    // the interval, and every checkpoint barrier, only add to it, and the
    // sink cannot see the checkpoint interval.
    const auto interval =
        std::max<std::chrono::milliseconds::rep>(options.batch_interval.count(), 1);
    line += " inserts_per_s_est=" +
            one_decimal(static_cast<double>(options.parallelism) * 1000.0 /
                        static_cast<double>(interval)) +
            "+ckpt";
    // The engine's bound on a bounded job's final checkpoint, as this process
    // reads it. Information only: the sink cannot tell whether its job is
    // bounded.
    line += " eos_bound_ms=" + std::to_string(eos_final_checkpoint_timeout().count());
    log(LogSeverity::Info, line);

    const std::string prefix = "clickhouse native sink: subtask=" + sub + " ";
    if (!target.tested_line) {
        log(LogSeverity::Warn,
            prefix +
                (target.server.display_name.empty() ? std::string("the server")
                                                    : target.server.display_name) +
                " " + version_text(target.server) + " at " + endpoint_text(target.server.endpoint) +
                " is on a line the native sink accepts but has not been tested against; 26.3 "
                "and 26.8 are tested");
    }
    if (!target.keeps_dedup_log) {
        log(LogSeverity::Warn,
            prefix + qualified + " keeps no deduplication log (" + target.dedup_report +
                "), so an INSERT resent after a failure that left it in doubt may land twice");
    }
    if (options.batch_rows < kSmallBatchRows) {
        log(LogSeverity::Warn,
            prefix + "batch_rows=" + std::to_string(options.batch_rows) +
                " is below 10000, so every INSERT is small, and each makes at least one part "
                "per partition it touches; many small parts slow the table's merges. Raise "
                "batch_rows unless the stream is slow enough for the merges to keep up");
    }
    if (reduced_from) {
        log(LogSeverity::Warn,
            prefix + "batch_bytes is reduced from " + std::to_string(*reduced_from) + " to " +
                std::to_string(options.batch_bytes) +
                " so that the writer's queue and up to twice batch_bytes held for one INSERT "
                "fit in half of the memory budget's limit of " +
                std::to_string(budget ? budget->limit() : 0) + " bytes");
    }

    if (options.subtask_idx == 0) {
        // Once per job: every subtask has the same plan and options.
        log(LogSeverity::Info, plan.report());
        std::string described = "clickhouse native sink options:";
        const std::string description = describe(options);
        std::string_view rest = description;
        while (!rest.empty()) {
            const std::size_t end = rest.find('\n');
            std::string_view entry = rest.substr(0, end);
            described += "\n  ";
            // The declared types are in the column plan just logged, and the
            // planner's spelling of them can be long. A typed sink takes them
            // from its batcher's schema instead.
            if (entry.starts_with("sql_column_types=")) {
                described +=
                    kind == InputKind::SqlTable
                        ? "sql_column_types=" + std::to_string(columns.size()) +
                              " columns, in the column plan"
                        : "columns=" + std::to_string(columns.size()) + " from the batcher schema";
            } else {
                described += entry;
            }
            rest = end == std::string_view::npos ? std::string_view{} : rest.substr(end + 1);
        }
        log(LogSeverity::Info, described);
    }
}

void SinkCore::Impl::open_failed(const NativeSinkError& e) const {
    if (is_refusal(e.code())) {
        count_refusal(e.code());
    }
    log(e.code() == code::kCancelled ? LogSeverity::Warn : LogSeverity::Error,
        "clickhouse native sink: subtask=" + subtask() + " did not open: " + e.what());
}

void SinkCore::Impl::require_writer(const char* call) const {
    if (!writer) {
        throw std::logic_error(std::string("clickhouse native sink: ") + call +
                               " before open() completed");
    }
}

void SinkCore::Impl::submit(std::shared_ptr<arrow::RecordBatch> rows,
                            Carrier carrier,
                            bool shares_input) {
    Chunk chunk;
    // A reused array is counted in full, even when the chunk holds a slice of
    // it, so the charge is never below what the chunk keeps alive.
    chunk.bytes = chunk_bytes(*rows);
    chunk.reservation = MemoryReservation(budget, MemoryCategory::Queue, chunk.bytes);
    chunk.batch = std::move(rows);
    chunk.carrier = carrier;
    chunk.shares_input = shares_input;
    writer->submit(std::move(chunk));
    if (Counter* c = carrier == Carrier::Columnar ? columnar_batches : row_batches) {
        c->increment();
    }
}

void SinkCore::Impl::conversion_failed(const ConversionError& e) const {
    // The reason names the column, the row of the batch, the declared type
    // and the kind of the cell, and never its text.
    log(LogSeverity::Error,
        "clickhouse native sink: subtask=" + subtask() + " cannot write a row of this batch to " +
            qualified + ": " + e.what());
}

void SinkCore::Impl::count_refusal(const std::string& code) const {
    if (metrics != nullptr) {
        metrics->counter(tagged(metric::kRefusalsTotal, op_id, "reason", code)).increment();
    }
}

void SinkCore::Impl::log(LogSeverity level, const std::string& message) const {
    try {
        clink::logging::op_log(logger, level, kLogSource, message);
    } catch (...) {
        // A log line that cannot be written is not a reason to fail the task.
    }
}

// ---------------------------------------------------------------------------
// SinkCore

SinkCore::SinkCore(SinkOptions options,
                   std::vector<SqlColumn> columns,
                   InputKind kind,
                   TransportFactory factory)
    : impl_(std::make_unique<Impl>(
          std::move(options), std::move(columns), kind, std::move(factory))) {}

SinkCore::~SinkCore() = default;

void SinkCore::open(const RuntimeContext* ctx,
                    OperatorId fallback_id,
                    const std::string& fallback_name,
                    const std::function<void()>& prepare) {
    Impl& s = *impl_;
    if (s.writer) {
        throw std::logic_error("clickhouse native sink: open() called twice");
    }
    s.cache(ctx, fallback_id.value(), fallback_name);
    s.opened_at = Clock::now();
    try {
        // The checks that need no server come first, so a refusal never waits
        // out an outage. Every checkpoint barrier mode is accepted: a fan-in
        // upstream aligns every barrier whatever its stamp, so one stamped
        // Unaligned reaches this sink only along a single in-order path,
        // where it cuts exactly where an aligned one would.
        s.apply_memory_cap();
        s.start(s.open_target(), prepare);
    } catch (const NativeSinkError& e) {
        s.open_failed(e);
        throw;
    }
}

void SinkCore::require_writer(const char* call) const {
    impl_->require_writer(call);
}

void SinkCore::drain_released() noexcept {
    if (impl_->writer) {
        impl_->writer->drain_released();
    }
}

void SinkCore::submit(std::shared_ptr<arrow::RecordBatch> rows,
                      Carrier carrier,
                      bool shares_input) {
    impl_->submit(std::move(rows), carrier, shares_input);
}

void SinkCore::abort() noexcept {
    if (impl_->writer) {
        impl_->writer->abort();
    }
}

void SinkCore::barrier(CheckpointBarrier barrier) {
    Impl& s = *impl_;
    s.require_writer("on_barrier");
    s.writer->drain_released();
    try {
        s.writer->flush(barrier.id().value());
    } catch (...) {
        s.writer->abort();
        throw;
    }
    // The flush let go of every chunk acknowledged before the barrier.
    s.writer->drain_released();
}

void SinkCore::flush() {
    Impl& s = *impl_;
    if (!s.writer) {
        return;
    }
    s.writer->drain_released();
    try {
        s.writer->flush(0);
    } catch (...) {
        s.writer->abort();
        throw;
    }
    s.writer->drain_released();
}

void SinkCore::close() {
    Impl& s = *impl_;
    if (!s.writer) {
        return;
    }
    // A failure here is right: the rows since the last checkpoint were not
    // acknowledged, and the job's restart replays them. finish() has then
    // logged the cancelled summary itself.
    s.writer->finish();
    // The writer has stopped, so nothing more reaches the list.
    s.writer->drain_released();
    s.log(LogSeverity::Info,
          summary_line(
              "closed",
              s.options,
              s.writer->stats(),
              std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - s.opened_at)));
    s.writer.reset();
}

void SinkCore::close_cancelled() noexcept {
    Impl& s = *impl_;
    if (s.writer) {
        s.writer->abort();
        // A writer that joined has let go of everything; one that abort()
        // detached keeps what it still holds.
        s.writer->drain_released();
    }
}

void SinkCore::conversion_failed(const ConversionError& e) const {
    impl_->conversion_failed(e);
}

void SinkCore::log(LogSeverity level, const std::string& message) const {
    impl_->log(level, message);
}

std::string SinkCore::subtask() const {
    return impl_->subtask();
}

const std::vector<SqlColumn>& SinkCore::columns() const noexcept {
    return impl_->columns;
}

}  // namespace clink::clickhouse::native
