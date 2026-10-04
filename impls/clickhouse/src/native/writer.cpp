#include "native/writer.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <exception>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/counter.hpp"
#include "clink/metrics/gauge.hpp"
#include "clink/metrics/histogram.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "native/errors.hpp"
#include "native/fault_points.hpp"
#include "native/metrics.hpp"
#include "native/retry.hpp"
#include "native/statements.hpp"

namespace clink::clickhouse::native {

namespace {

namespace mm = clink::metrics;
using Clock = std::chrono::steady_clock;

// The longest any wait, on either thread, goes without looking at the task's
// cancel signal and the stop flag.
constexpr std::chrono::milliseconds kSlice{50};
// How long abort() waits for the writer before it detaches it. Only DNS, the
// TCP connect poll and the TLS handshake can hold a writer this long: they run
// before the socket has an fd that interrupt() could shut down.
constexpr std::chrono::seconds kJoinWait{5};
constexpr std::string_view kLogSource = "sink.clickhouse";
// Below this mean an INSERT counts as small for the part-rate rule.
constexpr std::uint64_t kSmallInsertRows = 10000;

// Thrown on the writer thread once the stop flag or the task's cancel is
// seen: the writer abandons what is in flight and exits.
struct Stopped {};
// Thrown on the writer thread once a permanent failure has been stored and
// reported, to unwind to the thread's exit.
struct Failed {};

// `t + d`, held at the clock's maximum rather than overflowing: a window or
// an interval built directly need not respect the option bounds.
Clock::time_point later(Clock::time_point t, std::chrono::milliseconds d) {
    if (d <= std::chrono::milliseconds::zero()) {
        return t;
    }
    const auto headroom = Clock::time_point::max() - t;
    if (d >= std::chrono::duration_cast<std::chrono::milliseconds>(headroom)) {
        return Clock::time_point::max();
    }
    return t + std::chrono::duration_cast<Clock::duration>(d);
}

std::uint64_t to_ns(Clock::duration d) {
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
    return ns < 0 ? 0 : static_cast<std::uint64_t>(ns);
}

std::chrono::milliseconds to_ms(Clock::duration d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

// The registry has no first-class labels, so they are inlined in the name in
// the {key="value"} form the Prometheus renderer splits back out.
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

std::string endpoint_text(const Endpoint& endpoint) {
    return endpoint.host + ":" + std::to_string(endpoint.port);
}

std::string identity_text(const ServerIdentity& server) {
    return (server.display_name.empty() ? std::string("an unnamed server") : server.display_name) +
           " " + std::to_string(server.major) + "." + std::to_string(server.minor) + "." +
           std::to_string(server.patch) + " at " + endpoint_text(server.endpoint);
}

// Two clients reach the same server when its name, its version and the
// endpoint all agree. A replica, or the same host restarted into another
// version, may squash or hash a resend differently, so either change counts.
bool same_server(const ServerIdentity& a, const ServerIdentity& b) {
    return a.display_name == b.display_name && a.major == b.major && a.minor == b.minor &&
           a.patch == b.patch && a.revision == b.revision && a.endpoint == b.endpoint;
}

// The text of a failure without the "[code] " a NativeSinkError of ours puts
// in front, since the error built from it carries the code again.
std::string failure_text(const Failure& f) {
    if (f.sink_code != nullptr) {
        const std::string prefix = std::string("[") + f.sink_code + "] ";
        if (f.message.starts_with(prefix)) {
            return f.message.substr(prefix.size());
        }
    }
    return f.message;
}

std::string error_text(const NativeSinkError& e) {
    const std::string prefix = "[" + e.code() + "] ";
    const std::string text = e.what();
    return text.starts_with(prefix) ? text.substr(prefix.size()) : text;
}

std::string one_decimal(double value) {
    std::array<char, 64> buffer{};
    const auto result = std::to_chars(
        buffer.data(), buffer.data() + buffer.size(), value, std::chars_format::fixed, 1);
    return result.ec == std::errc{} ? std::string(buffer.data(), result.ptr) : std::string("?");
}

template <std::size_t N>
std::vector<double> bounds_of(const std::array<double, N>& bounds) {
    return {bounds.begin(), bounds.end()};
}

// The sink's own series, created once on the task thread before the writer
// starts, so the hot paths only touch atomics. Every pointer is null when
// there is no registry.
struct SinkMetrics {
    SinkMetrics(MetricsRegistry* reg, std::uint64_t id) : registry(reg), op_id(id) {
        if (registry == nullptr) {
            return;
        }
        inserts_ok =
            &registry->counter(tagged(metric::kInsertsTotal, op_id, "outcome", metric::kOutcomeOk));
        inserts_retried_ok = &registry->counter(
            tagged(metric::kInsertsTotal, op_id, "outcome", metric::kOutcomeRetriedOk));
        inserts_failed = &registry->counter(
            tagged(metric::kInsertsTotal, op_id, "outcome", metric::kOutcomeFailed));
        inserts_abandoned = &registry->counter(
            tagged(metric::kInsertsTotal, op_id, "outcome", metric::kOutcomeAbandoned));
        rows = &registry->counter(tagged(metric::kRowsTotal, op_id));
        insert_latency = &registry->histogram(tagged(metric::kInsertLatencyNs, op_id),
                                              bounds_of(metric::kNsBounds));
        block_rows = &registry->histogram(tagged(metric::kBlockRows, op_id),
                                          bounds_of(metric::kBlockRowsBounds));
        for (std::size_t c = 0; c < kFailureClasses; ++c) {
            const auto cls = static_cast<FailureClass>(c);
            // A permanent failure is never retried, so it has no series here.
            if (cls != FailureClass::Permanent) {
                retries.at(c) = &registry->counter(
                    tagged(metric::kRetriesTotal, op_id, "class", to_string(cls)));
            }
        }
        in_doubt = &registry->counter(tagged(metric::kInDoubtTotal, op_id));
        retry_wait =
            &registry->histogram(tagged(metric::kRetryWaitNs, op_id), bounds_of(metric::kNsBounds));
        queue_bytes = &registry->gauge(tagged(metric::kQueueBytes, op_id));
        backpressure = &registry->histogram(tagged(metric::kBackpressureBlockedNs, op_id),
                                            bounds_of(metric::kNsBounds));
        barrier_flush = &registry->histogram(tagged(metric::kBarrierFlushNs, op_id),
                                             bounds_of(metric::kNsBounds));
        parts_backoff = &registry->counter(tagged(metric::kPartsBackoffTotal, op_id));
        reconnects = &registry->counter(tagged(metric::kReconnectsTotal, op_id));
        maybe_duplicated = &registry->counter(tagged(metric::kRowsMaybeDuplicatedTotal, op_id));
    }

    void refusal(const std::string& code) const {
        if (registry != nullptr) {
            registry->counter(tagged(metric::kRefusalsTotal, op_id, "reason", code)).increment();
        }
    }

    MetricsRegistry* registry;
    std::uint64_t op_id;
    Counter* inserts_ok{nullptr};
    Counter* inserts_retried_ok{nullptr};
    Counter* inserts_failed{nullptr};
    Counter* inserts_abandoned{nullptr};
    Counter* rows{nullptr};
    Histogram* insert_latency{nullptr};
    Histogram* block_rows{nullptr};
    std::array<Counter*, kFailureClasses> retries{};
    Counter* in_doubt{nullptr};
    Histogram* retry_wait{nullptr};
    Gauge* queue_bytes{nullptr};
    Histogram* backpressure{nullptr};
    Histogram* barrier_flush{nullptr};
    Counter* parts_backoff{nullptr};
    Counter* reconnects{nullptr};
    Counter* maybe_duplicated{nullptr};
};

// Arms the transport's attempt deadline for one call that reads a server
// reply, and clears it however the call ends. It is never left armed while an
// INSERT accumulates blocks, which may legitimately take the whole interval.
class DeadlineGuard {
public:
    DeadlineGuard(InsertTransport& transport, Clock::time_point deadline) : transport_(transport) {
        transport_.set_deadline(deadline);
    }
    ~DeadlineGuard() { transport_.set_deadline(std::nullopt); }
    DeadlineGuard(const DeadlineGuard&) = delete;
    DeadlineGuard& operator=(const DeadlineGuard&) = delete;
    DeadlineGuard(DeadlineGuard&&) = delete;
    DeadlineGuard& operator=(DeadlineGuard&&) = delete;

private:
    InsertTransport& transport_;
};

// A Native block of an INSERT, held until the server acknowledges it, so a
// resend sends exactly the bytes the first attempt sent.
struct HeldBlock {
    ::clickhouse::Block block;
    std::size_t rows{0};
    std::size_t payload{0};
    std::size_t owned{0};
};

// One INSERT: its token, its blocks, the chunks their zero-copy strings point
// into, its memory charge and its retry state. Owned by the writer thread.
struct Insert {
    Insert(Token t, std::shared_ptr<MemoryBudget> budget, RetryWindow w)
        : token(std::move(t)), charge(std::move(budget), MemoryCategory::Queue), window(w) {}

    // A whole block, for a split half: its rows join the INSERT with it.
    void adopt(HeldBlock held) {
        rows += held.rows;
        payload += held.payload;
        owned += held.owned;
        blocks.push_back(std::move(held));
    }

    Token token;
    std::vector<HeldBlock> blocks;
    std::vector<std::shared_ptr<const Chunk>> retained;
    const Chunk* last_retained{nullptr};
    std::size_t retained_bytes{0};
    std::uint64_t rows{0};     // every row of the INSERT, the block builder's included
    std::size_t payload{0};    // of the cut blocks
    std::size_t owned{0};      // of the cut blocks
    MemoryReservation charge;  // the cut blocks' owned bytes, and the builder's
    std::optional<Clock::time_point> first_row;
    std::optional<Clock::time_point> first_begin;
    bool begun{false};  // begin_insert succeeded on the current client
    RetryWindow window;
    std::uint32_t failures{0};
    std::uint32_t unclassified{0};
    std::uint32_t validation{0};
    // The server that took the last attempt to get past a send under
    // `token`: the rows may have landed there. Empty while nothing can have.
    std::optional<ServerIdentity> doubt;
    bool counted_resent{false};
    bool counted_duplicate{false};
    bool retried{false};
    // The current attempt: whether a send_block call has started, and how
    // many blocks it has sent.
    bool attempt_sent{false};
    std::size_t attempt_blocks{0};
};

struct Item {
    enum class Kind : std::uint8_t { Chunk, Flush, Finish };
    Kind kind{Kind::Chunk};
    Chunk chunk;
    std::uint64_t ticket{0};
};

// The rows of the chunks in `items`, which a stopping writer drops unsent.
std::uint64_t chunk_rows(const std::deque<Item>& items) {
    std::uint64_t rows = 0;
    for (const Item& item : items) {
        if (item.kind == Item::Kind::Chunk && item.chunk.batch) {
            rows += static_cast<std::uint64_t>(item.chunk.batch->num_rows());
        }
    }
    return rows;
}

}  // namespace

struct Writer::Core {
    Core(WriterConfig config, std::unique_ptr<InsertTransport> transport, TokenSource tokens);
    Core(const Core&) = delete;
    Core& operator=(const Core&) = delete;
    Core(Core&&) = delete;
    Core& operator=(Core&&) = delete;
    ~Core() = default;

    // What stops a call on the task thread, checked in this order.
    enum class Gate : std::uint8_t { Open, Failed, Cancelled, Stopped, Finished };
    enum class Life : std::uint8_t { Running, Finished, Aborted };

    // --- task thread -------------------------------------------------------

    // Under mu_. `waiting` is for a wait on work already queued: a finished
    // queue does not stop it, a writer that has exited does.
    [[nodiscard]] Gate gate(bool waiting) const;
    [[noreturn]] void raise(Gate why, const std::exception_ptr& failure);
    void abort_and_join() noexcept;

    // --- writer thread -----------------------------------------------------

    void run() noexcept;
    void loop();
    std::optional<Item> next_item();
    void consume(Chunk chunk);
    void open_insert();
    void finish_current();
    void track_in_flight() noexcept;
    void append(Insert& in,
                const std::shared_ptr<const Chunk>& chunk,
                std::int64_t offset,
                std::int64_t rows);
    [[nodiscard]] std::size_t slice_payload(const arrow::RecordBatch& batch,
                                            std::int64_t offset,
                                            std::int64_t rows) const;
    [[nodiscard]] std::int64_t rows_that_fit(const arrow::RecordBatch& batch,
                                             std::int64_t offset,
                                             std::int64_t max_rows,
                                             std::size_t room) const;
    [[nodiscard]] std::int64_t rows_the_charge_takes(const Insert& in,
                                                     const Chunk& chunk,
                                                     std::int64_t max_rows) const;
    [[nodiscard]] std::size_t charged(const Insert& in) const;
    [[nodiscard]] bool should_close(const Insert& in, bool by_charge) const;
    void cut_and_send();
    void close_current();
    template <class Step>
    bool first_attempt(Step&& step);
    void freeze(Insert& in);
    void deliver(Insert& in, Failure f);
    void deliver_halves(Insert& parent, bool may_have_landed);
    std::array<std::unique_ptr<Insert>, 2> split(Insert& parent);
    void attempt(Insert& in, Phase& phase);
    void ensure_client(Phase& phase);
    void choose_token(Insert& in);
    void begin(Insert& in, Phase& phase, Clock::time_point deadline);
    void send(Insert& in, const HeldBlock& block, Phase& phase);
    void end(Phase& phase, Clock::time_point deadline);
    HeldBlock& cut(Insert& in);
    void acknowledged(Insert& in);
    void count_resent(Insert& in);
    void count_duplicate(Insert& in);
    void after_attempt(bool failed);
    [[noreturn]] void fail(const char* code, const std::string& message);
    [[noreturn]] void fail_insert(Insert& in, const Failure& f, const char* code);
    [[noreturn]] void fail_exhausted(Insert& in, const Failure& f, FailureClass cls);
    [[noreturn]] void conversion_failed(Insert& in,
                                        const arrow::RecordBatch& batch,
                                        const ConversionError& e);
    void fail_quietly(const char* code, const std::string& what) noexcept;
    void give_up(std::uint64_t rows, Counter* outcome) noexcept;
    void on_stopped() noexcept;
    void store(std::exception_ptr failure);
    void complete_ticket(std::uint64_t ticket);
    [[nodiscard]] std::uint64_t unacknowledged_rows() const;
    [[nodiscard]] bool stopping() const noexcept;
    void check_stop() const;
    [[nodiscard]] std::string statement(const Insert& in) const;
    [[nodiscard]] std::string insert_label(const Insert& in) const;
    [[nodiscard]] std::string subtask_text() const;

    // --- reports -----------------------------------------------------------

    template <class F>
    void report(F&& f) noexcept;
    void log(LogSeverity level, const std::string& message) noexcept;
    void report_queue_bytes() noexcept;
    [[nodiscard]] WriterStats snapshot_stats() const;

    // Fixed at construction.
    const WriterConfig config_;
    const std::unique_ptr<InsertTransport> transport_;
    const Clock::time_point started_;
    const std::string qualified_;
    const std::uint64_t batch_rows_;
    const std::size_t batch_bytes_;
    // An INSERT also closes at twice batch_bytes of charged memory: the
    // 16-byte string views are invisible to the payload measure, and short,
    // high-cardinality strings can make them rival it. Slices are sized
    // against it too, so one large chunk cannot carry the charge past it.
    const std::size_t charge_limit_;
    const std::chrono::milliseconds batch_interval_;
    const std::chrono::milliseconds retry_window_;
    // A resend under the same token is only safe if the server squashes it
    // into the same blocks, so every INSERT sends the opener's thresholds,
    // whatever server a reconnect reaches.
    const std::uint64_t pinned_rows_;
    const std::uint64_t pinned_bytes_;
    const SinkMetrics metrics_;

    // Writer thread only.
    TokenSource tokens_;
    TargetInfo live_;  // as the current client's server describes itself
    std::size_t endpoint_{0};
    Backoff backoff_;
    BlockBuilder builder_;
    std::unique_ptr<Insert> current_;
    std::uint64_t current_acked_{0};  // rows of current_ acknowledged through split halves
    // Rows of the chunk consume() is taking in that no INSERT holds yet. They
    // have left the queue, so without this a stop mid-chunk would lose them
    // from the abandoned count.
    std::uint64_t consuming_{0};
    std::uint64_t written_seen_{0};
    PartRateMonitor part_rate_;

    // The queue, and what the task thread waits for. Under mu_.
    mutable std::mutex mu_;
    std::condition_variable work_cv_;  // the writer waits on it
    std::condition_variable done_cv_;  // the task thread waits on it
    std::deque<Item> items_;
    std::size_t queued_bytes_{0};
    std::size_t queued_chunks_{0};
    bool closed_{false};
    bool exited_{false};
    std::uint64_t tickets_issued_{0};
    std::uint64_t tickets_done_{0};
    std::exception_ptr failure_;
    std::atomic<std::size_t> queue_bytes_now_{0};
    std::atomic<bool> stop_{false};
    // The unacknowledged rows of the INSERT in flight and of the chunk being
    // taken in, kept by the writer thread so that abort() can count them when
    // it detaches a writer that will never get as far as counting them itself.
    std::atomic<std::uint64_t> in_flight_rows_{0};

    // Joining or detaching the thread, once.
    std::mutex life_mu_;
    Life life_{Life::Running};
    std::thread thread_;

    // The registry and the logger belong to the host, and a detached writer
    // can outlive the job, so every metric and log line the writer reports
    // goes through report(), which stops for good once abort() detaches.
    std::mutex report_mu_;
    bool detached_{false};

    mutable std::mutex stats_mu_;
    WriterStats stats_;
};

template <class F>
void Writer::Core::report(F&& f) noexcept {
    try {
        const std::lock_guard<std::mutex> lock(report_mu_);
        if (detached_) {
            return;
        }
        std::forward<F>(f)();
    } catch (...) {
        // A report that fails must not take the writer down with it.
    }
}

void Writer::Core::log(LogSeverity level, const std::string& message) noexcept {
    report([&] { clink::logging::op_log(config_.logger, level, kLogSource, message); });
}

Writer::Core::Core(WriterConfig config,
                   std::unique_ptr<InsertTransport> transport,
                   TokenSource tokens)
    : config_(std::move(config)),
      transport_(std::move(transport)),
      started_(Clock::now()),
      qualified_(qualified_table(config_.options.database, config_.options.table)),
      batch_rows_(std::max<std::uint64_t>(config_.options.batch_rows, 1)),
      batch_bytes_(
          static_cast<std::size_t>(std::max<std::uint64_t>(config_.options.batch_bytes, 1))),
      charge_limit_(batch_bytes_ > std::numeric_limits<std::size_t>::max() / 2
                        ? std::numeric_limits<std::size_t>::max()
                        : 2 * batch_bytes_),
      batch_interval_(config_.options.batch_interval),
      retry_window_(config_.options.retry_window),
      pinned_rows_(config_.target.caps.min_insert_block_size_rows),
      pinned_bytes_(config_.target.caps.min_insert_block_size_bytes),
      metrics_(config_.metrics, config_.op_id),
      tokens_(std::move(tokens)),
      live_(config_.target),
      builder_(config_.plan),
      part_rate_(std::max<std::uint32_t>(config_.options.parallelism, 1),
                 config_.options.batch_interval,
                 started_,
                 config_.part_rate_span,
                 config_.part_rate_quiet) {
    if (!transport_) {
        throw std::invalid_argument("clickhouse native sink: the writer needs a transport");
    }
    const auto& endpoints = config_.options.endpoints;
    if (endpoints.empty()) {
        throw std::invalid_argument("clickhouse native sink: the writer needs an endpoint");
    }
    // A rebuild starts from the endpoint that last worked, which at first is
    // the one the opener reached.
    const auto it = std::find(endpoints.begin(), endpoints.end(), config_.target.server.endpoint);
    endpoint_ = it == endpoints.end() ? 0 : static_cast<std::size_t>(it - endpoints.begin());
    written_seen_ = transport_->counters().bytes_written;
}

// ---------------------------------------------------------------------------
// The task thread's side

Writer::Core::Gate Writer::Core::gate(bool waiting) const {
    if (failure_) {
        return Gate::Failed;
    }
    if (config_.cancel.requested()) {
        return Gate::Cancelled;
    }
    if (stop_.load(std::memory_order_acquire)) {
        return Gate::Stopped;
    }
    if (waiting) {
        return exited_ ? Gate::Stopped : Gate::Open;
    }
    return closed_ ? Gate::Finished : Gate::Open;
}

void Writer::Core::raise(Gate why, const std::exception_ptr& failure) {
    switch (why) {
        case Gate::Failed:
            if (config_.cancel.requested()) {
                // The writer may have seen the cancel first and stored it as
                // its failure. The task is cancelled either way, so the writer
                // is joined and its summary logged before the throw, as on
                // every cancel.
                abort_and_join();
            }
            std::rethrow_exception(failure);
        case Gate::Cancelled:
            // A normal return here would let the runner acknowledge a
            // checkpoint over rows that never reached the table.
            abort_and_join();
            throw NativeSinkError(code::kCancelled,
                                  "clickhouse native sink: the task was cancelled");
        case Gate::Stopped:
            throw NativeSinkError(code::kCancelled,
                                  "clickhouse native sink: the writer has been stopped");
        case Gate::Finished:
            throw std::logic_error("clickhouse native sink: the writer has already finished");
        case Gate::Open:
            break;
    }
    throw std::logic_error("clickhouse native sink: no reason to stop the call");
}

void Writer::Core::abort_and_join() noexcept {
    const std::lock_guard<std::mutex> life(life_mu_);
    if (life_ != Life::Running) {
        return;
    }
    life_ = Life::Aborted;
    stop_.store(true, std::memory_order_release);
    // Shuts the socket down, so a writer blocked in a recv or a send returns
    // at once. Sticky, so a writer about to connect cannot escape it either.
    transport_->interrupt();
    std::deque<Item> dropped;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        closed_ = true;
        dropped.swap(items_);
        queued_bytes_ = 0;
        queued_chunks_ = 0;
        queue_bytes_now_.store(0, std::memory_order_relaxed);
    }
    work_cv_.notify_all();
    done_cv_.notify_all();
    // Rows the writer never took from the queue are as unacknowledged as
    // those it holds, and the restart replays them too: the summary below
    // counts every submitted row the server did not acknowledge, however far
    // the writer had got.
    if (const std::uint64_t rows = chunk_rows(dropped); rows > 0) {
        const std::lock_guard<std::mutex> stats(stats_mu_);
        stats_.abandoned_rows += rows;
    }
    dropped.clear();
    report_queue_bytes();

    bool exited = false;
    {
        std::unique_lock<std::mutex> lock(mu_);
        exited = done_cv_.wait_for(lock, kJoinWait, [this] { return exited_; });
    }
    try {
        if (thread_.joinable()) {
            if (exited && thread_.get_id() != std::this_thread::get_id()) {
                thread_.join();
            } else {
                // Still inside a call with no fd to shut down. The thread
                // owns everything it touches through its share of this Core,
                // and its poisoned socket cannot commit when it goes; from
                // here on it reports nothing. So the INSERT it holds, and
                // the rest of the chunk it was taking in, which the job's
                // restart replays, are counted as abandoned here, for the
                // summary below, and give_up() on the thread then finds them
                // counted.
                {
                    const std::lock_guard<std::mutex> lock(report_mu_);
                    const std::uint64_t rows =
                        in_flight_rows_.exchange(0, std::memory_order_acq_rel);
                    if (rows > 0) {
                        {
                            const std::lock_guard<std::mutex> stats(stats_mu_);
                            stats_.abandoned_rows += rows;
                        }
                        if (metrics_.inserts_abandoned != nullptr) {
                            metrics_.inserts_abandoned->increment();
                        }
                    }
                    detached_ = true;
                }
                thread_.detach();
            }
        }
    } catch (...) {
        // join and detach throw only for a thread that cannot be joined,
        // which joinable() has ruled out.
    }
    try {
        clink::logging::op_log(
            config_.logger,
            LogSeverity::Warn,
            kLogSource,
            summary_line(
                "cancelled", config_.options, snapshot_stats(), to_ms(Clock::now() - started_)));
    } catch (...) {
        // A summary that cannot be written is not a reason to fail a cancel.
    }
}

// ---------------------------------------------------------------------------
// The writer thread

void Writer::Core::run() noexcept {
    bool clean = false;
    try {
        loop();
        clean = true;
    } catch (const Stopped&) {
        on_stopped();
    } catch (const Failed&) {
        // Stored and reported where it happened.
    } catch (const std::exception& e) {
        fail_quietly(code::kInsertFailed, e.what());
    } catch (...) {
        fail_quietly(code::kInsertFailed, "unknown exception");
    }
    // abandon() is the only way the sink disposes of a client, so an INSERT
    // left open here is never ended by a destructor.
    transport_->abandon();
    try {
        // The builder may still hold views into a chunk the INSERT retains,
        // so it lets go before the INSERT does.
        builder_.reset();
    } catch (...) {
        // Only an allocation for the fresh columns can fail, and nothing
        // uses the builder after this.
    }
    current_.reset();
    std::deque<Item> dropped;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (!clean && !failure_) {
            failure_ = std::make_exception_ptr(NativeSinkError(
                code::kInsertFailed, "clickhouse native sink: the writer stopped unexpectedly"));
        }
        exited_ = true;
        closed_ = true;
        dropped.swap(items_);
        // Counted before abort() can see exited_ and log its summary.
        if (const std::uint64_t rows = chunk_rows(dropped); rows > 0) {
            const std::lock_guard<std::mutex> stats(stats_mu_);
            stats_.abandoned_rows += rows;
        }
        queued_bytes_ = 0;
        queued_chunks_ = 0;
        queue_bytes_now_.store(0, std::memory_order_relaxed);
    }
    done_cv_.notify_all();
    dropped.clear();
    report_queue_bytes();
}

void Writer::Core::loop() {
    for (;;) {
        std::optional<Item> item = next_item();
        if (!item) {
            // The batch interval passed on a stream that went quiet.
            close_current();
            continue;
        }
        switch (item->kind) {
            case Item::Kind::Chunk:
                consume(std::move(item->chunk));
                break;
            case Item::Kind::Flush:
                close_current();
                complete_ticket(item->ticket);
                break;
            case Item::Kind::Finish:
                close_current();
                return;
        }
    }
}

std::optional<Item> Writer::Core::next_item() {
    std::unique_lock<std::mutex> lock(mu_);
    for (;;) {
        if (stopping()) {
            throw Stopped{};
        }
        if (!items_.empty()) {
            Item item = std::move(items_.front());
            items_.pop_front();
            const bool chunk = item.kind == Item::Kind::Chunk;
            if (chunk) {
                queued_bytes_ -= std::min(queued_bytes_, item.chunk.bytes);
                queued_chunks_ -= std::min<std::size_t>(queued_chunks_, 1);
                queue_bytes_now_.store(queued_bytes_, std::memory_order_relaxed);
            }
            lock.unlock();
            if (chunk) {
                done_cv_.notify_all();
                report_queue_bytes();
            }
            return item;
        }
        // The interval is timed here too, so it fires on an idle stream.
        Clock::duration wait = kSlice;
        if (current_ && current_->first_row) {
            const Clock::time_point due = later(*current_->first_row, batch_interval_);
            const Clock::time_point now = Clock::now();
            if (now >= due) {
                return std::nullopt;
            }
            wait = std::min<Clock::duration>(wait, due - now);
        }
        work_cv_.wait_for(lock, wait);
    }
}

void Writer::Core::consume(Chunk chunk) {
    const auto shared = std::make_shared<const Chunk>(std::move(chunk));
    const arrow::RecordBatch& batch = *shared->batch;
    const std::int64_t total = batch.num_rows();
    std::int64_t offset = 0;
    consuming_ = static_cast<std::uint64_t>(total);
    track_in_flight();
    while (offset < total) {
        check_stop();
        if (!current_) {
            open_insert();
        }
        Insert& in = *current_;
        const std::size_t block_payload = builder_.payload_bytes();
        const std::size_t insert_payload = in.payload + block_payload;
        const std::size_t block_room = kMaxBlockBytes - std::min(block_payload, kMaxBlockBytes);
        const std::size_t insert_room = batch_bytes_ - std::min(insert_payload, batch_bytes_);
        const std::uint64_t row_room = batch_rows_ - std::min(in.rows, batch_rows_);
        auto max_rows = static_cast<std::int64_t>(
            std::min(static_cast<std::uint64_t>(total - offset), row_room));
        if (max_rows == 0) {
            close_current();
            continue;
        }
        // Nothing in a new INSERT says yet what a row costs in Native
        // copies, so its first row goes in on its own and later slices are
        // sized from what the rows so far cost. The charge trigger waits for
        // the slice after that first row, so a chunk that alone takes the
        // INSERT to the limit still gives it more than one row.
        const bool first = in.rows == 0;
        if (first) {
            max_rows = 1;
        } else {
            max_rows = rows_the_charge_takes(in, *shared, max_rows);
            if (max_rows == 0) {
                // Its Native copies have taken what the charge allows.
                close_current();
                continue;
            }
        }
        std::int64_t rows =
            rows_that_fit(batch, offset, max_rows, std::min(block_room, insert_room));
        if (rows == 0) {
            // The next row does not fit in what is left: close the INSERT or
            // cut the block it would overrun, and take the row again.
            const std::size_t next = slice_payload(batch, offset, 1);
            if (in.rows > 0 && next > insert_room) {
                close_current();
                continue;
            }
            if (builder_.rows() > 0 && next > block_room) {
                cut_and_send();
                continue;
            }
            // Larger than a whole block on its own, so it goes alone.
            rows = 1;
        }
        append(in, shared, offset, rows);
        offset += rows;
        if (builder_.payload_bytes() >= kMaxBlockBytes) {
            cut_and_send();
        }
        if (current_ && should_close(*current_, !first)) {
            close_current();
        }
    }
    // Without a zero-copy column nothing points into the chunk once it is
    // converted, so `shared` releases it, and its memory charge, here.
}

void Writer::Core::open_insert() {
    current_ = std::make_unique<Insert>(tokens_.next(), config_.budget, RetryWindow(retry_window_));
    current_acked_ = 0;
    track_in_flight();
    backoff_.reset();
}

void Writer::Core::finish_current() {
    current_.reset();
    current_acked_ = 0;
    track_in_flight();
}

void Writer::Core::track_in_flight() noexcept {
    in_flight_rows_.store(unacknowledged_rows() + consuming_, std::memory_order_release);
}

void Writer::Core::append(Insert& in,
                          const std::shared_ptr<const Chunk>& chunk,
                          std::int64_t offset,
                          std::int64_t rows) {
    if (config_.plan.retains_chunks && in.last_retained != chunk.get()) {
        // The String columns hold views into the chunk's buffers, so the
        // chunk lives as long as any block of the INSERT that points into it.
        in.retained.push_back(chunk);
        in.retained_bytes += chunk->bytes;
        in.last_retained = chunk.get();
    }
    try {
        builder_.append(*chunk->batch, offset, rows);
    } catch (const ConversionError& e) {
        conversion_failed(in, *chunk->batch, e);
    }
    if (!in.first_row) {
        in.first_row = Clock::now();
    }
    in.rows += static_cast<std::uint64_t>(rows);
    // The rows move from the chunk to the INSERT in one step, so a detaching
    // abort() never counts them twice.
    consuming_ -= std::min(consuming_, static_cast<std::uint64_t>(rows));
    track_in_flight();
    in.charge.resize(in.owned + builder_.owned_bytes());
}

std::size_t Writer::Core::slice_payload(const arrow::RecordBatch& batch,
                                        std::int64_t offset,
                                        std::int64_t rows) const {
    if (rows <= 0) {
        return 0;
    }
    // The estimate reads the Arrow buffers of exactly these rows, so it is the
    // payload the block builder will count for them, or a little more.
    const std::shared_ptr<arrow::RecordBatch> slice = batch.Slice(offset, rows);
    const double per_row = builder_.bytes_per_row(*slice);
    return static_cast<std::size_t>(std::llround(per_row * static_cast<double>(rows)));
}

std::int64_t Writer::Core::rows_that_fit(const arrow::RecordBatch& batch,
                                         std::int64_t offset,
                                         std::int64_t max_rows,
                                         std::size_t room) const {
    if (slice_payload(batch, offset, max_rows) <= room) {
        return max_rows;
    }
    // The most rows whose payload fits: the payload only grows with the row
    // count, so a binary search finds the boundary.
    std::int64_t fits = 0;
    std::int64_t overruns = max_rows;
    while (overruns - fits > 1) {
        const std::int64_t mid = fits + (overruns - fits) / 2;
        if (slice_payload(batch, offset, mid) <= room) {
            fits = mid;
        } else {
            overruns = mid;
        }
    }
    return fits;
}

// How many of `max_rows` more rows of `chunk` the INSERT can take before its
// Native copies, with the other chunks it holds, would pass the charge limit.
// A row is taken to cost what the INSERT's rows have cost so far: exact for
// flat columns, an estimate for arrays whose rows vary in length. The chunk
// itself is not counted. It is held whole whatever the INSERT takes of it, so
// an INSERT may pass the limit by that one chunk, but not by the copies of
// its rows.
std::int64_t Writer::Core::rows_the_charge_takes(const Insert& in,
                                                 const Chunk& chunk,
                                                 std::int64_t max_rows) const {
    const std::size_t block_owned = builder_.owned_bytes();
    const std::size_t owned = in.owned + block_owned;
    const std::size_t others = in.last_retained == &chunk
                                   ? in.retained_bytes - std::min(in.retained_bytes, chunk.bytes)
                                   : in.retained_bytes;
    const std::size_t held = others + owned;
    if (held >= charge_limit_) {
        return 0;
    }
    const std::size_t room = charge_limit_ - held;
    // The builder grows a column by half again when it runs out of room, so
    // the next slice can add half of what the block owns however few rows it
    // has.
    if (block_owned / 2 >= room) {
        return 0;
    }
    if (owned == 0 || in.rows == 0) {
        return max_rows;
    }
    // What the rows so far own includes the builder's growth slack, so it
    // errs high, which is the safe side.
    const double per_row = static_cast<double>(owned) / static_cast<double>(in.rows);
    const double fits = std::floor(static_cast<double>(room) / per_row);
    return fits >= static_cast<double>(max_rows) ? max_rows : static_cast<std::int64_t>(fits);
}

std::size_t Writer::Core::charged(const Insert& in) const {
    return in.retained_bytes + in.owned + builder_.owned_bytes();
}

bool Writer::Core::should_close(const Insert& in, bool by_charge) const {
    if (in.rows >= batch_rows_) {
        return true;
    }
    if (in.payload + builder_.payload_bytes() >= batch_bytes_) {
        return true;
    }
    if (by_charge && charged(in) >= charge_limit_) {
        return true;
    }
    return in.first_row && Clock::now() >= later(*in.first_row, batch_interval_);
}

template <class Step>
bool Writer::Core::first_attempt(Step&& step) {
    Insert& in = *current_;
    Phase phase = Phase::Begin;
    std::optional<Failure> failure;
    try {
        step(in, phase);
    } catch (const Stopped&) {
        throw;
    } catch (const Failed&) {
        throw;
    } catch (...) {
        failure = to_failure(std::current_exception(), phase, in.attempt_sent);
    }
    if (!failure) {
        return true;
    }
    if (stopping()) {
        throw Stopped{};
    }
    // The first failure closes the INSERT and freezes its content, so every
    // resend is the first attempt's blocks, cut where it cut them. Rows that
    // arrive meanwhile wait in the queue for the next INSERT.
    freeze(in);
    deliver(in, std::move(*failure));
    finish_current();
    return false;
}

void Writer::Core::cut_and_send() {
    // BeginInsert comes before the cut, so a failure there finds the rows
    // still in the builder, and freezing them keeps the cut the server would
    // have seen.
    (void)first_attempt([this](Insert& in, Phase& phase) {
        if (!in.begun) {
            ensure_client(phase);
            begin(in, phase, later(Clock::now(), retry_window_));
        }
        const HeldBlock& block = cut(in);
        send(in, block, phase);
    });
}

void Writer::Core::close_current() {
    if (!current_) {
        return;
    }
    if (current_->rows == 0) {
        // An empty interval sends nothing: never a 0-row block, never an
        // empty INSERT.
        finish_current();
        return;
    }
    const bool first_try = first_attempt([this](Insert& in, Phase& phase) {
        if (builder_.rows() > 0) {
            if (!in.begun) {
                ensure_client(phase);
                begin(in, phase, later(Clock::now(), retry_window_));
            }
            const HeldBlock& block = cut(in);
            send(in, block, phase);
        }
        if (!in.begun) {
            // Rows in sent blocks imply a BeginInsert on this client. Ending
            // without one would acknowledge rows that never left, so it fails
            // the INSERT instead.
            throw std::logic_error("clickhouse native sink: an INSERT closed without BeginInsert");
        }
        end(phase, later(Clock::now(), retry_window_));
    });
    if (first_try) {
        acknowledged(*current_);
        finish_current();
    }
}

void Writer::Core::freeze(Insert& in) {
    if (builder_.rows() > 0) {
        (void)cut(in);
    }
}

void Writer::Core::deliver(Insert& in, Failure f) {
    for (;;) {
        // Any exception from any client call abandons the client: poison
        // first, then destroy, so nothing it has sent can be committed.
        transport_->abandon();
        in.begun = false;
        after_attempt(true);

        const FailureClass cls = classify(f, live_.caps.quorum);
        const Decision decision =
            action_for(cls, f, AttemptState{in.rows, in.unclassified, in.validation});
        ++in.failures;
        if (cls == FailureClass::Unclassified) {
            ++in.unclassified;
        }
        if (f.signal == Signal::Validation) {
            ++in.validation;
        }
        const bool past_send = f.after_send || f.phase == Phase::Send || f.phase == Phase::End;
        if (past_send) {
            in.doubt = live_.server;
        }
        if (cls == FailureClass::InDoubt) {
            {
                const std::lock_guard<std::mutex> lock(stats_mu_);
                ++stats_.in_doubt;
            }
            report([this] {
                if (metrics_.in_doubt != nullptr) {
                    metrics_.in_doubt->increment();
                }
            });
        }
        if (f.phase == Phase::Connect) {
            // Stay on an endpoint that works; move on from one that refuses.
            endpoint_ = (endpoint_ + 1) % config_.options.endpoints.size();
        }
        if (decision.action == Action::Fail) {
            fail_insert(
                in, f, decision.fail_code != nullptr ? decision.fail_code : code::kInsertFailed);
        }

        const Clock::time_point now = Clock::now();
        in.window.start(now);
        const std::chrono::milliseconds wait = backoff_.next();
        if (!in.window.allows(now, wait)) {
            fail_exhausted(in, f, cls);
        }
        in.retried = true;
        {
            const std::lock_guard<std::mutex> lock(stats_mu_);
            ++stats_.retries.at(static_cast<std::size_t>(cls));
        }
        report([&] {
            if (Counter* retries = metrics_.retries.at(static_cast<std::size_t>(cls))) {
                retries->increment();
            }
            if (cls == FailureClass::MergeBackpressure && metrics_.parts_backoff != nullptr) {
                metrics_.parts_backoff->increment();
            }
        });
        const bool splitting = decision.action == Action::SplitHalves;
        log(LogSeverity::Warn,
            insert_label(in) + ": attempt " + std::to_string(in.failures) + " failed at " +
                phase_name(f.phase) + " (" + to_string(cls) + "): " + failure_text(f) + "; " +
                (splitting ? "splitting it in two" : "retrying") + " in " +
                std::to_string(wait.count()) + " ms");

        CLINK_FAULT_POINT(points::kBeforeRetryWait);
        const Clock::time_point wait_start = Clock::now();
        if (!cancellable_wait(wait, config_.cancel, stop_)) {
            throw Stopped{};
        }
        const std::uint64_t waited = to_ns(Clock::now() - wait_start);
        report([&] {
            if (metrics_.retry_wait != nullptr) {
                metrics_.retry_wait->observe(static_cast<double>(waited));
            }
        });

        if (splitting) {
            // Whether rows sent under the parent's token may have landed:
            // set by this failure or by any earlier attempt that got past a
            // send, and cleared only by a fresh token, which counted them.
            deliver_halves(in, in.doubt.has_value());
            return;
        }
        Phase phase = Phase::Connect;
        std::optional<Failure> next;
        try {
            attempt(in, phase);
        } catch (const Stopped&) {
            throw;
        } catch (const Failed&) {
            throw;
        } catch (...) {
            next = to_failure(std::current_exception(), phase, in.attempt_sent);
        }
        if (!next) {
            acknowledged(in);
            return;
        }
        if (stopping()) {
            throw Stopped{};
        }
        f = std::move(*next);
    }
}

void Writer::Core::deliver_halves(Insert& parent, bool may_have_landed) {
    if (may_have_landed) {
        // The parent may have landed in part under its own token, on this
        // attempt or an earlier one, and each half goes under a fresh one.
        count_duplicate(parent);
    }
    std::array<std::unique_ptr<Insert>, 2> halves = split(parent);
    log(LogSeverity::Info,
        insert_label(parent) + ": the server ran out of memory; sending it as two INSERTs, seq=" +
            std::to_string(halves[0]->token.seq) + " of " + std::to_string(halves[0]->rows) +
            " rows and seq=" + std::to_string(halves[1]->token.seq) + " of " +
            std::to_string(halves[1]->rows) + " rows");
    for (auto& half : halves) {
        Phase phase = Phase::Connect;
        std::optional<Failure> failure;
        try {
            attempt(*half, phase);
        } catch (const Stopped&) {
            throw;
        } catch (const Failed&) {
            throw;
        } catch (...) {
            failure = to_failure(std::current_exception(), phase, half->attempt_sent);
        }
        if (!failure) {
            acknowledged(*half);
        } else {
            if (stopping()) {
                throw Stopped{};
            }
            deliver(*half, std::move(*failure));
        }
        half.reset();
    }
}

std::array<std::unique_ptr<Insert>, 2> Writer::Core::split(Insert& parent) {
    const std::uint64_t first_rows = parent.rows / 2;
    std::array<std::unique_ptr<Insert>, 2> halves;
    for (auto& half : halves) {
        // A copy of the parent's window, deadline included, so one window
        // covers the whole split tree however often memory runs out.
        half = std::make_unique<Insert>(tokens_.next(), config_.budget, parent.window);
        half->retained = parent.retained;
        half->retained_bytes = parent.retained_bytes;
        half->first_begin = parent.first_begin;
        half->counted_resent = parent.counted_resent;
        half->counted_duplicate = parent.counted_duplicate;
        half->retried = true;
        half->failures = parent.failures;
    }
    std::uint64_t seen = 0;
    for (HeldBlock& held : parent.blocks) {
        const std::uint64_t end = seen + held.rows;
        if (end <= first_rows) {
            halves[0]->adopt(std::move(held));
        } else if (seen >= first_rows) {
            halves[1]->adopt(std::move(held));
        } else {
            // Column::Slice copies the strings of each part into storage the
            // part owns, and those copies are charged as they are made.
            const auto at = static_cast<std::size_t>(first_rows - seen);
            for (int part = 0; part < 2; ++part) {
                const std::size_t begin = part == 0 ? 0 : at;
                const std::size_t length = part == 0 ? at : held.rows - at;
                BlockSlice slice = slice_block(held.block, begin, length);
                HeldBlock piece;
                piece.block = std::move(slice.block);
                piece.rows = slice.rows;
                piece.payload = slice.payload_bytes;
                piece.owned = slice.owned_bytes;
                halves[static_cast<std::size_t>(part)]->adopt(std::move(piece));
            }
        }
        seen = end;
    }
    parent.blocks.clear();
    parent.retained.clear();
    parent.payload = 0;
    parent.owned = 0;
    parent.charge.resize(0);
    for (auto& half : halves) {
        half->charge.resize(half->owned);
    }
    return halves;
}

void Writer::Core::attempt(Insert& in, Phase& phase) {
    in.attempt_sent = false;
    in.attempt_blocks = 0;
    phase = Phase::Connect;
    ensure_client(phase);
    choose_token(in);
    // A later attempt's replies are read under the window's own deadline, so
    // the INSERT ends at most one receive timeout after the window does.
    const Clock::time_point deadline = in.window.deadline();
    begin(in, phase, deadline);
    for (const HeldBlock& held : in.blocks) {
        send(in, held, phase);
    }
    end(phase, deadline);
}

void Writer::Core::ensure_client(Phase& phase) {
    if (transport_->connected()) {
        return;
    }
    check_stop();
    phase = Phase::Connect;
    const Endpoint& endpoint = config_.options.endpoints.at(endpoint_);
    try {
        transport_->connect(endpoint);
    } catch (const NativeSinkError& e) {
        report([&] { metrics_.refusal(e.code()); });
        throw;
    }
    check_stop();
    report([this] {
        if (metrics_.reconnects != nullptr) {
            metrics_.reconnects->increment();
        }
    });

    // Every check made at open describes the server the opener reached. This
    // client may be another replica, or the same host restarted into another
    // version, with its own settings and async_insert defaults.
    phase = Phase::Metadata;
    TargetInfo fresh = live_;
    if (config_.reprobe) {
        try {
            fresh = config_.reprobe(*transport_);
        } catch (const NativeSinkError& e) {
            report([&] { metrics_.refusal(e.code()); });
            throw;
        }
    }
    fresh.server = transport_->server();
    fresh.caps.min_insert_block_size_rows = pinned_rows_;
    fresh.caps.min_insert_block_size_bytes = pinned_bytes_;
    if (!same_server(fresh.server, live_.server)) {
        log(LogSeverity::Warn,
            "clickhouse native sink: subtask=" + subtask_text() + " now writes to " +
                identity_text(fresh.server) + " (was " + identity_text(live_.server) + ")" +
                (fresh.tested_line ? "" : "; that line is accepted but untested"));
    }
    live_ = std::move(fresh);
}

void Writer::Core::choose_token(Insert& in) {
    if (!in.doubt) {
        // Nothing sent under this token can have landed.
        return;
    }
    // The token is kept only where a kept-token resend was proved safe, and
    // only to the server that took the attempt: another replica or version
    // may hash or squash it differently. A fresh token can only duplicate,
    // never lose.
    if (live_.keep_token_on_resend && same_server(live_.server, *in.doubt)) {
        if (live_.keeps_dedup_log && live_.family != EngineFamily::Distributed) {
            count_resent(in);
        } else {
            count_duplicate(in);
        }
        return;
    }
    in.token = tokens_.next();
    in.doubt.reset();
    count_duplicate(in);
}

void Writer::Core::begin(Insert& in, Phase& phase, Clock::time_point deadline) {
    check_stop();
    phase = Phase::Begin;
    const std::string sql = statement(in);
    if (!in.first_begin) {
        in.first_begin = Clock::now();
    }
    std::vector<HeaderColumn> header;
    {
        const DeadlineGuard guard(*transport_, deadline);
        header = transport_->begin_insert(sql);
    }
    in.begun = true;
    // Columns are not re-read on a reconnect: this check covers them, on
    // every INSERT, against the plan made at open.
    const std::vector<std::string> drift = header_drift(config_.plan, header);
    if (!drift.empty()) {
        std::string message = qualified_ + " no longer matches the column plan made at open:";
        for (const auto& line : drift) {
            message += "\n  - " + line;
        }
        message += "\nRestart the job so that the sink plans against the table as it is now.";
        throw NativeSinkError(code::kHeaderDrift, message);
    }
}

void Writer::Core::send(Insert& in, const HeldBlock& block, Phase& phase) {
    check_stop();
    phase = Phase::Send;
    in.attempt_sent = true;
    transport_->send_block(block.block);
    if (in.attempt_blocks++ == 0) {
        CLINK_FAULT_POINT(points::kAfterFirstBlock);
    }
}

void Writer::Core::end(Phase& phase, Clock::time_point deadline) {
    check_stop();
    phase = Phase::End;
    const DeadlineGuard guard(*transport_, deadline);
    transport_->end_insert();
}

HeldBlock& Writer::Core::cut(Insert& in) {
    HeldBlock held;
    held.rows = builder_.rows();
    held.payload = builder_.payload_bytes();
    held.owned = builder_.owned_bytes();
    held.block = builder_.take();
    // The builder's memory moves into the block, so the charge stays as it is.
    in.payload += held.payload;
    in.owned += held.owned;
    const std::size_t rows = held.rows;
    in.blocks.push_back(std::move(held));
    report([&] {
        if (metrics_.block_rows != nullptr) {
            metrics_.block_rows->observe(static_cast<double>(rows));
        }
    });
    return in.blocks.back();
}

void Writer::Core::acknowledged(Insert& in) {
    CLINK_FAULT_POINT(points::kAfterEndInsert);
    in.begun = false;
    after_attempt(false);
    const Clock::time_point now = Clock::now();
    const std::uint64_t rows = in.rows;
    const std::uint64_t latency = in.first_begin ? to_ns(now - *in.first_begin) : 0;
    {
        const std::lock_guard<std::mutex> lock(stats_mu_);
        stats_.rows_acknowledged += rows;
        ++stats_.inserts;
    }
    current_acked_ += rows;
    track_in_flight();
    const bool retried = in.retried;
    report([&] {
        if (Counter* outcome = retried ? metrics_.inserts_retried_ok : metrics_.inserts_ok) {
            outcome->increment();
        }
        if (metrics_.rows != nullptr) {
            metrics_.rows->increment(rows);
        }
        if (metrics_.insert_latency != nullptr) {
            metrics_.insert_latency->observe(static_cast<double>(latency));
        }
        // Counted here and nowhere else: a row is out once the server has it.
        mm::connector::records_out_inc(metric::kConnector, rows);
    });
    // The rows are in the table, so their blocks and chunks can go.
    in.blocks.clear();
    in.retained.clear();
    in.charge.resize(0);
    if (std::optional<std::string> warning = part_rate_.on_insert(now, rows)) {
        log(LogSeverity::Warn,
            "clickhouse native sink: subtask=" + subtask_text() + " " + *warning);
    }
}

void Writer::Core::count_resent(Insert& in) {
    if (in.counted_resent || in.counted_duplicate) {
        return;
    }
    in.counted_resent = true;
    const std::lock_guard<std::mutex> lock(stats_mu_);
    stats_.rows_resent_with_token += in.rows;
}

void Writer::Core::count_duplicate(Insert& in) {
    if (in.counted_duplicate) {
        return;
    }
    in.counted_duplicate = true;
    const std::uint64_t rows = in.rows;
    {
        const std::lock_guard<std::mutex> lock(stats_mu_);
        stats_.rows_maybe_duplicated += rows;
        if (in.counted_resent) {
            // The two counts never hold the same rows.
            stats_.rows_resent_with_token -= std::min(stats_.rows_resent_with_token, rows);
            in.counted_resent = false;
        }
    }
    report([&] {
        if (metrics_.maybe_duplicated != nullptr) {
            metrics_.maybe_duplicated->increment(rows);
        }
    });
}

void Writer::Core::after_attempt(bool failed) {
    const std::uint64_t written = transport_->counters().bytes_written;
    const std::uint64_t delta = written > written_seen_ ? written - written_seen_ : 0;
    written_seen_ = written;
    {
        const std::lock_guard<std::mutex> lock(stats_mu_);
        stats_.wire_bytes += delta;
    }
    report([&] {
        if (delta > 0) {
            mm::connector::bytes_out_inc(metric::kConnector, delta);
        }
        if (failed) {
            mm::connector::error_inc(metric::kConnector);
        }
    });
}

// Counts no attempt: deliver() has counted the failed attempt before it gets
// here, and conversion_failed() and fail_quietly() count theirs first.
void Writer::Core::fail(const char* code, const std::string& message) {
    transport_->abandon();
    const NativeSinkError error(code, message);
    give_up(unacknowledged_rows() + consuming_,
            current_ != nullptr ? metrics_.inserts_failed : nullptr);
    log(LogSeverity::Error, error.what());
    // Stored last: the task thread may act on the failure the moment it sees
    // it, and everything reported about it is in place by then.
    store(std::make_exception_ptr(error));
    throw Failed{};
}

void Writer::Core::fail_insert(Insert& in, const Failure& f, const char* code) {
    std::string message = insert_label(in) + " failed";
    if (std::string_view(code) == code::kTooManyPartitions) {
        message += ": it touches more partitions than the server's max_partitions_per_insert_block";
        if (live_.caps.max_partitions_per_insert_block != 0) {
            message += "=" + std::to_string(live_.caps.max_partitions_per_insert_block);
        }
        message +=
            " allows. Check the table's PARTITION BY, or lower batch_rows (now " +
            std::to_string(batch_rows_) +
            ") so that each INSERT spans fewer partitions. The server said: " + failure_text(f);
    } else {
        message += " at " + std::string(phase_name(f.phase)) + ": " + failure_text(f);
    }
    fail(code, message);
}

void Writer::Core::fail_exhausted(Insert& in, const Failure& f, FailureClass cls) {
    fail(code::kRetryWindowExhausted,
         insert_label(in) + " was not acknowledged within retry_window_ms=" +
             std::to_string(retry_window_.count()) + " after " + std::to_string(in.failures) +
             " failed attempts; the last failed at " + phase_name(f.phase) + " (" + to_string(cls) +
             "): " + failure_text(f));
}

void Writer::Core::conversion_failed(Insert& in,
                                     const arrow::RecordBatch& batch,
                                     const ConversionError& e) {
    // The INSERT may have sent blocks before this row: the bytes they took
    // and the failure count as for any failed attempt.
    after_attempt(true);
    // Row data may be confidential: the sample row shows types, lengths and
    // the offending value only where it is a number or a date.
    fail(code::kConversionFailed,
         insert_label(in) + " failed: " + error_text(e) +
             "; row: " + redacted_row(config_.plan, batch, e.row(), e.column()));
}

void Writer::Core::fail_quietly(const char* code, const std::string& what) noexcept {
    try {
        // As for a conversion failure: a charge the budget refused, or
        // anything unexpected, may come after blocks went out.
        after_attempt(true);
        fail(code,
             (current_ ? insert_label(*current_) + " failed: "
                       : std::string("clickhouse native sink: the writer failed: ")) +
                 what);
    } catch (...) {
        // fail() always ends by throwing Failed.
    }
}

// Counts the INSERT in flight as given up: `rows` unacknowledged, and one
// `outcome`. abort() does the same for a writer it detaches, which reports
// nothing afterwards, so both count under report_mu_ and whichever comes
// second finds nothing left to count.
void Writer::Core::give_up(std::uint64_t rows, Counter* outcome) noexcept {
    try {
        const std::lock_guard<std::mutex> lock(report_mu_);
        in_flight_rows_.store(0, std::memory_order_release);
        if (detached_) {
            return;
        }
        {
            const std::lock_guard<std::mutex> stats(stats_mu_);
            stats_.abandoned_rows += rows;
        }
        if (outcome != nullptr) {
            outcome->increment();
        }
    } catch (...) {
        // Only a lock can throw here, and a count is not worth the writer.
    }
}

void Writer::Core::on_stopped() noexcept {
    try {
        transport_->abandon();
        after_attempt(false);
        // Only an INSERT counts as an abandoned one; the rows of a chunk not
        // yet in one count as abandoned rows all the same.
        const std::uint64_t insert_rows = unacknowledged_rows();
        const std::uint64_t rows = insert_rows + consuming_;
        give_up(rows, insert_rows > 0 ? metrics_.inserts_abandoned : nullptr);
        store(std::make_exception_ptr(NativeSinkError(
            code::kCancelled,
            "clickhouse native sink: the writer stopped on cancel" +
                (rows > 0 ? "; " + std::to_string(rows) + " unacknowledged rows were abandoned"
                          : std::string()))));
    } catch (...) {
        // run() stores a failure of its own if none was stored here.
    }
}

void Writer::Core::store(std::exception_ptr failure) {
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (!failure_) {
            failure_ = std::move(failure);
        }
    }
    done_cv_.notify_all();
}

void Writer::Core::complete_ticket(std::uint64_t ticket) {
    {
        const std::lock_guard<std::mutex> lock(mu_);
        tickets_done_ = std::max(tickets_done_, ticket);
    }
    done_cv_.notify_all();
}

std::uint64_t Writer::Core::unacknowledged_rows() const {
    if (!current_) {
        return 0;
    }
    return current_->rows - std::min(current_acked_, current_->rows);
}

bool Writer::Core::stopping() const noexcept {
    return stop_.load(std::memory_order_acquire) || config_.cancel.requested();
}

void Writer::Core::check_stop() const {
    if (stopping()) {
        throw Stopped{};
    }
}

std::string Writer::Core::statement(const Insert& in) const {
    InsertText text;
    text.database = config_.options.database;
    text.table = config_.options.table;
    text.plan = &config_.plan;
    text.caps = live_.caps;
    text.token = in.token;
    text.sink_id = config_.sink_id;
    text.subtask = config_.options.subtask_idx;
    return insert_statement(text);
}

std::string Writer::Core::insert_label(const Insert& in) const {
    return "clickhouse native sink: INSERT seq=" + std::to_string(in.token.seq) + " of " +
           std::to_string(in.rows) + " rows into " + qualified_;
}

std::string Writer::Core::subtask_text() const {
    return std::to_string(config_.options.subtask_idx) + "/" +
           std::to_string(config_.options.parallelism);
}

void Writer::Core::report_queue_bytes() noexcept {
    report([this] {
        if (metrics_.queue_bytes != nullptr) {
            metrics_.queue_bytes->set(
                static_cast<std::int64_t>(queue_bytes_now_.load(std::memory_order_relaxed)));
        }
    });
}

WriterStats Writer::Core::snapshot_stats() const {
    const std::lock_guard<std::mutex> lock(stats_mu_);
    return stats_;
}

// ---------------------------------------------------------------------------
// Writer

Writer::Writer(WriterConfig config, std::unique_ptr<InsertTransport> transport, TokenSource tokens)
    : core_(std::make_shared<Core>(std::move(config), std::move(transport), std::move(tokens))) {
    // The thread's own share of the Core is what makes a detach safe: nothing
    // it touches can be destroyed under it.
    core_->thread_ = std::thread([core = core_] { core->run(); });
}

Writer::~Writer() {
    abort();
}

void Writer::submit(Chunk chunk) {
    Core& c = *core_;
    if (!chunk.batch) {
        throw std::invalid_argument("clickhouse native sink: a chunk without a batch");
    }
    const Clock::time_point start = Clock::now();
    bool waited = false;
    {
        std::unique_lock<std::mutex> lock(c.mu_);
        for (;;) {
            const Core::Gate gate = c.gate(false);
            if (gate != Core::Gate::Open) {
                const std::exception_ptr failure = c.failure_;
                lock.unlock();
                c.raise(gate, failure);
            }
            if (chunk.batch->num_rows() == 0) {
                return;
            }
            const bool full = c.queued_chunks_ > 0 &&
                              (c.queued_bytes_ >= kQueueBytes || c.queued_chunks_ >= kQueueChunks);
            if (!full) {
                break;
            }
            // This is the backpressure on the source: while the writer
            // retries, the task thread stops here once the queue is full.
            waited = true;
            c.done_cv_.wait_for(lock, kSlice);
        }
        {
            const std::lock_guard<std::mutex> stats(c.stats_mu_);
            ++(chunk.carrier == Carrier::Columnar ? c.stats_.columnar_batches
                                                  : c.stats_.row_batches);
        }
        c.queued_bytes_ += chunk.bytes;
        ++c.queued_chunks_;
        c.queue_bytes_now_.store(c.queued_bytes_, std::memory_order_relaxed);
        Item item;
        item.kind = Item::Kind::Chunk;
        item.chunk = std::move(chunk);
        c.items_.push_back(std::move(item));
    }
    c.work_cv_.notify_one();
    const std::uint64_t blocked = to_ns(Clock::now() - start);
    c.report_queue_bytes();
    if (waited) {
        c.report([&] {
            if (c.metrics_.backpressure != nullptr) {
                c.metrics_.backpressure->observe(static_cast<double>(blocked));
            }
        });
    }
}

void Writer::flush(std::uint64_t /*checkpoint_id*/) {
    Core& c = *core_;
    const Clock::time_point start = Clock::now();
    std::uint64_t ticket = 0;
    {
        std::unique_lock<std::mutex> lock(c.mu_);
        const Core::Gate gate = c.gate(false);
        if (gate != Core::Gate::Open) {
            const std::exception_ptr failure = c.failure_;
            lock.unlock();
            c.raise(gate, failure);
        }
        ticket = ++c.tickets_issued_;
        Item item;
        item.kind = Item::Kind::Flush;
        item.ticket = ticket;
        c.items_.push_back(std::move(item));
    }
    c.work_cv_.notify_one();
    {
        std::unique_lock<std::mutex> lock(c.mu_);
        while (c.tickets_done_ < ticket) {
            const Core::Gate gate = c.gate(true);
            if (gate != Core::Gate::Open) {
                const std::exception_ptr failure = c.failure_;
                lock.unlock();
                c.raise(gate, failure);
            }
            c.done_cv_.wait_for(lock, kSlice);
        }
    }
    const std::uint64_t waited = to_ns(Clock::now() - start);
    c.report([&] {
        if (c.metrics_.barrier_flush != nullptr) {
            c.metrics_.barrier_flush->observe(static_cast<double>(waited));
        }
    });
}

void Writer::finish() {
    Core& c = *core_;
    {
        const std::lock_guard<std::mutex> lock(c.mu_);
        if (!c.closed_ && !c.exited_) {
            Item item;
            item.kind = Item::Kind::Finish;
            c.items_.push_back(std::move(item));
            c.closed_ = true;
        }
    }
    c.work_cv_.notify_one();
    std::exception_ptr failure;
    {
        std::unique_lock<std::mutex> lock(c.mu_);
        while (!c.exited_) {
            if (c.config_.cancel.requested()) {
                lock.unlock();
                c.raise(Core::Gate::Cancelled, nullptr);
            }
            if (c.stop_.load(std::memory_order_acquire)) {
                // A concurrent abort() owns the thread now.
                lock.unlock();
                c.raise(Core::Gate::Stopped, nullptr);
            }
            c.done_cv_.wait_for(lock, kSlice);
        }
        failure = c.failure_;
    }
    if (failure) {
        // A writer that ended on a failure or a cancel did not finish
        // cleanly: abort() joins it and logs its cancelled summary, once,
        // so the rows it abandoned are reported however the task ends.
        c.abort_and_join();
        std::rethrow_exception(failure);
    }
    const std::lock_guard<std::mutex> life(c.life_mu_);
    if (c.life_ == Core::Life::Running) {
        if (c.thread_.joinable()) {
            c.thread_.join();
        }
        c.life_ = Core::Life::Finished;
    }
}

void Writer::abort() noexcept {
    core_->abort_and_join();
}

WriterStats Writer::stats() const {
    return core_->snapshot_stats();
}

std::size_t Writer::queue_bytes() const noexcept {
    return core_->queue_bytes_now_.load(std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// Summaries and the part-rate rule

std::string summary_line(std::string_view outcome,
                         const SinkOptions& options,
                         const WriterStats& stats,
                         std::chrono::milliseconds elapsed) {
    // Permanent failures are never retried, so they have no entry here.
    constexpr std::array<FailureClass, 6> kRetried = {FailureClass::TransientNotWritten,
                                                      FailureClass::InDoubt,
                                                      FailureClass::MergeBackpressure,
                                                      FailureClass::Resource,
                                                      FailureClass::ClientDefect,
                                                      FailureClass::Unclassified};
    std::string out = "clickhouse native sink ";
    out += outcome;
    out += ": subtask=" + std::to_string(options.subtask_idx) + "/" +
           std::to_string(options.parallelism);
    out += " rows_acknowledged=" + std::to_string(stats.rows_acknowledged);
    out += " inserts=" + std::to_string(stats.inserts);
    out += " retries=";
    for (std::size_t i = 0; i < kRetried.size(); ++i) {
        if (i > 0) {
            out += ",";
        }
        out += to_string(kRetried.at(i));
        out += ":";
        out += std::to_string(stats.retries.at(static_cast<std::size_t>(kRetried.at(i))));
    }
    out += " in_doubt=" + std::to_string(stats.in_doubt);
    out += " rows_resent_with_token=" + std::to_string(stats.rows_resent_with_token);
    out += " rows_maybe_duplicated=" + std::to_string(stats.rows_maybe_duplicated);
    out += " abandoned_rows=" + std::to_string(stats.abandoned_rows);
    out += " wire_bytes=" + std::to_string(stats.wire_bytes);
    out += " elapsed_ms=" + std::to_string(std::max<std::int64_t>(elapsed.count(), 0));
    out += " columnar_batches=" + std::to_string(stats.columnar_batches);
    out += " row_batches=" + std::to_string(stats.row_batches);
    return out;
}

PartRateMonitor::PartRateMonitor(std::uint32_t parallelism,
                                 std::chrono::milliseconds batch_interval,
                                 std::chrono::steady_clock::time_point start,
                                 std::chrono::milliseconds span,
                                 std::chrono::milliseconds quiet)
    : parallelism_(std::max<std::uint32_t>(parallelism, 1)),
      batch_interval_(batch_interval),
      span_(std::max(span, std::chrono::milliseconds{1})),
      quiet_(quiet),
      span_start_(start) {}

std::optional<std::string> PartRateMonitor::on_insert(std::chrono::steady_clock::time_point now,
                                                      std::uint64_t rows) {
    ++inserts_;
    rows_ += rows;
    const auto elapsed = now - span_start_;
    if (elapsed < span_) {
        return std::nullopt;
    }
    const double seconds = std::chrono::duration<double>(elapsed).count();
    const double rate = static_cast<double>(inserts_) / seconds;
    const double job_rate = rate * static_cast<double>(parallelism_);
    const std::uint64_t mean = rows_ / inserts_;
    span_start_ = now;
    inserts_ = 0;
    rows_ = 0;
    if (job_rate <= 1.0 || mean >= kSmallInsertRows) {
        return std::nullopt;
    }
    if (last_warning_ && now - *last_warning_ < quiet_) {
        return std::nullopt;
    }
    last_warning_ = now;
    return "acknowledged " + one_decimal(rate) + " INSERTs a second over the last " +
           std::to_string(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count()) +
           " s, " + std::to_string(mean) + " rows each on average; across parallelism " +
           std::to_string(parallelism_) + " that is about " + one_decimal(job_rate) +
           " INSERTs a second, and every INSERT makes at least one part per partition it "
           "touches. Raise batch_interval_ms (now " +
           std::to_string(batch_interval_.count()) + ") or lower the sink's parallelism.";
}

}  // namespace clink::clickhouse::native
