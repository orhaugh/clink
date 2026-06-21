// clink_mfast_bench - quantifies two of the three M-Fast async wins on a
// controlled-latency state tier. (The third, the ASYNC-9A io_threads
// serialization fix, is the io_threads sweep in clink_async_state_bench:
// `--pool-sizes=1,8` async rows = the pre-9A default (1) vs the post-9A
// default (8). Run that for the 9A number; this bench covers 10 and 12.)
//
// Both scenarios are clink-vs-clink A/B on the SAME backend tier; the only
// variable is the M-Fast mechanism. Controlled-latency SYNTHETIC model (no
// MinIO noise); the indisputable counter (round-trip count / emit position) is
// reported alongside wall-clock, and structural invariants HARD-FAIL so the
// bench cannot silently regress to a meaningless number.
//
// --- ASYNC-10 (coalescing) -------------------------------------------------
//   PREMISE: N records over N DISTINCT cold keys, one running-SUM operator, on
//   a pool-backed RemoteReadBackend whose RemotePool models RTT-dominated cost:
//   read() sleeps L (one round-trip per key); read_many() sleeps L ONCE (one
//   round-trip for the whole batch, as S3RemotePool's manifest-load + batched/
//   coalesced gets do). io_threads = P on both arms.
//     * NON-COALESCING (coalesce_reads=false): N per-key get_async -> N pool
//       round-trips, fanned across P IO threads -> ~ceil(N/P) waves of L.
//     * COALESCING (coalesce_reads=true): the runner wraps the backend in a
//       CoalescingBackend; the batch's N get_async collapse to ONE
//       get_many_async -> ONE pool round-trip -> ~L.
//   The round-trip count (N -> 1) is the premise-free headline; wall-clock
//   corroborates under the RTT model. (S3RemotePool ADDITIONALLY dedups
//   same-content-hash keys into one GET; proven in the s3 suite, not re-run here.)
//
// --- ASYNC-12 (deadline resume) --------------------------------------------
//   A QoS/ordering property, not throughput. N reads all park then release
//   together (one completion batch); u urgent records (lowest deadline) ARRIVE
//   LAST. Metric: the mean EMIT POSITION of the urgent subset.
//     * FIFO (a non-deadline-aware operator): emit = arrival order -> urgent
//       records emit late (mean ~ N - u/2).
//     * PRIORITY (a deadline_aware operator): the runner resumes the ready
//       batch lowest-deadline-first -> urgent records emit first (mean ~ u/2).
//   With per-record downstream cost, emit position maps directly to completion
//   latency: Priority finishes the urgent work first.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/operators/deadline_keyed_operator.hpp"
#include "clink/operators/keyed_aggregate_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend.hpp"

using namespace clink;

namespace {

using KV = std::pair<std::int64_t, std::int64_t>;

std::string get_arg(int argc, char** argv, std::string_view flag, std::string_view def = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{def};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

int g_failures = 0;
void require(bool ok, const std::string& msg) {
    if (!ok) {
        std::fprintf(stderr, "FATAL: %s\n", msg.c_str());
        ++g_failures;
    }
}

// =================== ASYNC-10: coalescing scenario =========================

// RTT-dominated RemotePool: read() = one round-trip (sleep L); read_many() =
// ONE round-trip for the whole batch (sleep L once). Values are irrelevant to
// the bench (the operator folds value_or(0)); the latency + the round-trip
// COUNT are what we measure. Thread-safe (read runs on the IO threads).
class BenchPool final : public RemotePool {
public:
    explicit BenchPool(std::int64_t latency_us) : delay_(latency_us) {}
    void commit(CheckpointId,
                CheckpointId,
                const std::vector<RemotePoolEntry>&,
                const std::vector<RemotePoolKey>&) override {}
    void purge(CheckpointId) override {}

    [[nodiscard]] std::optional<StateBackend::Value> read(CheckpointId,
                                                          OperatorId,
                                                          const std::string&) const override {
        reads_.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::microseconds(delay_));  // one round-trip
        return std::nullopt;
    }
    [[nodiscard]] std::vector<std::optional<StateBackend::Value>> read_many(
        CheckpointId, OperatorId, const std::vector<std::string>& keys) const override {
        read_manys_.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(
            std::chrono::microseconds(delay_));  // ONE round-trip for the batch
        return std::vector<std::optional<StateBackend::Value>>(keys.size(), std::nullopt);
    }

    std::uint64_t reads() const { return reads_.load(std::memory_order_relaxed); }
    std::uint64_t read_manys() const { return read_manys_.load(std::memory_order_relaxed); }

private:
    std::int64_t delay_;
    mutable std::atomic<std::uint64_t> reads_{0};
    mutable std::atomic<std::uint64_t> read_manys_{0};
};

// KeyedAggregateOperator twin that opts into read coalescing (its only
// difference from KeyedAggregateOperator).
class CoalescingSumOp final : public Operator<KV, KV> {
public:
    void process(const StreamElement<KV>&, Emitter<KV>&) override {}  // async-only in this bench
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    [[nodiscard]] bool coalesce_reads() const noexcept override { return true; }
    void process_async(const StreamElement<KV>& el,
                       Emitter<KV>& out,
                       AsyncExecutionController& aec) override {
        if (!el.is_data()) {
            return;
        }
        for (const auto& rec : el.as_data()) {
            const auto& [k, v] = rec.value();
            auto kv = state_();
            auto factory = [kv, k, v, &out]() mutable -> async::Task<void> {
                const auto cur = co_await kv.get_async(k);
                const std::int64_t next = cur.value_or(0) + v;
                kv.put(k, next);
                Batch<KV> b;
                b.emplace(KV{k, next});
                out.emit_data(std::move(b));
                co_return;
            };
            const auto kb = int64_codec().encode(k);
            std::string gate(reinterpret_cast<const char*>(kb.data()), kb.size());
            while (!aec.submit(gate, factory)) {
                aec.poll();
            }
        }
    }
    std::string name() const override { return "coalescing_sum"; }

private:
    KeyedState<std::int64_t, std::int64_t> state_() {
        return this->runtime()->keyed_state<std::int64_t, std::int64_t>(
            "agg", int64_codec(), int64_codec());
    }
};

struct CoalesceArm {
    double wall_ms{0};
    std::int64_t emits{0};
    std::uint64_t pool_reads{0};
    std::uint64_t pool_read_manys{0};
};

CoalesceArm run_coalesce_arm(std::int64_t n,
                             std::size_t io_threads,
                             std::int64_t latency_us,
                             bool coalescing) {
    std::vector<Record<KV>> input;
    input.reserve(static_cast<std::size_t>(n));
    for (std::int64_t i = 0; i < n; ++i) {
        input.emplace_back(KV{i, 1});  // distinct cold key i
    }
    auto pool = std::make_shared<BenchPool>(latency_us);
    auto backend = std::make_shared<RemoteReadBackend>(pool, io_threads);

    Dag dag;
    auto src = std::make_shared<VectorSource<KV>>(std::move(input));
    std::shared_ptr<Operator<KV, KV>> op;
    if (coalescing) {
        op = std::make_shared<CoalescingSumOp>();
    } else {
        op = std::make_shared<KeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
            int64_codec(),
            int64_codec(),
            "sum");
    }
    std::atomic<std::int64_t> emits{0};
    auto sink = std::make_shared<FunctionSink<KV>>(
        [&emits](const KV&) { emits.fetch_add(1, std::memory_order_relaxed); });
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, std::move(op));
    dag.add_sink<KV>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = backend;
    cfg.restore_from = Snapshot{CheckpointId{1}, {}};  // last_ckpt_=1 -> reads probe the pool

    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();

    CoalesceArm r;
    r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.emits = emits.load(std::memory_order_relaxed);
    r.pool_reads = pool->reads();
    r.pool_read_manys = pool->read_manys();
    return r;
}

void run_coalesce_scenario(std::int64_t n,
                           std::size_t io_threads,
                           std::int64_t latency_us,
                           const std::string& format) {
    const CoalesceArm base = run_coalesce_arm(n, io_threads, latency_us, /*coalescing=*/false);
    const CoalesceArm coal = run_coalesce_arm(n, io_threads, latency_us, /*coalescing=*/true);
    const double speedup = coal.wall_ms > 0 ? base.wall_ms / coal.wall_ms : 0;

    if (format == "json") {
        std::printf(
            "{\"scenario\":\"coalesce\",\"N\":%lld,\"P\":%zu,\"L_us\":%lld,"
            "\"base_wall_ms\":%.2f,\"coal_wall_ms\":%.2f,\"speedup\":%.2f,"
            "\"base_pool_reads\":%llu,\"base_read_manys\":%llu,"
            "\"coal_pool_reads\":%llu,\"coal_read_manys\":%llu}\n",
            static_cast<long long>(n),
            io_threads,
            static_cast<long long>(latency_us),
            base.wall_ms,
            coal.wall_ms,
            speedup,
            static_cast<unsigned long long>(base.pool_reads),
            static_cast<unsigned long long>(base.pool_read_manys),
            static_cast<unsigned long long>(coal.pool_reads),
            static_cast<unsigned long long>(coal.pool_read_manys));
    } else {
        std::printf(
            "scenario coalesce (ASYNC-10): N=%lld distinct COLD keys, P=%zu io_threads, "
            "L=%lldus per round-trip\n"
            "  %-14s %10s %14s %16s\n"
            "  %-14s %10.2f %14llu %16llu\n"
            "  %-14s %10.2f %14llu %16llu\n"
            "  -> coalescing collapses %llu per-key round-trips into %llu batched; "
            "%.1fx less wall on the RTT model.\n",
            static_cast<long long>(n),
            io_threads,
            static_cast<long long>(latency_us),
            "arm",
            "wall_ms",
            "pool_reads",
            "pool_read_manys",
            "non-coalescing",
            base.wall_ms,
            static_cast<unsigned long long>(base.pool_reads),
            static_cast<unsigned long long>(base.pool_read_manys),
            "coalescing",
            coal.wall_ms,
            static_cast<unsigned long long>(coal.pool_reads),
            static_cast<unsigned long long>(coal.pool_read_manys),
            static_cast<unsigned long long>(base.pool_reads),
            static_cast<unsigned long long>(coal.pool_read_manys),
            speedup);
    }

    // Structural invariants (HARD FAIL): both arms emit N; non-coalescing does
    // >= N per-key round-trips and 0 batched; coalescing does exactly 1 batched
    // round-trip and 0 per-key. These prove the mechanism actually ran.
    require(base.emits == n && coal.emits == n, "both arms must emit N records");
    require(base.pool_reads >= static_cast<std::uint64_t>(n),
            "non-coalescing arm must issue >= N per-key pool reads");
    require(base.pool_read_manys == 0, "non-coalescing arm must issue NO batched reads");
    require(coal.pool_read_manys == 1, "coalescing arm must issue exactly ONE batched read");
    // The N keyed DATA reads collapse into the one batched read; only the
    // identical infra reads (source offset / EOS) remain on the coalescing arm,
    // so base - coal == exactly N.
    require(coal.pool_reads + static_cast<std::uint64_t>(n) == base.pool_reads,
            "coalescing must move exactly the N per-key data reads into the one batched read");
}

// =================== ASYNC-12: deadline-resume scenario ====================

// Parks every read; releases the whole batch together once `release_at` are
// outstanding (so all completions land in ONE controller poll - the condition
// under which resume order is observable). The deadline-aware path carries the
// operator's order_key, posted through the deadline scheduler; otherwise FIFO.
class ParkBatchBackend final : public StateBackend {
public:
    explicit ParkBatchBackend(std::size_t release_at) : release_at_(release_at) {}
    void put(OperatorId op, KeyView key, ValueView value) override { store_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return store_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { store_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { store_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return store_.snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        store_.restore(snap, kg);
    }
    std::string description() const override { return "park-batch"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override { plain_ = std::move(s); }
    void set_deadline_resume_scheduler(DeadlineResumeScheduler s) override {
        deadline_ = std::move(s);
    }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Park{this, op, std::string(key), 0};
    }
    async::Task<std::optional<Value>> get_async(OperatorId op,
                                                KeyView key,
                                                std::uint64_t order_key) const override {
        co_return co_await Park{this, op, std::string(key), order_key};
    }

private:
    struct Park {
        const ParkBatchBackend* self;
        OperatorId op;
        std::string key;
        std::uint64_t order_key;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const { self->park_(h, order_key); }
        std::optional<Value> await_resume() const { return self->get(op, key); }
    };
    void park_(std::coroutine_handle<> h, std::uint64_t order_key) const {
        std::vector<std::pair<std::coroutine_handle<>, std::uint64_t>> rel;
        {
            std::lock_guard<std::mutex> lk(mu_);
            pending_.push_back({h, order_key});
            if (pending_.size() < release_at_) {
                return;
            }
            rel.swap(pending_);
        }
        for (const auto& [handle, ok] : rel) {
            if (deadline_) {
                deadline_(handle, ok);
            } else if (plain_) {
                plain_(handle);
            }
        }
    }
    InMemoryStateBackend store_;
    std::size_t release_at_;
    AsyncResumeScheduler plain_;
    DeadlineResumeScheduler deadline_;
    mutable std::mutex mu_;
    mutable std::vector<std::pair<std::coroutine_handle<>, std::uint64_t>> pending_;
};

struct DeadlineArm {
    double mean_urgent_pos{0};
    std::int64_t emits{0};
};

// n records over n distinct keys; the LAST u (arrival) are URGENT (lowest
// deadline = value). `priority`=true runs the deadline_aware operator (the
// runner flips the controller to Priority); false runs the plain aggregate
// (FIFO). Returns the mean emit position of the urgent subset.
DeadlineArm run_deadline_arm(std::int64_t n, std::int64_t u, bool priority) {
    std::vector<Record<KV>> input;
    input.reserve(static_cast<std::size_t>(n));
    std::set<std::int64_t> urgent;
    for (std::int64_t i = 0; i < n; ++i) {
        const std::int64_t key = i;
        std::int64_t deadline;
        if (i >= n - u) {
            deadline = i - (n - u);  // urgent: low deadline (0..u-1), arrives LAST
            urgent.insert(key);
        } else {
            deadline = 1'000'000 + i;  // non-urgent: high deadline
        }
        input.emplace_back(KV{key, deadline});  // value == deadline
    }

    auto backend = std::make_shared<ParkBatchBackend>(static_cast<std::size_t>(n));

    Dag dag;
    auto src = std::make_shared<VectorSource<KV>>(std::move(input));
    std::shared_ptr<Operator<KV, KV>> op;
    if (priority) {
        op = std::make_shared<
            DeadlineKeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
            [](const std::int64_t&, const std::int64_t& v) {
                return static_cast<std::uint64_t>(v);
            },
            int64_codec(),
            int64_codec(),
            "deadline_sum");
    } else {
        op = std::make_shared<KeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
            int64_codec(),
            int64_codec(),
            "sum");
    }

    std::mutex om;
    std::vector<std::int64_t> emit_keys;
    auto sink = std::make_shared<FunctionSink<KV>>([&](const KV& kv) {
        std::lock_guard<std::mutex> lk(om);
        emit_keys.push_back(kv.first);
    });
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, std::move(op));
    dag.add_sink<KV>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    DeadlineArm r;
    r.emits = static_cast<std::int64_t>(emit_keys.size());
    double sum_pos = 0;
    std::int64_t found = 0;
    for (std::size_t pos = 0; pos < emit_keys.size(); ++pos) {
        if (urgent.contains(emit_keys[pos])) {
            sum_pos += static_cast<double>(pos);
            ++found;
        }
    }
    r.mean_urgent_pos = found > 0 ? sum_pos / static_cast<double>(found) : 0;
    require(found == u, "all urgent records must be emitted");
    return r;
}

void run_deadline_scenario(std::int64_t n, std::int64_t u, const std::string& format) {
    const DeadlineArm fifo = run_deadline_arm(n, u, /*priority=*/false);
    const DeadlineArm prio = run_deadline_arm(n, u, /*priority=*/true);

    if (format == "json") {
        std::printf(
            "{\"scenario\":\"deadline\",\"N\":%lld,\"urgent\":%lld,"
            "\"fifo_mean_urgent_pos\":%.1f,\"priority_mean_urgent_pos\":%.1f}\n",
            static_cast<long long>(n),
            static_cast<long long>(u),
            fifo.mean_urgent_pos,
            prio.mean_urgent_pos);
    } else {
        std::printf(
            "scenario deadline (ASYNC-12): N=%lld reads complete together, u=%lld urgent "
            "(lowest deadline) ARRIVE LAST\n"
            "  %-10s %22s\n"
            "  %-10s %22.1f\n"
            "  %-10s %22.1f\n"
            "  -> Priority moves the urgent subset from mean emit position %.1f (FIFO, late) to "
            "%.1f (served first). QoS ordering, not throughput.\n",
            static_cast<long long>(n),
            static_cast<long long>(u),
            "resume",
            "mean_urgent_emit_pos",
            "fifo",
            fifo.mean_urgent_pos,
            "priority",
            prio.mean_urgent_pos,
            fifo.mean_urgent_pos,
            prio.mean_urgent_pos);
    }

    require(fifo.emits == n && prio.emits == n, "both arms must emit N records");
    // FIFO leaves the urgent (last-arriving) records late; Priority pulls them
    // to the front. The separation is the win.
    require(prio.mean_urgent_pos < fifo.mean_urgent_pos,
            "Priority must serve urgent records earlier than FIFO");
}

}  // namespace

int main(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        std::printf(
            "clink_mfast_bench - quantify the ASYNC-10 (coalescing) + ASYNC-12 (deadline) wins\n"
            "  (ASYNC-9A io_threads fix: run clink_async_state_bench --pool-sizes=1,8)\n"
            "  --records=N      records == distinct cold keys (default 2048)\n"
            "  --io-threads=P   IO pool size for the coalesce scenario (default 8)\n"
            "  --latency-us=L   per-round-trip latency (default 200)\n"
            "  --urgent=U       urgent (low-deadline) record count for deadline (default N/10)\n"
            "  --format=human|json\n");
        return 0;
    }

    const auto n = static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "records", "2048")));
    const auto io_threads =
        static_cast<std::size_t>(std::stoull(get_arg(argc, argv, "io-threads", "8")));
    const auto latency_us =
        static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "latency-us", "200")));
    const std::string urgent_arg = get_arg(argc, argv, "urgent", "");
    const std::int64_t u =
        urgent_arg.empty() ? std::max<std::int64_t>(1, n / 10) : std::stoll(urgent_arg);
    const std::string format = get_arg(argc, argv, "format", "human");

    run_coalesce_scenario(n, io_threads, latency_us, format);
    if (format != "json") {
        std::printf("\n");
    }
    run_deadline_scenario(n, u, format);

    if (g_failures > 0) {
        std::fprintf(stderr, "FAIL: %d structural invariant(s) violated\n", g_failures);
        return 2;
    }
    return 0;
}
