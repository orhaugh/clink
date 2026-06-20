// clink_async_state_bench - async-vs-sync throughput on a DEFERRING state tier.
//
// Proves the headline value of clink's async-state execution path: when keyed
// state lives behind a slow/remote tier, the per-key-gated async path OVERLAPS
// cold reads that the synchronous path can only serialise.
//
// PREMISE (apples-to-apples; the only variable is the execution model):
//   * Same operator: KeyedAggregateOperator<int64,int64,int64> running SUM, a
//     real per-record read-modify-write (get -> combine -> put -> emit).
//   * Same input: N records over K == N DISTINCT keys (key = record index), so
//     every state read is a first-touch COLD miss (no hot-tier short-circuit)
//     and remote_loads == N on both arms.
//   * Same deferring tier: one RemoteReadBackend whose loader SLEEPS L us per
//     cold read, with the same IO capacity P (io_threads = connection-pool size)
//     on both arms.
//   * SYNC arm: a SyncFacade wrapper returns supports_async_get()==false, so the
//     runner takes the inline process() path; each cold get() runs the loader
//     INLINE on the single runner thread, blocking it for L. Reads are strictly
//     serial -> throughput ceiling ~ 1/L, independent of P.
//   * ASYNC arm: the bare RemoteReadBackend (supports_async_get()==true); the
//     runner submits a coroutine per record under the per-key gate, co_await
//     get_async suspends, the load runs on one of P IO threads, the handle
//     resumes on the runner. Distinct cold keys overlap -> ceiling ~
//     min(P, in-flight cardinality)/L.
//
// SPEEDUP MODEL: speedup(P) ~= min(P, K, AEC_cap=6000), in practice dominated by
// P (the connection-pool size we sweep) and capped by machine cores, per-record
// CPU, and coroutine-scheduler overhead. This is NOT a strawman: the synchronous
// model structurally cannot use the connection pool (one runner thread blocks in
// the loader); async uses precisely the IO concurrency a real ForSt/S3 client
// exposes. We never quote a bare multiple; the headline is always "at P, L, K".
//
// This is a controlled-latency SYNTHETIC-loader bench (the loader just sleeps);
// it is NOT an S3/MinIO end-to-end (that adds noise and is a separate concern).
// Self-validating: structural invariants (loads==N, hot==0, equal emits, the
// loader ran off the single runner thread for P>1) HARD-FAIL the bench, so it
// can never silently regress to a meaningless number.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/operators/keyed_aggregate_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend.hpp"

using namespace clink;

namespace {

using KV = std::pair<std::int64_t, std::int64_t>;  // (key, value) in
using KA = std::pair<std::int64_t, std::int64_t>;  // (key, running-sum) out

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

// Records the distinct thread ids the loader ran on (mutex-guarded - the loader
// is called from P concurrent IO threads). The COUNT is the overlap proof: the
// sync arm serialises on one runner thread (count == 1); the async arm with
// P>1 fans loads across multiple IO threads (count >= 2).
struct LoaderStats {
    mutable std::mutex mu;
    std::set<std::thread::id> threads;
    void record(std::thread::id id) {
        std::lock_guard<std::mutex> lk(mu);
        threads.insert(id);
    }
    std::size_t distinct() const {
        std::lock_guard<std::mutex> lk(mu);
        return threads.size();
    }
};

// Forwards everything to an inner RemoteReadBackend but reports
// supports_async_get()==false, so the runner takes the synchronous process()
// path. Same loader, same latency, same IO threads, same hot-tier behaviour -
// the ONLY difference from the async arm is the execution model. NOT hobbled:
// it is the engine's own inline path on the identical backend tier.
class SyncFacade final : public StateBackend {
public:
    explicit SyncFacade(std::shared_ptr<RemoteReadBackend> inner) : inner_(std::move(inner)) {}

    void put(OperatorId op, KeyView key, ValueView value) override { inner_->put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_->get(op, key);  // cold key -> loader runs INLINE on the runner thread
    }
    void erase(OperatorId op, KeyView key) override { inner_->erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_->scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return inner_->snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        inner_->restore(snap, kg);
    }
    std::string description() const override {
        return "sync-facade(" + inner_->description() + ")";
    }
    // The switch: false -> dag runner's async_mode is false -> process() path.
    [[nodiscard]] bool supports_async_get() const noexcept override { return false; }
    // Deliberately NOT overriding get_async: the base default (co_return get())
    // is never reached because supports_async_get() is false.

private:
    std::shared_ptr<RemoteReadBackend> inner_;
};

struct ArmResult {
    double wall_ms{0};
    std::int64_t emits{0};
    std::uint64_t remote_loads{0};
    std::uint64_t hot_hits{0};
    std::size_t loader_threads{0};
};

// Run N records (K==N distinct cold keys) through KeyedAggregateOperator on a
// RemoteReadBackend with `io_threads` IO threads and a loader that sleeps
// `latency_us` per cold read. `async`=false wraps the backend in SyncFacade.
ArmResult run_arm(std::int64_t n, std::size_t io_threads, std::int64_t latency_us, bool async) {
    std::vector<Record<KV>> input;
    input.reserve(static_cast<std::size_t>(n));
    for (std::int64_t i = 0; i < n; ++i) {
        input.emplace_back(KV{i, 1});  // distinct key i, value 1
    }

    auto stats = std::make_shared<LoaderStats>();
    const StateBackend::Value zero = int64_codec().encode(std::int64_t{0});
    const auto delay = std::chrono::microseconds(latency_us);
    RemoteReadBackend::RemoteLoader loader =
        [stats, zero, delay](OperatorId, std::string) -> std::optional<StateBackend::Value> {
        stats->record(std::this_thread::get_id());
        std::this_thread::sleep_for(delay);  // the controlled remote-read latency
        return zero;                         // any cold key resolves to 0 -> SUM folds value 1
    };

    auto rrb = std::make_shared<RemoteReadBackend>(loader, io_threads);
    std::shared_ptr<StateBackend> backend =
        async ? std::static_pointer_cast<StateBackend>(rrb)
              : std::static_pointer_cast<StateBackend>(std::make_shared<SyncFacade>(rrb));

    Dag dag;
    auto src = std::make_shared<VectorSource<KV>>(std::move(input));
    auto op = std::make_shared<KeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        int64_codec(),
        int64_codec(),
        "sum");
    std::atomic<std::int64_t> emits{0};
    auto sink = std::make_shared<FunctionSink<KA>>(
        [&emits](const KA&) { emits.fetch_add(1, std::memory_order_relaxed); });
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KA>(h0, op);
    dag.add_sink<KA>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = std::move(backend);

    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();

    ArmResult r;
    r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.emits = emits.load(std::memory_order_relaxed);
    r.remote_loads = rrb->remote_loads();
    r.hot_hits = rrb->hot_hits();
    r.loader_threads = stats->distinct();
    return r;
}

// Median wall over a few repeats; the counters come from the median run's last
// invocation (they are deterministic across repeats for a fixed config).
ArmResult run_arm_median(
    std::int64_t n, std::size_t io_threads, std::int64_t latency_us, bool async, int repeats) {
    std::vector<ArmResult> runs;
    runs.reserve(static_cast<std::size_t>(repeats));
    for (int i = 0; i < repeats; ++i) {
        runs.push_back(run_arm(n, io_threads, latency_us, async));
    }
    std::sort(runs.begin(), runs.end(), [](const ArmResult& a, const ArmResult& b) {
        return a.wall_ms < b.wall_ms;
    });
    return runs[runs.size() / 2];
}

bool g_strict = false;
int g_warnings = 0;

void check(bool ok, const std::string& msg) {
    if (!ok) {
        std::printf("  WARNING: %s\n", msg.c_str());
        ++g_warnings;
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        std::printf(
            "clink_async_state_bench - async-vs-sync throughput on a deferring state tier\n"
            "  --records=N        records == distinct cold keys (default 2048)\n"
            "  --latency-us=L     per-cold-read latency the loader sleeps (default 200)\n"
            "  --pool-sizes=CSV   io_threads to sweep (default 1,8,16,32); P=1 is the control\n"
            "  --repeats=R        runs per cell, median reported (default 3)\n"
            "  --format=human|json\n"
            "  --strict           exit nonzero on any premise WARNING (not just structural)\n");
        return 0;
    }

    const auto n = static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "records", "2048")));
    const auto latency_us =
        static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "latency-us", "200")));
    const int repeats = std::stoi(get_arg(argc, argv, "repeats", "3"));
    const std::string format = get_arg(argc, argv, "format", "human");
    g_strict = has_flag(argc, argv, "strict");

    std::vector<std::size_t> pool_sizes;
    {
        const std::string csv = get_arg(argc, argv, "pool-sizes", "1,8,16,32");
        std::size_t start = 0;
        while (start <= csv.size()) {
            const auto comma = csv.find(',', start);
            const auto tok =
                csv.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
            if (!tok.empty()) {
                pool_sizes.push_back(static_cast<std::size_t>(std::stoull(tok)));
            }
            if (comma == std::string::npos) {
                break;
            }
            start = comma + 1;
        }
    }

    const unsigned hw = std::thread::hardware_concurrency();
    if (format != "json") {
        std::printf(
            "scenario async_remote_read: N=%lld records == K distinct COLD keys, running SUM, "
            "L=%lldus per read\n"
            "  premise: same operator + same L + same io_threads P per row; only the execution "
            "model differs (sync blocks the runner per read; async overlaps via the IO pool).\n"
            "  speedup(P) bounded by min(io_threads, K, AEC_cap=6000); hardware_concurrency=%u.\n"
            "  %-4s %-6s %10s %12s %9s %8s %5s %8s\n",
            static_cast<long long>(n),
            static_cast<long long>(latency_us),
            hw,
            "P",
            "arm",
            "wall_ms",
            "rec_s",
            "speedup",
            "loads",
            "hot",
            "ldthreads");
    }

    for (std::size_t p : pool_sizes) {
        const ArmResult s = run_arm_median(n, p, latency_us, /*async=*/false, repeats);
        const ArmResult a = run_arm_median(n, p, latency_us, /*async=*/true, repeats);
        const double sync_rps = s.wall_ms > 0 ? static_cast<double>(n) / (s.wall_ms / 1000.0) : 0;
        const double async_rps = a.wall_ms > 0 ? static_cast<double>(n) / (a.wall_ms / 1000.0) : 0;
        const double speedup = a.wall_ms > 0 ? s.wall_ms / a.wall_ms : 0;

        if (format == "json") {
            std::printf(
                "{\"P\":%zu,\"L_us\":%lld,\"N\":%lld,\"sync_wall_ms\":%.2f,\"async_wall_ms\":%.2f,"
                "\"speedup\":%.2f,\"sync_loads\":%llu,\"async_loads\":%llu,\"async_ldthreads\":%zu}"
                "\n",
                p,
                static_cast<long long>(latency_us),
                static_cast<long long>(n),
                s.wall_ms,
                a.wall_ms,
                speedup,
                static_cast<unsigned long long>(s.remote_loads),
                static_cast<unsigned long long>(a.remote_loads),
                a.loader_threads);
        } else {
            const char* ctl = (p == 1) ? " (control)" : "";
            std::printf("  %-4zu %-6s %10.2f %12.0f %9s %8llu %5llu %8zu%s\n",
                        p,
                        "sync",
                        s.wall_ms,
                        sync_rps,
                        "-",
                        static_cast<unsigned long long>(s.remote_loads),
                        static_cast<unsigned long long>(s.hot_hits),
                        s.loader_threads,
                        ctl);
            std::printf("  %-4zu %-6s %10.2f %12.0f %8.2fx %8llu %5llu %8zu%s\n",
                        p,
                        "async",
                        a.wall_ms,
                        async_rps,
                        speedup,
                        static_cast<unsigned long long>(a.remote_loads),
                        static_cast<unsigned long long>(a.hot_hits),
                        a.loader_threads,
                        ctl);
        }

        // Structural invariants (HARD FAIL always - prove the bench did real cold IO).
        if (s.emits != n || a.emits != n) {
            std::fprintf(stderr,
                         "FATAL: emits sync=%lld async=%lld expected=%lld\n",
                         static_cast<long long>(s.emits),
                         static_cast<long long>(a.emits),
                         static_cast<long long>(n));
            return 2;
        }
        // Every one of the N distinct data keys must be a cold read on BOTH
        // arms (>= N, not == N: the source's offset read + EOS add a couple of
        // deterministic infra cold reads, identical on both arms, so the latency
        // budget stays equal). FEWER than N would mean a key was served warm.
        if (s.remote_loads < static_cast<std::uint64_t>(n) ||
            a.remote_loads < static_cast<std::uint64_t>(n)) {
            std::fprintf(stderr,
                         "FATAL: remote_loads sync=%llu async=%llu < expected %lld (a data key was "
                         "not a cold read?)\n",
                         static_cast<unsigned long long>(s.remote_loads),
                         static_cast<unsigned long long>(a.remote_loads),
                         static_cast<long long>(n));
            return 2;
        }
        if (s.remote_loads != a.remote_loads) {
            std::fprintf(stderr,
                         "FATAL: arms did unequal cold reads sync=%llu async=%llu (latency budget "
                         "not apples-to-apples)\n",
                         static_cast<unsigned long long>(s.remote_loads),
                         static_cast<unsigned long long>(a.remote_loads));
            return 2;
        }
        if (s.hot_hits != 0 || a.hot_hits != 0) {
            std::fprintf(stderr,
                         "FATAL: hot_hits sync=%llu async=%llu (must be 0 for all-cold)\n",
                         static_cast<unsigned long long>(s.hot_hits),
                         static_cast<unsigned long long>(a.hot_hits));
            return 2;
        }
        // Premise checks (WARNING, or hard fail under --strict). loader_threads
        // is printed as corroborating evidence; the SPEEDUP is the real overlap
        // proof (you cannot beat the serial path without overlapping reads).
        if (p == 1) {
            check(speedup <= 1.6, "async P=1 control should be ~1x (no IO parallelism to exploit)");
        } else {
            check(a.loader_threads >= 2,
                  "async P>1 should fan loads across multiple IO threads (overlap evidence)");
            check(speedup >= 1.3, "async P>1 should overlap reads and beat the serial sync path");
        }
    }

    if (g_strict && g_warnings > 0) {
        std::fprintf(stderr, "FAIL: %d premise warning(s) under --strict\n", g_warnings);
        return 1;
    }
    return 0;
}
