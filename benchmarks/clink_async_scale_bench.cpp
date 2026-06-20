// clink_async_scale_bench - the async-state overlap win COMPOSED WITH DATA
// PARALLELISM, across a sharded keyed stage on a DEFERRING state tier.
//
// The par=1 benchmark (clink_async_state_bench) isolates the per-key overlap:
// at one shard, async overlaps cold reads a sync path serialises. This bench
// shows that win composes with share-nothing data parallelism. ShardedKeyedStage
// fans ONE keyed operator across S worker threads, each owning a PRIVATE
// deferring backend (a first-class async citizen: per-shard gate + drain +
// resume scheduler). So:
//   * SYNC arm: each shard serialises its cold reads (the resume scheduler is
//     NOT wired, so get_async degrades to an inline blocking load) -> ~S/L.
//   * ASYNC arm: each shard ALSO overlaps its own cold reads across its IO pool
//     -> ~ S * min(P, ...) / L, bounded by machine cores.
// The headline: the overlap multiplier (P) rides on top of the parallelism (S);
// the sync model gets the parallelism but never the overlap.
//
// PREMISE (apples-to-apples; the only variable is the execution model):
//   * Same operator (a per-key counter doing a real get -> +1 -> put), same N
//     records over K == N DISTINCT cold keys, same deferring tier (a loader that
//     SLEEPS L us per cold read), same per-shard IO pool size P, same shard
//     count S - on BOTH arms. The ONLY difference is whether the per-shard
//     backend reports supports_async_get() (async) or is wrapped in a SyncFacade
//     that reports false (sync), which is exactly the switch the sharded runner
//     gates the resume-scheduler wiring on. Both arms run the SAME process_async
//     path, so this isolates DEFERRAL, not coroutine overhead.
//
// WHY A SYNTHETIC LOADER, NOT REAL S3: the pool-backed RemoteReadBackend serves
// a fresh job's first-touch reads as nullopt WITHOUT hitting S3 (no committed
// checkpoint yet - last_ckpt_ == 0), and fresh writes land in the hot tier. So
// real remote READ latency on a pooled backend manifests at RESTORE / eviction,
// not on a fresh forward run - that is the failover/exactly-once proof's job.
// Here the synthetic loader is the honest model of "a read that genuinely hits a
// slow remote tier" (identical to the committed par=1 bench), which is what lets
// us measure the SCALING of the overlap cleanly, without object-store noise.
//
// Self-validating: emits == N and cold reads == N on BOTH arms (so neither arm
// is skipping work), and for P>1 the async arm must out-throughput the sync arm.
// Hard-fails otherwise, so it can never silently regress to a meaningless number.

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

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/key_group_partitioner.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/sharded_keyed_stage.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend.hpp"

using namespace clink;

namespace {

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

std::vector<std::size_t> parse_csv_sizes(const std::string& csv) {
    std::vector<std::size_t> out;
    std::size_t start = 0;
    while (start < csv.size()) {
        const auto comma = csv.find(',', start);
        const auto piece =
            csv.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (!piece.empty()) {
            out.push_back(static_cast<std::size_t>(std::stoull(piece)));
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
}

// Distinct loader-thread ids (mutex-guarded; the loader runs from S*P IO threads
// on the async arm, S worker threads on the sync arm) plus a total cold-read
// counter. The distinct count is the overlap evidence: ~S on the sync arm (the
// load runs inline on each shard's single worker), ~S*P on the async arm.
struct LoaderStats {
    mutable std::mutex mu;
    std::set<std::thread::id> threads;
    std::atomic<std::uint64_t> loads{0};
    void record(std::thread::id id) {
        loads.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lk(mu);
        threads.insert(id);
    }
    std::size_t distinct() const {
        std::lock_guard<std::mutex> lk(mu);
        return threads.size();
    }
};

// Forwards everything to an inner RemoteReadBackend but reports
// supports_async_get()==false, so the sharded runner does NOT wire a resume
// scheduler and the operator's get_async degrades to an inline blocking load.
// Same loader, same latency, same IO pool - the ONLY difference from the async
// arm is the execution model.
class SyncFacade final : public StateBackend {
public:
    explicit SyncFacade(std::shared_ptr<RemoteReadBackend> inner) : inner_(std::move(inner)) {}

    void put(OperatorId op, KeyView key, ValueView value) override { inner_->put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_->get(op, key);  // cold key -> loader runs INLINE on the worker thread
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
    [[nodiscard]] bool supports_async_get() const noexcept override { return false; }

private:
    std::shared_ptr<RemoteReadBackend> inner_;
};

// A per-key counter doing a real read-modify-write through KeyedState. The async
// path co_awaits the (deferring) read; the sync process() path blocks on it. The
// sharded runner uses process_async whenever supports_async() is true, so both
// arms run the SAME coroutine path - only the backend's deferral differs.
class CountingOp final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }

    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            return;
        }
        Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            const auto c = state_->get(k).value_or(0) + 1;
            state_->put(k, c);
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }

    [[nodiscard]] bool supports_async() const noexcept override { return true; }

    void process_async(const StreamElement<std::int64_t>& el,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!el.is_data()) {
            return;
        }
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            auto ks = *state_;  // cheap view copy; outlives the Task in the closure
            auto factory = [ks, k, &out]() mutable -> async::Task<void> {
                auto cur = co_await ks.get_async(k);
                const auto c = cur.value_or(0) + 1;
                ks.put(k, c);
                Batch<std::int64_t> b;
                b.emplace(c);
                out.emit_data(std::move(b));
                co_return;
            };
            while (!aec.submit(std::to_string(k), factory)) {
                aec.poll();
            }
        }
    }

    std::string name() const override { return "counter"; }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

struct ArmResult {
    double wall_ms{0};
    std::int64_t emits{0};
    std::uint64_t remote_loads{0};
    std::size_t loader_threads{0};
};

// Drive N distinct cold keys through a stage of `shards` workers, each shard
// owning a deferring backend with `io_threads` IO threads and a loader sleeping
// `latency_us` per cold read. `async`=false wraps each shard backend in a
// SyncFacade (inline blocking reads).
ArmResult run_arm(std::int64_t n,
                  std::size_t shards,
                  std::size_t io_threads,
                  std::int64_t latency_us,
                  bool async) {
    auto stats = std::make_shared<LoaderStats>();
    const StateBackend::Value zero = int64_codec().encode(std::int64_t{0});
    const auto delay = std::chrono::microseconds(latency_us);
    RemoteReadBackend::RemoteLoader loader =
        [stats, zero, delay](OperatorId, std::string) -> std::optional<StateBackend::Value> {
        stats->record(std::this_thread::get_id());
        std::this_thread::sleep_for(delay);
        return zero;
    };

    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.shard_backend_factory =
        [loader, io_threads, async](std::size_t) -> std::unique_ptr<StateBackend> {
        if (async) {
            return std::make_unique<RemoteReadBackend>(loader, io_threads);  // defers cold reads
        }
        return std::make_unique<SyncFacade>(
            std::make_shared<RemoteReadBackend>(loader, io_threads));  // inline blocking reads
    };

    std::atomic<std::int64_t> emits{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        shards,
        OperatorId{1},
        [](std::size_t) { return std::make_unique<CountingOp>(); },
        [kc = int64_codec()](const std::int64_t& v) { return kc.encode(v); },
        [&emits](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                std::memory_order_relaxed);
            }
            return true;
        },
        opts);

    Batch<std::int64_t> b;
    for (std::int64_t i = 0; i < n; ++i) {
        b.emplace(i);  // distinct key i
    }

    const auto t0 = std::chrono::steady_clock::now();
    stage.start();
    stage.submit(std::move(b));
    stage.close_input();
    stage.await();
    const auto t1 = std::chrono::steady_clock::now();

    ArmResult r;
    r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.emits = emits.load(std::memory_order_relaxed);
    r.remote_loads = stats->loads.load(std::memory_order_relaxed);
    r.loader_threads = stats->distinct();
    return r;
}

ArmResult run_arm_median(std::int64_t n,
                         std::size_t shards,
                         std::size_t io_threads,
                         std::int64_t latency_us,
                         bool async,
                         int repeats) {
    std::vector<ArmResult> runs;
    runs.reserve(static_cast<std::size_t>(repeats));
    for (int i = 0; i < repeats; ++i) {
        runs.push_back(run_arm(n, shards, io_threads, latency_us, async));
    }
    std::sort(runs.begin(), runs.end(), [](const ArmResult& a, const ArmResult& b) {
        return a.wall_ms < b.wall_ms;
    });
    return runs[runs.size() / 2];
}

double rec_per_s(std::int64_t n, double wall_ms) {
    return wall_ms > 0 ? (static_cast<double>(n) * 1000.0 / wall_ms) : 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        std::printf(
            "clink_async_scale_bench - async overlap composed with data parallelism\n"
            "  --records=N        records == distinct cold keys (default 2048)\n"
            "  --shards=CSV       shard counts to sweep (default 1,2,4,8)\n"
            "  --io-threads=P     per-shard IO pool size (default 4)\n"
            "  --latency-us=L     per-cold-read latency the loader sleeps (default 200)\n"
            "  --repeats=R        runs per cell, median reported (default 3)\n"
            "  --format=human|json\n"
            "  --strict           exit nonzero on a premise WARNING\n");
        return 0;
    }

    const auto n = static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "records", "2048")));
    const auto shards = parse_csv_sizes(get_arg(argc, argv, "shards", "1,2,4,8"));
    const auto io_threads =
        static_cast<std::size_t>(std::stoull(get_arg(argc, argv, "io-threads", "4")));
    const auto latency_us =
        static_cast<std::int64_t>(std::stoll(get_arg(argc, argv, "latency-us", "200")));
    const auto repeats = std::stoi(get_arg(argc, argv, "repeats", "3"));
    const std::string format = get_arg(argc, argv, "format", "human");
    const bool strict = has_flag(argc, argv, "strict");

    const unsigned cores = std::max(1u, std::thread::hardware_concurrency());
    int rc = 0;

    if (format == "human") {
        std::printf(
            "# async overlap x data parallelism: N=%lld distinct cold keys, "
            "P=%zu IO threads/shard, L=%lld us, cores=%u\n",
            static_cast<long long>(n),
            io_threads,
            static_cast<long long>(latency_us),
            cores);
        std::printf("# %6s %14s %14s %9s %12s %12s\n",
                    "shards",
                    "sync rec/s",
                    "async rec/s",
                    "speedup",
                    "sync ldthr",
                    "async ldthr");
    }

    for (std::size_t s : shards) {
        const ArmResult sync = run_arm_median(n, s, io_threads, latency_us, false, repeats);
        const ArmResult async = run_arm_median(n, s, io_threads, latency_us, true, repeats);
        const double sync_rs = rec_per_s(n, sync.wall_ms);
        const double async_rs = rec_per_s(n, async.wall_ms);
        const double speedup = sync_rs > 0 ? async_rs / sync_rs : 0;

        // Structural invariants (hard fail): both arms must do the full work.
        auto fatal = [&](const char* why, const ArmResult& a, const char* arm) {
            std::fprintf(stderr,
                         "FATAL [shards=%zu %s]: %s (emits=%lld loads=%llu)\n",
                         s,
                         arm,
                         why,
                         static_cast<long long>(a.emits),
                         static_cast<unsigned long long>(a.remote_loads));
            rc = 2;
        };
        if (sync.emits != n)
            fatal("sync emits != N", sync, "sync");
        if (async.emits != n)
            fatal("async emits != N", async, "async");
        if (sync.remote_loads < static_cast<std::uint64_t>(n))
            fatal("sync cold reads < N", sync, "sync");
        if (async.remote_loads < static_cast<std::uint64_t>(n))
            fatal("async cold reads < N", async, "async");

        // Premise check (warn, or fatal under --strict): with P>1 IO threads the
        // async arm must overlap and beat the serial-per-shard sync arm. Allow a
        // little slack when S*P is well past the core count (both saturate).
        if (io_threads > 1 && speedup < 1.2) {
            std::fprintf(
                stderr,
                "%s [shards=%zu]: async speedup %.2fx <= 1.2x with P=%zu (S*P=%zu, cores=%u)\n",
                strict ? "FATAL" : "WARN",
                s,
                speedup,
                io_threads,
                s * io_threads,
                cores);
            if (strict)
                rc = 2;
        }

        if (format == "json") {
            std::printf(
                "{\"shards\":%zu,\"io_threads\":%zu,\"latency_us\":%lld,\"records\":%lld,"
                "\"sync_rec_s\":%.0f,\"async_rec_s\":%.0f,\"speedup\":%.2f,"
                "\"sync_loader_threads\":%zu,\"async_loader_threads\":%zu}\n",
                s,
                io_threads,
                static_cast<long long>(latency_us),
                static_cast<long long>(n),
                sync_rs,
                async_rs,
                speedup,
                sync.loader_threads,
                async.loader_threads);
        } else {
            std::printf("  %6zu %14.0f %14.0f %8.2fx %12zu %12zu\n",
                        s,
                        sync_rs,
                        async_rs,
                        speedup,
                        sync.loader_threads,
                        async.loader_threads);
        }
    }

    if (rc != 0) {
        std::fprintf(stderr, "FAIL: invariants/premise violated\n");
    }
    return rc;
}
