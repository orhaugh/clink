// clink_disagg_bench - PARITY-P3: matched-premise proof of the two core
// disaggregated-state properties, clink-vs-clink, controlled and in-process.
//
// The disaggregated mechanism (RemoteReadBackend + a RemotePool: off-thread
// commit, hot-tier-over-remote with eviction, delta checkpoints) is already
// built. This bench PROVES the two properties that make it worth having, on a
// premise you can run on a laptop, with premise-free counters beside wall-clock
// and structural invariants that HARD-FAIL so it cannot silently regress to a
// meaningless number. It does NOT attempt a cluster-scale ratio (that needs a
// cluster); it establishes the properties, not a headline "Nx".
//
// --- Scenario 1: checkpoint cost ~ delta, not total state ------------------
//   PREMISE: build T distinct keys, checkpoint (both backends now hold T),
//   then dirty only D << T keys and checkpoint again. A full-snapshot backend
//   (InMemory, Arrow-IPC) re-serialises ALL T every checkpoint; the
//   disaggregated backend commits only the D changed since the last checkpoint.
//   METRIC (premise-free): entries committed / bytes written by the SECOND
//   checkpoint. Disagg = D; full-snapshot = T. The ratio is the property.
//
// --- Scenario 2: working set > hot RAM -------------------------------------
//   PREMISE: K clean keys committed to the pool, then R reads across all K with
//   the hot tier capped BELOW the working set. The bounded backend still serves
//   every read correctly by spilling clean keys and cold-loading them back
//   (remote_loads > 0); the unbounded backend keeps everything resident
//   (remote_loads ~ 0). Proves state larger than RAM is a working configuration,
//   not a failure. METRIC: correctness (hard invariant) + remote_loads + wall.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend.hpp"

using namespace clink;

namespace {

std::string get_arg(int argc, char** argv, std::string_view flag, std::string_view def) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{def};
}

int g_failures = 0;
void require(bool ok, const std::string& msg) {
    if (!ok) {
        std::fprintf(stderr, "FATAL: %s\n", msg.c_str());
        ++g_failures;
    }
}

// 8-byte little-endian key/value helpers - fixed width so every entry costs the
// same, keeping "entries" and "bytes" proportional (a clean premise).
std::string key_str(std::int64_t k) {
    std::string s(8, '\0');
    for (int i = 0; i < 8; ++i) {
        s[static_cast<std::size_t>(i)] = static_cast<char>((k >> (i * 8)) & 0xFF);
    }
    return s;
}
StateBackend::Value val_bytes(std::int64_t v) {
    StateBackend::Value b(8);
    for (int i = 0; i < 8; ++i) {
        b[static_cast<std::size_t>(i)] = static_cast<std::byte>((v >> (i * 8)) & 0xFF);
    }
    return b;
}
std::int64_t decode(const StateBackend::Value& b) {
    std::int64_t v = 0;
    for (int i = 0; i < 8 && i < static_cast<int>(b.size()); ++i) {
        v |=
            static_cast<std::int64_t>(std::to_integer<std::uint8_t>(b[static_cast<std::size_t>(i)]))
            << (i * 8);
    }
    return v;
}

void put_key(StateBackend& be, OperatorId op, std::int64_t k, std::int64_t v) {
    const std::string ks = key_str(k);
    const StateBackend::Value vb = val_bytes(v);
    be.put(op,
           StateBackend::KeyView{ks.data(), ks.size()},
           StateBackend::ValueView{reinterpret_cast<const char*>(vb.data()), vb.size()});
}
std::optional<StateBackend::Value> get_key(const StateBackend& be, OperatorId op, std::int64_t k) {
    const std::string ks = key_str(k);
    return be.get(op, StateBackend::KeyView{ks.data(), ks.size()});
}

double ms_since(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
}

// A minimal RemotePool (latest-value-wins store) that counts what each
// checkpoint actually commits - the premise-free measure of checkpoint cost.
// Self-contained rather than wrapping InMemoryRemotePool (which is final).
class CountingPool final : public RemotePool {
public:
    void commit(CheckpointId,
                CheckpointId,
                const std::vector<RemotePoolEntry>& entries,
                const std::vector<RemotePoolKey>& deletes) override {
        std::lock_guard<std::mutex> lk(mu_);
        last_commit_entries_ = entries.size();
        last_commit_bytes_ = 0;
        for (const auto& e : entries) {
            last_commit_bytes_ += e.key.size() + e.value.size();
            store_[{e.op.value(), e.key}] = e.value;
        }
        for (const auto& d : deletes) {
            store_.erase({d.op.value(), d.key});
        }
    }
    [[nodiscard]] std::optional<StateBackend::Value> read(CheckpointId,
                                                          OperatorId op,
                                                          const std::string& key) const override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = store_.find({op.value(), key});
        return it == store_.end() ? std::nullopt : std::optional<StateBackend::Value>{it->second};
    }
    [[nodiscard]] std::vector<std::optional<StateBackend::Value>> read_many(
        CheckpointId, OperatorId op, const std::vector<std::string>& keys) const override {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::optional<StateBackend::Value>> out;
        out.reserve(keys.size());
        for (const auto& k : keys) {
            auto it = store_.find({op.value(), k});
            out.push_back(it == store_.end() ? std::nullopt
                                             : std::optional<StateBackend::Value>{it->second});
        }
        return out;
    }
    void purge(CheckpointId) override {}

    void reset_last() {
        std::lock_guard<std::mutex> lk(mu_);
        last_commit_entries_ = 0;
        last_commit_bytes_ = 0;
    }
    std::size_t last_commit_entries() const {
        std::lock_guard<std::mutex> lk(mu_);
        return last_commit_entries_;
    }
    std::size_t last_commit_bytes() const {
        std::lock_guard<std::mutex> lk(mu_);
        return last_commit_bytes_;
    }

private:
    mutable std::mutex mu_;
    std::map<std::pair<std::uint64_t, std::string>, StateBackend::Value> store_;
    std::size_t last_commit_entries_ = 0;
    std::size_t last_commit_bytes_ = 0;
};

// =================== Scenario 1: checkpoint cost ~ delta ====================

void scenario_checkpoint_delta(std::int64_t total, std::int64_t delta) {
    const OperatorId op{1};
    std::printf("\n== Scenario 1: checkpoint cost ~ delta, not total ==\n");
    std::printf("   total state T=%lld keys, delta per checkpoint D=%lld keys\n",
                static_cast<long long>(total),
                static_cast<long long>(delta));

    // --- Disaggregated arm: RemoteReadBackend over a counting pool. ---
    auto pool = std::make_shared<CountingPool>();
    RemoteReadBackend disagg(pool, /*io_threads=*/4, /*hot_max_bytes=*/0);
    for (std::int64_t k = 0; k < total; ++k) {
        put_key(disagg, op, k, k);
    }
    disagg.snapshot(CheckpointId{1});  // first checkpoint: all T are dirty
    const std::size_t ckpt1_entries = pool->last_commit_entries();
    pool->reset_last();
    for (std::int64_t k = 0; k < delta; ++k) {
        put_key(disagg, op, k, k + 1000);  // dirty only D keys
    }
    auto t0 = std::chrono::steady_clock::now();
    disagg.snapshot(CheckpointId{2});  // second checkpoint: only D dirty
    const double disagg_ms = ms_since(t0);
    const std::size_t ckpt2_entries = pool->last_commit_entries();
    const std::size_t ckpt2_bytes = pool->last_commit_bytes();

    // --- Full-snapshot baseline: InMemory (Arrow-IPC), same workload. ---
    InMemoryStateBackend full;
    for (std::int64_t k = 0; k < total; ++k) {
        put_key(full, op, k, k);
    }
    full.snapshot(CheckpointId{1});
    for (std::int64_t k = 0; k < delta; ++k) {
        put_key(full, op, k, k + 1000);
    }
    t0 = std::chrono::steady_clock::now();
    const auto full_snap = full.snapshot(CheckpointId{2});
    const double full_ms = ms_since(t0);
    const std::size_t full_bytes = full_snap.bytes.size();

    std::printf(
        "   disagg  : ckpt1 committed %zu entries; ckpt2 committed %zu entries, %zu bytes, %.3f "
        "ms\n",
        ckpt1_entries,
        ckpt2_entries,
        ckpt2_bytes,
        disagg_ms);
    std::printf(
        "   full-snap: ckpt2 serialised whole state, %zu bytes, %.3f ms\n", full_bytes, full_ms);
    if (ckpt2_bytes > 0) {
        std::printf(
            "   >> disagg checkpoint is %.1fx smaller than full-snapshot (%zu vs %zu bytes)\n",
            static_cast<double>(full_bytes) / static_cast<double>(ckpt2_bytes),
            full_bytes,
            ckpt2_bytes);
    }

    // Structural invariants (premise-free): the property is that the disagg
    // checkpoint scales with the DELTA and the full-snapshot with the TOTAL.
    require(ckpt1_entries == static_cast<std::size_t>(total),
            "disagg first checkpoint must commit all T entries");
    require(ckpt2_entries == static_cast<std::size_t>(delta),
            "disagg second checkpoint must commit exactly D (delta) entries, not T");
    require(full_bytes > static_cast<std::size_t>(total) * 8,
            "full-snapshot checkpoint must serialise the whole T-key state");
}

// =================== Scenario 2: working set > hot RAM ======================

// Runs R random reads over K clean keys with the given hot cap; returns
// (checksum, remote_loads, wall_ms). hot_cap_bytes=0 means unbounded.
struct ReadArm {
    std::int64_t checksum = 0;
    std::uint64_t remote_loads = 0;
    double wall_ms = 0;
};

ReadArm run_read_arm(std::int64_t keys, std::int64_t reads, std::size_t hot_cap_bytes) {
    const OperatorId op{1};
    auto pool = std::make_shared<InMemoryRemotePool>();
    RemoteReadBackend be(pool, /*io_threads=*/4, hot_cap_bytes);
    for (std::int64_t k = 0; k < keys; ++k) {
        put_key(be, op, k, k * 7 + 1);
    }
    be.snapshot(CheckpointId{1});  // commit all K to the pool -> now clean + evictable

    std::mt19937_64 rng(12345);
    std::uniform_int_distribution<std::int64_t> pick(0, keys - 1);
    ReadArm arm;
    const auto t0 = std::chrono::steady_clock::now();
    for (std::int64_t i = 0; i < reads; ++i) {
        const std::int64_t k = pick(rng);
        auto v = get_key(be, op, k);
        require(v.has_value(), "scenario 2: every clean key must read back");
        if (v) {
            arm.checksum += decode(*v);
        }
    }
    arm.wall_ms = ms_since(t0);
    arm.remote_loads = be.remote_loads();
    return arm;
}

void scenario_working_set(std::int64_t keys, std::int64_t reads) {
    std::printf("\n== Scenario 2: working set > hot RAM ==\n");
    std::printf("   working set K=%lld keys, R=%lld reads\n",
                static_cast<long long>(keys),
                static_cast<long long>(reads));

    // Bounded hot tier: cap at ~1/8 of the working set (each entry ~ key+value
    // hot bytes; 16 bytes payload + index overhead, so budget generously per key
    // and still land well under K).
    const std::size_t per_key = 64;  // conservative hot bytes per key incl. index
    const std::size_t bounded_cap = static_cast<std::size_t>(keys) / 8 * per_key;

    const ReadArm bounded = run_read_arm(keys, reads, bounded_cap);
    const ReadArm unbounded = run_read_arm(keys, reads, /*hot_cap_bytes=*/0);

    std::printf("   bounded  (hot<=%zu B ~ K/8): checksum=%lld remote_loads=%llu wall=%.1f ms\n",
                bounded_cap,
                static_cast<long long>(bounded.checksum),
                static_cast<unsigned long long>(bounded.remote_loads),
                bounded.wall_ms);
    std::printf("   unbounded(hot=0, all resident): checksum=%lld remote_loads=%llu wall=%.1f ms\n",
                static_cast<long long>(unbounded.checksum),
                static_cast<unsigned long long>(unbounded.remote_loads),
                unbounded.wall_ms);
    std::printf(
        "   >> a working set of %lld keys ran correctly with a hot tier ~1/8 its size, "
        "spilling+refetching %llu times\n",
        static_cast<long long>(keys),
        static_cast<unsigned long long>(bounded.remote_loads));

    // Structural invariants: correctness is identical (state > RAM is not a
    // failure), the bounded arm actually spilled+refetched, the unbounded arm
    // stayed resident.
    require(bounded.checksum == unbounded.checksum,
            "bounded and unbounded arms must produce identical results");
    require(bounded.remote_loads > 0,
            "bounded arm must spill + cold-load (else the hot tier was not exceeded)");
    require(unbounded.remote_loads == 0, "unbounded arm must never cold-load (all state resident)");
}

}  // namespace

int main(int argc, char** argv) {
    const std::int64_t total = std::stoll(get_arg(argc, argv, "total", "100000"));
    const std::int64_t delta = std::stoll(get_arg(argc, argv, "delta", "1000"));
    const std::int64_t keys = std::stoll(get_arg(argc, argv, "keys", "40000"));
    const std::int64_t reads = std::stoll(get_arg(argc, argv, "reads", "200000"));

    std::printf("clink_disagg_bench (PARITY-P3): disaggregated-state property proof\n");
    scenario_checkpoint_delta(total, delta);
    scenario_working_set(keys, reads);

    if (g_failures > 0) {
        std::fprintf(stderr, "\n%d structural invariant(s) FAILED\n", g_failures);
        return 1;
    }
    std::printf("\nAll structural invariants held.\n");
    return 0;
}
