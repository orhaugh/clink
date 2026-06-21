// get_many_async (ASYNC-10): batched non-blocking reads. Covers the base
// default (loops get_async, correct for every backend), the RemoteReadBackend
// override (hot hits served inline, cold misses fetched in ONE batched call),
// the batching proof (a counting pool sees exactly one read_many for the whole
// cold batch, not N reads from the backend), and the typed KeyedState surface
// incl. TTL. The S3 same-content-hash coalescing is covered (MinIO-gated) in
// impls/s3/tests/test_s3_remote_pool.cpp.

#include <chrono>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

StateBackend::Value to_value(const std::string& s) {
    StateBackend::Value v(s.size());
    if (!s.empty()) {
        std::memcpy(v.data(), s.data(), s.size());
    }
    return v;
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    if (!v.empty()) {
        std::memcpy(out.data(), v.data(), v.size());
    }
    return out;
}

// Drive a synchronous-completion Task to done (the paths under test never
// suspend: base default over InMemory, and RemoteReadBackend with no resume
// scheduler wired runs the inline batch).
template <class T>
T drive(async::Task<T> t) {
    t.resume();
    while (!t.done()) {
        t.resume();
    }
    return t.get();
}

// A RemotePool that counts read() vs read_many() so a test can prove the
// backend issues ONE batched read_many for the whole cold set, not N reads.
class CountingPool : public RemotePool {
public:
    void commit(CheckpointId id,
                CheckpointId base,
                const std::vector<RemotePoolEntry>& changed,
                const std::vector<RemotePoolKey>& deleted) override {
        std::map<std::string, StateBackend::Value> m;
        if (auto it = data_.find(base.value()); it != data_.end()) {
            m = it->second;
        }
        for (const auto& e : changed) {
            m[e.key] = e.value;
        }
        for (const auto& d : deleted) {
            m.erase(d.key);
        }
        data_[id.value()] = std::move(m);
    }
    std::optional<StateBackend::Value> read(CheckpointId id,
                                            OperatorId,
                                            const std::string& key) const override {
        ++reads_;
        auto it = data_.find(id.value());
        if (it == data_.end()) {
            return std::nullopt;
        }
        auto kit = it->second.find(key);
        return kit == it->second.end() ? std::nullopt : std::optional{kit->second};
    }
    std::vector<std::optional<StateBackend::Value>> read_many(
        CheckpointId id, OperatorId op, const std::vector<std::string>& keys) const override {
        ++read_many_calls_;
        last_batch_ = keys.size();
        return RemotePool::read_many(id, op, keys);  // default loop -> read() per key
    }
    void purge(CheckpointId id) override { data_.erase(id.value()); }

    [[nodiscard]] int read_many_calls() const { return read_many_calls_; }
    [[nodiscard]] std::size_t last_batch() const { return last_batch_; }
    [[nodiscard]] int reads() const { return reads_; }

private:
    std::map<std::uint64_t, std::map<std::string, StateBackend::Value>> data_;
    mutable int reads_{0};
    mutable int read_many_calls_{0};
    mutable std::size_t last_batch_{0};
};

}  // namespace

TEST(GetManyAsync, BaseDefaultMatchesIndividualGets) {
    InMemoryStateBackend backend;
    const OperatorId op{1};
    backend.put(op, sv("a"), sv("va"));
    backend.put(op, sv("c"), sv("vc"));

    const std::vector<std::string> keys{"a", "b", "c"};  // b absent
    auto out = drive(backend.get_many_async(op, keys));

    ASSERT_EQ(out.size(), 3u);
    ASSERT_TRUE(out[0].has_value());
    EXPECT_EQ(to_string(*out[0]), "va");
    EXPECT_FALSE(out[1].has_value());
    ASSERT_TRUE(out[2].has_value());
    EXPECT_EQ(to_string(*out[2]), "vc");
}

TEST(GetManyAsync, RemoteReadLoaderBatchServesHotAndCold) {
    RemoteReadBackend backend([](OperatorId, std::string k) -> std::optional<StateBackend::Value> {
        if (k == "cold") {
            return to_value("loaded");
        }
        return std::nullopt;  // "missing" is absent remotely
    });
    const OperatorId op{2};
    backend.put(op, sv("hot"), sv("hotval"));  // hot tier

    const std::vector<std::string> keys{"hot", "cold", "missing"};
    auto out = drive(backend.get_many_async(op, keys));  // no scheduler -> inline batch

    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(to_string(*out[0]), "hotval");  // hot hit, no load
    EXPECT_EQ(to_string(*out[1]), "loaded");  // cold load
    EXPECT_FALSE(out[2].has_value());         // absent
    // The cold value is now cached hot (write-through).
    EXPECT_EQ(to_string(*backend.get(op, sv("cold"))), "loaded");
}

TEST(GetManyAsync, PoolBackedColdBatchHitsReadManyOnce) {
    auto pool = std::make_shared<CountingPool>();
    const OperatorId op{3};
    {
        RemoteReadBackend writer(pool);
        writer.put(op, sv("a"), sv("va"));
        writer.put(op, sv("b"), sv("vb"));
        writer.snapshot(CheckpointId{1});  // commit {a,b} to the pool
    }

    RemoteReadBackend reader(pool);
    reader.restore(Snapshot{.checkpoint_id = CheckpointId{1}});  // lazy

    const std::vector<std::string> keys{"a", "b", "x"};  // all cold, x absent
    auto out = drive(reader.get_many_async(op, keys));

    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(to_string(*out[0]), "va");
    EXPECT_EQ(to_string(*out[1]), "vb");
    EXPECT_FALSE(out[2].has_value());
    // The whole cold batch went through ONE read_many, not three read() calls
    // from the backend (the backend batched; read_many's default impl then
    // looped read internally, which is the pool's business, not N round-trips
    // from the operator).
    EXPECT_EQ(pool->read_many_calls(), 1);
    EXPECT_EQ(pool->last_batch(), 3u);
}

TEST(GetManyAsync, KeyedStateTypedMatchesPerKeyGet) {
    InMemoryStateBackend backend;
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{4}, "slot", string_codec(), int64_codec());
    kv.put("a", 10);
    kv.put("c", 30);

    auto out = drive(kv.get_many_async({"a", "b", "c"}));  // b absent
    ASSERT_EQ(out.size(), 3u);
    ASSERT_TRUE(out[0].has_value());
    EXPECT_EQ(*out[0], 10);
    EXPECT_FALSE(out[1].has_value());
    ASSERT_TRUE(out[2].has_value());
    EXPECT_EQ(*out[2], 30);
    // Matches the per-key typed get.
    EXPECT_EQ(kv.get("a"), out[0]);
    EXPECT_EQ(kv.get("c"), out[2]);
}

TEST(GetManyAsync, KeyedStateTypedHonoursTtlExpiryInBatch) {
    InMemoryStateBackend backend;
    TtlConfig ttl;
    ttl.ttl = 100ms;  // refresh_on_write defaults true: a re-put resets expiry
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{5}, "ttlslot", string_codec(), int64_codec(), ttl);
    kv.put("stale", 2);
    std::this_thread::sleep_for(150ms);  // cross the 100ms expiry for everything so far
    kv.put("fresh", 3);                  // re-stamped now -> still live

    auto out = drive(kv.get_many_async({"fresh", "stale"}));
    ASSERT_EQ(out.size(), 2u);
    ASSERT_TRUE(out[0].has_value());
    EXPECT_EQ(*out[0], 3);             // fresh (just written) still live
    EXPECT_FALSE(out[1].has_value());  // stale expired -> nullopt + lazy-purged in the batch
}
