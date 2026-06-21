// CoalescingBackend (transparent cross-record read coalescer, ASYNC-10): N
// records that each co_await get_async(distinct key) collapse into ONE inner
// get_many_async when the controller flushes the pending batch. Driven through
// the real AsyncExecutionController + its flush hook, with a counting inner
// backend so the test can prove the coalescing (one get_many, zero per-record
// get_async) and the correct scatter-back.

#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/state/coalescing_backend.hpp"
#include "clink/state/state_backend.hpp"

using namespace clink;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string to_string(const StateBackend::Value& v) {
    return std::string(reinterpret_cast<const char*>(v.data()), v.size());
}

// Minimal inner backend over a map that counts get_async vs get_many_async, so
// a test can prove the coalescer routed the batch through ONE get_many_async
// and never the per-key get_async. Reads are synchronous (the coalescing is
// orthogonal to whether the inner read blocks - the production RemoteReadBackend
// exercises the async get_many path in test_get_many_async / test_s3_remote_pool).
class CountingInner final : public StateBackend {
public:
    mutable int get_async_calls{0};
    mutable int get_many_calls{0};

    void put(OperatorId op, KeyView key, ValueView value) override {
        data_[{op.value(), std::string(key)}] =
            Value{reinterpret_cast<const std::byte*>(value.data()),
                  reinterpret_cast<const std::byte*>(value.data() + value.size())};
    }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        auto it = data_.find({op.value(), std::string(key)});
        return it == data_.end() ? std::nullopt : std::optional<Value>{it->second};
    }
    void erase(OperatorId op, KeyView key) override { data_.erase({op.value(), std::string(key)}); }
    void scan(OperatorId, const ScanVisitor&) const override {}
    Snapshot snapshot(CheckpointId id) override {
        Snapshot s;
        s.checkpoint_id = id;
        return s;
    }
    void restore(const Snapshot&, const KeyGroupRange& = {}) override {}
    [[nodiscard]] std::string description() const override { return "counting-inner"; }

    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        ++get_async_calls;
        co_return get(op, key);
    }
    async::Task<std::vector<std::optional<Value>>> get_many_async(
        OperatorId op, const std::vector<std::string>& keys) const override {
        ++get_many_calls;
        std::vector<std::optional<Value>> out;
        out.reserve(keys.size());
        for (const auto& k : keys) {
            out.push_back(get(op, KeyView{k}));
        }
        co_return out;
    }

private:
    std::map<std::pair<std::uint64_t, std::string>, Value> data_;
};

// A record that reads one key through the coalescer and stores the result.
AsyncExecutionController::CoroFactory reading(
    CoalescingBackend* cb,
    OperatorId op,
    std::string key,
    std::map<std::string, std::optional<StateBackend::Value>>* out) {
    return [cb, op, key, out]() -> async::Task<void> {
        auto v = co_await cb->get_async(op, std::string_view{key});
        (*out)[key] = std::move(v);
        co_return;
    };
}

}  // namespace

TEST(CoalescingBackend, DistinctKeyReadsCollapseIntoOneGetMany) {
    CountingInner inner;
    const OperatorId op{1};
    inner.put(op, sv("a"), sv("va"));
    inner.put(op, sv("b"), sv("vb"));
    // "c" intentionally absent.

    CoalescingBackend cb(inner);
    AsyncExecutionController aec;
    aec.set_flush_hook([&] { return cb.flush(); });

    std::map<std::string, std::optional<StateBackend::Value>> out;
    ASSERT_TRUE(aec.submit("a", reading(&cb, op, "a", &out)));
    ASSERT_TRUE(aec.submit("b", reading(&cb, op, "b", &out)));
    ASSERT_TRUE(aec.submit("c", reading(&cb, op, "c", &out)));
    // All three parked on their reads; nothing issued to the inner yet.
    EXPECT_EQ(aec.in_flight(), 3u);
    EXPECT_EQ(cb.pending_reads(), 3u);
    EXPECT_EQ(inner.get_many_calls, 0);

    aec.drain();  // stuck -> flush hook -> ONE get_many for {a,b,c}

    EXPECT_EQ(aec.in_flight(), 0u);
    EXPECT_EQ(inner.get_many_calls, 1);   // the whole batch in one round-trip
    EXPECT_EQ(inner.get_async_calls, 0);  // never the per-key path
    ASSERT_TRUE(out["a"].has_value());
    EXPECT_EQ(to_string(*out["a"]), "va");
    ASSERT_TRUE(out["b"].has_value());
    EXPECT_EQ(to_string(*out["b"]), "vb");
    EXPECT_FALSE(out["c"].has_value());  // absent scatters back as nullopt
}

TEST(CoalescingBackend, SingleRecordStillResolvesThroughGetMany) {
    CountingInner inner;
    const OperatorId op{2};
    inner.put(op, sv("solo"), sv("v"));

    CoalescingBackend cb(inner);
    AsyncExecutionController aec;
    aec.set_flush_hook([&] { return cb.flush(); });

    std::map<std::string, std::optional<StateBackend::Value>> out;
    aec.submit("solo", reading(&cb, op, "solo", &out));
    aec.drain();

    EXPECT_EQ(inner.get_many_calls, 1);
    ASSERT_TRUE(out["solo"].has_value());
    EXPECT_EQ(to_string(*out["solo"]), "v");
}

// Two reads of the SAME key cannot be in flight together (the controller's
// per-key gate parks the second), so each flush batch is distinct-key. The
// second read runs in its own flush round after the first completes -> two
// get_many calls, each for one key, both correct.
TEST(CoalescingBackend, SameKeyTwiceSerialisesAcrossFlushRounds) {
    CountingInner inner;
    const OperatorId op{3};
    inner.put(op, sv("k"), sv("v"));

    CoalescingBackend cb(inner);
    AsyncExecutionController aec;
    aec.set_flush_hook([&] { return cb.flush(); });

    std::map<std::string, std::optional<StateBackend::Value>> out1;
    std::map<std::string, std::optional<StateBackend::Value>> out2;
    aec.submit("k", reading(&cb, op, "k", &out1));
    aec.submit("k", reading(&cb, op, "k", &out2));  // parks behind the first
    EXPECT_EQ(aec.in_flight(), 1u);                 // only one in flight (gate)
    EXPECT_EQ(aec.parked(), 1u);

    aec.drain();

    EXPECT_EQ(aec.in_flight(), 0u);
    EXPECT_EQ(inner.get_many_calls, 2);  // one per flush round (gate serialised them)
    EXPECT_EQ(to_string(*out1["k"]), "v");
    EXPECT_EQ(to_string(*out2["k"]), "v");
}
