// The fluent API's stateful operators keep their state where a checkpoint
// holds it. Upgrade validation found that every fluent window aggregate but
// one, the evicting window and reduce kept theirs in memory only, so a restore
// started them empty without a word: an open window lost what it had folded,
// a reduce started again from its next record. Each test here runs an
// operator, passes a barrier, snapshots the backend as a checkpoint would,
// restores it into a fresh operator and shows the state came back; and a type
// with no codec is refused in a checkpointed job rather than held in memory.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/pipeline.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace clink::api;
using namespace std::chrono_literals;

namespace {

using I64 = std::int64_t;

I64 key0(const I64&) {
    return 0;
}
I64 zero() {
    return 0;
}
I64 add(const I64& a, const I64& v) {
    return a + v;
}

template <typename Out>
std::vector<Out> drain(BoundedChannel<StreamElement<Out>>& ch) {
    std::vector<Out> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                out.push_back(r.value());
            }
        }
    }
    return out;
}

// One run of an operator against a backend: open it, feed it, and close it.
template <typename In, typename Out>
class OpRun {
public:
    OpRun(Operator<In, Out>& op, std::shared_ptr<InMemoryStateBackend> backend, bool checkpointed)
        : op_(op),
          backend_(std::move(backend)),
          rc_(OperatorId{77}, "durable", backend_.get(), nullptr),
          ch_(1024),
          em_(&ch_) {
        rc_.set_checkpointed(checkpointed);
        op_.set_id(OperatorId{77});
        op_.attach_runtime(&rc_);
    }
    void open() { op_.open(); }
    void record(In v, I64 ts) {
        Batch<In> b;
        b.emplace(std::move(v), EventTime{ts});
        op_.process(StreamElement<In>::data(std::move(b)), em_);
    }
    void watermark(I64 ts) {
        op_.process(StreamElement<In>::watermark(Watermark{EventTime{ts}}), em_);
    }
    // The barrier, then the backend's snapshot: what a checkpoint holds.
    Snapshot checkpoint() {
        op_.process(StreamElement<In>::barrier(CheckpointBarrier{CheckpointId{1}}), em_);
        return backend_->snapshot(CheckpointId{1});
    }
    std::vector<Out> emitted() { return drain<Out>(ch_); }

private:
    Operator<In, Out>& op_;
    std::shared_ptr<InMemoryStateBackend> backend_;
    RuntimeContext rc_;
    BoundedChannel<StreamElement<Out>> ch_;
    Emitter<Out> em_;
};

std::shared_ptr<InMemoryStateBackend> restored(const Snapshot& snap) {
    auto b = std::make_shared<InMemoryStateBackend>();
    b->restore(snap);
    return b;
}

const auto kI64 = std::optional<Codec<I64>>{int64_codec()};

// Folds 1 and 2 into the window [0, 1000) of key 0, checkpoints, and returns
// what a restored operator emits when the watermark closes it.
template <typename Make, typename Out>
std::vector<Out> window_survives(Make make) {
    auto first = make();
    OpRun<I64, Out> a(*first, std::make_shared<InMemoryStateBackend>(), true);
    a.open();
    a.record(1, 100);
    a.record(2, 200);
    const auto snap = a.checkpoint();
    EXPECT_TRUE(a.emitted().empty());

    auto second = make();
    OpRun<I64, Out> b(*second, restored(snap), true);
    b.open();
    b.watermark(5000);
    return b.emitted();
}

}  // namespace

TEST(FluentStateDurability, ATumblingWindowOpenAtACheckpointComesBackWhole) {
    const auto got = window_survives<std::function<std::shared_ptr<Operator<I64, I64>>()>, I64>([] {
        return std::make_shared<api::detail::KeyedTumblingWindowFullOperator<I64, I64>>(
            key0, zero, add, 1000ms, "tumbling", kI64);
    });
    EXPECT_EQ(got, std::vector<I64>{3});
}

TEST(FluentStateDurability, ATumblingEmitWindowComesBackWhole) {
    const auto got = window_survives<std::function<std::shared_ptr<Operator<I64, I64>>()>, I64>([] {
        return std::make_shared<api::detail::KeyedTumblingWindowEmitOperator<I64, I64, I64>>(
            key0,
            zero,
            add,
            [](I64, const TimeWindow& w, const I64& a) { return a * 10000 + w.end; },
            1000ms,
            int64_codec(),
            "tumbling-emit");
    });
    EXPECT_EQ(got, std::vector<I64>{3 * 10000 + 1000});
}

TEST(FluentStateDurability, SlidingWindowsComeBackWhole) {
    auto got = window_survives<std::function<std::shared_ptr<Operator<I64, I64>>()>, I64>([] {
        return std::make_shared<api::detail::KeyedSlidingWindowFullOperator<I64, I64>>(
            key0, zero, add, 1000ms, 500ms, "sliding", kI64);
    });
    // [-500, 500) and [0, 1000) each hold both records.
    EXPECT_EQ(got, (std::vector<I64>{3, 3}));
    got = window_survives<std::function<std::shared_ptr<Operator<I64, I64>>()>, I64>([] {
        return std::make_shared<api::detail::KeyedSlidingWindowEmitOperator<I64, I64, I64>>(
            key0,
            zero,
            add,
            [](I64, const TimeWindow&, const I64& a) { return a; },
            1000ms,
            500ms,
            "sliding-emit",
            kI64);
    });
    EXPECT_EQ(got, (std::vector<I64>{3, 3}));
}

TEST(FluentStateDurability, ASessionOpenAtACheckpointComesBackWhole) {
    const auto codec = std::optional<Codec<std::vector<SessionRow<I64>>>>{
        vector_codec(session_row_codec(int64_codec()))};
    auto got = window_survives<std::function<std::shared_ptr<Operator<I64, I64>>()>, I64>([&] {
        return std::make_shared<api::detail::KeyedSessionWindowAggregateOperator<I64, I64>>(
            key0, zero, add, add, 500ms, "session", codec);
    });
    EXPECT_EQ(got, std::vector<I64>{3});
    got = window_survives<std::function<std::shared_ptr<Operator<I64, I64>>()>, I64>([&] {
        return std::make_shared<api::detail::KeyedSessionWindowEmitOperator<I64, I64, I64>>(
            key0,
            zero,
            add,
            add,
            [](I64, const TimeWindow& w, const I64& a) { return a * 10000 + w.start; },
            500ms,
            "session-emit",
            codec);
    });
    EXPECT_EQ(got, std::vector<I64>{3 * 10000 + 100});
}

TEST(FluentStateDurability, AnEvictingWindowsBufferedRecordsComeBack) {
    const auto got = window_survives<std::function<std::shared_ptr<Operator<I64, I64>>()>, I64>([] {
        return std::make_shared<api::detail::KeyedEvictingTumblingWindowFullOperator<I64, I64>>(
            key0,
            1000ms,
            [](const std::vector<Record<I64>>& rs, const TimeWindow&) {
                I64 sum = 0;
                for (const auto& r : rs) {
                    sum += r.value();
                }
                return sum;
            },
            std::make_unique<CountEvictor<I64>>(10),
            "evicting",
            kI64);
    });
    EXPECT_EQ(got, std::vector<I64>{3});
}

TEST(FluentStateDurability, AReduceContinuesFromWhatItHadReduced) {
    auto make = [] {
        return std::make_shared<api::detail::KeyedReduceOperator<I64>>(
            [](const I64& v) { return v % 2; }, add, "reduce", kI64);
    };
    auto first = make();
    OpRun<I64, I64> a(*first, std::make_shared<InMemoryStateBackend>(), true);
    a.open();
    a.record(4, 100);
    a.record(6, 200);
    const auto snap = a.checkpoint();
    EXPECT_EQ(a.emitted(), (std::vector<I64>{4, 10}));

    auto second = make();
    OpRun<I64, I64> b(*second, restored(snap), true);
    b.open();
    b.record(8, 300);
    EXPECT_EQ(b.emitted(), std::vector<I64>{18});
}

TEST(FluentStateDurability, AClosedSessionIsNotCarriedIntoTheNextCheckpoint) {
    const auto codec = std::optional<Codec<std::vector<SessionRow<I64>>>>{
        vector_codec(session_row_codec(int64_codec()))};
    auto op = std::make_shared<api::detail::KeyedSessionWindowAggregateOperator<I64, I64>>(
        key0, zero, add, add, 500ms, "session", codec);
    OpRun<I64, I64> a(*op, std::make_shared<InMemoryStateBackend>(), true);
    a.open();
    a.record(1, 100);
    (void)a.checkpoint();
    a.watermark(5000);  // fires and purges the session
    const auto snap = a.checkpoint();
    EXPECT_EQ(a.emitted(), std::vector<I64>{1});

    auto again = std::make_shared<api::detail::KeyedSessionWindowAggregateOperator<I64, I64>>(
        key0, zero, add, add, 500ms, "session", codec);
    OpRun<I64, I64> b(*again, restored(snap), true);
    b.open();
    b.watermark(9000);
    EXPECT_TRUE(b.emitted().empty());
}

TEST(FluentStateDurability, AStateTypeWithNoCodecIsRefusedInACheckpointedJob) {
    auto window = std::make_shared<api::detail::KeyedTumblingWindowFullOperator<I64, double>>(
        key0,
        [] { return 0.0; },
        [](const double& a, const I64& v) { return a + v; },
        1000ms,
        "tumbling-double");
    OpRun<I64, double> checkpointed(*window, std::make_shared<InMemoryStateBackend>(), true);
    try {
        checkpointed.open();
        FAIL() << "a window whose accumulator has no codec ran in a checkpointed job";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("register_type"), std::string::npos) << e.what();
    }
    // A job that never restores keeps it in memory, as before.
    auto again = std::make_shared<api::detail::KeyedTumblingWindowFullOperator<I64, double>>(
        key0,
        [] { return 0.0; },
        [](const double& a, const I64& v) { return a + v; },
        1000ms,
        "tumbling-double");
    OpRun<I64, double> plain(*again, std::make_shared<InMemoryStateBackend>(), false);
    EXPECT_NO_THROW(plain.open());
    plain.record(2, 100);
    plain.watermark(5000);
    EXPECT_EQ(plain.emitted(), std::vector<double>{2.0});
}

TEST(FluentStateDurability, BuiltInAndRegisteredTypesFindTheirCodec) {
    plugin::PluginRegistry reg;
    EXPECT_TRUE(api::detail::fluent_state_codec<std::int64_t>(reg).has_value());
    EXPECT_TRUE(api::detail::fluent_state_codec<std::uint64_t>(reg).has_value());
    EXPECT_TRUE(api::detail::fluent_state_codec<std::string>(reg).has_value());
    EXPECT_FALSE(api::detail::fluent_state_codec<double>(reg).has_value());
    EXPECT_TRUE(api::detail::session_state_codec<std::int64_t>(reg).has_value());
    EXPECT_FALSE(api::detail::session_state_codec<double>(reg).has_value());
    reg.register_type<double>("upgrade-test.double", trivial_codec<double>());
    EXPECT_TRUE(api::detail::fluent_state_codec<double>(reg).has_value());
}
