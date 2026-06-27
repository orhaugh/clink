// PollingSource<T> base: the produce loop calls poll(cursor), emits the returned
// records, advances + checkpoints the cursor, and resumes from it on restart.
// Driven with a stub poll callback - no external service.

#include <chrono>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/polling_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::PollingSource;
using clink::StreamElement;

namespace {

template <typename T>
struct Captured {
    std::vector<T> values;
};

template <typename T>
Emitter<T> capturing(Captured<T>& sink) {
    return Emitter<T>{[&sink](StreamElement<T> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.values.push_back(r.value());
            }
        }
        return true;
    }};
}

// A poll callback that emits two records per poll: "<n>" and "<n+1>", where n is
// the integer cursor (empty cursor == 0). next_cursor advances by 2. The
// cursor is EXCLUSIVE (we start AT the cursor value), so no overlap.
PollingSource<std::string>::PollFn counting_poll() {
    return [](const std::string& cursor) -> PollingSource<std::string>::PollResult {
        const long n = cursor.empty() ? 0 : std::stol(cursor);
        return {{std::to_string(n), std::to_string(n + 1)}, std::to_string(n + 2)};
    };
}

PollingSource<std::string>::Options fast_opts() {
    PollingSource<std::string>::Options o;
    o.interval = std::chrono::milliseconds{0};  // no sleeping in the functional tests
    return o;
}

}  // namespace

TEST(PollingSource, PollsAdvanceCursorAndEmit) {
    PollingSource<std::string> src(fast_opts(), counting_poll());
    Captured<std::string> cap;
    auto em = capturing(cap);
    src.produce(em);  // cursor "" -> emits "0","1", cursor -> "2"
    src.produce(em);  // cursor "2" -> emits "2","3", cursor -> "4"
    EXPECT_EQ(cap.values, (std::vector<std::string>{"0", "1", "2", "3"}));
    EXPECT_EQ(src.cursor(), "4");
}

TEST(PollingSource, CursorCheckpointResumesAcrossRestart) {
    InMemoryStateBackend backend;
    const OperatorId op_id{7};
    std::vector<std::string> seen;

    {
        PollingSource<std::string> s1(fast_opts(), counting_poll());
        Captured<std::string> cap;
        auto em = capturing(cap);
        s1.produce(em);  // emits "0","1"; cursor -> "2"
        s1.snapshot_offset(backend, op_id, clink::CheckpointId{1});
        for (auto& v : cap.values) {
            seen.push_back(v);
        }
    }
    {
        PollingSource<std::string> s2(fast_opts(), counting_poll());
        ASSERT_TRUE(s2.restore_offset(backend, op_id));
        EXPECT_EQ(s2.cursor(), "2");
        Captured<std::string> cap;
        auto em = capturing(cap);
        s2.produce(em);  // resumes at "2" -> emits "2","3" (no replay of 0,1)
        for (auto& v : cap.values) {
            seen.push_back(v);
        }
    }
    EXPECT_EQ(seen, (std::vector<std::string>{"0", "1", "2", "3"}));
}

TEST(PollingSource, RestoreWithNoStateReturnsFalse) {
    InMemoryStateBackend backend;
    PollingSource<std::string> src(fast_opts(), counting_poll());
    EXPECT_FALSE(src.restore_offset(backend, OperatorId{99}));
}

TEST(PollingSource, EmptyPollEmitsNothingAndKeepsCursor) {
    auto empty_poll = [](const std::string&) -> PollingSource<std::string>::PollResult {
        return {{}, {}};  // no records, no cursor change
    };
    PollingSource<std::string> src(fast_opts(), empty_poll);
    Captured<std::string> cap;
    auto em = capturing(cap);
    EXPECT_TRUE(src.produce(em));
    EXPECT_TRUE(cap.values.empty());
    EXPECT_EQ(src.cursor(), "");
}

TEST(PollingSource, IsUnbounded) {
    PollingSource<std::string> src(fast_opts(), counting_poll());
    EXPECT_FALSE(src.is_bounded());
}

TEST(PollingSource, JitteredIntervalBounds) {
    using P = PollingSource<std::string>;
    const std::chrono::milliseconds base{1000};
    // frac 0 -> unchanged regardless of the draw.
    EXPECT_EQ(P::jittered_interval(base, 0.0, 0.0).count(), 1000);
    EXPECT_EQ(P::jittered_interval(base, 0.0, 1.0).count(), 1000);
    // frac 0.5 -> [500, 1500] at the extremes, base at the midpoint.
    EXPECT_EQ(P::jittered_interval(base, 0.5, 0.0).count(), 500);
    EXPECT_EQ(P::jittered_interval(base, 0.5, 1.0).count(), 1500);
    EXPECT_EQ(P::jittered_interval(base, 0.5, 0.5).count(), 1000);
    // frac clamps to 1.0 so the interval never goes negative.
    EXPECT_EQ(P::jittered_interval(base, 5.0, 0.0).count(), 0);
}
