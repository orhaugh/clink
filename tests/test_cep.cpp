// CEP - Complex Event Processing library tests. Exercises:
//
//   * Strict-next pattern emits on contiguous match
//   * FollowedBy skips non-matching events between steps
//   * Strict-next kills the partial when a non-matching event arrives
//   * within() evicts partials whose start_ts is older than (wm - within)
//   * Keyed routing keeps per-key partials isolated
//   * Checkpoint + restore round-trips an in-flight partial match

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cep/cep_operator.hpp"
#include "clink/cep/pattern.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace clink::cep;
using namespace std::chrono_literals;

namespace {

// User event type for the tests. Trivially-copyable so we can use
// trivial_codec<Event>() and avoid hand-rolling a codec.
struct Event {
    std::int64_t key{0};
    int kind{0};  // 1=start, 2=mid, 3=end, 0=other
    int payload{0};

    bool operator==(const Event& other) const noexcept {
        return key == other.key && kind == other.kind && payload == other.payload;
    }
};

inline auto event_codec() {
    return trivial_codec<Event>();
}

// Build a CepOperator directly (bypasses the fluent API to keep the
// unit test focused on the operator's behaviour).
template <typename U>
std::shared_ptr<CepOperator<Event, U>> make_op(
    Pattern<Event> p,
    std::function<std::int64_t(const Event&)> key_fn,
    std::function<U(const PatternMatch<Event>&)> select_fn) {
    return std::make_shared<CepOperator<Event, U>>(
        std::move(p), event_codec(), std::move(key_fn), std::move(select_fn), "cep_test");
}

}  // namespace

TEST(Cep, StrictNextEmitsOnContiguousMatch) {
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 11}},
        Record<Event>{Event{0, 2, 12}},
        Record<Event>{Event{0, 3, 13}},
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .next("b")
                 .where([](const Event& e) { return e.kind == 2; })
                 .next("c")
                 .where([](const Event& e) { return e.kind == 3; });

    auto op = make_op<int>(
        p,
        [](const Event&) -> std::int64_t { return 0; },
        [](const PatternMatch<Event>& m) -> int {
            return m.at("a").front().payload + m.at("b").front().payload +
                   m.at("c").front().payload;
        });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    ASSERT_EQ(sink->collected().size(), 1u);
    EXPECT_EQ(sink->collected().front(), 11 + 12 + 13);
}

TEST(Cep, FollowedBySkipsBetweenSteps) {
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 100}},  // start
        Record<Event>{Event{0, 0, 0}},    // filler (skipped by followed_by)
        Record<Event>{Event{0, 0, 0}},    // filler (skipped by followed_by)
        Record<Event>{Event{0, 3, 300}},  // end
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; });

    auto op = make_op<int>(
        p,
        [](const Event&) -> std::int64_t { return 0; },
        [](const PatternMatch<Event>& m) -> int {
            return m.at("a").front().payload + m.at("b").front().payload;
        });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    ASSERT_EQ(sink->collected().size(), 1u);
    EXPECT_EQ(sink->collected().front(), 100 + 300);
}

TEST(Cep, StrictNextDropsPartialOnNonMatchingEvent) {
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 1}},  // start a
        Record<Event>{Event{0, 0, 0}},  // filler - kills partial (strict next expected kind=2)
        Record<Event>{Event{0, 2, 2}},  // mid - no surviving partial to advance
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .next("b")
                 .where([](const Event& e) { return e.kind == 2; });

    auto op = make_op<int>(
        p,
        [](const Event&) -> std::int64_t { return 0; },
        [](const PatternMatch<Event>& m) -> int {
            return m.at("a").front().payload + m.at("b").front().payload;
        });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_TRUE(sink->collected().empty());
}

// Source that emits in three phases so the within() watermark fires
// BETWEEN the start and the late "would-be end" event, exercising the
// pruning path deterministically.
class SplitEventSource final : public Source<Event> {
public:
    bool produce(Emitter<Event>& out) override {
        if (this->cancelled() || phase_ >= 3) {
            return false;
        }
        if (phase_ == 0) {
            Batch<Event> b;
            b.push(Record<Event>{Event{0, 1, 1}, EventTime{0}});  // start at t=0
            out.emit_data(std::move(b));
        } else if (phase_ == 1) {
            out.emit_watermark(Watermark{EventTime{1500}});  // > within(1s) past start
        } else if (phase_ == 2) {
            Batch<Event> b;
            b.push(Record<Event>{Event{0, 3, 3}, EventTime{2000}});  // end
            out.emit_data(std::move(b));
            out.emit_watermark(Watermark{EventTime{3000}});
        }
        ++phase_;
        return phase_ < 3;
    }
    std::string name() const override { return "split"; }

private:
    int phase_{0};
};

TEST(Cep, WithinEvictsBeforeLaterMatchingEvent) {
    Dag dag;
    auto src = std::make_shared<SplitEventSource>();
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; })
                 .within(1s);

    auto op = make_op<int>(
        p,
        [](const Event&) -> std::int64_t { return 0; },
        [](const PatternMatch<Event>&) -> int { return 1; });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_TRUE(sink->collected().empty()) << "within() should have evicted the partial before 'b'";
}

TEST(Cep, KeyedRoutingIsolatesPerKeyPartials) {
    Dag dag;
    // Two keys interleaved. Key 1's start must NOT pair with key 2's
    // end and vice versa.
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{1, 1, 10}},  // key=1 start
        Record<Event>{Event{2, 1, 20}},  // key=2 start
        Record<Event>{Event{1, 3, 11}},  // key=1 end -> match (10, 11)
        Record<Event>{Event{2, 3, 22}},  // key=2 end -> match (20, 22)
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; });

    auto op = make_op<std::int64_t>(
        p,
        [](const Event& e) -> std::int64_t { return e.key; },
        [](const PatternMatch<Event>& m) -> std::int64_t {
            // Encode (key, sum) into a single int for easy assertion.
            return m.at("a").front().key * 1000 + m.at("a").front().payload +
                   m.at("b").front().payload;
        });
    auto h_op = dag.add_operator<Event, std::int64_t>(h_src, op);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto out = sink->collected();
    ASSERT_EQ(out.size(), 2u);
    std::sort(out.begin(), out.end());
    EXPECT_EQ(out[0], 1 * 1000 + 10 + 11);
    EXPECT_EQ(out[1], 2 * 1000 + 20 + 22);
}

TEST(Cep, RestoreFromSnapshotResumesInflightPartial) {
    // Phase 1: feed only the "start" event, snapshot, stop.
    auto backend = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        Dag dag;
        auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
            Record<Event>{Event{0, 1, 7}},  // start event
        });
        auto h_src = dag.add_source<Event>(src);

        auto p = Pattern<Event>::begin("a")
                     .where([](const Event& e) { return e.kind == 1; })
                     .followed_by("b")
                     .where([](const Event& e) { return e.kind == 3; });

        auto op = make_op<int>(
            p,
            [](const Event&) -> std::int64_t { return 0; },
            [](const PatternMatch<Event>& m) -> int {
                return m.at("a").front().payload + m.at("b").front().payload;
            });
        auto h_op = dag.add_operator<Event, int>(h_src, op);
        auto sink = std::make_shared<CollectingSink<int>>();
        dag.add_sink<int>(h_op, sink);

        JobConfig cfg;
        cfg.state_backend = backend;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        // No match yet (only "start" was fed).
        EXPECT_TRUE(sink->collected().empty());
        // Capture the post-run backend state as a snapshot. CheckpointId
        // is informational here.
        snap = backend->snapshot(CheckpointId{1});
    }

    // Phase 2: fresh backend restored from the snapshot. Feed only the
    // "end" event - the restored partial should complete and emit.
    {
        auto fresh = std::make_shared<InMemoryStateBackend>();
        Dag dag;
        auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
            Record<Event>{Event{0, 3, 13}},  // end event
        });
        auto h_src = dag.add_source<Event>(src);

        auto p = Pattern<Event>::begin("a")
                     .where([](const Event& e) { return e.kind == 1; })
                     .followed_by("b")
                     .where([](const Event& e) { return e.kind == 3; });

        auto op = make_op<int>(
            p,
            [](const Event&) -> std::int64_t { return 0; },
            [](const PatternMatch<Event>& m) -> int {
                return m.at("a").front().payload + m.at("b").front().payload;
            });
        auto h_op = dag.add_operator<Event, int>(h_src, op);
        auto sink = std::make_shared<CollectingSink<int>>();
        dag.add_sink<int>(h_op, sink);

        JobConfig cfg;
        cfg.state_backend = fresh;
        cfg.restore_from = std::move(snap);
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        ASSERT_EQ(sink->collected().size(), 1u);
        EXPECT_EQ(sink->collected().front(), 7 + 13);
    }
}

// ---------------------------------------------------------------------
// CEP v2 - negative patterns, quantifiers, iterative conditions,
// timed-out side output.
// ---------------------------------------------------------------------

// Helper that runs a 3-event sequence through a CEP operator and
// returns whatever the sink collected. Saves boilerplate across v2
// tests.
static std::vector<int> run_cep_events(Pattern<Event> p, std::vector<Record<Event>> events) {
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::move(events));
    auto h_src = dag.add_source<Event>(src);
    auto op = make_op<int>(
        p,
        [](const Event& e) -> std::int64_t { return e.key; },
        [](const PatternMatch<Event>& m) -> int {
            int sum = 0;
            for (const auto& [_, vec] : m) {
                for (const auto& e : vec)
                    sum += e.payload;
            }
            return sum;
        });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);
    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected();
}

TEST(CepV2, NotNextKillsPartialOnMatchingEvent) {
    // begin(a, kind=1) not_next(bad, kind=99) followed_by(b, kind=3)
    // - when the immediate next event after `a` matches `bad`,
    // the partial dies; subsequent `b` doesn't resurrect it.
    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .not_next("bad")
                 .where([](const Event& e) { return e.kind == 99; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; });

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 1, 100}},   // a
                                  Record<Event>{Event{0, 99, 999}},  // matches not_next -> die
                                  Record<Event>{Event{0, 3, 300}},   // b - no partial to advance
                              });
    EXPECT_TRUE(out.empty()) << "not_next match should have killed the partial";
}

TEST(CepV2, NotNextSurvivesOnNonMatchingEvent) {
    // Same pattern; immediate next event after `a` doesn't match `bad`,
    // so the partial survives. The b-step then completes.
    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .not_next("bad")
                 .where([](const Event& e) { return e.kind == 99; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; });

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 1, 100}},  // a
                                  Record<Event>{Event{0, 0, 5}},    // ignored by not_next
                                  Record<Event>{Event{0, 3, 300}},  // b
                              });
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out.front(), 100 + 300);
}

TEST(CepV2, OneOrMoreCapturesMultipleEvents) {
    // begin(start, kind=10) next(loop, kind=1).one_or_more()
    // followed_by(b, kind=3). One START event means a single partial
    // spawns; the loop step captures the kind=1 events until a
    // non-match, then b completes the pattern.
    auto p = Pattern<Event>::begin("start")
                 .where([](const Event& e) { return e.kind == 10; })
                 .next("loop")
                 .where([](const Event& e) { return e.kind == 1; })
                 .one_or_more()
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; });

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 10, 5}},   // start
                                  Record<Event>{Event{0, 1, 10}},   // loop #1
                                  Record<Event>{Event{0, 1, 20}},   // loop #2
                                  Record<Event>{Event{0, 1, 30}},   // loop #3
                                  Record<Event>{Event{0, 3, 300}},  // b
                              });
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out.front(), 5 + 10 + 20 + 30 + 300);
}

TEST(CepV2, TimesExactlyTakesNAndStops) {
    // begin(start, kind=10) next(loop, kind=1).times(2)
    // followed_by(b, kind=3). Single partial; the loop step captures
    // exactly 2 kind=1 events then advances on the third (which the
    // followed_by step ignores while waiting for kind=3).
    auto p = Pattern<Event>::begin("start")
                 .where([](const Event& e) { return e.kind == 10; })
                 .next("loop")
                 .where([](const Event& e) { return e.kind == 1; })
                 .times(2)
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; });

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 10, 5}},   // start
                                  Record<Event>{Event{0, 1, 10}},   // loop #1
                                  Record<Event>{Event{0, 1, 20}},   // loop #2 - max hit
                                  Record<Event>{Event{0, 1, 30}},   // skipped by b followed_by
                                  Record<Event>{Event{0, 3, 300}},  // b
                              });
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out.front(), 5 + 10 + 20 + 300);
}

TEST(CepV2, OptionalStepAdvancesEvenWithoutMatch) {
    // begin(a).optional() followed_by(b) - `a` may or may not occur.
    // First event is b: a is skipped, pattern completes immediately.
    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .optional()
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; });

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 3, 300}},  // b directly
                              });
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out.front(), 300);
}

TEST(CepV2, IterativePredicateSeesPriorMatches) {
    // begin(a) followed_by(b, where b.payload > last(a).payload)
    // - iterative predicate uses the partial match view.
    auto p =
        Pattern<Event>::begin("a")
            .where([](const Event& e) { return e.kind == 1; })
            .followed_by("b")
            .where(IterativePredicate<Event>{[](const Event& e, const PatternMatch<Event>& match) {
                if (e.kind != 3)
                    return false;
                auto it = match.find("a");
                if (it == match.end() || it->second.empty())
                    return false;
                return e.payload > it->second.back().payload;
            }});

    // b at payload 50 < a at payload 100: doesn't match.
    // b at payload 200 > a at payload 100: matches.
    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 1, 100}},  // a
                                  Record<Event>{Event{0, 3, 50}},   // b too small
                                  Record<Event>{Event{0, 3, 200}},  // b big enough
                              });
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out.front(), 100 + 200);
}

TEST(CepV2, TimedOutTagReceivesExpiredPartials) {
    // begin(a, kind=1) followed_by(b, kind=3).within(50ms)
    // - `a` arrives, no `b` follows; watermark advances past
    // start_ts+within, partial is evicted and the timed-out tag
    // receives the partial's PatternMatch.
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 11}, EventTime{0}},
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; })
                 .within(50ms);

    auto op = make_op<int>(
        p,
        [](const Event& e) -> std::int64_t { return e.key; },
        [](const PatternMatch<Event>&) -> int { return 0; });
    OutputTag<int> timed_out_tag("timed_out");
    op->template with_timed_out_output<int>(timed_out_tag, [](const PatternMatch<Event>& m) -> int {
        auto it = m.find("a");
        return it == m.end() || it->second.empty() ? 0 : it->second.front().payload;
    });

    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto h_side = dag.template side_output<int>(h_op, timed_out_tag);
    auto main_sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, main_sink);
    auto timed_out_sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_side, timed_out_sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_TRUE(main_sink->collected().empty());
    ASSERT_EQ(timed_out_sink->collected().size(), 1u);
    EXPECT_EQ(timed_out_sink->collected().front(), 11);
}

TEST(CepV2, LazyQuantifierAdvancesAtMinWhenNextStepCouldAlsoMatch) {
    // begin(start, kind=10) next(loop, kind=1).one_or_more().lazy()
    // followed_by(b, kind=1). The loop step matches kind=1 events;
    // so does the b step. With lazy, the loop captures exactly its
    // min (1) and immediately advances on the next kind=1, which b
    // also matches. Greedy would keep capturing - see the contrast
    // test below.
    auto p = Pattern<Event>::begin("start")
                 .where([](const Event& e) { return e.kind == 10; })
                 .next("loop")
                 .where([](const Event& e) { return e.kind == 1; })
                 .one_or_more()
                 .lazy()
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 1; });

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 10, 5}},  // start
                                  Record<Event>{Event{0, 1, 10}},  // loop #1 (captured)
                                  Record<Event>{Event{0, 1, 20}},  // b (lazy advance)
                                  Record<Event>{Event{0, 1, 30}},  // ignored (pattern complete)
                              });
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out.front(), 5 + 10 + 20);
}

TEST(CepV2, FlatSelectEmitsZeroOrMoreRecordsPerMatch) {
    // FlatSelect returns a vector; the operator emits each element.
    // Test mixes empty (zero emits) and multi-element results from
    // a single pattern.
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 10}},
        Record<Event>{Event{0, 2, 20}},
        Record<Event>{Event{0, 1, 30}},
        Record<Event>{Event{0, 2, 40}},
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .next("b")
                 .where([](const Event& e) { return e.kind == 2; });

    // Flat selector: emit 0 records when payload sum is odd, 2 records
    // when even. (10+20=30 even → 2 emits; 30+40=70 even → 2 emits;
    // total = 4.)
    typename CepOperator<Event, int>::FlatSelectFn flat_fn =
        [](const PatternMatch<Event>& m) -> std::vector<int> {
        const int sum = m.at("a").front().payload + m.at("b").front().payload;
        if (sum % 2 != 0)
            return {};
        return {sum, sum + 1};
    };
    auto op = std::make_shared<CepOperator<Event, int>>(
        p, event_codec(), [](const Event&) -> std::int64_t { return 0; }, flat_fn, "cep_flat_test");
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    ASSERT_EQ(out.size(), 4u);
    // (10+20)=30,31 and (30+40)=70,71 - but only when payload-sum even.
    EXPECT_EQ(out[0], 30);
    EXPECT_EQ(out[1], 31);
    EXPECT_EQ(out[2], 70);
    EXPECT_EQ(out[3], 71);
}
// Skip-strategy fixtures: a stream where the same predicate matches
// multiple events so several partials co-exist, letting us probe
// which survive after a completion.
namespace {

std::vector<int> run_skip_test(SkipStrategy strategy) {
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 1}, EventTime{100}},
        Record<Event>{Event{0, 1, 2}, EventTime{200}},
        Record<Event>{Event{0, 1, 3}, EventTime{300}},
        Record<Event>{Event{0, 2, 99}, EventTime{400}},  // completes earlier partials
    });
    auto h_src = dag.add_source<Event>(src);

    // begin(a, kind=1) followed_by(b, kind=2). Each kind=1 event
    // spawns a new partial. At the kind=2 event, every surviving
    // partial completes simultaneously (NoSkip) - 3 emissions.
    // Other strategies prune some.
    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 2; })
                 .after_match_skip(std::move(strategy));

    auto op = make_op<int>(
        p,
        [](const Event&) -> std::int64_t { return 0; },
        [](const PatternMatch<Event>& m) -> int {
            return m.at("a").front().payload * 100 + m.at("b").front().payload;
        });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(CepV2Skip, NoSkipEmitsEveryMatch) {
    // Default. All three partials (started at events 1, 2, 3)
    // complete at event 99 → three emissions.
    auto out = run_skip_test(SkipStrategy::no_skip());
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0], 199);  // a=1, b=99
    EXPECT_EQ(out[1], 299);  // a=2, b=99
    EXPECT_EQ(out[2], 399);  // a=3, b=99
}

TEST(CepV2Skip, SkipPastLastEventLeavesOnlyOneMatch) {
    // SkipPastLastEvent: the FIRST partial to complete (at event 99)
    // evicts all others. Order of completion is insertion order in
    // next_partials - partials walked oldest-first, so the partial
    // started at event 1 completes first. Its match's last event ts
    // is 400; all surviving partials (start_ts 200, 300) have
    // start_ts <= 400 → evicted. Only the first match survives.
    auto out = run_skip_test(SkipStrategy::skip_past_last_event());
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 199);
}

TEST(CepV2Skip, SkipToNextDropsOnlySameStartPartials) {
    // SkipToNext: drop partials whose start_ts equals the completed
    // partial's start_ts. Each of our partials has a distinct
    // start_ts (100, 200, 300) so no other partial is evicted by
    // the first completion. All three matches emit normally.
    auto out = run_skip_test(SkipStrategy::skip_to_next());
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0], 199);
    EXPECT_EQ(out[1], 299);
    EXPECT_EQ(out[2], 399);
}

TEST(CepV2Skip, SkipToFirstDropsEarlierStartPartials) {
    // SkipToFirst("a"): on first completion, find ts of first event
    // for step "a" in the match → 100. Drop partials with start_ts <
    // 100 → none (all start at >= 100). Then partial started at 200
    // completes; its "a" first event is at 200; survivors with start
    // < 200 already gone. The 300 one is still around. Net: still
    // 3 emissions because the cutoff is always the completed
    // partial's own start.
    auto out = run_skip_test(SkipStrategy::skip_to_first("a"));
    ASSERT_EQ(out.size(), 3u);
}

TEST(CepV2Skip, SkipToLastDropsPartialsBeforeLastNamedEvent) {
    // SkipToLast("a"): same single-capture step has last==first ts.
    // Same as SkipToFirst for this pattern.
    auto out = run_skip_test(SkipStrategy::skip_to_last("a"));
    ASSERT_EQ(out.size(), 3u);
}

TEST(CepV2, TrailingNotFollowedBySucceedsWhenWithinClosesWithoutMatch) {
    // begin(a) followed_by(b) not_followed_by("bad", kind=99) within(50ms)
    // - after b matches, the partial waits for the within-window
    // (50ms) to close. If no kind=99 event arrives, the partial
    // completes as a success.
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 10}, EventTime{0}},
        Record<Event>{Event{0, 3, 30}, EventTime{20}},
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; })
                 .not_followed_by("bad")
                 .where([](const Event& e) { return e.kind == 99; })
                 .within(50ms);

    auto op = make_op<int>(
        p,
        [](const Event& e) -> std::int64_t { return e.key; },
        [](const PatternMatch<Event>& m) -> int {
            return m.at("a").front().payload + m.at("b").front().payload;
        });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // End-of-stream synthesises Watermark::max(); the trailing
    // not_followed_by deadline (50ms past start_ts=0) is long since
    // crossed, so the partial completes successfully.
    ASSERT_EQ(sink->collected().size(), 1u);
    EXPECT_EQ(sink->collected().front(), 10 + 30);
}

TEST(CepV2, TrailingNotFollowedByKilledByMatchingEventInWindow) {
    // Same pattern; but a kind=99 event arrives within the window
    // and the partial dies before the deadline.
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{0, 1, 10}, EventTime{0}},
        Record<Event>{Event{0, 3, 30}, EventTime{20}},
        Record<Event>{Event{0, 99, 999}, EventTime{40}},  // violates the trailing negative
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; })
                 .not_followed_by("bad")
                 .where([](const Event& e) { return e.kind == 99; })
                 .within(50ms);

    auto op = make_op<int>(
        p,
        [](const Event& e) -> std::int64_t { return e.key; },
        [](const PatternMatch<Event>& m) -> int {
            return m.at("a").front().payload + m.at("b").front().payload;
        });
    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_TRUE(sink->collected().empty())
        << "kind=99 within the window should have killed the partial";
}

TEST(CepV2, TrailingNotFollowedByRejectedWithoutWithin) {
    // Trailing not_followed_by REQUIRES within() - otherwise the
    // partial would wait forever. Validation throws at operator
    // construction.
    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .not_followed_by("bad")
                 .where([](const Event& e) { return e.kind == 99; });
    // No .within(). Construction throws via Pattern::validate() ->
    // CepOperator ctor.
    EXPECT_THROW(make_op<int>(
                     p,
                     [](const Event&) -> std::int64_t { return 0; },
                     [](const PatternMatch<Event>&) -> int { return 0; }),
                 std::runtime_error);
}

TEST(CepV2, FlatTimedOutEmitsZeroOrMoreRecordsPerEvictedPartial) {
    // begin(a) followed_by(b).within(50ms). One a-only partial sits
    // pending, never sees b, and is evicted. The flat timed-out
    // selector returns 0 records when the captured 'a' payload is
    // less than 10, else 2 records. Tests both branches via two
    // separate partials (different keys so they stay independent).
    Dag dag;
    auto src = std::make_shared<VectorSource<Event>>(std::vector<Record<Event>>{
        Record<Event>{Event{1, 1, 5}, EventTime{0}},   // small - flat returns 0
        Record<Event>{Event{2, 1, 50}, EventTime{0}},  // big - flat returns 2
    });
    auto h_src = dag.add_source<Event>(src);

    auto p = Pattern<Event>::begin("a")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 3; })
                 .within(50ms);

    auto op = make_op<int>(
        p,
        [](const Event& e) -> std::int64_t { return e.key; },
        [](const PatternMatch<Event>&) -> int { return 0; });
    OutputTag<int> timed_out_tag("timed_out");
    op->template with_timed_out_flat_output<int>(
        timed_out_tag, [](const PatternMatch<Event>& m) -> std::vector<int> {
            auto it = m.find("a");
            if (it == m.end() || it->second.empty())
                return {};
            const int pay = it->second.front().payload;
            if (pay < 10)
                return {};
            return {pay, pay + 1};
        });

    auto h_op = dag.add_operator<Event, int>(h_src, op);
    auto h_side = dag.template side_output<int>(h_op, timed_out_tag);
    auto main_sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, main_sink);
    auto timed_out_sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_side, timed_out_sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_TRUE(main_sink->collected().empty());
    auto out = timed_out_sink->collected();
    std::sort(out.begin(), out.end());
    // payload=5 partial → 0 emits; payload=50 partial → 2 emits (50, 51).
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], 50);
    EXPECT_EQ(out[1], 51);
}

TEST(CepV2, FollowedByGroupPatternFlattensInline) {
    // Compose a sub-pattern and append it via followed_by(sub_pattern).
    // Behaviour should match inlining its steps with FollowedBy as
    // the entry edge.
    auto sub = Pattern<Event>::begin("mid")
                   .where([](const Event& e) { return e.kind == 2; })
                   .next("end")
                   .where([](const Event& e) { return e.kind == 3; });

    auto p = Pattern<Event>::begin("start")
                 .where([](const Event& e) { return e.kind == 1; })
                 .followed_by(sub);

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 1, 10}},  // start
                                  Record<Event>{Event{0, 0, 0}},   // skipped by followed_by
                                  Record<Event>{Event{0, 2, 20}},  // mid
                                  Record<Event>{Event{0, 3, 30}},  // end
                              });
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out.front(), 10 + 20 + 30);
}

TEST(CepV2, GreedyContrastsLazyOnSamePatternShape) {
    // Same shape but greedy (no .lazy()): the loop step now captures
    // every kind=1 event because the same predicate matches. It only
    // advances when a non-matching event arrives - but kind=1 always
    // matches. We never advance past loop, so b never fires; no match.
    auto p = Pattern<Event>::begin("start")
                 .where([](const Event& e) { return e.kind == 10; })
                 .next("loop")
                 .where([](const Event& e) { return e.kind == 1; })
                 .one_or_more()
                 .followed_by("b")
                 .where([](const Event& e) { return e.kind == 1; });

    auto out = run_cep_events(p,
                              {
                                  Record<Event>{Event{0, 10, 5}},
                                  Record<Event>{Event{0, 1, 10}},
                                  Record<Event>{Event{0, 1, 20}},
                                  Record<Event>{Event{0, 1, 30}},
                              });
    EXPECT_TRUE(out.empty())
        << "greedy loop should never advance to b when b's predicate also matches loop's events";
}
