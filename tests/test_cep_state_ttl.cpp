// Retention on CEP partial matches.
//
// cep_operator.hpp and pattern.hpp both said it plainly: "without
// within(), partials live indefinitely". A pattern whose first step
// matches often and whose later steps rarely complete accumulates one
// partial per unmatched start, for ever - the same unbounded-state shape
// the SQL gate refuses for a windowless GROUP BY, in a surface the gate
// cannot see.
//
// `Pattern::state_ttl()` bounds it. The distinction from `within()` is the
// point of most of these tests:
//
//   within()    semantic. A match spanning more than the bound is not a
//               match, so it binds at MATCH time too - a too-late record
//               expires the partial before any predicate sees it.
//   state_ttl() resource. Prunes on watermark advance only, so it never
//               changes what a match IS, and results cannot depend on
//               where watermarks happen to fall between records.
//
// A TTL can nonetheless SUPPRESS a match that would have completed. That
// is the cost of bounding the state, and it is why an evicted partial
// routes to the timed-out side output exactly as a within()-evicted one
// does: the loss is visible rather than silent.

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
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace clink;
using namespace clink::cep;
using namespace std::chrono_literals;

struct Ev {
    std::int64_t key{0};
    int kind{0};
    int payload{0};
    bool operator==(const Ev& o) const noexcept {
        return key == o.key && kind == o.kind && payload == o.payload;
    }
};

constexpr OperatorId kOp{31};

// A two-step pattern: kind==1 starts, kind==2 completes. Feeding starts
// without completions is the shape that leaks partials.
Pattern<Ev> start_then_end() {
    return Pattern<Ev>::begin("a")
        .where([](const Ev& e) { return e.kind == 1; })
        .next("b")
        .where([](const Ev& e) { return e.kind == 2; });
}

// Drive the operator directly so watermarks can be interleaved with
// records at exact points - a Dag run would not give that control.
class CepProbe {
public:
    explicit CepProbe(Pattern<Ev> p)
        : backend_(std::make_shared<InMemoryStateBackend>()),
          op_(std::make_shared<CepOperator<Ev, int>>(
              std::move(p),
              trivial_codec<Ev>(),
              [](const Ev& e) { return e.key; },
              [](const PatternMatch<Ev>& m) { return m.at("a").front().payload; },
              "cep_ttl_test")) {
        op_->set_id(kOp);
        rctx_ = std::make_unique<RuntimeContext>(kOp, "cep", backend_.get(), /*metrics=*/nullptr);
        op_->attach_runtime(rctx_.get());
        op_->open();
    }

    void feed(std::int64_t key, int kind, int payload, std::int64_t ts) {
        Batch<Ev> b;
        b.push(Record<Ev>{Ev{key, kind, payload}, EventTime::from_millis(ts)});
        op_->process(StreamElement<Ev>::data(std::move(b)), emitter_);
    }

    void watermark(std::int64_t ms) {
        op_->process(StreamElement<Ev>::watermark(Watermark{EventTime::from_millis(ms)}), emitter_);
    }

    // Partial-match rows resident in the backend. The number that grows
    // without bound when nothing prunes.
    std::size_t resident() const {
        std::size_t n = 0;
        backend_->scan(kOp, [&](std::string_view, std::string_view) { ++n; });
        return n;
    }

    const std::vector<int>& emitted() const { return emitted_; }

private:
    std::shared_ptr<InMemoryStateBackend> backend_;
    std::shared_ptr<CepOperator<Ev, int>> op_;
    std::unique_ptr<RuntimeContext> rctx_;
    std::vector<int> emitted_;
    Emitter<int> emitter_{Emitter<int>::Forward{[this](StreamElement<int> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data().records()) {
                emitted_.push_back(r.value());
            }
        }
        return true;
    }}};
};

// --- the leak ---------------------------------------------------------------

TEST(CepStateTtl, WithoutABoundPartialsAccumulateForEver) {
    // The behaviour the header documented and nothing bounded. Pinned so
    // the fix below is measured against a real baseline rather than an
    // assumed one.
    CepProbe p(start_then_end());
    for (int i = 0; i < 20; ++i) {
        p.feed(i, /*kind=*/1, i, 1000 + i);  // a start that never completes
    }
    p.watermark(1'000'000);
    EXPECT_EQ(p.resident(), 20U)
        << "a pattern with no within() and no state_ttl retains every partial, by design";
}

// --- state_ttl bounds it ----------------------------------------------------

TEST(CepStateTtl, StateTtlEvictsStalePartialsOnWatermarkAdvance) {
    auto pat = start_then_end();
    pat.state_ttl(1000ms);
    CepProbe p(pat);

    for (int i = 0; i < 20; ++i) {
        p.feed(i, 1, i, 10'000);
    }
    p.watermark(10'500);  // inside the TTL
    EXPECT_EQ(p.resident(), 20U) << "partials were evicted while still inside the retention";

    p.watermark(12'000);  // past 10'000 + 1'000
    EXPECT_EQ(p.resident(), 0U) << "stale partials were not released";
}

TEST(CepStateTtl, APartialThatCompletesInsideTheTtlStillMatches) {
    auto pat = start_then_end();
    pat.state_ttl(1000ms);
    CepProbe p(pat);

    p.feed(1, 1, 42, 10'000);
    p.watermark(10'500);
    p.feed(1, 2, 0, 10'600);  // completes, well inside the TTL
    ASSERT_EQ(p.emitted().size(), 1U) << "retention suppressed a match it should not have";
    EXPECT_EQ(p.emitted().front(), 42);
}

TEST(CepStateTtl, StateTtlDoesNotBindAtMatchTimeTheWayWithinDoes) {
    // The load-bearing difference. within() is semantic and expires a
    // partial the moment a too-late record arrives, whatever the
    // watermark. state_ttl is a resource bound and prunes only on
    // watermark advance, so a completion that arrives before the next
    // watermark still matches even though it is past the TTL span.
    //
    // If this ever changes, results would start depending on where
    // watermarks happen to fall between records - which is exactly what a
    // resource bound must not do.
    auto ttl_pat = start_then_end();
    ttl_pat.state_ttl(1000ms);
    CepProbe with_ttl(ttl_pat);
    with_ttl.feed(1, 1, 42, 10'000);
    with_ttl.feed(1, 2, 0, 99'000);  // far past the TTL span, no watermark between
    EXPECT_EQ(with_ttl.emitted().size(), 1U)
        << "state_ttl bound at match time; it must only prune on watermark advance";

    auto within_pat = start_then_end();
    within_pat.within(1000ms);
    CepProbe with_within(within_pat);
    with_within.feed(1, 1, 42, 10'000);
    with_within.feed(1, 2, 0, 99'000);
    EXPECT_TRUE(with_within.emitted().empty())
        << "within() must bind at match time - a match spanning more than the bound is not a match";
}

TEST(CepStateTtl, TheTighterOfWithinAndStateTtlWins) {
    auto pat = start_then_end();
    pat.within(10'000ms).state_ttl(500ms);
    EXPECT_EQ(pat.eviction_bound().value(), 500ms);

    auto other = start_then_end();
    other.within(300ms).state_ttl(5000ms);
    EXPECT_EQ(other.eviction_bound().value(), 300ms);
}

TEST(CepStateTtl, NeitherBoundMeansNoEvictionBound) {
    EXPECT_FALSE(start_then_end().eviction_bound().has_value());
}

TEST(CepStateTtl, EitherBoundAloneIsTheEffectiveOne) {
    auto only_within = start_then_end();
    only_within.within(700ms);
    EXPECT_EQ(only_within.eviction_bound().value(), 700ms);

    auto only_ttl = start_then_end();
    only_ttl.state_ttl(900ms);
    EXPECT_EQ(only_ttl.eviction_bound().value(), 900ms);
}

TEST(CepStateTtl, EvictionIsPerKeyNotGlobal) {
    auto pat = start_then_end();
    pat.state_ttl(1000ms);
    CepProbe p(pat);

    p.feed(1, 1, 1, 10'000);  // deadline 11'000
    p.feed(2, 1, 2, 10'800);  // deadline 11'800
    p.watermark(11'200);

    // Key 1's partial is gone; key 2's is not, so key 2 can still complete.
    p.feed(2, 2, 0, 11'300);
    EXPECT_EQ(p.emitted().size(), 1U) << "a live key's partial was evicted with an expired one";

    p.feed(1, 2, 0, 11'400);  // key 1's start is gone, so nothing completes
    EXPECT_EQ(p.emitted().size(), 1U) << "an evicted partial still completed";
}

TEST(CepStateTtl, AnEvictedPartialIsVisibleThroughTheTimedOutSideOutput) {
    // A TTL can suppress a match that would have completed. That must not
    // be silent: a partial dropped by retention routes to the timed-out
    // tag exactly as a within()-evicted one does, so a user can see what
    // retention cost them.
    Dag dag;
    auto src = std::make_shared<VectorSource<Ev>>(
        std::vector<Record<Ev>>{Record<Ev>{Ev{0, 1, 77}, EventTime{0}}});
    auto h_src = dag.add_source<Ev>(src);

    auto pat = start_then_end();
    pat.state_ttl(50ms);  // no within(); retention alone bounds the partial

    auto op = std::make_shared<CepOperator<Ev, int>>(
        pat,
        trivial_codec<Ev>(),
        [](const Ev& e) { return e.key; },
        [](const PatternMatch<Ev>&) { return 0; },
        "cep_ttl_sideout");
    OutputTag<int> timed_out_tag("timed_out");
    op->template with_timed_out_output<int>(timed_out_tag, [](const PatternMatch<Ev>& m) -> int {
        auto it = m.find("a");
        return it == m.end() || it->second.empty() ? 0 : it->second.front().payload;
    });

    auto h_op = dag.add_operator<Ev, int>(h_src, op);
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
    ASSERT_EQ(timed_out_sink->collected().size(), 1U)
        << "a partial dropped by retention vanished without reaching the timed-out output";
    EXPECT_EQ(timed_out_sink->collected().front(), 77);
}

}  // namespace
