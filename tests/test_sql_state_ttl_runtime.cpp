// `state_ttl` enforced by the running GROUP BY operator.
//
// The planner-side tests (test_sql_bounded_state.cpp) prove the option
// reaches the operator's params. That is necessary and not sufficient: the
// claim a user cares about is that memory is actually reclaimed. These
// tests build the real aggregate_row operator through the registry, drive
// records and watermarks into it, and assert on the group count it is
// holding.
//
// The distinction being tested is between hiding and releasing. An
// aggregate that stops REPORTING an expired group but still holds its
// accumulator has not bounded anything.

#include <algorithm>
#include <any>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/state_ttl.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace clink;
using namespace clink::sql;

void ensure_installed() {
    static const bool once = [] {
        // Built-ins first: install() registers Row-channel operators, and
        // register_operator<In,Out> throws unless register_type<Row> has
        // already run.
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
        return true;
    }();
    (void)once;
}

// Build aggregate_row with the given params, exactly as a deployed job
// would: through the registry, from a param map.
std::shared_ptr<Operator<Row, Row>> build_agg(const std::map<std::string, std::string>& extra) {
    ensure_installed();
    const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
        "aggregate_row", std::string{kChannelRow}, std::string{kChannelRow});
    EXPECT_NE(factory, nullptr);
    cluster::OperatorBuildContext octx;
    octx.params["group_keys"] = "k";
    octx.params["aggregates"] = R"([{"name":"s","fn":"sum","input_column":"v","distinct":false}])";
    for (const auto& [key, value] : extra) {
        octx.params[key] = value;
    }
    return std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));
}

Row row_of(std::int64_t k, std::int64_t v) {
    Row r;
    r.values["k"] = clink::config::JsonValue{static_cast<double>(k)};
    r.values["v"] = clink::config::JsonValue{static_cast<double>(v)};
    return r;
}

// Count how many distinct groups the operator still reports on. An
// unbounded GROUP BY emits the running total per input record, so probing
// a key and seeing an aggregate that STARTS FROM ZERO means its
// accumulator was released.
class Probe {
public:
    Probe() : backend_(std::make_shared<InMemoryStateBackend>()) {}

    void start(const std::map<std::string, std::string>& params) {
        op_ = build_agg(params);
        op_->set_id(OperatorId{99});
        rctx_ = std::make_unique<RuntimeContext>(
            OperatorId{99}, "agg", backend_.get(), /*metrics=*/nullptr);
        op_->attach_runtime(rctx_.get());
        op_->open();
    }

    void feed(std::int64_t k, std::int64_t v) {
        Batch<Row> b;
        b.emplace(row_of(k, v));
        op_->process(StreamElement<Row>::data(std::move(b)), emitter_);
    }

    // Feed one row carrying an explicit record event time, as stamped by
    // the assigner in a real chain.
    void feed_record(Row row, std::int64_t ts_ms) {
        Batch<Row> b;
        b.emplace(std::move(row), EventTime::from_millis(ts_ms));
        op_->process(StreamElement<Row>::data(std::move(b)), emitter_);
    }

    void watermark(std::int64_t ms) {
        op_->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(ms)}),
                     emitter_);
    }

    // The running total the operator reports for `k` on the next record.
    // A released group restarts from zero, so this is how a test tells
    // "still holding the accumulator" from "released it".
    std::int64_t running_total_after_feeding(std::int64_t k, std::int64_t v) {
        collected_.clear();
        feed(k, v);
        if (collected_.empty()) {
            return -1;
        }
        const auto& last = collected_.back();
        const auto it = last.values.find("s");
        return it == last.values.end() ? -1 : static_cast<std::int64_t>(it->second.as_number());
    }

private:
    std::shared_ptr<StateBackend> backend_;
    std::shared_ptr<Operator<Row, Row>> op_;
    std::unique_ptr<RuntimeContext> rctx_;
    std::vector<Row> collected_;
    // Emitter's Forward mode is the cheapest way to observe emissions
    // without standing up a Dag: the operator emits, this records.
    Emitter<Row> emitter_{Emitter<Row>::Forward{[this](StreamElement<Row> e) {
        if (e.is_data()) {
            for (const auto& rec : e.as_data().records()) {
                collected_.push_back(rec.value());
            }
        }
        return true;
    }}};
};

// --- the property that matters ----------------------------------------------

TEST(SqlStateTtlRuntime, WithoutRetentionAGroupAccumulatesForEver) {
    // The baseline the TTL is measured against, and the behaviour a job
    // that never asked for retention must keep.
    Probe p;
    p.start({});
    p.feed(1, 10);
    p.watermark(1'000'000);  // a long time passes
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 15)
        << "the group's accumulator was dropped despite no retention being configured";
}

TEST(SqlStateTtlRuntime, AnExpiredGroupsAccumulatorIsReleasedNotJustHidden) {
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    p.feed(1, 10);
    // Past the deadline (last touch 10'000 + 1'000).
    p.watermark(20'000);
    // A released group restarts from zero. Still holding it would give 15.
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 5)
        << "the expired group's accumulator survived, so state_ttl bounded nothing";
}

TEST(SqlStateTtlRuntime, ALiveGroupIsNotEvicted) {
    Probe p;
    p.start({{"state_ttl_ms", "10000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    p.feed(1, 10);
    p.watermark(15'000);  // well inside the 10 s deadline
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 15) << "a live group was evicted early";
}

TEST(SqlStateTtlRuntime, EachWriteRefreshesTheDeadline) {
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    p.feed(1, 10);
    // Touch again just before the deadline: the group's clock restarts.
    p.watermark(10'900);
    p.feed(1, 10);
    // Past the ORIGINAL deadline but inside the refreshed one.
    p.watermark(11'500);
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 25)
        << "a group that kept receiving data was evicted on its first deadline";
}

TEST(SqlStateTtlRuntime, OnlyExpiredGroupsAreEvicted) {
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    p.feed(1, 10);  // deadline 11'000
    p.watermark(10'500);
    p.feed(2, 100);       // deadline 11'500
    p.watermark(11'200);  // key 1 is dead, key 2 is not

    EXPECT_EQ(p.running_total_after_feeding(1, 5), 5) << "key 1 should have been released";
    EXPECT_EQ(p.running_total_after_feeding(2, 1), 101) << "key 2 was evicted early";
}

TEST(SqlStateTtlRuntime, NothingIsEvictedBeforeTheFirstWatermark) {
    // Event time has not started, and nothing can be judged expired
    // against a clock that has not started. A zero watermark would
    // otherwise wipe the operator's state the instant it began.
    Probe p;
    p.start({{"state_ttl_ms", "1"}, {"state_ttl_domain", "event_time"}});
    p.feed(1, 10);
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 15)
        << "state was evicted against a watermark that had not arrived";
}

TEST(SqlStateTtlRuntime, AWatermarkRegressionDoesNotResurrectAnEvictedGroup) {
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    p.feed(1, 10);
    p.watermark(20'000);
    ASSERT_EQ(p.running_total_after_feeding(1, 5), 5);  // evicted, restarted at 5

    // A regression must not move the clock back: retention would otherwise
    // depend on the order watermarks happened to arrive in.
    p.watermark(11'000);
    // The group re-created at watermark 20'000 has deadline 21'000, so it
    // is still live and keeps accumulating.
    EXPECT_EQ(p.running_total_after_feeding(1, 1), 6);
}

// --- DISTINCT ---------------------------------------------------------------

namespace {

// A generic single-input driver for an operator built from the registry.
class OpProbe {
public:
    // op_id is settable because the metrics registry is process-global and
    // keys its retention gauges by operator: a test asserting that a gauge
    // is ABSENT needs an id no earlier test has published under.
    OpProbe(const std::string& type,
            std::map<std::string, std::string> params,
            std::uint64_t op_id = 77)
        : backend_(std::make_shared<InMemoryStateBackend>()) {
        ensure_installed();
        const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
            type, std::string{kChannelRow}, std::string{kChannelRow});
        EXPECT_NE(factory, nullptr) << type;
        cluster::OperatorBuildContext octx;
        octx.params = std::move(params);
        op_ = std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));
        op_->set_id(OperatorId{op_id});
        rctx_ = std::make_unique<RuntimeContext>(
            OperatorId{op_id}, type, backend_.get(), /*metrics=*/nullptr);
        op_id_ = OperatorId{op_id};
        op_->attach_runtime(rctx_.get());
        op_->open();
    }

    // Feed one row and report whether the operator emitted anything.
    bool emits(std::int64_t k, std::int64_t v) {
        collected_.clear();
        Batch<Row> b;
        b.emplace(row_of(k, v));
        op_->process(StreamElement<Row>::data(std::move(b)), emitter_);
        return !collected_.empty();
    }

    // Same, with the record stamped at an explicit event time.
    bool emits_at(std::int64_t k, std::int64_t v, std::int64_t ts_ms) {
        collected_.clear();
        Batch<Row> b;
        b.emplace(row_of(k, v), EventTime::from_millis(ts_ms));
        op_->process(StreamElement<Row>::data(std::move(b)), emitter_);
        return !collected_.empty();
    }

    void watermark(std::int64_t ms) {
        op_->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(ms)}),
                     emitter_);
    }

    // Entries physically resident in the backend for this operator. The
    // distinction between hiding and releasing.
    std::size_t resident() const {
        std::size_t n = 0;
        backend_->scan(op_id_, [&](std::string_view, std::string_view) { ++n; });
        return n;
    }

private:
    std::shared_ptr<InMemoryStateBackend> backend_;
    OperatorId op_id_{77};
    std::shared_ptr<Operator<Row, Row>> op_;
    std::unique_ptr<RuntimeContext> rctx_;
    std::vector<Row> collected_;
    Emitter<Row> emitter_{Emitter<Row>::Forward{[this](StreamElement<Row> e) {
        if (e.is_data()) {
            for (const auto& rec : e.as_data().records()) {
                collected_.push_back(rec.value());
            }
        }
        return true;
    }}};
};

}  // namespace

TEST(SqlStateTtlRuntime, DistinctWithoutRetentionRemembersForEver) {
    OpProbe p("distinct_row", {{"columns", "k"}});
    EXPECT_TRUE(p.emits(1, 10));
    p.watermark(1'000'000);
    EXPECT_FALSE(p.emits(1, 10)) << "DISTINCT forgot a value with no retention configured";
}

TEST(SqlStateTtlRuntime, DistinctForgetsAndReleasesAnExpiredValue) {
    OpProbe p("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    EXPECT_TRUE(p.emits(1, 10));
    EXPECT_FALSE(p.emits(1, 10)) << "a duplicate inside the window was emitted";
    ASSERT_GT(p.resident(), 0U);

    p.watermark(20'000);
    // Forgotten, so the value is emitted again...
    EXPECT_TRUE(p.emits(1, 10)) << "DISTINCT did not forget the expired value";
    // ... and the sweep released it rather than merely hiding it. A
    // DISTINCT that hides without releasing still grows without bound.
    OpProbe q("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    q.watermark(10'000);
    for (int i = 0; i < 20; ++i) {
        q.emits(i, i);
    }
    ASSERT_EQ(q.resident(), 20U);
    q.watermark(100'000);
    EXPECT_EQ(q.resident(), 0U) << "expired DISTINCT values were hidden but not released";
}

TEST(SqlStateTtlRuntime, DistinctKeepsALiveValue) {
    OpProbe p("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "10000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    EXPECT_TRUE(p.emits(1, 10));
    p.watermark(15'000);  // inside the 10 s deadline
    EXPECT_FALSE(p.emits(1, 10)) << "a live DISTINCT value was forgotten early";
}

TEST(SqlStateTtlRuntime, DistinctEvictsNothingBeforeTheFirstWatermark) {
    OpProbe p("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "1"}, {"state_ttl_domain", "event_time"}});
    EXPECT_TRUE(p.emits(1, 10));
    EXPECT_FALSE(p.emits(1, 10))
        << "the value was expired against a watermark that had not arrived";
}

// The property a `state_ttl` declaration actually promises: over a stream
// whose distinct values keep turning over, state reaches a plateau instead
// of growing for as long as the job runs.
//
// Every other DISTINCT retention test here runs at a cardinality of 20 or
// 1, which is inside the sweep's per-call budget, so all of them pass
// whether or not the sweep can make progress across calls. This one runs
// well above the budget with values arriving and expiring continuously,
// which is the shape a real job has and the only shape that can tell a
// bounded sweep from one that stalls.
TEST(SqlStateTtlRuntime, DistinctStatePlateausAboveTheSweepBudget) {
    OpProbe p("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});

    constexpr int kRounds = 20;
    constexpr int kFreshPerRound = 500;  // well above the 256 sweep budget
    std::int64_t wm = 10'000;
    p.watermark(wm);

    std::size_t resident_after_warmup = 0;
    for (int round = 0; round < kRounds; ++round) {
        for (int i = 0; i < kFreshPerRound; ++i) {
            p.emits(static_cast<std::int64_t>(round) * kFreshPerRound + i, i);
        }
        // Past every previous round's deadline, so only this round's
        // values are live by the time the sweep runs. No watermark after
        // the FINAL round, so it is still live when the loop ends: a bound
        // satisfied by holding nothing is not a plateau.
        if (round + 1 < kRounds) {
            wm += 2'000;
            p.watermark(wm);
        }
        if (round == 1) {
            resident_after_warmup = p.resident();
        }
    }

    // Steady state: one round's values are live, and the round before it
    // has just expired. Anything beyond a couple of rounds' worth means
    // expired entries are accumulating faster than retention releases
    // them, which is unbounded growth with extra steps.
    const std::size_t resident = p.resident();
    EXPECT_LE(resident, static_cast<std::size_t>(3 * kFreshPerRound))
        << "DISTINCT state grew to " << resident << " entries over " << kRounds
        << " rounds (after two rounds it held " << resident_after_warmup
        << "); a declared state_ttl did not bound it";
    EXPECT_GE(resident, static_cast<std::size_t>(kFreshPerRound))
        << "the final round's values were released early, so the bound above proves nothing";
}

// The F97 contract, for DISTINCT: a value written before event time
// exists gets its full retention measured from the first watermark.
//
// The mechanism this replaced stamped such a value with `0 + ttl`, because
// KeyedState has no clock to measure from before a watermark arrives. The
// first real watermark is then years past that stamp, so the entire
// pre-clock batch - in a real job, everything DISTINCT saw before the
// source established event time - was forgotten in one go and re-emitted.
// The aggregate's equivalent is APreClockGroupGetsTheFullTtlFromTheFirstWatermark.
TEST(SqlStateTtlRuntime, APreClockDistinctValueGetsTheFullTtlFromTheFirstWatermark) {
    OpProbe p("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    EXPECT_TRUE(p.emits(1, 10)) << "the first sighting of a value must emit";

    // The clock starts here, well past `ttl` in absolute terms.
    p.watermark(10'000);
    EXPECT_FALSE(p.emits(1, 10))
        << "a value written before the first watermark was expired against a clock that had "
           "not started when it was written";

    // ... and it still expires on schedule once the clock is running.
    p.watermark(11'001);
    EXPECT_TRUE(p.emits(1, 10)) << "the pre-clock value never expired";
}

TEST(SqlStateTtlRuntime, ProcessingTimeDomainIsHonoured) {
    // With processing time selected, watermarks are irrelevant and the
    // wall clock decides. A 1 ms TTL plus a real sleep must evict.
    //
    // The last remaining sleep in the TTL tests, and deliberately kept. The
    // others were converted to TtlConfig::clock_ms, but the SQL path builds
    // its TtlConfig internally from table properties, so there is no seam to
    // inject through without adding a test-only hook to the runtime.
    //
    // It is safe in the direction that matters: the assertion is that the
    // entry HAS gone, the TTL is 1ms and the sleep is 20ms, so a slow or
    // loaded runner only oversleeps and still evicts. The flaky shape is the
    // opposite one - asserting something has NOT yet expired, where a
    // scheduler hiccup crosses the boundary and the failure reads as a TTL
    // bug. Do not copy this pattern for that case.
    Probe p;
    p.start({{"state_ttl_ms", "1"}, {"state_ttl_domain", "processing_time"}});
    p.feed(1, 10);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    // Any watermark drives the eviction sweep; its value is ignored in
    // this domain.
    p.watermark(1);
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 5) << "processing-time retention did not evict";
}

}  // namespace

// --- the co-operators: joins release expired state, end to end --------------
//
// The aggregate and distinct suites above prove the tracker's semantics for
// single-input operators. The two tracker-based CO-operators (equi_join_row,
// semi_join_row) received the same `state_ttl` params from the planner with
// NOTHING proving their eviction sweep actually runs - and their sweep is
// hand-rolled per operator (ttl_evict_ erases the in-memory maps AND the
// persisted KeyedState slots), exactly the kind of per-member wiring this
// repository has found silently missing before. SQL co-ops are not reachable
// through OperatorRegistry (no find_co_operator exists); the
// DagBuilderRegistry seam is how a test builds one, so these run the real
// operator inside a two-source Dag on a shared InMemoryStateBackend and read
// the backend AFTER the run: a released key is gone from the persisted slot,
// a hidden one is not.
//
// Determinism, by construction rather than by timing:
//   * the scripts emit a BARRIER after their rows, so the equi join's
//     flush-at-barrier persistence happens without a coordinator;
//   * the first watermark establishes event time (enrolling the pre-clock
//     keys with deadline = watermark + ttl - the F97 fix), the second
//     expires them; both are in-stream on both inputs, so the sweep has
//     provably run before run() returns, independent of cross-input
//     interleaving.
// (set_op_row is different by design: its TTL rides KeyedState's native
// TtlConfig, whose expiry, invisibility and physical cleanup are gated by
// the state-layer suites; there is no per-operator sweep to test.)

namespace {

struct ClinkTtlScriptStep {
    std::optional<Row> row;
    std::optional<std::int64_t> wm_ms;
    std::optional<std::uint64_t> barrier_id;
};

ClinkTtlScriptStep step_row(Row r) {
    return ClinkTtlScriptStep{.row = std::move(r)};
}
ClinkTtlScriptStep step_wm(std::int64_t ms) {
    return ClinkTtlScriptStep{.wm_ms = ms};
}
ClinkTtlScriptStep step_barrier(std::uint64_t id) {
    return ClinkTtlScriptStep{.barrier_id = id};
}

class ClinkTtlScriptSource final : public Source<Row> {
public:
    explicit ClinkTtlScriptSource(std::vector<ClinkTtlScriptStep> steps)
        : steps_(std::move(steps)) {}

    bool produce(Emitter<Row>& out) override {
        if (i_ >= steps_.size()) {
            return false;
        }
        const auto& s = steps_[i_++];
        if (s.row.has_value()) {
            Batch<Row> b;
            b.emplace(*s.row);
            if (!out.emit_data(std::move(b))) {
                return false;
            }
        }
        if (s.wm_ms.has_value()) {
            out.emit_watermark(Watermark{EventTime::from_millis(*s.wm_ms)});
        }
        if (s.barrier_id.has_value()) {
            out.emit_barrier(CheckpointBarrier{
                CheckpointId{*s.barrier_id}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned});
        }
        return i_ < steps_.size();
    }

    // Unbounded on purpose: the in-process runner emits Watermark::max()
    // at a BOUNDED source's end of input, which legitimately expires every
    // TTL'd key - event time has run to infinity - and would erase exactly
    // the live state the keeps-probes assert on. The script still ends
    // (produce returns false); only the max-watermark ceremony is skipped.
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return "ttl_script_source"; }

private:
    std::vector<ClinkTtlScriptStep> steps_;
    std::size_t i_{0};
};

Row join_row(std::int64_t id, const char* col, std::int64_t v) {
    Row r;
    r.values["id"] = clink::config::JsonValue{static_cast<double>(id)};
    r.values[col] = clink::config::JsonValue{static_cast<double>(v)};
    return r;
}

// Run the named co-op builder over scripted left/right inputs on a shared
// backend; return every persisted state key at the co-op's OperatorId.
struct CoOpRunResult {
    std::vector<std::string> state_keys;
    std::vector<Row> output;
};

CoOpRunResult co_op_run(const std::string& builder_name,
                        const std::map<std::string, std::string>& params,
                        std::vector<ClinkTtlScriptStep> left,
                        std::vector<ClinkTtlScriptStep> right) {
    ensure_installed();
    const auto* builder = cluster::DagBuilderRegistry::default_instance().find(builder_name);
    EXPECT_NE(builder, nullptr) << builder_name << " Dag builder not registered";
    if (builder == nullptr) {
        return {};
    }
    Dag dag;
    auto hl = dag.add_source<Row>(std::make_shared<ClinkTtlScriptSource>(std::move(left)));
    auto hr = dag.add_source<Row>(std::make_shared<ClinkTtlScriptSource>(std::move(right)));
    clink::plugin::BuildContext ctx;
    ctx.params = params;
    std::vector<std::any> upstream = {std::any{hl}, std::any{hr}};
    auto built = (*builder)(dag, upstream, ctx);
    auto h_op = std::any_cast<StageHandle<Row>>(built.main_handle);
    const OperatorId op_id{dag.runner_id_at(h_op.runner_index)};
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(h_op, sink);

    auto backend = std::make_shared<InMemoryStateBackend>();
    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    CoOpRunResult result;
    for (const auto& rec : sink->collected_records()) {
        result.output.push_back(rec.value());
    }
    auto& keys = result.state_keys;
    backend->scan(op_id,
                  [&](StateBackend::KeyView k, StateBackend::ValueView) { keys.emplace_back(k); });
    return result;
}

std::vector<std::string> co_op_state_keys_after_run(
    const std::string& builder_name,
    const std::map<std::string, std::string>& params,
    std::vector<ClinkTtlScriptStep> left,
    std::vector<ClinkTtlScriptStep> right) {
    return co_op_run(builder_name, params, std::move(left), std::move(right)).state_keys;
}

bool any_key_contains(const std::vector<std::string>& keys, const std::string& needle) {
    return std::any_of(keys.begin(), keys.end(), [&](const std::string& k) {
        return k.find(needle) != std::string::npos;
    });
}

const std::map<std::string, std::string> kEquiParams = {
    {"left_key_column", "id"},
    {"right_key_column", "id"},
    {"left_alias", "l"},
    {"right_alias", "r"},
    {"join_type", "inner"},
    {"left_columns", "id,lv"},
    {"right_columns", "id,rv"},
};

const std::map<std::string, std::string> kSetOpParams = {
    {"mode", "intersect"},
    {"all", "false"},
    {"left_columns", "id"},
    {"right_columns", "id"},
};

const std::map<std::string, std::string> kSemiParams = {
    {"left_key_column", "id"},
    {"right_key_column", "id"},
    {"anti", "0"},
    {"null_aware", "0"},
};

std::map<std::string, std::string> with_ttl(std::map<std::string, std::string> p, std::int64_t ms) {
    p[std::string{kStateTtlMsParam}] = std::to_string(ms);
    p[std::string{kStateTtlDomainParam}] = "event_time";
    return p;
}

std::vector<ClinkTtlScriptStep> script_rows_only(Row r) {
    return {step_row(std::move(r)), step_barrier(1)};
}

std::vector<ClinkTtlScriptStep> script_expiring(Row r) {
    return {step_row(std::move(r)), step_barrier(1), step_wm(0), step_wm(1'000'000)};
}

}  // namespace

TEST(SqlStateTtlRuntime, EquiJoinWithoutRetentionKeepsItsPerKeyStateOnTheBackend) {
    // The probe's vacuity guard: with no TTL, both sides' slots must be
    // visible on the backend after the run (the barrier step drives the
    // sync path's flush), or the release assertions below would pass
    // against a probe that cannot see state at all.
    const auto keys = co_op_state_keys_after_run("equi_join_row",
                                                 kEquiParams,
                                                 script_rows_only(join_row(1, "lv", 10)),
                                                 script_rows_only(join_row(1, "rv", 100)));
    EXPECT_TRUE(any_key_contains(keys, "ejL")) << "left slot not visible to the probe";
    EXPECT_TRUE(any_key_contains(keys, "ejR")) << "right slot not visible to the probe";
}

TEST(SqlStateTtlRuntime, EquiJoinReleasesExpiredStateFromTheBackend) {
    // One row each side, a barrier so the sync path's flush persists both
    // slots, a first watermark to establish event time (which enrols the
    // pre-clock keys with deadline = watermark + ttl - the F97 fix), then a
    // watermark far past that deadline. Every step is in-stream on both
    // inputs, so the eviction sweep has provably run by the time run()
    // returns: the persisted slots AND the deadline bookkeeping must be
    // gone - released, not hidden.
    const auto keys = co_op_state_keys_after_run("equi_join_row",
                                                 with_ttl(kEquiParams, 1000),
                                                 script_expiring(join_row(1, "lv", 10)),
                                                 script_expiring(join_row(1, "rv", 100)));
    EXPECT_FALSE(any_key_contains(keys, "ejL"))
        << "expired left-side state still persisted: hidden, not released";
    EXPECT_FALSE(any_key_contains(keys, "ejR"))
        << "expired right-side state still persisted: hidden, not released";
    EXPECT_FALSE(any_key_contains(keys, "ej_ttl"))
        << "deadline bookkeeping for the expired key still persisted";
}

TEST(SqlStateTtlRuntime, ALiveKeyStillJoinsWhileTheWatermarkMovesBelowItsDeadline) {
    // The keeps-arm cannot be asserted on post-run state: the co-op runner
    // treats a closed input as +infinity, so END OF STREAM legitimately
    // expires every TTL'd key - event time has completed. The live property
    // is therefore asserted on the OUTPUT: with the watermark advanced but
    // still below the deadline (wm 500 against ttl 1000 established at wm
    // 0), a right-side row arriving after those advances must still find
    // the left row and emit the match. A sweep that confuses "watermark
    // moved" with "deadline passed" evicts the left row first and emits
    // nothing. (Sub-deadline non-eviction is also pinned tracker-level by
    // ALiveGroupIsNotEvicted and APreClockGroupGetsTheFullTtl... above.)
    const auto result = co_op_run("equi_join_row",
                                  with_ttl(kEquiParams, 1000),
                                  {step_row(join_row(1, "lv", 10)), step_wm(0), step_wm(500)},
                                  {step_wm(0), step_wm(500), step_row(join_row(1, "rv", 100))});
    bool matched = false;
    for (const auto& row : result.output) {
        const auto lv = row.values.find("l_lv");
        const auto rv = row.values.find("r_rv");
        if (lv != row.values.end() && rv != row.values.end() &&
            static_cast<std::int64_t>(lv->second.as_number()) == 10 &&
            static_cast<std::int64_t>(rv->second.as_number()) == 100) {
            matched = true;
        }
    }
    EXPECT_TRUE(matched)
        << "a live left row (deadline 1000, watermark at 500) no longer joined: the sweep "
           "evicted below the deadline";
}

TEST(SqlStateTtlRuntime, SemiJoinWithoutRetentionKeepsItsPerKeyStateOnTheBackend) {
    const auto keys = co_op_state_keys_after_run("semi_join_row",
                                                 kSemiParams,
                                                 script_rows_only(join_row(1, "lv", 10)),
                                                 script_rows_only(join_row(1, "rv", 100)));
    EXPECT_TRUE(any_key_contains(keys, "saL")) << "probe-side slot not visible to the probe";
    EXPECT_TRUE(any_key_contains(keys, "saR")) << "build-side slot not visible to the probe";
}

TEST(SqlStateTtlRuntime, SemiJoinReleasesExpiredStateFromTheBackend) {
    const auto keys = co_op_state_keys_after_run("semi_join_row",
                                                 with_ttl(kSemiParams, 1000),
                                                 script_expiring(join_row(1, "lv", 10)),
                                                 script_expiring(join_row(1, "rv", 100)));
    EXPECT_FALSE(any_key_contains(keys, "saL"))
        << "expired probe-side state still persisted: hidden, not released";
    EXPECT_FALSE(any_key_contains(keys, "saR"))
        << "expired build-side state still persisted: hidden, not released";
    EXPECT_FALSE(any_key_contains(keys, "sj_ttl"))
        << "deadline bookkeeping for the expired key still persisted";
}

// The F97 regression, pinned at the tracker's own consumer level: a group
// whose ONLY write happened before the first watermark must still expire
// once event time is established and passes its deadline. Before the fix,
// touch() before the clock dropped the key from tracking entirely, making
// every pre-first-watermark key immortal - in a real job, the whole first
// batch, silently exempt from the bound the state gate accepted.
TEST(SqlStateTtlRuntime, AGroupWrittenBeforeTheFirstWatermarkStillExpires) {
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.feed(1, 10);        // before any watermark: pre-clock
    p.watermark(10'000);  // clock starts; deadline = 10'000 + 1'000
    p.watermark(20'000);  // long past it
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 5)
        << "a group written before the first watermark never expired (F97)";
}

TEST(SqlStateTtlRuntime, APreClockGroupGetsTheFullTtlFromTheFirstWatermark) {
    // The enrolment must grant the FULL ttl measured from the first
    // watermark - not judge the key instantly expired against it.
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.feed(1, 10);
    p.watermark(10'000);  // clock starts; deadline = 11'000
    p.watermark(10'500);  // inside the deadline
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 15)
        << "a pre-clock group was expired against the watermark that established the clock";
}

// --- INTERSECT / EXCEPT ------------------------------------------------------
//
// The set operation had no runtime retention coverage at all: it was pinned
// only at the planner level (SetOperationReceivesTheRetention), which proves
// the option reaches the operator and nothing about what the operator then
// does with it. It shares its retention mechanism with DISTINCT, so it
// shared DISTINCT's defect.

TEST(SqlStateTtlRuntime, SetOpWithoutRetentionKeepsItsPerTupleStateOnTheBackend) {
    // Vacuity guard: without retention the tuple state must be visible to
    // the probe after the run, or the release assertion below would pass
    // against a probe that cannot see state at all.
    const auto keys = co_op_state_keys_after_run("set_op_row",
                                                 kSetOpParams,
                                                 script_rows_only(join_row(1, "lv", 10)),
                                                 script_rows_only(join_row(1, "rv", 100)));
    EXPECT_TRUE(any_key_contains(keys, "setop")) << "tuple slot not visible to the probe";
}

TEST(SqlStateTtlRuntime, SetOpReleasesExpiredStateFromTheBackend) {
    const auto keys = co_op_state_keys_after_run("set_op_row",
                                                 with_ttl(kSetOpParams, 1000),
                                                 script_expiring(join_row(1, "lv", 10)),
                                                 script_expiring(join_row(1, "rv", 100)));
    EXPECT_FALSE(any_key_contains(keys, "setop|"))
        << "expired tuple state still persisted: hidden, not released";
    EXPECT_FALSE(any_key_contains(keys, "setop_ttl"))
        << "the deadline slot outlived the tuples it was tracking";
}

TEST(SqlStateTtlRuntime, SetOpStatePlateausAboveTheSweepBudget) {
    // The same shape as DistinctStatePlateausAboveTheSweepBudget, over the
    // other operator that shared the budgeted-scan sweep. Tuples arrive on
    // the left only, so each one is stored (left_count > 0) and nothing is
    // drained by matching; retention is the only thing that can release
    // them.
    //
    // Watermarks AND barriers ride both inputs. A co-op's event time is the
    // minimum of its two sides, and its barriers align across both: a
    // barrier on one input only waits for a partner that never arrives, and
    // the run deadlocks rather than failing.
    constexpr int kRounds = 12;
    constexpr int kFreshPerRound = 400;  // above the 256 budget the sweep used

    std::vector<ClinkTtlScriptStep> left;
    std::vector<ClinkTtlScriptStep> right;
    std::int64_t wm = 10'000;
    left.push_back(step_wm(wm));
    right.push_back(step_wm(wm));
    for (int round = 0; round < kRounds; ++round) {
        for (int i = 0; i < kFreshPerRound; ++i) {
            left.push_back(step_row(
                join_row(static_cast<std::int64_t>(round) * kFreshPerRound + i, "lv", 10)));
        }
        left.push_back(step_barrier(round + 1));
        right.push_back(step_barrier(round + 1));
        // No watermark after the FINAL round, so its tuples are still live
        // when the run ends. Advancing past every round including the last
        // would leave the operator holding nothing, and an upper bound on
        // nothing is not a plateau.
        if (round + 1 < kRounds) {
            wm += 2'000;  // past every previous round's deadline
            left.push_back(step_wm(wm));
            right.push_back(step_wm(wm));
        }
    }

    const auto count_tuples = [](const std::vector<std::string>& keys) {
        return static_cast<std::size_t>(
            std::count_if(keys.begin(), keys.end(), [](const std::string& k) {
                return k.find("setop|") != std::string::npos;
            }));
    };

    // The control arm carries the non-vacuity, and it has to: a co-op's
    // runner treats a CLOSED input as event time +infinity, so at the end
    // of any run every TTL'd key is legitimately expired and the retention
    // arm settles at zero whatever the sweep did. Asserting a surviving
    // population post-run is therefore not available here (the same reason
    // the join keeps-arms assert on output rather than on state). What the
    // identical script without retention holds is the honest measure of
    // what retention had to release.
    const auto without =
        count_tuples(co_op_state_keys_after_run("set_op_row", kSetOpParams, left, right));
    ASSERT_GE(without, static_cast<std::size_t>(kRounds * kFreshPerRound) / 2)
        << "the control arm held only " << without
        << " tuples, so this script does not accumulate state and bounds it proves nothing";

    const auto with = count_tuples(
        co_op_state_keys_after_run("set_op_row", with_ttl(kSetOpParams, 1000), left, right));
    EXPECT_LE(with, static_cast<std::size_t>(3 * kFreshPerRound))
        << "set-operation state reached " << with << " tuples over " << kRounds
        << " rounds against " << without
        << " without retention; a declared state_ttl did not bound it";
}

// --- retention is observable -------------------------------------------------

namespace {

std::int64_t gauge_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.gauges) {
        if (n == name) {
            return v;
        }
    }
    return -1;  // absent, which is distinct from present-and-zero
}

}  // namespace

// A job that declares a state_ttl has asked for a bound; these two gauges are
// how its operator answers whether the bound is holding. They existed as
// in-process counters with no reader at all - TtlStats' header said "read by
// the operator's metrics reporting" and nothing read it, and
// StateTtlTracker::tracked() had no caller anywhere - so an operator running a
// TTL'd job had no way to tell retention that is keeping up from retention
// that has silently stopped.
TEST(SqlStateTtlRuntime, RetentionPublishesItsPopulationAndItsReleases) {
    const std::string tracked = "clink_state_ttl_tracked_keys{op_id=\"77\"}";
    const std::string expired = "clink_state_ttl_expired_total{op_id=\"77\"}";

    OpProbe p("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(10'000);
    for (int i = 0; i < 50; ++i) {
        p.emits(i, i);
    }
    p.watermark(10'500);  // advances the clock, nothing due yet
    EXPECT_EQ(gauge_value(tracked), 50)
        << "the live retention population was not published (-1 means the gauge is absent)";
    EXPECT_EQ(gauge_value(expired), 0) << "nothing was due, so nothing should have been released";

    p.watermark(20'000);  // past every deadline
    EXPECT_EQ(gauge_value(tracked), 0) << "the population did not fall as retention released";
    EXPECT_EQ(gauge_value(expired), 50) << "released keys were not counted";
}

TEST(SqlStateTtlRuntime, AJobWithoutRetentionPublishesNoRetentionGauges) {
    // Absence is the right answer for a job that declared no bound: a zero
    // would read as "retention configured and releasing nothing", which is
    // the alarm condition.
    OpProbe p("distinct_row", {{"columns", "k"}}, /*op_id=*/78);
    p.watermark(10'000);
    p.emits(1, 10);
    p.watermark(20'000);
    EXPECT_EQ(gauge_value("clink_state_ttl_tracked_keys{op_id=\"78\"}"), -1)
        << "a job with no state_ttl published a retention gauge";
}

// --- retention versus records ahead of the watermark --------------------------
//
// The QUAL-05 defect (followups item 70). Under partition skew - a
// catch-up burst, a recovery replay, one lagging partition - a fast input
// delivers records whose event time is far AHEAD of the aligned
// min-watermark. Stamping their deadline watermark + ttl put the deadline
// BEFORE the record's own timestamp, so the group evicted while the slow
// input's exactly-on-time records were still approaching it; they then
// re-opened it from zero and the upsert sink overwrote the correct row.
// Measured: 13,868 of 53,550 events under-counted on a fault-free
// two-partition backlog read.

namespace {

// Feed one row stamped with an explicit event time, the way records
// arrive from a real source chain (the assigner stamps Record::event_time
// and it rides the wire's event_time column).
void feed_at(Probe& p, std::int64_t k, std::int64_t v, std::int64_t ts_ms) {
    p.feed_record(row_of(k, v), ts_ms);
}

}  // namespace

TEST(SqlStateTtlRuntime, AGroupAheadOfTheWatermarkOutlivesTheWatermarkPassingItsStamp) {
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(78'000);
    // A fast input's record: event time 150s while the aligned watermark
    // is 78s. The old stamp gave it deadline 79s - before its own ts.
    feed_at(p, 1, 10, 150'000);
    // The slow input's watermark advances to 130s on its way to 150s.
    // The group must still be there: event time has not yet REACHED the
    // record that created it, so no ttl can have elapsed since.
    p.watermark(130'000);
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 15)
        << "a group was evicted before the stream's event time reached the record that "
           "created it (followups item 70)";

    // And it still expires on schedule measured from its own time: 150s
    // record + 1s ttl, so past 151s (the probe feed at 130s refreshed the
    // deadline to 150s + 1s via the observed high event time).
    p.watermark(152'000);
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 5)
        << "the ahead-of-watermark group never expired";
}

TEST(SqlStateTtlRuntime, APreClockRecordWithItsOwnTimestampUsesItAsTheClock) {
    // Before any watermark, a record's own event time is a valid clock
    // base: deadline = ts + ttl. The pre-clock enrolment (first watermark
    // + ttl) remains the fallback for records with no timestamp at all.
    Probe p;
    p.start({{"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    feed_at(p, 1, 10, 5'000);  // no watermark yet
    // The first watermark lands far past ts + ttl: 95s of event time have
    // genuinely passed since the data's own moment, so it expires.
    p.watermark(100'000);
    EXPECT_EQ(p.running_total_after_feeding(1, 5), 5)
        << "a pre-clock record with its own timestamp was granted first-watermark + ttl "
           "retention it had already outlived";
}

TEST(SqlStateTtlRuntime, DistinctAheadOfTheWatermarkIsNotForgottenEarly) {
    // The same property at the DISTINCT: an entry created by a record
    // ahead of the watermark must not be forgotten before the stream's
    // event time reaches that record - or a replayed/slow-path duplicate
    // would be re-emitted and folded twice.
    OpProbe p("distinct_row",
              {{"columns", "k"}, {"state_ttl_ms", "1000"}, {"state_ttl_domain", "event_time"}});
    p.watermark(78'000);
    EXPECT_TRUE(p.emits_at(1, 10, 150'000));
    p.watermark(130'000);  // past old stamp (79s), before the record's own ts
    EXPECT_FALSE(p.emits_at(1, 10, 150'000))
        << "DISTINCT forgot a value before event time reached it; a duplicate would fold twice";
}
