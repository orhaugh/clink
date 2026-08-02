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

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row.hpp"
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
    OpProbe(const std::string& type, std::map<std::string, std::string> params)
        : backend_(std::make_shared<InMemoryStateBackend>()) {
        ensure_installed();
        const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
            type, std::string{kChannelRow}, std::string{kChannelRow});
        EXPECT_NE(factory, nullptr) << type;
        cluster::OperatorBuildContext octx;
        octx.params = std::move(params);
        op_ = std::static_pointer_cast<Operator<Row, Row>>(factory->build(octx));
        op_->set_id(OperatorId{77});
        rctx_ = std::make_unique<RuntimeContext>(
            OperatorId{77}, type, backend_.get(), /*metrics=*/nullptr);
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

    void watermark(std::int64_t ms) {
        op_->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(ms)}),
                     emitter_);
    }

    // Entries physically resident in the backend for this operator. The
    // distinction between hiding and releasing.
    std::size_t resident() const {
        std::size_t n = 0;
        backend_->scan(OperatorId{77}, [&](std::string_view, std::string_view) { ++n; });
        return n;
    }

private:
    std::shared_ptr<InMemoryStateBackend> backend_;
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

TEST(SqlStateTtlRuntime, ProcessingTimeDomainIsHonoured) {
    // With processing time selected, watermarks are irrelevant and the
    // wall clock decides. A 1 ms TTL plus a real sleep must evict.
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
