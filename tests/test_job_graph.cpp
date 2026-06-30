// JobGraphSpec is the JM-side description of a job that gets serialized
// into the DeployMsg.extra_config string and parsed by the TM-side
// OperatorRegistry. It's the boundary between "what to run" (JM) and
// "how to run it" (TM), so silent serialization regressions break the
// dispatch path. These tests pin:
//   - serialize/parse round-trip for empty, single-op, multi-op,
//     parameter-bearing specs.
//   - Whitespace tolerance in parse (multiple spaces, trailing newlines,
//     missing trailing newline).
//   - Param helpers (param_int64, param_string) honour fallbacks and
//     parse defensively on garbage.
//   - Empty / blank lines do not produce ghost ops.

#include <cstdint>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/core/types.hpp"

using namespace clink::cluster;

namespace {

// True iff serialize ∘ parse is identity for this spec, by re-parsing
// the serialized form and comparing operator types and params.
::testing::AssertionResult round_trip_equal(const JobGraphSpec& original) {
    const auto reparsed = JobGraphSpec::parse(original.serialize());
    if (reparsed.ops.size() != original.ops.size()) {
        return ::testing::AssertionFailure() << "size mismatch: original=" << original.ops.size()
                                             << " reparsed=" << reparsed.ops.size();
    }
    for (std::size_t i = 0; i < original.ops.size(); ++i) {
        if (reparsed.ops[i].type != original.ops[i].type) {
            return ::testing::AssertionFailure() << "op[" << i << "].type mismatch";
        }
        if (reparsed.ops[i].params != original.ops[i].params) {
            return ::testing::AssertionFailure() << "op[" << i << "].params mismatch";
        }
    }
    return ::testing::AssertionSuccess();
}

}  // namespace

TEST(JobGraphSpec, EmptySpecRoundTrips) {
    JobGraphSpec s;
    EXPECT_EQ(s.serialize(), "");
    EXPECT_TRUE(JobGraphSpec::parse("").ops.empty());
    EXPECT_TRUE(round_trip_equal(s));
}

TEST(JobGraphSpec, SingleOpNoParamsRoundTrips) {
    JobGraphSpec s;
    s.ops.push_back(OperatorSpec{.type = "int64_vector_source", .params = {}});
    EXPECT_TRUE(round_trip_equal(s));
}

TEST(JobGraphSpec, SingleOpWithParamsRoundTrips) {
    JobGraphSpec s;
    s.ops.push_back(OperatorSpec{
        .type = "int64_vector_source",
        .params = {{"count", "10"}, {"offset", "100"}},
    });
    EXPECT_TRUE(round_trip_equal(s));
}

TEST(JobGraphSpec, MultiOpChainRoundTrips) {
    JobGraphSpec s;
    s.ops.push_back(OperatorSpec{
        .type = "int64_vector_source",
        .params = {{"count", "5"}},
    });
    s.ops.push_back(OperatorSpec{
        .type = "network_bridge_sink",
        .params = {{"host", "127.0.0.1"}, {"port", "18000"}},
    });
    EXPECT_TRUE(round_trip_equal(s));
}

TEST(JobGraphSpec, SerializeProducesNewlineTerminatedLines) {
    JobGraphSpec s;
    s.ops.push_back(OperatorSpec{.type = "a", .params = {}});
    s.ops.push_back(OperatorSpec{.type = "b", .params = {{"x", "1"}}});
    EXPECT_EQ(s.serialize(), "a\nb x=1\n");
}

TEST(JobGraphSpec, ParseSkipsBlankLines) {
    auto s = JobGraphSpec::parse("\n\nfoo\n\nbar key=val\n\n");
    ASSERT_EQ(s.ops.size(), 2u);
    EXPECT_EQ(s.ops[0].type, "foo");
    EXPECT_EQ(s.ops[1].type, "bar");
    EXPECT_EQ(s.ops[1].params.at("key"), "val");
}

TEST(JobGraphSpec, ParseHandlesMissingTrailingNewline) {
    auto s = JobGraphSpec::parse("only_op key=value");
    ASSERT_EQ(s.ops.size(), 1u);
    EXPECT_EQ(s.ops[0].type, "only_op");
    EXPECT_EQ(s.ops[0].params.at("key"), "value");
}

TEST(JobGraphSpec, ParseTolerantOfMultipleSpaces) {
    auto s = JobGraphSpec::parse("op   k1=v1   k2=v2");
    ASSERT_EQ(s.ops.size(), 1u);
    EXPECT_EQ(s.ops[0].params.at("k1"), "v1");
    EXPECT_EQ(s.ops[0].params.at("k2"), "v2");
}

TEST(JobGraphSpec, ParseIgnoresMalformedKvWithoutEquals) {
    // Token without '=' should be silently dropped, not crash or pollute
    // the params map.
    auto s = JobGraphSpec::parse("op good=1 garbage other=2");
    ASSERT_EQ(s.ops.size(), 1u);
    EXPECT_EQ(s.ops[0].params.size(), 2u);
    EXPECT_EQ(s.ops[0].params.at("good"), "1");
    EXPECT_EQ(s.ops[0].params.at("other"), "2");
}

TEST(JobGraphSpec, ParseAcceptsEmptyValue) {
    // "key=" is a valid empty-string value.
    auto s = JobGraphSpec::parse("op empty=");
    ASSERT_EQ(s.ops.size(), 1u);
    EXPECT_EQ(s.ops[0].params.at("empty"), "");
}

TEST(JobGraphSpec, ParamInt64ReturnsParsed) {
    OperatorSpec op{.type = "x", .params = {{"n", "12345"}, {"neg", "-99"}}};
    EXPECT_EQ(param_int64(op, "n"), 12345);
    EXPECT_EQ(param_int64(op, "neg"), -99);
}

TEST(JobGraphSpec, ParamInt64ReturnsFallbackOnMissing) {
    OperatorSpec op{.type = "x", .params = {}};
    EXPECT_EQ(param_int64(op, "missing"), 0);
    EXPECT_EQ(param_int64(op, "missing", 42), 42);
}

TEST(JobGraphSpec, ParamInt64ReturnsFallbackOnGarbage) {
    OperatorSpec op{.type = "x", .params = {{"n", "not_a_number"}}};
    EXPECT_EQ(param_int64(op, "n", 7), 7);
}

TEST(JobGraphSpec, ParamStringReturnsParsed) {
    OperatorSpec op{.type = "x", .params = {{"host", "example.com"}}};
    EXPECT_EQ(param_string(op, "host"), "example.com");
}

TEST(JobGraphSpec, ParamStringReturnsFallbackOnMissing) {
    OperatorSpec op{.type = "x", .params = {}};
    EXPECT_EQ(param_string(op, "missing"), "");
    EXPECT_EQ(param_string(op, "missing", "default"), "default");
}

// from_json auto-validates: the planner used to be the only line of
// defence against malformed specs. These tests pin the contract that
// from_json itself throws on the three invariants validate() checks
// (unique ids, inputs resolve, no cycles) so callers that go through
// from_json without subsequently calling plan_job still get caught.

TEST(JobGraphSpec, FromJsonRejectsCycle) {
    // Two ops referencing each other -> 2-cycle. Both have indegree 1
    // so Kahn's algorithm never finds a seed to start from.
    const std::string json = R"({"ops":[
        {"id":"a","type":"identity_int64","out_channel":"int64","inputs":["b"]},
        {"id":"b","type":"identity_int64","out_channel":"int64","inputs":["a"]}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(json), std::runtime_error);
}

TEST(JobGraphSpec, FromJsonRejectsDanglingInput) {
    const std::string json = R"({"ops":[
        {"id":"a","type":"identity_int64","out_channel":"int64","inputs":["does_not_exist"]}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(json), std::runtime_error);
}

TEST(JobGraphSpec, FromJsonRejectsDuplicateId) {
    const std::string json = R"({"ops":[
        {"id":"x","type":"int64_range_source","out_channel":"int64"},
        {"id":"x","type":"collecting_int64_sink","out_channel":"int64","inputs":["x"]}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(json), std::runtime_error);
}

// --- Job name -----------------------------------------

TEST(JobGraphSpec, NameRoundTripsThroughJson) {
    const std::string json = R"({"name":"orders-etl","ops":[
        {"id":"src","type":"int64_range_source","out_channel":"int64"}
    ]})";
    auto spec = JobGraphSpec::from_json(json);
    EXPECT_EQ(spec.name, "orders-etl");
    const auto serialised = spec.to_json();
    EXPECT_NE(serialised.find("\"name\":\"orders-etl\""), std::string::npos);
    EXPECT_EQ(JobGraphSpec::from_json(serialised).name, "orders-etl");
}

TEST(JobGraphSpec, NameOmittedWhenEmpty) {
    // A spec with no name emits no "name" key, so the JSON shape is
    // unchanged for unnamed jobs.
    const std::string json = R"({"ops":[
        {"id":"src","type":"int64_range_source","out_channel":"int64"}
    ]})";
    auto spec = JobGraphSpec::from_json(json);
    EXPECT_TRUE(spec.name.empty());
    EXPECT_EQ(spec.to_json().find("\"name\""), std::string::npos);
}

// --- Per-operator parallelism bounds ----------------

TEST(JobGraphSpec, BoundsDefaultToZeroNoAutoscaling) {
    // The default OperatorSpec carries 0 / 0 bounds, signalling "this
    // operator does not participate in autoscaling." Round-tripping
    // a JSON without bounds keys leaves both at 0.
    const std::string json = R"({"ops":[
        {"id":"src","type":"int64_range_source","out_channel":"int64","parallelism":2}
    ]})";
    auto spec = JobGraphSpec::from_json(json);
    ASSERT_EQ(spec.ops.size(), 1u);
    EXPECT_EQ(spec.ops[0].min_parallelism, 0u);
    EXPECT_EQ(spec.ops[0].max_parallelism, 0u);

    // to_json omits the bounds keys when both are zero.
    const auto serialised = spec.to_json();
    EXPECT_EQ(serialised.find("min_parallelism"), std::string::npos);
    EXPECT_EQ(serialised.find("max_parallelism"), std::string::npos);
}

TEST(JobGraphSpec, BoundsRoundTripThroughJson) {
    const std::string json = R"({"ops":[
        {"id":"src","type":"int64_range_source","out_channel":"int64",
         "parallelism":2,"min_parallelism":1,"max_parallelism":8}
    ]})";
    auto spec = JobGraphSpec::from_json(json);
    ASSERT_EQ(spec.ops.size(), 1u);
    EXPECT_EQ(spec.ops[0].parallelism, 2u);
    EXPECT_EQ(spec.ops[0].min_parallelism, 1u);
    EXPECT_EQ(spec.ops[0].max_parallelism, 8u);

    // Round-trip: emitted JSON must include both bounds keys.
    const auto serialised = spec.to_json();
    EXPECT_NE(serialised.find("\"min_parallelism\":1"), std::string::npos);
    EXPECT_NE(serialised.find("\"max_parallelism\":8"), std::string::npos);

    auto spec2 = JobGraphSpec::from_json(serialised);
    EXPECT_EQ(spec2.ops[0].min_parallelism, 1u);
    EXPECT_EQ(spec2.ops[0].max_parallelism, 8u);
}

// State schema evolution: the expected-version map declared via
// env.expect_state_version(...) rides the spec to the JM/TM. It is keyed
// by operator_id_from_uid (the same derivation the runtime stamps state
// under) and survives the JSON round-trip; absent when nothing declared.
TEST(JobGraphSpec, ExpectedStateVersionsRoundTripThroughJson) {
    const char* base =
        R"({"ops":[{"id":"src","type":"int64_range_source","out_channel":"int64"}]})";
    auto spec = JobGraphSpec::from_json(base);
    const auto agg = clink::operator_id_from_uid("agg");
    const auto ctr = clink::operator_id_from_uid("ctr");
    spec.expected_state_versions.set(agg, "WindowAgg", 3);
    spec.expected_state_versions.set(ctr, "Counter", 2);

    const auto serialised = spec.to_json();
    EXPECT_NE(serialised.find("expected_state_versions"), std::string::npos);

    auto spec2 = JobGraphSpec::from_json(serialised);
    EXPECT_EQ(spec2.expected_state_versions.get(agg, "WindowAgg"), std::optional<std::uint32_t>{3});
    EXPECT_EQ(spec2.expected_state_versions.get(ctr, "Counter"), std::optional<std::uint32_t>{2});

    // A spec that declares nothing omits the key entirely (shape stable
    // for version-unaware jobs).
    EXPECT_EQ(JobGraphSpec::from_json(base).to_json().find("expected_state_versions"),
              std::string::npos);
}

// Slot-aware: a slotted expected entry must survive to_json/from_json
// (the HA-recovery path: recover_persisted_jobs reparses graph_json).
// get() is slot-blind, so assert via entries().
TEST(JobGraphSpec, ExpectedStateVersionsSlotRoundTripsThroughJson) {
    const char* base =
        R"({"ops":[{"id":"src","type":"int64_range_source","out_channel":"int64"}]})";
    auto spec = JobGraphSpec::from_json(base);
    const auto join = clink::operator_id_from_uid("join");
    spec.expected_state_versions.set(join, "JoinLeft", 2, "left_buf");
    spec.expected_state_versions.set(join, "JoinRight", 4, "right_buf");

    auto spec2 = JobGraphSpec::from_json(spec.to_json());
    const auto entries = spec2.expected_state_versions.entries();
    ASSERT_EQ(entries.size(), 2u);
    for (const auto& e : entries) {
        if (e.state_type == "JoinLeft") {
            EXPECT_EQ(e.slot, "left_buf");
            EXPECT_EQ(e.version, 2u);
        } else {
            EXPECT_EQ(e.state_type, "JoinRight");
            EXPECT_EQ(e.slot, "right_buf");
            EXPECT_EQ(e.version, 4u);
        }
    }
}

// operator_id_from_uid is deterministic and nonzero - the contract the
// runtime (Dag::derive_id_from_uid_, which calls it) and the expected-
// version keying both rely on.
TEST(JobGraphSpec, OperatorIdFromUidIsStableAndNonzero) {
    EXPECT_EQ(clink::operator_id_from_uid("agg"), clink::operator_id_from_uid("agg"));
    EXPECT_NE(clink::operator_id_from_uid("agg"), clink::operator_id_from_uid("ctr"));
    EXPECT_NE(clink::operator_id_from_uid("agg").value(), 0u);
}

TEST(JobGraphSpec, RejectsHalfSpecifiedBounds) {
    // Either both bounds are set or neither - having only one is a
    // configuration mistake that's better surfaced at parse time
    // than producing ambiguous autoscaler behaviour later.
    const std::string min_only = R"({"ops":[
        {"id":"a","type":"int64_range_source","out_channel":"int64",
         "parallelism":2,"min_parallelism":1}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(min_only), std::runtime_error);

    const std::string max_only = R"({"ops":[
        {"id":"a","type":"int64_range_source","out_channel":"int64",
         "parallelism":2,"max_parallelism":8}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(max_only), std::runtime_error);
}

TEST(JobGraphSpec, RejectsCurrentOutsideBounds) {
    // parallelism=4 with min=5 - operator can't be running below
    // its declared floor. Reject up front.
    const std::string below_min = R"({"ops":[
        {"id":"a","type":"int64_range_source","out_channel":"int64",
         "parallelism":4,"min_parallelism":5,"max_parallelism":8}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(below_min), std::runtime_error);

    // parallelism=10 with max=8 - same issue, above ceiling.
    const std::string above_max = R"({"ops":[
        {"id":"a","type":"int64_range_source","out_channel":"int64",
         "parallelism":10,"min_parallelism":2,"max_parallelism":8}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(above_max), std::runtime_error);
}

TEST(JobGraphSpec, RejectsInvertedBounds) {
    // min=8, max=2 - autoscaler would have an empty range to work in.
    const std::string inverted = R"({"ops":[
        {"id":"a","type":"int64_range_source","out_channel":"int64",
         "parallelism":4,"min_parallelism":8,"max_parallelism":2}
    ]})";
    EXPECT_THROW(JobGraphSpec::from_json(inverted), std::runtime_error);
}

TEST(JobGraphSpec, BoundsEqualToCurrentParallelismAccepted) {
    // min == max == parallelism is the degenerate "autoscaling
    // declared but pinned to current value" case. Accept it; the
    // autoscaler will simply never move this operator.
    const std::string json = R"({"ops":[
        {"id":"a","type":"int64_range_source","out_channel":"int64",
         "parallelism":4,"min_parallelism":4,"max_parallelism":4}
    ]})";
    auto spec = JobGraphSpec::from_json(json);
    EXPECT_EQ(spec.ops[0].min_parallelism, 4u);
    EXPECT_EQ(spec.ops[0].max_parallelism, 4u);
}

TEST(JobGraphSpec, FromJsonAcceptsWellFormedDag) {
    // Linear source -> op -> sink. No cycle, all inputs resolve,
    // ids unique. Should round-trip cleanly.
    const std::string json = R"({"ops":[
        {"id":"src","type":"int64_range_source","out_channel":"int64"},
        {"id":"op","type":"identity_int64","out_channel":"int64","inputs":["src"]},
        {"id":"snk","type":"collecting_int64_sink","out_channel":"int64","inputs":["op"]}
    ]})";
    EXPECT_NO_THROW({
        auto spec = JobGraphSpec::from_json(json);
        EXPECT_EQ(spec.ops.size(), 3u);
    });
}

TEST(JobGraphSpec, ComplexFanOutGraphRoundTrips) {
    // Models a producer → join → sink graph the JM might assemble.
    JobGraphSpec s;
    s.ops.push_back(OperatorSpec{
        .type = "int64_vector_source",
        .params = {{"count", "100"}, {"start", "0"}},
    });
    s.ops.push_back(OperatorSpec{
        .type = "key_by",
        .params = {{"key_codec", "int64"}, {"parallelism", "4"}},
    });
    s.ops.push_back(OperatorSpec{
        .type = "tumbling_window",
        .params = {{"size_ms", "60000"}, {"trigger", "event_time"}},
    });
    s.ops.push_back(OperatorSpec{
        .type = "collecting_sink",
        .params = {},
    });
    EXPECT_TRUE(round_trip_equal(s));
}
