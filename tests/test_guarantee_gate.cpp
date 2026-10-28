// Submission-time delivery-guarantee gate.
//
// The analyser's own logic is covered by test_connector_capability. What
// is under test here is the BRIDGE: does a real JobGraphSpec produce the
// right facts, does op-type-to-connector resolution pick the right record,
// and does a job that asks for more than it can have get refused.

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/guarantee_gate.hpp"
#include "clink/connectors/capability.hpp"

namespace {

using namespace clink;
using namespace clink::cluster;

OperatorSpec op(std::string type,
                std::string id,
                std::vector<std::string> inputs = {},
                std::map<std::string, std::string> params = {}) {
    OperatorSpec s;
    s.type = std::move(type);
    s.id = std::move(id);
    s.inputs = std::move(inputs);
    s.params = std::move(params);
    return s;
}

CheckpointConfig durable_checkpointing() {
    CheckpointConfig c;
    c.checkpoint_dir = "/tmp/ckpt";
    c.interval_ms = 1000;
    c.state_backend_uri = "file:///tmp/state";
    return c;
}

class GuaranteeGateTest : public ::testing::Test {
protected:
    void SetUp() override { ensure_built_ins_registered(); }
};

// --- op type -> connector name ---------------------------------------------

TEST_F(GuaranteeGateTest, LongestPrefixWinsSoTwoPhaseVariantsAreNotMistakenForPlainOnes) {
    // The single most consequential rule in the bridge. "file_2pc_sink_string"
    // starts with BOTH "file" and "file_2pc"; picking the shorter one would
    // report an at-least-once sink as exactly-once, or vice versa.
    EXPECT_EQ(connector_name_for_op_type("file_2pc_sink_string"), "file_2pc");
    EXPECT_EQ(connector_name_for_op_type("file_line_sink"), "file");
    EXPECT_EQ(connector_name_for_op_type("parquet_2pc_sink_row"), "parquet_2pc");
    EXPECT_EQ(connector_name_for_op_type("parquet_int64_sink"), "parquet");
}

// The SQL row Parquet 2PC sink is parquet_row_2pc_sink, which does not begin
// with "parquet_2pc" (the case above pins a spelling no factory registers). It
// resolved to "parquet", the at-least-once rolling sink, so a SQL job asking for
// exactly-once Parquet was refused at submission.
TEST_F(GuaranteeGateTest, TheSqlRowParquet2PCSinkResolvesToTheTwoPhaseRecord) {
    EXPECT_EQ(connector_name_for_op_type("parquet_row_2pc_sink"), "parquet_2pc");
    EXPECT_EQ(connector_name_for_op_type("parquet_row_sink"), "parquet");

    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("parquet_row_2pc_sink",
                       "snk",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    connectors::GuaranteeReport report;
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), &report);
    EXPECT_TRUE(reject.empty()) << reject;
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::EndToEndExactlyOnce);
}

TEST_F(GuaranteeGateTest, AnUnknownOpTypeResolvesToNothing) {
    EXPECT_TRUE(connector_name_for_op_type("some_third_party_sink").empty());
}

TEST_F(GuaranteeGateTest, TheNexmarkSourceResolvesToAReplayableRecord) {
    // Until the nexmark record existed, every nexmark job was at-most-once at
    // the source even when checkpointed: the gate reads "no record" as
    // "replayability cannot be established". GeneratorSource now snapshots
    // its count and restores by regenerating, and the record says so.
    EXPECT_EQ(connector_name_for_op_type("nexmark_source"), "nexmark");
    const auto* record = connectors::CapabilityRegistry::instance().find("nexmark");
    ASSERT_NE(record, nullptr);
    EXPECT_TRUE(record->is_source);
    EXPECT_TRUE(record->replayable);
    EXPECT_TRUE(record->checkpoint_integrated);
    EXPECT_TRUE(record->available_in_sql);
}

TEST_F(GuaranteeGateTest, ACheckpointedNexmarkJobIsNoLongerCappedAtItsSource) {
    JobGraphSpec g;
    g.ops.push_back(op("nexmark_source", "src", {}, {{"events_num", "1000"}}));
    g.ops.push_back(op("file_2pc_sink_string", "snk", {"src"}, {{"dir", "/tmp/o"}}));
    connectors::GuaranteeReport report;
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), &report);
    EXPECT_TRUE(reject.empty()) << reject;
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::EndToEndExactlyOnce)
        << report.limiting_factor;
}

// Declares records for one test and puts back whatever the registry held
// under those names before, so the process-wide registry is left as found.
class GateScopedRecords {
public:
    explicit GateScopedRecords(std::vector<connectors::ConnectorCapabilities> records) {
        auto& registry = connectors::CapabilityRegistry::instance();
        for (auto& record : records) {
            const auto* existing = registry.find(record.name);
            previous_.emplace_back(record.name,
                                   existing != nullptr
                                       ? std::optional<connectors::ConnectorCapabilities>(*existing)
                                       : std::nullopt);
            registry.declare(std::move(record));
        }
    }
    ~GateScopedRecords() {
        auto& registry = connectors::CapabilityRegistry::instance();
        for (auto& [name, record] : previous_) {
            registry.undeclare(name);
            if (record) {
                registry.declare(std::move(*record));
            }
        }
    }
    GateScopedRecords(const GateScopedRecords&) = delete;
    GateScopedRecords& operator=(const GateScopedRecords&) = delete;

private:
    std::vector<std::pair<std::string, std::optional<connectors::ConnectorCapabilities>>> previous_;
};

// The native ClickHouse sink's factory name starts with both "clickhouse" and
// "clickhouse_native". Resolving it to the legacy record would report the
// legacy sink's claims (no retries, no TLS) for the native sink. The core
// suite links no impl, so the two records the ClickHouse install() declares
// are stood in for here, with the fields that tell them apart.
TEST_F(GuaranteeGateTest, TheNativeClickHouseSinkResolvesToItsOwnRecordNotTheLegacyOne) {
    const GateScopedRecords records({
        connectors::ConnectorCapabilities{
            .name = "clickhouse",
            .is_source = true,
            .is_sink = true,
            .checkpoint_integrated = true,
            .delivery = connectors::DeliveryGuarantee::AtLeastOnce,
            .tls = false,
            .retries = false,
        },
        connectors::ConnectorCapabilities{
            .name = "clickhouse_native",
            .is_source = false,
            .is_sink = true,
            .checkpoint_integrated = true,
            .delivery = connectors::DeliveryGuarantee::AtLeastOnce,
            .tls = true,
            .retries = true,
        },
    });
    EXPECT_EQ(connector_name_for_op_type("clickhouse_native_sink"), "clickhouse_native");
    EXPECT_EQ(connector_name_for_op_type("clickhouse_sink"), "clickhouse");
    EXPECT_EQ(connector_name_for_op_type("clickhouse_row_source"), "clickhouse");
    // The typed helper's default op type keeps the prefix first, so a sink
    // registered for a struct still finds the native record; the reverse
    // spelling would leave it undeclared.
    EXPECT_EQ(connector_name_for_op_type("clickhouse_native_sink_Trade"), "clickhouse_native");
    EXPECT_NE(connector_name_for_op_type("Trade_clickhouse_native_sink"), "clickhouse_native");

    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("clickhouse_native_sink", "snk", {"src"}));
    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    ASSERT_EQ(facts.connectors.size(), 2U);
    EXPECT_EQ(facts.connectors[1].connector_name, "clickhouse_native");
    EXPECT_FALSE(facts.connectors[1].declaration_missing);

    // At-least-once is all it claims, so asking it for exactly-once is refused.
    g.ops[1].params["delivery_guarantee"] = "exactly_once";
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("clickhouse_native_sink"), std::string::npos) << reject;
}

// --- graph -> facts ---------------------------------------------------------

TEST_F(GuaranteeGateTest, RolesComeFromTheGraphNotFromTheTypeName) {
    // Source = no inputs; sink = nothing consumes it. Derived from edges,
    // because a name-based guess ("*_sink") misses every connector that
    // does not follow the convention.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("identity_string", "mid", {"src"}));
    g.ops.push_back(op("file_line_sink", "snk", {"mid"}));

    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    // The mid-chain operator has no delivery semantics and must not appear.
    ASSERT_EQ(facts.connectors.size(), 2U);
    EXPECT_TRUE(facts.connectors[0].is_source);
    EXPECT_FALSE(facts.connectors[1].is_source);
}

TEST_F(GuaranteeGateTest, MemoryBackendIsNotCountedAsDurable) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string", "snk", {"src"}, {{"dir", "/tmp/o"}}));

    CheckpointConfig c = durable_checkpointing();
    c.state_backend_uri = "memory://";
    const auto facts = pipeline_facts_from_graph(g, c);
    EXPECT_FALSE(facts.durable_state_backend)
        << "memory:// state does not survive a process restart and must not count as durable";
}

TEST_F(GuaranteeGateTest, NoCheckpointIntervalMeansCheckpointingIsOff) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}));
    CheckpointConfig c = durable_checkpointing();
    c.interval_ms = 0;
    EXPECT_FALSE(pipeline_facts_from_graph(g, c).checkpointing_enabled);
}

TEST_F(GuaranteeGateTest, AnUndeclaredConnectorIsMarkedMissingNotDropped) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("some_third_party_sink", "snk", {"src"}));

    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    ASSERT_EQ(facts.connectors.size(), 2U);
    // Dropping it would let an unrecognised sink silently not count towards
    // the weakest link, which is the failure this whole gate exists to stop.
    EXPECT_TRUE(facts.connectors[1].declaration_missing);
}

TEST_F(GuaranteeGateTest, TheStrongestRequestedGuaranteeAcrossTheGraphIsTheOneAsked) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(
        op("file_line_sink", "weak", {"src"}, {{"delivery_guarantee", "at_least_once"}}));
    g.ops.push_back(op("file_2pc_sink_string",
                       "strong",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));

    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    ASSERT_TRUE(facts.requested.has_value());
    EXPECT_EQ(*facts.requested, connectors::DeliveryGuarantee::ExactlyOnceTwoPhaseCommit);
}

// --- the decision -----------------------------------------------------------

TEST_F(GuaranteeGateTest, AWeakJobThatAsksForNothingIsAllowed) {
    // Most jobs are at-least-once and that is a legitimate choice. The gate
    // must not turn "weak" into "rejected".
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}));
    connectors::GuaranteeReport report;
    EXPECT_TRUE(check_delivery_guarantee(g, durable_checkpointing(), &report).empty());
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsAllowedWhenThePipelineCanActuallyProvideIt) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string",
                       "snk",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    connectors::GuaranteeReport report;
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), &report);
    EXPECT_TRUE(reject.empty()) << reject;
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::EndToEndExactlyOnce);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedWhenASinkCannotProvideIt) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}, {{"delivery_guarantee", "exactly_once"}}));
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    ASSERT_FALSE(reject.empty()) << "a plain append sink cannot provide exactly-once";
    // The message must be actionable: what was asked, and what caps it.
    EXPECT_NE(reject.find("exactly_once_two_phase_commit"), std::string::npos);
    EXPECT_NE(reject.find("file_line_sink"), std::string::npos);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedWhenCheckpointingIsOff) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string",
                       "snk",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    CheckpointConfig c = durable_checkpointing();
    c.interval_ms = 0;
    const auto reject = check_delivery_guarantee(g, c, nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("checkpointing is disabled"), std::string::npos);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedWhenTheSinkIsMissingItsRequiredOption) {
    // file_2pc declares 'dir' as required for its mechanism. Without it the
    // sink cannot stage anything, so its declared level must not be taken
    // at face value.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(
        op("file_2pc_sink_string", "snk", {"src"}, {{"delivery_guarantee", "exactly_once"}}));
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    EXPECT_FALSE(reject.empty())
        << "a 2PC sink with no 'dir' cannot stage, so exactly-once must be refused";
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedOnANonDurableStateBackend) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string",
                       "snk",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    CheckpointConfig c = durable_checkpointing();
    c.state_backend_uri = "memory://";
    const auto reject = check_delivery_guarantee(g, c, nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("non-durable state backend"), std::string::npos);
}

// --- sinks that declare different guarantees -----------------------------
//
// A job's sinks can each declare a delivery_guarantee, as the tables of a
// statement set do. Each sink is held to its own, through its own sources;
// one that declares none is held to the strongest the job declares.

TEST_F(GuaranteeGateTest, AnAtLeastOnceSinkThatSaysSoIsAllowedBesideAnExactlyOnceOne) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(
        op("file_line_sink", "graph", {"src"}, {{"delivery_guarantee", "at_least_once"}}));
    g.ops.push_back(op("file_2pc_sink_string",
                       "fence",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    connectors::GuaranteeReport report;
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), &report);
    EXPECT_TRUE(reject.empty()) << reject;
    // The job is reported as what its weakest sink gives, as any job is,
    // and the report says what each sink gives.
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce);
    const auto text = report.render_text();
    EXPECT_NE(text.find("sink 'fence' (file_2pc_sink_string) is held to "
                        "exactly_once_two_phase_commit, which it declares, and through its own "
                        "sources gives END_TO_END_EXACTLY_ONCE"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("sink 'graph' (file_line_sink) is held to at_least_once, which it "
                        "declares"),
              std::string::npos)
        << text;
}

TEST_F(GuaranteeGateTest, ASinkThatDeclaresNothingIsHeldToTheStrongestTheJobDeclares) {
    // Beside an exactly-once sink, a sink with no declaration is the job
    // asking for exactly-once everywhere, and a plain sink cannot give it.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_line_sink", "graph", {"src"}));
    g.ops.push_back(op("file_2pc_sink_string",
                       "fence",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("file_line_sink"), std::string::npos) << reject;
}

TEST_F(GuaranteeGateTest, AnUndeclaredSinkIsHeldToTheStrongestEvenBesideADeclaredWeakOne) {
    // One sink saying at-least-once does not lower what the others are held
    // to: the undeclared one is still held to exactly-once, and refused.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(
        op("file_line_sink", "said", {"src"}, {{"delivery_guarantee", "at_least_once"}}));
    g.ops.push_back(op("file_line_sink", "silent", {"src"}));
    g.ops.push_back(op("file_2pc_sink_string",
                       "fence",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("sink 'silent'"), std::string::npos) << reject;
    EXPECT_EQ(reject.find("sink 'said':"), std::string::npos) << reject;
    EXPECT_EQ(reject.find("sink 'fence':"), std::string::npos) << reject;
}

TEST_F(GuaranteeGateTest, AnotherSinksSourceDoesNotCapAnExactlyOnceSink) {
    // A source that cannot replay caps the sinks it feeds, not the job.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "replayable"));
    g.ops.push_back(op("custom_feed_source", "unreplayable"));
    g.ops.push_back(op("file_2pc_sink_string",
                       "fence",
                       {"replayable"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    g.ops.push_back(
        op("file_line_sink", "trace", {"unreplayable"}, {{"delivery_guarantee", "at_most_once"}}));
    connectors::GuaranteeReport report;
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), &report);
    EXPECT_TRUE(reject.empty()) << reject;
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::AtMostOnceSource);
}

TEST_F(GuaranteeGateTest, AnExactlyOnceSinkIsRefusedThroughAnUnreplayableSourceOfItsOwn) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "replayable"));
    g.ops.push_back(op("custom_feed_source", "unreplayable"));
    g.ops.push_back(op("identity_string", "both", {"replayable", "unreplayable"}));
    g.ops.push_back(op("file_2pc_sink_string",
                       "fence",
                       {"both"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    g.ops.push_back(
        op("file_line_sink", "graph", {"replayable"}, {{"delivery_guarantee", "at_least_once"}}));
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("sink 'fence': requested exactly_once_two_phase_commit"),
              std::string::npos)
        << reject;
    EXPECT_NE(reject.find("custom_feed_source"), std::string::npos) << reject;
    EXPECT_EQ(reject.find("sink 'graph':"), std::string::npos) << reject;
}

TEST_F(GuaranteeGateTest, ASinkOverAnAcknowledgingSourceMayDeclareAtLeastOnceButNotExactlyOnce) {
    // A broker that redelivers what was not acknowledged at a completed
    // checkpoint loses nothing, so a sink's own declaration of at least
    // once is honoured through such a source; exactly once is refused,
    // because a checkpoint acknowledged late is delivered again.
    connectors::CapabilityRegistry::instance().declare(connectors::ConnectorCapabilities{
        .name = "ackfeed",
        .version = "1",
        .is_source = true,
        .replayable = false,
        .offset_model = connectors::OffsetModel::Acknowledgement,
        .checkpoint_integrated = true,
        .delivery = connectors::DeliveryGuarantee::AtLeastOnce,
    });
    struct Undeclare {
        ~Undeclare() { connectors::CapabilityRegistry::instance().undeclare("ackfeed"); }
    } cleanup;
    JobGraphSpec g;
    g.ops.push_back(op("ackfeed_source", "bus"));
    g.ops.push_back(
        op("file_line_sink", "out", {"bus"}, {{"delivery_guarantee", "at_least_once"}}));
    connectors::GuaranteeReport report;
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), &report);
    EXPECT_TRUE(reject.empty()) << reject;
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::AtLeastOnceSource);

    JobGraphSpec h;
    h.ops.push_back(op("ackfeed_source", "bus"));
    h.ops.push_back(op("file_2pc_sink_string",
                       "fence",
                       {"bus"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    const auto refused = check_delivery_guarantee(h, durable_checkpointing(), nullptr);
    ASSERT_FALSE(refused.empty());
    EXPECT_NE(refused.find("AT_LEAST_ONCE_SOURCE"), std::string::npos) << refused;
    EXPECT_NE(refused.find("ackfeed_source"), std::string::npos) << refused;
}

// --- replay determinism -------------------------------------------------
//
// Delivery and determinism are orthogonal answers and must not be
// conflated: a pipeline can commit every record exactly once and still
// produce different values on a replay. These tests pin the
// classification the gate derives from the spec, and - as important -
// what SILENCE means under each coverage claim.

JobGraphSpec sql_graph() {
    JobGraphSpec g;
    g.determinism_coverage = "sql-planner";
    g.ops.push_back(op("kafka_source_string", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}));
    return g;
}

TEST_F(GuaranteeGateTest, ASqlGraphWithNoOutwardReachingOpsIsDeterministic) {
    const auto facts = pipeline_facts_from_graph(sql_graph(), durable_checkpointing());
    EXPECT_TRUE(facts.determinism.deterministic());
    EXPECT_TRUE(facts.determinism.classification_complete)
        << "the planner classified every op; silence means deterministic";
    EXPECT_TRUE(facts.determinism.unclassified_operators.empty());
}

TEST_F(GuaranteeGateTest, MlPredictOverHttpIsAnExternalServiceCall) {
    auto g = sql_graph();
    g.ops.insert(g.ops.begin() + 1,
                 op("ml_predict_row",
                    "mlp",
                    {"src"},
                    {{"model_name", "scorer"}, {"model.provider", "http"}}));
    g.ops[2].inputs = {"mlp"};
    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    EXPECT_TRUE(facts.determinism.calls_external_service);
    ASSERT_FALSE(facts.determinism.sources_of_nondeterminism.empty());
    EXPECT_NE(facts.determinism.sources_of_nondeterminism[0].find("scorer"), std::string::npos);
    // A LOCAL provider is not an external call: same model file, same answer.
    auto local = sql_graph();
    local.ops.insert(local.ops.begin() + 1,
                     op("ml_predict_row",
                        "mlp",
                        {"src"},
                        {{"model_name", "scorer"}, {"model.provider", "onnx"}}));
    local.ops[2].inputs = {"mlp"};
    EXPECT_TRUE(
        pipeline_facts_from_graph(local, durable_checkpointing()).determinism.deterministic());
}

TEST_F(GuaranteeGateTest, AsyncLookupsAndWasmUdfsAreClassifiedConservatively) {
    auto g = sql_graph();
    g.ops.insert(g.ops.begin() + 1,
                 op("async_lookup_row", "enrich", {"src"}, {{"function_name", "profile_of"}}));
    g.ops[2].inputs = {"enrich"};
    UdfSpec wasm;
    wasm.name = "custom_score";
    wasm.language = "wasm";
    g.udfs.push_back(wasm);
    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    EXPECT_TRUE(facts.determinism.calls_external_service);
    EXPECT_TRUE(facts.determinism.has_nondeterministic_udf);
    ASSERT_EQ(facts.determinism.sources_of_nondeterminism.size(), 2u);
    EXPECT_NE(facts.determinism.sources_of_nondeterminism[0].find("profile_of"), std::string::npos);
    EXPECT_NE(facts.determinism.sources_of_nondeterminism[1].find("custom_score"),
              std::string::npos);
}

TEST_F(GuaranteeGateTest, ANeo4jLookupIsCurrentStateAndSoNotReplayDeterministic) {
    auto g = sql_graph();
    g.ops.insert(
        g.ops.begin() + 1,
        op("neo4j_lookup_join_row", "lookupjoin_1", {"src"}, {{"function_name", "neo4j"}}));
    g.ops[2].inputs = {"lookupjoin_1"};
    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    EXPECT_FALSE(facts.determinism.deterministic());
    EXPECT_TRUE(facts.determinism.calls_external_service);
    ASSERT_EQ(facts.determinism.sources_of_nondeterminism.size(), 1u);
    EXPECT_NE(facts.determinism.sources_of_nondeterminism[0].find("Neo4j lookup 'lookupjoin_1'"),
              std::string::npos);
}

TEST_F(GuaranteeGateTest, APluginGraphWithNoDeclarationsIsUnknownNotClean) {
    // No determinism_coverage claim and no per-op declarations: the ops are
    // native code the engine cannot inspect. "Nothing found" must be
    // reported as UNKNOWN - a clean verdict would be inferred from silence.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("my_custom_operator", "mid", {"src"}));
    g.ops.push_back(op("file_line_sink", "snk", {"mid"}));
    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    EXPECT_TRUE(facts.determinism.deterministic()) << "nothing was FOUND...";
    EXPECT_FALSE(facts.determinism.classification_complete) << "...but nothing was CLASSIFIED";
    EXPECT_EQ(facts.determinism.unclassified_operators.size(), 3u);

    connectors::GuaranteeReport report;
    (void)check_delivery_guarantee(g, durable_checkpointing(), &report);
    bool warned_unknown = false;
    for (const auto& w : report.warnings) {
        warned_unknown =
            warned_unknown || w.find("replay determinism is UNKNOWN") != std::string::npos;
    }
    EXPECT_TRUE(warned_unknown) << report.render_text();
}

TEST_F(GuaranteeGateTest, AFullyDeclaredPluginGraphIsClassified) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src", {}, {{"determinism", "deterministic"}}));
    g.ops.push_back(op("my_custom_operator",
                       "mid",
                       {"src"},
                       {{"determinism", "nondeterministic:reads a remote profile service"}}));
    g.ops.push_back(op("file_line_sink", "snk", {"mid"}, {{"determinism", "deterministic"}}));
    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    EXPECT_TRUE(facts.determinism.classification_complete)
        << "every op declared itself, so the classification is complete";
    EXPECT_TRUE(facts.determinism.has_nondeterministic_udf);
    ASSERT_EQ(facts.determinism.sources_of_nondeterminism.size(), 1u);
    EXPECT_NE(facts.determinism.sources_of_nondeterminism[0].find("remote profile service"),
              std::string::npos);
}

TEST_F(GuaranteeGateTest, DeterminismCoverageSurvivesTheSpecJsonRoundTrip) {
    // The coverage claim rides the spec through submission; a round trip
    // that drops it would silently turn every SQL job's report to UNKNOWN.
    auto g = sql_graph();
    const auto back = JobGraphSpec::from_json(g.to_json());
    EXPECT_EQ(back.determinism_coverage, "sql-planner");
    JobGraphSpec plain;
    plain.ops.push_back(op("file_line_source", "src"));
    plain.ops.push_back(op("file_line_sink", "snk", {"src"}));
    EXPECT_TRUE(JobGraphSpec::from_json(plain.to_json()).determinism_coverage.empty());
}

}  // namespace
