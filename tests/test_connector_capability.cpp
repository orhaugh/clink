// Connector capability contract + end-to-end delivery-guarantee analysis.
//
// Two things are under test. First, that every declaration in this binary
// is internally coherent - a record claiming two-phase commit while not
// being transactional is a bug in the declaration and must be caught here
// rather than believed by the planner. Second, that the analyser computes
// the weakest link and refuses a request the pipeline cannot honour.

#include <algorithm>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/delivery_guarantee.hpp"

namespace {

using namespace clink::connectors;

class ConnectorCapabilityTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Built-ins declare themselves from the same call that registers
        // their factories.
        clink::cluster::ensure_built_ins_registered();
    }
};

// --- the declarations themselves -------------------------------------------

TEST_F(ConnectorCapabilityTest, EveryDeclarationIsSelfConsistent) {
    const auto all = CapabilityRegistry::instance().all();
    ASSERT_FALSE(all.empty()) << "built-ins must declare capabilities";
    std::vector<std::string> problems;
    for (const auto& c : all) {
        for (auto& p : c.self_check()) {
            problems.push_back(std::move(p));
        }
    }
    EXPECT_TRUE(problems.empty()) << [&] {
        std::string s = "incoherent capability declarations:\n";
        for (const auto& p : problems) {
            s += "  - " + p + "\n";
        }
        return s;
    }();
}

TEST_F(ConnectorCapabilityTest, SelfCheckRejectsTwoPhaseCommitWithoutTransactionality) {
    const ConnectorCapabilities bad{
        .name = "liar",
        .is_sink = true,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::ExactlyOnceTwoPhaseCommit,
        .transactional = false,
    };
    const auto problems = bad.self_check();
    ASSERT_FALSE(problems.empty());
    EXPECT_NE(problems[0].find("not marked transactional"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest, SelfCheckRejectsExactlyOnceWithoutCheckpointing) {
    const ConnectorCapabilities bad{
        .name = "untethered",
        .is_sink = true,
        .checkpoint_integrated = false,
        .delivery = DeliveryGuarantee::ExactlyOnceAtomicPublish,
        .transactional = true,
    };
    const auto problems = bad.self_check();
    ASSERT_FALSE(problems.empty());
    EXPECT_NE(problems[0].find("does not participate in checkpointing"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest,
       SelfCheckRejectsSourceWithNoRecoveryModelClaimingMoreThanAtMostOnce) {
    // No client-side offset to replay AND no checkpoint participation
    // (which is what broker-redelivery recovery hangs off): nothing relates
    // a restart to what was already processed, so at-least-once is an
    // overclaim.
    const ConnectorCapabilities bad{
        .name = "amnesiac",
        .is_source = true,
        .replayable = false,
        .checkpoint_integrated = false,
        .delivery = DeliveryGuarantee::AtLeastOnce,
    };
    const auto problems = bad.self_check();
    ASSERT_FALSE(problems.empty());
    EXPECT_NE(problems[0].find("at-most-once"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest, SelfCheckAcceptsBrokerRedeliverySourceAtLeastOnce) {
    // The second no-loss recovery model: no offset to replay, but the ack
    // is deferred to checkpoint completion, so the broker redelivers
    // everything unacknowledged after a crash (AMQP, JetStream, Pulsar,
    // Pub/Sub, Redis PEL, MQTT persistent sessions). At-least-once is an
    // honest claim for this shape and must not be rejected.
    const ConnectorCapabilities broker_redelivery{
        .name = "ack_deferred",
        .is_source = true,
        .replayable = false,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::AtLeastOnce,
    };
    EXPECT_TRUE(broker_redelivery.self_check().empty());
}

TEST_F(ConnectorCapabilityTest, SelfCheckRejectsIdempotentClaimWithNoNamedKey) {
    const ConnectorCapabilities bad{
        .name = "keyless",
        .is_sink = true,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::EffectivelyOnceIdempotent,
        .idempotency_key_option = "",
    };
    const auto problems = bad.self_check();
    ASSERT_FALSE(problems.empty());
    EXPECT_NE(problems[0].find("names no option carrying the key"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest, GuaranteeNamesRoundTripAndAreOrdered) {
    for (const auto g : {DeliveryGuarantee::AtMostOnce,
                         DeliveryGuarantee::AtLeastOnce,
                         DeliveryGuarantee::EffectivelyOnceIdempotent,
                         DeliveryGuarantee::ExactlyOnceAtomicPublish,
                         DeliveryGuarantee::ExactlyOnceTwoPhaseCommit,
                         DeliveryGuarantee::NoDurableRestartGuarantee}) {
        const auto parsed = delivery_from_string(to_string(g));
        ASSERT_TRUE(parsed.has_value()) << to_string(g);
        EXPECT_EQ(*parsed, g);
    }
    EXPECT_LT(strength(DeliveryGuarantee::AtMostOnce), strength(DeliveryGuarantee::AtLeastOnce));
    EXPECT_LT(strength(DeliveryGuarantee::AtLeastOnce),
              strength(DeliveryGuarantee::EffectivelyOnceIdempotent));
    EXPECT_LT(strength(DeliveryGuarantee::EffectivelyOnceIdempotent),
              strength(DeliveryGuarantee::ExactlyOnceTwoPhaseCommit));
    // The legacy DDL spelling is accepted as input so existing scripts
    // keep working, but is never produced as output.
    EXPECT_EQ(delivery_from_string("exactly_once"), DeliveryGuarantee::ExactlyOnceTwoPhaseCommit);
    EXPECT_FALSE(delivery_from_string("mostly_once").has_value());
}

// --- the manifest ----------------------------------------------------------

TEST_F(ConnectorCapabilityTest, ManifestReportsThisBinaryNotTheProject) {
    const auto facts = current_build_facts();
    const auto text = render_manifest_text(facts, CapabilityRegistry::instance().all());
    EXPECT_NE(text.find("clink capability manifest"), std::string::npos);
    EXPECT_NE(text.find("file_2pc"), std::string::npos);
    // The build's own weakening surfaces are on the face of the manifest.
    EXPECT_NE(text.find("fault injection compiled in"), std::string::npos);
    EXPECT_NE(text.find("unverified checkpoints"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest, ManifestJsonDeclaresItsSchemaVersionAndOrigin) {
    const auto json =
        render_manifest_json(current_build_facts(), CapabilityRegistry::instance().all());
    // The schema version is the first key, so a consumer can decide whether
    // it understands the layout before reading anything else. 1 is pinned:
    // bumping it is a deliberate act (a key renamed, removed, or changed in
    // meaning), not a side effect - adding keys does not bump it.
    EXPECT_EQ(json.rfind("{\"schema_version\":1,", 0), 0u)
        << "manifest must open with its schema version: " << json.substr(0, 60);
    // git_sha maps the manifest back to the code that produced it. The
    // value varies per build; presence and non-emptiness are the contract.
    EXPECT_NE(json.find("\"git_sha\":\""), std::string::npos);
    EXPECT_EQ(json.find("\"git_sha\":\"\""), std::string::npos) << "git_sha must not be empty";
    const auto facts = current_build_facts();
    EXPECT_FALSE(facts.git_sha.empty());
}

TEST_F(ConnectorCapabilityTest, ManifestJsonIsWellFormedAndCarriesEveryConnector) {
    const auto all = CapabilityRegistry::instance().all();
    const auto json = render_manifest_json(current_build_facts(), all);
    EXPECT_EQ(json.front(), '{');
    EXPECT_EQ(json.back(), '}');
    for (const auto& c : all) {
        EXPECT_NE(json.find("\"name\":\"" + c.name + "\""), std::string::npos) << c.name;
    }
    // Balanced braces is a cheap structural check that catches the class of
    // bug a hand-rolled encoder actually produces.
    int depth = 0;
    bool in_string = false;
    bool escaped = false;
    for (const char ch : json) {
        if (escaped) {
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
        } else if (ch == '"') {
            in_string = !in_string;
        } else if (!in_string && (ch == '{' || ch == '[')) {
            ++depth;
        } else if (!in_string && (ch == '}' || ch == ']')) {
            --depth;
        }
        ASSERT_GE(depth, 0);
    }
    EXPECT_EQ(depth, 0);
}

// --- the analyser ----------------------------------------------------------

PipelineConnector src(std::string connector, std::vector<std::string> opts = {}) {
    return PipelineConnector{.op_type = connector + "_source",
                             .connector_name = std::move(connector),
                             .is_source = true,
                             .supplied_options = std::move(opts)};
}

PipelineConnector snk(std::string connector, std::vector<std::string> opts = {}) {
    return PipelineConnector{.op_type = connector + "_sink",
                             .connector_name = std::move(connector),
                             .is_source = false,
                             .supplied_options = std::move(opts)};
}

// The same, with a commit_group, and a distinct op_type so two sinks of
// the same connector kind are distinguishable in the diagnostic.
PipelineConnector snk_grouped(std::string connector,
                              std::string group,
                              std::string suffix,
                              std::vector<std::string> opts = {}) {
    return PipelineConnector{.op_type = connector + "_sink" + suffix,
                             .connector_name = std::move(connector),
                             .is_source = false,
                             .supplied_options = std::move(opts),
                             .declaration_missing = false,
                             .commit_group = std::move(group)};
}

TEST_F(ConnectorCapabilityTest, NoCheckpointingMeansNoRecoveryGuarantee) {
    PipelineFacts f;
    f.connectors = {src("file"), snk("file_2pc", {"dir"})};
    f.checkpointing_enabled = false;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::NoRecoveryGuarantee);
    EXPECT_EQ(r.limiting_factor, "checkpointing disabled");
}

TEST_F(ConnectorCapabilityTest, ReplayableSourceWithTwoPhaseSinkIsEndToEndExactlyOnce) {
    PipelineFacts f;
    f.connectors = {src("file"), snk("file_2pc", {"dir"})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::EndToEndExactlyOnce) << r.render_text();
    EXPECT_TRUE(r.acceptable());
}

TEST_F(ConnectorCapabilityTest, AtLeastOnceSinkCapsTheWholePipeline) {
    PipelineFacts f;
    // A 2PC sink alongside a plain one: the weakest link decides, which is
    // the whole point of computing this rather than reporting the best
    // sink's own claim.
    f.connectors = {src("file"), snk("file_2pc", {"dir"}), snk("file")};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce) << r.render_text();
    EXPECT_EQ(r.limiting_factor, "sink 'file_sink'");
}

TEST_F(ConnectorCapabilityTest, NonDurableStateBackendCapsBelowExactlyOnce) {
    PipelineFacts f;
    f.connectors = {src("file"), snk("file_2pc", {"dir"})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = false;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce);
    EXPECT_EQ(r.limiting_factor, "non-durable state backend");
}

TEST_F(ConnectorCapabilityTest, MissingRequiredOptionDowngradesAClaimedExactlyOnceSink) {
    PipelineFacts f;
    // file_2pc requires 'dir'. Without it the mechanism cannot run, so the
    // declared level must not be taken at face value.
    f.connectors = {src("file"), snk("file_2pc", {})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce) << r.render_text();
    bool explained = false;
    for (const auto& reason : r.reasons) {
        // file_2pc's requirement is "dir|path" (the C++ factory takes one
        // spelling, the SQL DDL the other), rendered readably for whoever
        // has to act on it.
        explained =
            explained || reason.find("'dir' or 'path' was not supplied") != std::string::npos;
    }
    EXPECT_TRUE(explained) << r.render_text();
}

TEST_F(ConnectorCapabilityTest, NonReplayableSourceIsAtMostOnceRegardlessOfSink) {
    PipelineFacts f;
    // parquet's source keeps no position (declared, verified against the
    // implementation), so no sink can rescue the pipeline.
    f.connectors = {src("parquet"), snk("file_2pc", {"dir"})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::AtMostOnceSource) << r.render_text();
}

TEST_F(ConnectorCapabilityTest, RequestingExactlyOnceFromAWeakPipelineIsRejected) {
    PipelineFacts f;
    f.connectors = {src("file"), snk("file")};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    f.requested = DeliveryGuarantee::ExactlyOnceTwoPhaseCommit;
    const auto r = analyse_pipeline(f);
    EXPECT_FALSE(r.acceptable());
    ASSERT_FALSE(r.rejections.empty());
    // The rejection names both what was asked for and what caps it, so the
    // user can act on it without reading the source.
    EXPECT_NE(r.rejections[0].find("exactly_once_two_phase_commit"), std::string::npos);
    EXPECT_NE(r.rejections[0].find("limited by sink 'file_sink'"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest, RequestingExactlyOnceWithoutCheckpointingIsRejected) {
    PipelineFacts f;
    f.connectors = {src("file"), snk("file_2pc", {"dir"})};
    f.checkpointing_enabled = false;
    f.durable_state_backend = true;
    f.requested = DeliveryGuarantee::ExactlyOnceTwoPhaseCommit;
    const auto r = analyse_pipeline(f);
    EXPECT_FALSE(r.acceptable());
    EXPECT_NE(r.rejections[0].find("checkpointing is disabled"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest, UndeclaredConnectorIsAssumedWeakNotAssumedGood) {
    PipelineFacts f;
    f.connectors = {src("file"),
                    PipelineConnector{.op_type = "mystery_sink",
                                      .connector_name = "mystery",
                                      .is_source = false,
                                      .declaration_missing = true}};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    f.requested = DeliveryGuarantee::ExactlyOnceTwoPhaseCommit;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce);
    EXPECT_FALSE(r.acceptable());
}

TEST_F(ConnectorCapabilityTest, NonDeterminismIsWarnedAboutSeparatelyFromDelivery) {
    PipelineFacts f;
    f.connectors = {src("file"), snk("file_2pc", {"dir"})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    f.determinism.reads_wall_clock = true;
    f.determinism.sources_of_nondeterminism = {"NOW() in the projection"};
    const auto r = analyse_pipeline(f);
    // Delivery is still exactly-once: each record is committed once. What
    // changes is that a replay would not produce the SAME records.
    EXPECT_EQ(r.level, EndToEndGuarantee::EndToEndExactlyOnce);
    bool warned = false;
    for (const auto& w : r.warnings) {
        warned = warned || w.find("non-deterministic") != std::string::npos;
    }
    EXPECT_TRUE(warned) << r.render_text();
}

TEST_F(ConnectorCapabilityTest, ReportRendersBothTextAndJson) {
    PipelineFacts f;
    f.connectors = {src("file"), snk("file")};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_NE(r.render_text().find("delivery guarantee:"), std::string::npos);
    const auto json = r.render_json();
    EXPECT_NE(json.find("\"level\":\"STATE_EXACTLY_ONCE_OUTPUT_AT_LEAST_ONCE\""),
              std::string::npos);
    EXPECT_NE(json.find("\"acceptable\":true"), std::string::npos);
}

TEST_F(ConnectorCapabilityTest, SinkOnlyPipelineWithNoSinksStillReportsSomething) {
    PipelineFacts f;
    f.connectors = {src("file")};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_EQ(r.level, EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce);
}

// --- cross-sink atomicity -------------------------------------------

// Every step of the analysis before this reasons about connectors one at
// a time and takes the weakest, so two strong sinks produce a strong
// answer - and per-sink exactly-once does not compose into job-level
// atomicity. A failure between two commits can publish one and not the
// other, with both sinks still correctly exactly-once on their own.

bool mentions(const GuaranteeReport& r, std::string_view needle) {
    return std::any_of(r.warnings.begin(), r.warnings.end(), [needle](const std::string& w) {
        return w.find(needle) != std::string::npos;
    });
}

// These four cases changed shape once the engine was measured rather than
// read. The warning used to fire only on UNGROUPED sinks, saying they
// "commit INDEPENDENTLY" and prescribing a commit_group. Running a two-sink
// job with and without a group showed identical per-checkpoint agreement
// (tests/integration/test_commit_group_atomicity.cpp): commit is one
// per-checkpoint job-wide broadcast after every subtask acks, so grouping
// changes nothing here. The old assertions were faithful to the old
// comments and wrong about the code, and the prescription sent operators to
// set an option that does not do what they were told.
//
// So the warning now fires on sink count alone, and these tests assert the
// residual limitation that IS real: the commits are not atomic with each
// other, and a restart is what repairs a split.

TEST_F(ConnectorCapabilityTest, TwoTransactionalSinksAreFlaggedAsNonAtomic) {
    PipelineFacts f;
    f.connectors = {src("file"),
                    snk_grouped("file_2pc", "", "_a", {"dir"}),
                    snk_grouped("file_2pc", "", "_b", {"dir"})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);

    // The LEVEL is still exactly-once, and correctly so: each sink is.
    // Downgrading would misreport the opposite way and would refuse jobs
    // whose two outputs are genuinely independent.
    EXPECT_EQ(r.level, EndToEndGuarantee::EndToEndExactlyOnce) << r.render_text();
    EXPECT_TRUE(r.acceptable());

    EXPECT_TRUE(mentions(r, "NOT atomic"))
        << "two transactional sinks were reported as end-to-end exactly-once with nothing said "
           "about cross-sink atomicity: "
        << r.render_text();
    EXPECT_TRUE(mentions(r, "restart"))
        << "the warning does not say what resolves a split: " << r.render_text();
    EXPECT_TRUE(mentions(r, "file_2pc_sink_a") && mentions(r, "file_2pc_sink_b"))
        << "the warning does not name the sinks: " << r.render_text();
}

TEST_F(ConnectorCapabilityTest, TheWarningDoesNotPrescribeACommitGroupAsTheFix) {
    // The specific regression to keep out. A commit_group does not make
    // two sinks' commits atomic, so telling an operator to set one is
    // advice that cannot work - worse than saying nothing, because it
    // reads as a resolved issue.
    PipelineFacts f;
    f.connectors = {src("file"),
                    snk_grouped("file_2pc", "", "_a", {"dir"}),
                    snk_grouped("file_2pc", "", "_b", {"dir"})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    const auto r = analyse_pipeline(f);
    EXPECT_FALSE(mentions(r, "Set the same commit_group"))
        << "the warning prescribes a commit_group, which does not deliver cross-sink commit "
           "atomicity in this engine: "
        << r.render_text();
    EXPECT_TRUE(mentions(r, "commit_group does"))
        << "the warning should say outright that a commit_group does not fix this, since the "
           "option exists and an operator will reach for it: "
        << r.render_text();
}

TEST_F(ConnectorCapabilityTest, GroupingDoesNotSilenceTheCrossSinkWarning) {
    // Grouped, and grouped separately, both still warn. Gating the warning
    // on grouping would hide a live limitation from exactly the jobs whose
    // authors thought they had addressed it.
    for (const auto& [group_a, group_b] :
         {std::pair{"atomic-out", "atomic-out"}, std::pair{"group-a", "group-b"}}) {
        PipelineFacts f;
        f.connectors = {src("file"),
                        snk_grouped("file_2pc", group_a, "_a", {"dir"}),
                        snk_grouped("file_2pc", group_b, "_b", {"dir"})};
        f.checkpointing_enabled = true;
        f.durable_state_backend = true;
        const auto r = analyse_pipeline(f);
        EXPECT_EQ(r.level, EndToEndGuarantee::EndToEndExactlyOnce);
        EXPECT_TRUE(mentions(r, "NOT atomic"))
            << "groups '" << group_a << "'/'" << group_b
            << "' silenced the cross-sink warning, but grouping does not change the exposure: "
            << r.render_text();
    }
}

TEST_F(ConnectorCapabilityTest, ASingleTransactionalSinkIsNotFlagged) {
    // One sink cannot disagree with itself, and a spurious warning here
    // would fire on the commonest pipeline there is.
    PipelineFacts f;
    f.connectors = {src("file"), snk("file_2pc", {"dir"})};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    EXPECT_FALSE(mentions(analyse_pipeline(f), "NOT atomic"));
}

TEST_F(ConnectorCapabilityTest, ANonTransactionalSecondSinkIsNotFlagged) {
    // The warning is about two things that could disagree about a
    // TRANSACTION. A plain at-least-once sink alongside a 2PC one has no
    // transaction to be atomic with, and the level already reflects it.
    PipelineFacts f;
    f.connectors = {src("file"), snk("file_2pc", {"dir"}), snk("file")};
    f.checkpointing_enabled = true;
    f.durable_state_backend = true;
    EXPECT_FALSE(mentions(analyse_pipeline(f), "NOT atomic"));
}

}  // namespace
