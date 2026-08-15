// The submission-time connector-availability gate: a job naming a
// connector this binary was not built with is refused before planning or
// allocation, with the connector, the available set and the rebuild flag
// in the message. Availability is decided by the registries the job
// would deploy against - never a hardcoded list - so the same function
// models both a local `clink run` (local registries) and a distributed
// submission (the target coordinator's registries are the ones passed,
// which is what makes the cluster, not the submitting CLI,
// authoritative).

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/connector_availability.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"

namespace {

using namespace clink::cluster;

OperatorSpec op(const std::string& type,
                const std::string& id,
                std::vector<std::string> inputs = {}) {
    OperatorSpec o;
    o.type = type;
    o.id = id;
    o.inputs = std::move(inputs);
    return o;
}

JobGraphSpec kafka_to_file_graph() {
    JobGraphSpec g;
    g.ops.push_back(op("kafka_source_string", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}));
    return g;
}

TEST(ConnectorAvailability, AMissingConnectorIsRefusedWithTheFixNamed) {
    // Bare registries with no parent: the shape of a binary built without
    // the connector.
    OperatorRegistry ops;
    RunnerRegistry runners;
    const auto reject = check_connector_availability(kafka_to_file_graph(), ops, runners);
    ASSERT_FALSE(reject.empty()) << "an unbuilt connector was allowed through";
    EXPECT_NE(reject.find("connector 'kafka'"), std::string::npos) << reject;
    EXPECT_NE(reject.find("CLINK_WITH_KAFKA=ON"), std::string::npos)
        << "the message does not name the rebuild flag: " << reject;
    EXPECT_NE(reject.find("Available source connectors"), std::string::npos)
        << "the message does not list what this build has: " << reject;
}

TEST(ConnectorAvailability, TheRegistriesPassedDecideNotAnyGlobal) {
    // The same graph against two registry sets: one that knows the type
    // (the cluster that was built with Kafka) and one that does not (the
    // submitting CLI). The decision must follow the registries passed -
    // this is the CLI-and-cluster-differ scenario.
    JobGraphSpec g = kafka_to_file_graph();

    OperatorRegistry without;
    RunnerRegistry no_runners;
    EXPECT_FALSE(check_connector_availability(g, without, no_runners).empty());

    OperatorRegistry with;
    with.register_source("kafka_source_string",
                         SourceFactory{std::string{kChannelString},
                                       [](const OperatorBuildContext&) { return nullptr; }});
    EXPECT_TRUE(check_connector_availability(g, with, no_runners).empty())
        << "a registry that knows the type must pass, whatever the local binary lacks";
}

TEST(ConnectorAvailability, ARunnerOnlyRegistrationCounts) {
    // Deploy resolves through the RunnerRegistry, so a runner-only
    // registration is availability even with an empty OperatorRegistry.
    JobGraphSpec g = kafka_to_file_graph();
    OperatorRegistry ops;
    RunnerRegistry runners;
    runners.register_source(
        "kafka_source_string", std::string{kChannelString}, [](const RunnerContext&) {});
    EXPECT_TRUE(check_connector_availability(g, ops, runners).empty());
}

TEST(ConnectorAvailability, NonConnectorOpsAreLeftForDeployToResolve) {
    // A plugin or inline op the gate has no vocabulary for must pass
    // through: deploy resolves it (and refuses loudly if unknown).
    JobGraphSpec g;
    g.ops.push_back(op("my_plugin_transform", "xf"));
    OperatorRegistry ops;
    RunnerRegistry runners;
    EXPECT_TRUE(check_connector_availability(g, ops, runners).empty());
}

TEST(ConnectorAvailability, AlwaysBuiltConnectorsPassAgainstTheDefaultRegistries) {
    ensure_built_ins_registered();
    JobGraphSpec g;
    g.ops.push_back(op("file_text_source", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}));
    EXPECT_TRUE(check_connector_availability(
                    g, OperatorRegistry::default_instance(), RunnerRegistry::default_instance())
                    .empty());
}

TEST(ConnectorAvailability, VocabularyMatchesLongestPrefixOnUnderscoreBoundaries) {
    // s3_parquet before s3, http_poll before http; a prefix inside a
    // word ("kafkaish") is not a match.
    const auto s3p = connector_vocabulary_lookup("s3_parquet_string_source");
    ASSERT_TRUE(s3p.has_value());
    EXPECT_EQ(s3p->connector, "s3_parquet");
    EXPECT_EQ(s3p->build_flag, "CLINK_WITH_AWS_S3");

    const auto s3 = connector_vocabulary_lookup("s3_2pc_sink_string");
    ASSERT_TRUE(s3.has_value());
    EXPECT_EQ(s3->connector, "s3");

    const auto poll = connector_vocabulary_lookup("http_poll_source");
    ASSERT_TRUE(poll.has_value());
    EXPECT_EQ(poll->connector, "http_poll");

    const auto exact = connector_vocabulary_lookup("kafka");
    ASSERT_TRUE(exact.has_value());
    EXPECT_EQ(exact->connector, "kafka");

    EXPECT_FALSE(connector_vocabulary_lookup("kafkaish_source").has_value());
    EXPECT_FALSE(connector_vocabulary_lookup("tumbling_window_int64").has_value());
}

TEST(ConnectorAvailability, EveryDeclaredCapabilityResolvesThroughTheVocabulary) {
    // The vocabulary is diagnostics-only, but an entry missing from it
    // means a missing connector gets the generic pass-through instead of
    // its rebuild flag. Pin: every capability THIS binary declares either
    // resolves through the vocabulary or is one of the always-built
    // connectors that cannot be missing. A new connector cannot land
    // without either an entry here or a deliberate exemption.
    ensure_built_ins_registered();
    const std::vector<std::string> always_built = {
        "blackhole",
        "file",
        "file_2pc",
        "generator",
        "parquet",
        "parquet_2pc",
        "changelog",
    };
    for (const auto& cap : clink::connectors::CapabilityRegistry::instance().all()) {
        bool exempt = false;
        for (const auto& a : always_built) {
            if (cap.name == a || cap.name.rfind(a + "_", 0) == 0) {
                exempt = true;
                break;
            }
        }
        if (exempt) {
            continue;
        }
        EXPECT_TRUE(connector_vocabulary_lookup(cap.name).has_value())
            << "capability '" << cap.name
            << "' has no vocabulary entry: a build without it cannot name its rebuild flag";
    }
}

TEST(ConnectorAvailability, SubmissionIsRefusedAtTheCoordinatorBeforeAnyDeploy) {
    // The gate as wired: Coordinator::submit_job throws before planning,
    // capacity waits or worker deploys - no worker even exists here. The
    // registry passed models the cluster's binary; a bare one with no
    // parent is a build without the connector.
    ensure_built_ins_registered();
    Coordinator coordinator;
    OperatorRegistry bare;
    try {
        (void)coordinator.submit_job(kafka_to_file_graph(), bare);
        FAIL() << "a job naming an unbuilt connector was accepted";
    } catch (const std::exception& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("connector 'kafka'"), std::string::npos) << msg;
        EXPECT_NE(msg.find("CLINK_WITH_KAFKA=ON"), std::string::npos) << msg;
    }
}

}  // namespace
