// A SQL job submitted to a cluster must be able to ask for durability.
//
// Before this, /api/v1/jobs/spec and /jobs/sql accepted only
// ?state_backend and clink_submit_sql had no checkpoint flags, so a SQL
// cluster job could not enable periodic checkpointing at all: no
// completed checkpoints, therefore no exactly-once commit for a 2PC sink
// and no worker-loss recovery - while the identical compiled-job path had
// both. Nothing failed; the job simply could never recover. The QUAL-01
// campaign could not be run at all until this was fixed.
//
// Two halves, both pinned here: the client emits the parameters, and the
// server parses them back into the CheckpointConfig submit_job acts on.

#include <map>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/submit_query_config.hpp"
#include "clink/http/http_server.hpp"
#include "clink/sql/script_runner.hpp"

namespace {

using namespace clink::cluster;

// --- server side --------------------------------------------------------

TEST(SubmitCheckpointConfig, EveryDurabilityParameterReachesTheConfig) {
    std::string error;
    const auto ckpt = checkpoint_config_from_query(
        {
            {"state_backend", "remote-read://bucket/job?hot_max_bytes=42"},
            {"checkpoint_dir", "/var/clink/ckpt"},
            {"checkpoint_interval_ms", "2500"},
            {"restore_from_dir", "/var/clink/save"},
            {"restore_from_checkpoint_id", "17"},
            {"max_restarts_on_worker_loss", "4"},
            {"alignment", "adaptive"},
        },
        &error);
    ASSERT_TRUE(error.empty()) << error;
    // The state-backend URI carries its own query and must survive intact.
    EXPECT_EQ(ckpt.state_backend_uri, "remote-read://bucket/job?hot_max_bytes=42");
    EXPECT_EQ(ckpt.checkpoint_dir, "/var/clink/ckpt");
    EXPECT_EQ(ckpt.interval_ms, 2500);
    EXPECT_EQ(ckpt.restore_from_dir, "/var/clink/save");
    EXPECT_EQ(ckpt.restore_from_checkpoint_id, 17U);
    EXPECT_EQ(ckpt.max_restarts_on_worker_loss, 4U);
    EXPECT_EQ(ckpt.alignment, CheckpointAlignment::Adaptive);
}

TEST(SubmitCheckpointConfig, AnEmptyQueryLeavesEveryDefault) {
    std::string error;
    const auto ckpt = checkpoint_config_from_query({}, &error);
    ASSERT_TRUE(error.empty()) << error;
    const CheckpointConfig defaults;
    EXPECT_EQ(ckpt.checkpoint_dir, defaults.checkpoint_dir);
    EXPECT_EQ(ckpt.interval_ms, defaults.interval_ms);
    EXPECT_EQ(ckpt.alignment, defaults.alignment);
    EXPECT_EQ(ckpt.state_backend_uri, defaults.state_backend_uri);
}

TEST(SubmitCheckpointConfig, AMalformedValueIsRefusedNotDefaulted) {
    // A mistyped interval that silently became "no checkpointing" would
    // reproduce exactly the silence this whole change exists to remove.
    for (const auto& bad : {"abc", "12ms", "-5", "1e3"}) {
        std::string error;
        (void)checkpoint_config_from_query({{"checkpoint_interval_ms", bad}}, &error);
        EXPECT_FALSE(error.empty()) << "accepted checkpoint_interval_ms='" << bad << "'";
    }
    std::string error;
    (void)checkpoint_config_from_query({{"alignment", "unaligned-ish"}}, &error);
    EXPECT_FALSE(error.empty()) << "accepted an unknown alignment";
    EXPECT_NE(error.find("adaptive"), std::string::npos)
        << "the diagnostic does not name the valid values: " << error;
}

TEST(SubmitCheckpointConfig, EveryAlignmentSpellingIsAccepted) {
    const std::pair<const char*, CheckpointAlignment> cases[] = {
        {"aligned", CheckpointAlignment::Aligned},
        {"unaligned", CheckpointAlignment::Unaligned},
        {"adaptive", CheckpointAlignment::Adaptive},
    };
    for (const auto& [spelling, expected] : cases) {
        std::string error;
        const auto ckpt = checkpoint_config_from_query({{"alignment", spelling}}, &error);
        EXPECT_TRUE(error.empty()) << spelling << ": " << error;
        EXPECT_EQ(ckpt.alignment, expected) << spelling;
    }
}

// --- client side --------------------------------------------------------

// The client half against a real HTTP server: whatever the CLI is asked
// for must actually leave the process. A parser that understands
// parameters nobody sends is not a fix.
TEST(SubmitCheckpointConfig, TheClientSendsWhatTheServerParses) {
    clink::http::HttpServer server;
    std::map<std::string, std::string> seen_query;
    server.post("/api/v1/jobs/spec", [&seen_query](const clink::http::HttpRequest& req) {
        seen_query = req.query;
        clink::http::HttpResponse resp;
        resp.body = R"({"ok":true,"job_id":1,"name":"t"})";
        return resp;
    });
    const auto port = server.start("127.0.0.1", 0);

    std::ostringstream out;
    std::ostringstream err;
    auto submit = clink::sql::make_http_submit("127.0.0.1",
                                               port,
                                               clink::sql::SubmitCheckpointOptions{
                                                   .state_backend_uri = "rocksdb:///var/state",
                                                   .checkpoint_dir = "/var/clink/ckpt",
                                                   .checkpoint_interval_ms = 2500,
                                                   .restore_from_dir = "/var/clink/save",
                                                   .restore_from_checkpoint_id = 17,
                                                   .alignment = "adaptive",
                                               },
                                               out,
                                               err);

    JobGraphSpec graph;
    OperatorSpec op;
    op.type = "int64_range_source";
    op.id = "src";
    graph.ops.push_back(op);
    ASSERT_EQ(submit(graph, "qual-job"), 0) << err.str();
    server.stop();

    // The round trip that matters: the server's parse of what the client
    // actually sent must reproduce the options the caller asked for.
    std::string error;
    const auto ckpt = checkpoint_config_from_query(seen_query, &error);
    ASSERT_TRUE(error.empty()) << error;
    EXPECT_EQ(seen_query["name"], "qual-job");
    EXPECT_EQ(ckpt.state_backend_uri, "rocksdb:///var/state");
    EXPECT_EQ(ckpt.checkpoint_dir, "/var/clink/ckpt");
    EXPECT_EQ(ckpt.interval_ms, 2500);
    EXPECT_EQ(ckpt.restore_from_dir, "/var/clink/save");
    EXPECT_EQ(ckpt.restore_from_checkpoint_id, 17U);
    EXPECT_EQ(ckpt.alignment, CheckpointAlignment::Adaptive);
}

TEST(SubmitCheckpointConfig, UnsetClientOptionsSendNoParameters) {
    // Absent, not empty-valued: an empty checkpoint_dir= on the wire
    // would be indistinguishable from a caller asking for one.
    clink::http::HttpServer server;
    std::map<std::string, std::string> seen_query;
    server.post("/api/v1/jobs/spec", [&seen_query](const clink::http::HttpRequest& req) {
        seen_query = req.query;
        clink::http::HttpResponse resp;
        resp.body = R"({"ok":true,"job_id":1,"name":"t"})";
        return resp;
    });
    const auto port = server.start("127.0.0.1", 0);

    std::ostringstream out;
    std::ostringstream err;
    auto submit = clink::sql::make_http_submit(
        "127.0.0.1", port, clink::sql::SubmitCheckpointOptions{}, out, err);
    JobGraphSpec graph;
    OperatorSpec op;
    op.type = "int64_range_source";
    op.id = "src";
    graph.ops.push_back(op);
    ASSERT_EQ(submit(graph, "plain"), 0) << err.str();
    server.stop();

    EXPECT_EQ(seen_query.count("checkpoint_dir"), 0U);
    EXPECT_EQ(seen_query.count("checkpoint_interval_ms"), 0U);
    EXPECT_EQ(seen_query.count("alignment"), 0U);
    EXPECT_EQ(seen_query.count("state_backend"), 0U);
}

}  // namespace
