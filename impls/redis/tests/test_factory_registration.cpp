// Verifies clink::redis::install() makes the redis_source / redis_sink factories
// reachable through the RunnerRegistry on the "string" channel.

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/delivery_guarantee.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(RedisFactoryRegistration, SinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("redis_sink", "string"), nullptr);
}

TEST(RedisFactoryRegistration, UpsertSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("redis_upsert_sink", "string"), nullptr);
}

TEST(RedisFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("redis_source", "string"), nullptr);
}

// The record the delivery analyser and the coordinator read: the source
// acknowledges at checkpoint completion and the server delivers again what was
// not acknowledged, so a job over it is at-least-once. It used to declare no
// position model, which the analyser read as at-most-once, refusing even an
// at_least_once request.
TEST(SourceCapabilityRecord, TheSourceAcknowledgesAtCheckpointCompletion) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("redis");
    ASSERT_NE(caps, nullptr);
    EXPECT_EQ(caps->offset_model, clink::connectors::OffsetModel::Acknowledgement);
    EXPECT_FALSE(caps->replayable);
    EXPECT_TRUE(caps->checkpoint_integrated);
    EXPECT_TRUE(caps->self_check().empty());
    EXPECT_FALSE(caps->scale_down_refusal.empty());
    EXPECT_TRUE(caps->scale_down_refusal_param.empty()) << "the refusal holds in every mode";
    EXPECT_EQ(clink::cluster::source_scale_down_refusal("redis_source", {}),
              caps->scale_down_refusal)
        << "the coordinator would not refuse a scale-down of the source";
}

clink::cluster::OperatorSpec gate_op(std::string type,
                                     std::string id,
                                     std::vector<std::string> inputs = {},
                                     std::map<std::string, std::string> params = {}) {
    clink::cluster::OperatorSpec s;
    s.type = std::move(type);
    s.id = std::move(id);
    s.inputs = std::move(inputs);
    s.params = std::move(params);
    return s;
}

TEST(SourceCapabilityRecord, AJobOverTheSourceIsAtLeastOnceAndExactlyOnceIsRefused) {
    clink::cluster::CheckpointConfig ckpt;
    ckpt.checkpoint_dir = "/tmp/ckpt";
    ckpt.interval_ms = 1000;
    ckpt.state_backend_uri = "file:///tmp/state";
    clink::cluster::JobGraphSpec g;
    g.ops.push_back(gate_op("redis_source", "src"));
    g.ops.push_back(gate_op("file_2pc_sink_string",
                            "snk",
                            {"src"},
                            {{"dir", "/tmp/out"}, {"delivery_guarantee", "at_least_once"}}));
    clink::connectors::GuaranteeReport report;
    EXPECT_EQ(clink::cluster::check_delivery_guarantee(g, ckpt, &report), "");
    EXPECT_EQ(report.level, clink::connectors::EndToEndGuarantee::AtLeastOnceSource)
        << report.render_text();

    g.ops[1].params["delivery_guarantee"] = "exactly_once";
    const auto refused = clink::cluster::check_delivery_guarantee(g, ckpt, &report);
    EXPECT_NE(refused.find("AT_LEAST_ONCE_SOURCE"), std::string::npos) << refused;
    EXPECT_NE(refused.find("redis_source"), std::string::npos) << refused;

    // Without checkpointing nothing completes, so nothing stronger than
    // at-most-once can be asked for.
    g.ops[1].params["delivery_guarantee"] = "at_least_once";
    EXPECT_NE(
        clink::cluster::check_delivery_guarantee(g, clink::cluster::CheckpointConfig{}, &report)
            .find("checkpointing is disabled"),
        std::string::npos);
}

template <typename F>
std::string redis_thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// The sinks read their options strictly. Building a sink connects to nothing,
// so a value is refused here before any server is involved.
TEST(RedisFactoryRegistration, TheSinkFactoriesReadTheirOptionsStrictly) {
    for (const char* type : {"redis_sink", "redis_upsert_sink"}) {
        const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
            type, std::string{clink::cluster::kChannelString});
        ASSERT_NE(f, nullptr) << type;
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = {{"stream", "s"}, {"key_columns", "id"}};
        EXPECT_NO_THROW((void)f->build(ctx)) << type;
        auto every = ctx;
        every.params.insert({{"port", "65535"},
                             {"db", "15"},
                             {"tls", "true"},
                             {"tls_ca", "/ca.pem"},
                             {"tls_verify", "false"},
                             {"tls_verify_hostname", "false"},
                             {"batch_records", "1000000"},
                             {"max_bytes", "0"},
                             {"max_record_bytes", "536870912"},
                             {"command_timeout_ms", "3600000"}});
        EXPECT_NO_THROW((void)f->build(every)) << type;
        for (const auto& [key, value] :
             std::vector<std::pair<std::string, std::string>>{{"port", "0"},
                                                              {"port", "65536"},
                                                              {"port", "63 79"},
                                                              {"db", "-1"},
                                                              {"batch_records", "0"},
                                                              {"batch_records", "ten"},
                                                              {"max_bytes", "-1"},
                                                              {"max_record_bytes", "536870913"},
                                                              {"command_timeout_ms", "0"}}) {
            auto c = ctx;
            c.params[key] = value;
            EXPECT_NE(
                redis_thrown([&] { (void)f->build(c); }).find("'" + key + "' must be an integer"),
                std::string::npos)
                << type << " " << key << "=" << value;
        }
        // tls='TRUE' once read as false and connected in the clear.
        for (const char* key : {"tls", "tls_verify", "tls_verify_hostname"}) {
            auto c = ctx;
            c.params[key] = "TRUE";
            EXPECT_NE(redis_thrown([&] {
                          (void)f->build(c);
                      }).find("'" + std::string{key} + "' must be 'true' or 'false'"),
                      std::string::npos)
                << type << " " << key;
        }
        auto clear = ctx;
        clear.params["tls_ca"] = "/ca.pem";
        EXPECT_NE(redis_thrown([&] { (void)f->build(clear); }).find("would not use TLS"),
                  std::string::npos)
            << type;
    }
    const auto* stream = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "redis_sink", std::string{clink::cluster::kChannelString});
    clink::cluster::OperatorBuildContext ctx;
    for (const char* approx : {"1", "0", "true", "false"}) {
        ctx.params = {{"stream", "s"}, {"approx_maxlen", approx}};
        EXPECT_NO_THROW((void)stream->build(ctx)) << approx;
    }
    ctx.params = {{"stream", "s"}, {"approx_maxlen", "yes"}};
    EXPECT_NE(redis_thrown([&] { (void)stream->build(ctx); }).find("'approx_maxlen' must be 1, 0"),
              std::string::npos);
    ctx.params = {{"stream", "s"}, {"linger_ms", "-1"}};
    EXPECT_NE(
        redis_thrown([&] { (void)stream->build(ctx); }).find("'linger_ms' must be an integer"),
        std::string::npos);
    ctx.params = {{"stream", "s"}, {"maxlen", "-5"}};
    EXPECT_NE(redis_thrown([&] { (void)stream->build(ctx); }).find("'maxlen' must be an integer"),
              std::string::npos);
}

TEST(RedisFactoryRegistration, TheSinkRecordsStateTheirContracts) {
    const auto* plain = clink::connectors::CapabilityRegistry::instance().find("redis");
    const auto* upsert = clink::connectors::CapabilityRegistry::instance().find("redis_upsert");
    ASSERT_NE(plain, nullptr);
    ASSERT_NE(upsert, nullptr);
    EXPECT_TRUE(plain->self_check().empty());
    EXPECT_TRUE(upsert->self_check().empty());
    EXPECT_EQ(plain->version, "2");
    EXPECT_EQ(upsert->version, "2");
    EXPECT_EQ(plain->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_EQ(upsert->delivery, clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent);
    EXPECT_EQ(upsert->idempotency_key_option, "key_columns");
    EXPECT_FALSE(plain->retries);
    EXPECT_FALSE(upsert->retries);
    EXPECT_EQ(plain->timeout_options, (std::vector<std::string>{"command_timeout_ms"}));
    EXPECT_EQ(upsert->timeout_options, (std::vector<std::string>{"command_timeout_ms"}));
}

TEST(RedisFactoryRegistration, TheUpsertSinkIsAnalysedEffectivelyOnceWithItsKey) {
    clink::cluster::JobGraphSpec g;
    g.ops.push_back(gate_op("file_line_source", "src"));
    g.ops.push_back(gate_op("redis_upsert_sink", "snk", {"src"}, {{"key_columns", "id"}}));
    clink::cluster::CheckpointConfig ckpt;
    ckpt.checkpoint_dir = "/tmp/ckpt";
    ckpt.interval_ms = 1000;
    ckpt.state_backend_uri = "file:///tmp/state";
    clink::connectors::GuaranteeReport report;
    EXPECT_TRUE(clink::cluster::check_delivery_guarantee(g, ckpt, &report).empty());
    for (const auto& reason : report.reasons) {
        EXPECT_EQ(reason.find("was not supplied"), std::string::npos) << reason;
    }
}

}  // namespace
