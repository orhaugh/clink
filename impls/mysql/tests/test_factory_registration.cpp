// Verifies clink::mysql::install() makes the mysql_source / mysql_sink factories
// reachable through the RunnerRegistry on the "string" channel, that the change
// stream's factory reads its options strictly, and that the change stream's
// capability record states its delivery and what loses records.

#include <exception>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(MysqlFactoryRegistration, SinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("mysql_sink", "string"), nullptr);
}

TEST(MysqlFactoryRegistration, UpsertSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("mysql_upsert_sink", "string"), nullptr);
}

TEST(MysqlFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("mysql_source", "string"), nullptr);
}

TEST(MysqlFactoryRegistration, CdcSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("mysql_cdc_source", "string"), nullptr);
}

std::string thrown(const std::function<void()>& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// A value the source cannot use is refused rather than replaced by its default.
TEST(MysqlFactoryRegistration, TheCdcFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "mysql_cdc_source", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"server_id", "7"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    every.params = {{"server_id", "4294967295"},
                    {"on_decode_error", "drop"},
                    {"initial_snapshot", "true"},
                    {"tables", "shop.orders"},
                    {"snapshot_lock", "false"},
                    {"snapshot_lock_wait_s", "3600"},
                    {"heartbeat_ms", "1"},
                    {"start_file", "mysql-bin.000001"},
                    {"start_pos", "4"}};
    EXPECT_NO_THROW((void)f->build(every));
    for (const auto& [key, value, rule] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"server_id", "0", "'server_id' is required"},
             {"server_id", "seven", "'server_id' must be a whole number from 0 to 4294967295"},
             {"server_id", "4294967296", "'server_id' must be a whole number from 0 to 4294967295"},
             {"server_id", "7 ", "'server_id' must be a whole number"},
             {"on_decode_error", "skip", "'on_decode_error' must be 'fail' or 'drop'"},
             {"on_decode_error", "", "'on_decode_error' must be 'fail' or 'drop'"},
             {"initial_snapshot", "yes", "'initial_snapshot' must be 'true' or 'false'"},
             {"snapshot_lock", "TRUE", "'snapshot_lock' must be 'true' or 'false'"},
             {"snapshot_lock_wait_s", "0", "'snapshot_lock_wait_s' must be a whole number from 1"},
             {"heartbeat_ms", "1s", "'heartbeat_ms' must be a whole number from 1 to 3600000"},
             {"start_pos", "3", "'start_pos' must be a whole number from 4"},
             {"start_file", "../mysql-bin.000001", "'start_file' is not a binlog file name"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(thrown([&] { (void)f->build(c); }).find(rule), std::string::npos)
            << key << "=" << value << ": " << thrown([&] { (void)f->build(c); });
    }
}

TEST(MysqlFactoryRegistration, TheChangeStreamDeclaresItsOwnRecord) {
    const auto* cdc = clink::connectors::CapabilityRegistry::instance().find("mysql_cdc");
    ASSERT_NE(cdc, nullptr);
    EXPECT_TRUE(cdc->self_check().empty());
    EXPECT_TRUE(cdc->is_source);
    EXPECT_FALSE(cdc->is_sink);
    EXPECT_TRUE(cdc->replayable);
    EXPECT_EQ(cdc->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_EQ(cdc->replay_lost_with_options, std::vector<std::string>{"on_decode_error=drop"});
    // The factory resolves to it, not to the polling source's and sink's record.
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mysql_cdc_source"), "mysql_cdc");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mysql_source"), "mysql");
}

// The sinks read their options strictly too. Building a sink connects to
// nothing, so a value is refused here before any server is involved.
TEST(MysqlFactoryRegistration, TheSinkFactoriesReadTheirOptionsStrictly) {
    for (const char* type : {"mysql_sink", "mysql_upsert_sink"}) {
        const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
            type, std::string{clink::cluster::kChannelString});
        ASSERT_NE(f, nullptr) << type;
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = {{"table", "t"}, {"columns", "id,v"}, {"key_columns", "id"}};
        EXPECT_NO_THROW((void)f->build(ctx)) << type;
        auto every = ctx;
        every.params.insert({{"batch_records", "1000000"},
                             {"max_bytes", "0"},
                             {"max_statement_bytes", "1024"},
                             {"max_record_bytes", "1024"},
                             {"lock_wait_timeout_s", "3600"},
                             {"max_retries", "10"}});
        EXPECT_NO_THROW((void)f->build(every)) << type;
        for (const auto& [key, value, rule] :
             std::vector<std::tuple<std::string, std::string, std::string>>{
                 {"batch_records", "0", "'batch_records' must be a whole number from 1"},
                 {"batch_records", "ten", "'batch_records' must be a whole number"},
                 {"max_bytes", "-1", "'max_bytes' must be a whole number from 0"},
                 {"max_statement_bytes", "1023", "'max_statement_bytes' must be a whole number"},
                 {"max_record_bytes", "8388608", "'max_record_bytes' must be a whole number"},
                 {"lock_wait_timeout_s", "0", "'lock_wait_timeout_s' must be a whole number"},
                 {"max_retries", "11", "'max_retries' must be a whole number from 0 to 10"}}) {
            auto c = ctx;
            c.params[key] = value;
            EXPECT_NE(thrown([&] { (void)f->build(c); }).find(rule), std::string::npos)
                << type << " " << key << "=" << value << ": " << thrown([&] { (void)f->build(c); });
        }
    }
    const auto* append = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "mysql_sink", std::string{clink::cluster::kChannelString});
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"table", "t"}, {"columns", "id,v"}, {"on_duplicate", "updat"}};
    EXPECT_NE(thrown([&] { (void)append->build(ctx); })
                  .find("'on_duplicate' must be empty or "
                        "'update'"),
              std::string::npos);
    ctx.params = {{"table", "t"}, {"columns", "id,v"}, {"update_columns", "v"}};
    EXPECT_NE(thrown([&] { (void)append->build(ctx); }).find("which is not set"),
              std::string::npos);
    ctx.params = {
        {"table", "t"}, {"columns", "id,v"}, {"on_duplicate", "update"}, {"update_columns", "w"}};
    EXPECT_NE(thrown([&] { (void)append->build(ctx); }).find("update column 'w'"),
              std::string::npos);
}

TEST(MysqlFactoryRegistration, TheSinkRecordsStateTheirContracts) {
    const auto* append = clink::connectors::CapabilityRegistry::instance().find("mysql");
    const auto* upsert = clink::connectors::CapabilityRegistry::instance().find("mysql_upsert");
    ASSERT_NE(append, nullptr);
    ASSERT_NE(upsert, nullptr);
    EXPECT_TRUE(append->self_check().empty());
    EXPECT_TRUE(upsert->self_check().empty());
    EXPECT_EQ(append->version, "2");
    EXPECT_EQ(upsert->version, "2");
    EXPECT_EQ(append->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_EQ(upsert->delivery, clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent);
    EXPECT_EQ(upsert->idempotency_key_option, "key_columns");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mysql_upsert_sink"), "mysql_upsert");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mysql_sink"), "mysql");
}

}  // namespace
