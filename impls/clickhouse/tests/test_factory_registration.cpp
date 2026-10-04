// Verifies clink::clickhouse::install() makes every factory reachable
// through the RunnerRegistry: clickhouse_sink + clickhouse_row_source +
// clickhouse_text_source. The clickhouse_sink factory's option parsing is
// checked through the OperatorRegistry mirror, which hands back the sink it
// built without running it.

#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/clickhouse_builders.hpp"
#include "clink/clickhouse/clickhouse_row_codec.hpp"
#include "clink/clickhouse/native_sink.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/clickhouse_row.hpp"
#include "clink/connectors/clickhouse_sink.hpp"
#include "clink/plugin/plugin.hpp"

// A struct for the typed native sink's registration. CLINK_FIELDS specialises
// a clink template, so it lives at namespace scope.
struct FrTypedRow {
    std::int64_t id;
    std::string s;
};
CLINK_FIELDS(FrTypedRow, id, s);

namespace {

using clink::ClickHouseSink;
using clink::cluster::RunnerRegistry;

std::shared_ptr<ClickHouseSink> build_legacy_clickhouse_sink(
    std::map<std::string, std::string> params) {
    const auto* factory =
        clink::cluster::OperatorRegistry::default_instance().find_sink("clickhouse_sink", "string");
    if (factory == nullptr) {
        throw std::logic_error("clickhouse_sink is not registered");
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = std::move(params);
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(factory->build(ctx));
    auto typed = std::dynamic_pointer_cast<ClickHouseSink>(sink);
    if (!typed) {
        throw std::logic_error("clickhouse_sink built something other than a ClickHouseSink");
    }
    return typed;
}

// A copy of the options the factory built a sink with. A copy, because the sink
// itself goes once this returns.
ClickHouseSink::Options legacy_clickhouse_options(std::map<std::string, std::string> params) {
    return build_legacy_clickhouse_sink(std::move(params))->options();
}

// The factory's refusal for `params`, or a note that it built a sink instead.
std::string legacy_clickhouse_refusal(std::map<std::string, std::string> params) {
    try {
        build_legacy_clickhouse_sink(std::move(params));
    } catch (const std::runtime_error& e) {
        return e.what();
    }
    return "(built)";
}

std::map<std::string, std::string> legacy_clickhouse_params(const std::string& key,
                                                            const std::string& value) {
    return {{"table", "events"}, {key, value}};
}

TEST(ClickHouseFactoryRegistration, ClickHouseSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("clickhouse_sink", "string"), nullptr);
}

TEST(ClickHouseFactoryRegistration, ClickHouseTextSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("clickhouse_text_source", "string"), nullptr);
}

TEST(ClickHouseFactoryRegistration, ClickHouseJsonSourceIsRegistered) {
    // M2: the JSON source on the string channel (delimited text source kept).
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("clickhouse_source", "string"), nullptr);
}

TEST(ClickHouseFactoryRegistration, ClickHouseRowSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("clickhouse_row_source", "clickhouse.row"), nullptr);
}

TEST(ClickHouseRowCodec, RoundTripsNullMask) {
    // M5: the per-cell null mask must survive the codec (else a typed-channel
    // round-trip would silently lose NULLs).
    auto names = std::make_shared<std::vector<std::string>>(std::vector<std::string>{"id", "val"});
    auto types = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"Int64", "Nullable(String)"});
    clink::ClickHouseRow in{
        names, types, std::vector<std::string>{"1", ""}, std::vector<char>{0, 1}};  // val IS NULL

    const auto codec = clink::clickhouse_row_codec();
    auto round = codec.decode(codec.encode(in));
    ASSERT_TRUE(round.has_value());
    EXPECT_FALSE(round->is_null(0));
    EXPECT_TRUE(round->is_null(1)) << "NULL must survive the codec";
    EXPECT_EQ(round->values(), (std::vector<std::string>{"1", ""}));
}

TEST(ClickHouseRowCodec, LegacyThreeArgRowHasNoNulls) {
    clink::ClickHouseRow in{nullptr, nullptr, std::vector<std::string>{"a", "b"}};
    const auto codec = clink::clickhouse_row_codec();
    auto round = codec.decode(codec.encode(in));
    ASSERT_TRUE(round.has_value());
    EXPECT_FALSE(round->is_null(0));
    EXPECT_FALSE(round->is_null(1));
}

TEST(ClickHouseSinkFactory, OnlyTheTableIsRequiredAndEveryOtherKeyHasItsDefault) {
    const auto o = legacy_clickhouse_options({{"table", "events"}});
    EXPECT_EQ(o.host, "localhost");
    EXPECT_EQ(o.port, 9000);
    EXPECT_EQ(o.database, "default");
    EXPECT_EQ(o.table, "events");
    EXPECT_EQ(o.user, "default");
    EXPECT_EQ(o.password, "");
    EXPECT_EQ(o.format, ClickHouseSink::Format::TSV);
    EXPECT_EQ(o.unrecognised_format, "");
    EXPECT_EQ(o.batch_rows, 1000U);
    EXPECT_EQ(o.batch_interval, std::chrono::milliseconds{1000});
    EXPECT_EQ(o.connect_timeout, std::chrono::milliseconds{5000});
    EXPECT_EQ(o.send_timeout, std::chrono::milliseconds{30000});
    EXPECT_EQ(o.receive_timeout, std::chrono::milliseconds{30000});

    EXPECT_EQ(legacy_clickhouse_refusal({}), "clickhouse_sink: 'table' is required");
    EXPECT_EQ(legacy_clickhouse_refusal({{"table", ""}}), "clickhouse_sink: 'table' is required");
}

TEST(ClickHouseSinkFactory, EveryKeyIsParsed) {
    const auto o = legacy_clickhouse_options({{"host", "ch.internal"},
                                              {"port", "9440"},
                                              {"database", "analytics"},
                                              {"table", "events"},
                                              {"user", "writer"},
                                              {"password", "s3cret"},
                                              {"format", "json"},
                                              {"batch_rows", "250"},
                                              {"batch_interval_ms", "1500"},
                                              {"connect_timeout_ms", "1200"},
                                              {"send_timeout_ms", "4500"},
                                              {"receive_timeout_ms", "6700"}});
    EXPECT_EQ(o.host, "ch.internal");
    EXPECT_EQ(o.port, 9440);
    EXPECT_EQ(o.database, "analytics");
    EXPECT_EQ(o.table, "events");
    EXPECT_EQ(o.user, "writer");
    EXPECT_EQ(o.password, "s3cret");
    EXPECT_EQ(o.format, ClickHouseSink::Format::JSONEachRow);
    EXPECT_EQ(o.batch_rows, 250U);
    EXPECT_EQ(o.batch_interval, std::chrono::milliseconds{1500});
    EXPECT_EQ(o.connect_timeout, std::chrono::milliseconds{1200});
    EXPECT_EQ(o.send_timeout, std::chrono::milliseconds{4500});
    EXPECT_EQ(o.receive_timeout, std::chrono::milliseconds{6700});
}

// The capability record has always claimed `json`, but the factory compared
// only the two spellings of jsoneachrow, so format='json' wrote TSV.
TEST(ClickHouseSinkFactory, FormatIsComparedWithoutCaseAndJsonSelectsJsonEachRow) {
    for (const char* v : {"tsv", "TSV", "Tsv"}) {
        const auto o = legacy_clickhouse_options(legacy_clickhouse_params("format", v));
        EXPECT_EQ(o.format, ClickHouseSink::Format::TSV) << v;
        EXPECT_EQ(o.unrecognised_format, "") << v;
    }
    for (const char* v : {"json", "JSON", "Json", "jsoneachrow", "JSONEachRow", "JSONEACHROW"}) {
        const auto o = legacy_clickhouse_options(legacy_clickhouse_params("format", v));
        EXPECT_EQ(o.format, ClickHouseSink::Format::JSONEachRow) << v;
        EXPECT_EQ(o.unrecognised_format, "") << v;
    }
}

// The builder's format() takes any string, and such a job ran before, writing
// TSV. It keeps running and keeps writing TSV; the value goes to open's warning.
TEST(ClickHouseSinkFactory, AnUnrecognisedFormatKeepsTsvAndIsCarriedToOpen) {
    for (const char* v : {"csv", "Parquet", "json_each_row", ""}) {
        const auto o = legacy_clickhouse_options(legacy_clickhouse_params("format", v));
        EXPECT_EQ(o.format, ClickHouseSink::Format::TSV) << v;
        EXPECT_EQ(o.unrecognised_format, v);
    }
}

// What the tutorial's CREATE TABLE reaches the factory as: the planner strips
// format='json' as the channel selector and forces jsoneachrow.
TEST(ClickHouseSinkFactory, TheTutorialTableStillBuilds) {
    const auto o = legacy_clickhouse_options({{"format", "jsoneachrow"},
                                              {"host", "clickhouse"},
                                              {"port", "9000"},
                                              {"database", "default"},
                                              {"table", "sensor_window_stats"},
                                              {"user", "clink"},
                                              {"password", "clink"},
                                              {"batch_rows", "1"}});
    EXPECT_EQ(o.format, ClickHouseSink::Format::JSONEachRow);
    EXPECT_EQ(o.batch_rows, 1U);
    EXPECT_EQ(o.port, 9000);
    EXPECT_EQ(o.table, "sensor_window_stats");
}

TEST(ClickHouseSinkFactory, TheBuilderPathSelectsJsonEachRowForJson) {
    const auto d = clink::api::ClickHouseSink::builder()
                       .table("events")
                       .format("json")
                       .batch_rows(50)
                       .batch_interval_ms(200)
                       .build();
    const auto o = legacy_clickhouse_options(d.params);
    EXPECT_EQ(o.format, ClickHouseSink::Format::JSONEachRow);
    EXPECT_EQ(o.batch_rows, 50U);
    EXPECT_EQ(o.batch_interval, std::chrono::milliseconds{200});
}

// Keys the planner and the materialised-view code put on the op are not the
// sink's business, and refusing them would break every SQL job.
TEST(ClickHouseSinkFactory, KeysItDoesNotKnowAreIgnored) {
    EXPECT_EQ(legacy_clickhouse_refusal({{"table", "events"},
                                         {"schema_columns", "a,b"},
                                         {"insert_format", "jsoneachrow"},
                                         {"query", "SELECT 1"}}),
              "(built)");
}

// param_int64_or fell back to the default on garbage and the result was cast
// down, so batch_rows='0' flushed on every row and a negative value never
// flushed by count at all.
TEST(ClickHouseSinkFactory, BatchRowsMustBeAPositiveInteger) {
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("batch_rows", "0")),
              "clickhouse_sink: batch_rows must be a positive integer (got '0')");
    for (const char* v : {"-1", "-0", "abc", "10x", " 10", "10 ", "+10", "1.5", "1e3", ""}) {
        EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("batch_rows", v)),
                  std::string("clickhouse_sink: batch_rows must be a positive integer (got '") + v +
                      "')");
    }
    // Past int64 the value cannot be held at all.
    EXPECT_EQ(
        legacy_clickhouse_refusal(legacy_clickhouse_params("batch_rows", "99999999999999999999")),
        "clickhouse_sink: batch_rows must be at most 9223372036854775807 (got "
        "'99999999999999999999')");
    EXPECT_EQ(
        legacy_clickhouse_refusal(legacy_clickhouse_params("batch_rows", "-99999999999999999999")),
        "clickhouse_sink: batch_rows must be a positive integer (got "
        "'-99999999999999999999')");
    EXPECT_EQ(legacy_clickhouse_options(legacy_clickhouse_params("batch_rows", "1")).batch_rows,
              1U);
    // The Stable builder has always passed any count through, so a large one
    // still builds.
    EXPECT_EQ(
        legacy_clickhouse_options(legacy_clickhouse_params("batch_rows", "3000000000")).batch_rows,
        3000000000U);
}

TEST(ClickHouseSinkFactory, BatchIntervalMustBeAPositiveInteger) {
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("batch_interval_ms", "0")),
              "clickhouse_sink: batch_interval_ms must be a positive integer (got '0')");
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("batch_interval_ms", "-5")),
              "clickhouse_sink: batch_interval_ms must be a positive integer (got '-5')");
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("batch_interval_ms", "soon")),
              "clickhouse_sink: batch_interval_ms must be a positive integer (got 'soon')");

    EXPECT_EQ(legacy_clickhouse_options(legacy_clickhouse_params("batch_interval_ms", "1"))
                  .batch_interval,
              std::chrono::milliseconds{1});
    // Two hours: the Stable builder accepted it before the parse became
    // strict, so it still builds.
    EXPECT_EQ(legacy_clickhouse_options(legacy_clickhouse_params("batch_interval_ms", "7200000"))
                  .batch_interval,
              std::chrono::milliseconds{7200000});
}

// The port went through a truncating cast: 70000 connected to 4464.
TEST(ClickHouseSinkFactory, PortMustBeFromOneTo65535) {
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("port", "0")),
              "clickhouse_sink: port must be a positive integer (got '0')");
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("port", "nine")),
              "clickhouse_sink: port must be a positive integer (got 'nine')");
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("port", "65536")),
              "clickhouse_sink: port must be at most 65535 (got '65536')");
    EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params("port", "70000")),
              "clickhouse_sink: port must be at most 65535 (got '70000')");
    EXPECT_EQ(legacy_clickhouse_options(legacy_clickhouse_params("port", "1")).port, 1);
    EXPECT_EQ(legacy_clickhouse_options(legacy_clickhouse_params("port", "65535")).port, 65535);
}

TEST(ClickHouseSinkFactory, EachTimeoutMustBeFromOneMillisecondToTenMinutes) {
    for (const char* key : {"connect_timeout_ms", "send_timeout_ms", "receive_timeout_ms"}) {
        const std::string k = key;
        EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params(k, "0")),
                  "clickhouse_sink: " + k + " must be a positive integer (got '0')");
        EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params(k, "-1")),
                  "clickhouse_sink: " + k + " must be a positive integer (got '-1')");
        EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params(k, "5s")),
                  "clickhouse_sink: " + k + " must be a positive integer (got '5s')");
        EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params(k, "600001")),
                  "clickhouse_sink: " + k + " must be at most 600000 (got '600001')");
        EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params(k, "1")), "(built)") << k;
        EXPECT_EQ(legacy_clickhouse_refusal(legacy_clickhouse_params(k, "600000")), "(built)") << k;
    }
}

// Integer keys resolve env:// like every other key, and a refusal shows the
// reference as written rather than whatever the variable holds.
TEST(ClickHouseSinkFactory, IntegerKeysResolveEnvironmentReferences) {
    ::setenv("CLINK_TEST_LEGACY_CH_PORT", "9123", 1);
    EXPECT_EQ(legacy_clickhouse_options(
                  legacy_clickhouse_params("port", "env://CLINK_TEST_LEGACY_CH_PORT"))
                  .port,
              9123);
    ::setenv("CLINK_TEST_LEGACY_CH_PORT", "not-a-port", 1);
    EXPECT_EQ(legacy_clickhouse_refusal(
                  legacy_clickhouse_params("port", "env://CLINK_TEST_LEGACY_CH_PORT")),
              "clickhouse_sink: port must be a positive integer (got "
              "'env://CLINK_TEST_LEGACY_CH_PORT')");
    ::unsetenv("CLINK_TEST_LEGACY_CH_PORT");
    EXPECT_EQ(legacy_clickhouse_refusal(
                  legacy_clickhouse_params("port", "env://CLINK_TEST_LEGACY_CH_PORT")),
              "clickhouse_sink: port must be a positive integer (got "
              "'env://CLINK_TEST_LEGACY_CH_PORT')");
}

// The typed helper registers whatever the build: its op type keeps the native
// sink's prefix first, on the channel the struct was registered under.
TEST(ClickHouseFactoryRegistration, TheTypedNativeSinkRegistersPrefixFirstOnItsStructsChannel) {
    clink::plugin::PluginRegistry reg;
    reg.register_type<FrTypedRow>();
    clink::clickhouse::register_clickhouse_native_sink<FrTypedRow>(reg);
    EXPECT_NE(RunnerRegistry::default_instance().find_sink("clickhouse_native_sink_FrTypedRow",
                                                           "FrTypedRow"),
              nullptr);
}

#if !defined(CLINK_CLICKHOUSE_NATIVE)
// Without the native sink the typed helper still compiles and links, and
// making the sink refuses by name, as the SQL factory does.
TEST(ClickHouseFactoryRegistration, WithoutTheNativeSinkTheTypedHelperRefusesNativeUnavailable) {
    try {
        (void)clink::clickhouse::make_clickhouse_native_sink<FrTypedRow>({{"table", "t"}});
        ADD_FAILURE() << "a build without the native sink made one";
    } catch (const std::runtime_error& e) {
        EXPECT_EQ(std::string(e.what()).rfind("[clickhouse.native_unavailable] ", 0), 0U)
            << e.what();
    }
}
#else
// With it, making the sink parses the options and connects to nothing.
TEST(ClickHouseFactoryRegistration, WithTheNativeSinkTheTypedHelperMakesASinkThatGatesTheAck) {
    const auto sink = clink::clickhouse::make_clickhouse_native_sink<FrTypedRow>(
        {{"table", "t"}, {"host", "127.0.0.1"}, {"port", "1"}});
    EXPECT_EQ(sink->name(), "clickhouse_native_sink");
    EXPECT_TRUE(sink->gates_checkpoint_ack());
}
#endif

TEST(ClickHouseSinkFactory, TheRecordListsTheTimeoutKeysAndKeepsItsFormats) {
    const auto* rec = clink::connectors::CapabilityRegistry::instance().find("clickhouse");
    ASSERT_NE(rec, nullptr);
    EXPECT_EQ(
        rec->timeout_options,
        (std::vector<std::string>{
            "connect_timeout_ms", "send_timeout_ms", "receive_timeout_ms", "batch_interval_ms"}));
    EXPECT_EQ(rec->formats, (std::vector<std::string>{"tsv", "json"}));
    EXPECT_EQ(rec->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
}

}  // namespace
