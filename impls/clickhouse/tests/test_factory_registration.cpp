// Verifies clink::clickhouse::install() makes every factory reachable
// through the RunnerRegistry: clickhouse_sink + clickhouse_row_source +
// clickhouse_text_source.

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "clink/clickhouse/clickhouse_row_codec.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/clickhouse_row.hpp"

namespace {

using clink::cluster::RunnerRegistry;

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

}  // namespace
