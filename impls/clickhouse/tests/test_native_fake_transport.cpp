// The fake server and transport's own semantics, pinned before any sink test
// relies on them: what lands and when, the deduplication log, the landing
// modes, the counters a sink test asserts on, the client's call order, which
// column types the server takes, interrupt, release, outages, endpoint
// routing and the deadline.

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/json/from_string.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>
#include <gtest/gtest.h>

#include "fake_transport.hpp"
#include "native/arrow_to_block.hpp"
#include "native/column_plan.hpp"
#include "native/sql_text.hpp"
#include "native/statements.hpp"
#include "native/types.hpp"

namespace {

using namespace std::chrono_literals;
namespace ch = ::clickhouse;
using clink::clickhouse::native::ColumnPlan;
using clink::clickhouse::native::DefaultKind;
using clink::clickhouse::native::Endpoint;
using clink::clickhouse::native::InsertText;
using clink::clickhouse::native::InsertTransport;
using clink::clickhouse::native::MetaQuery;
using clink::clickhouse::native::ResultSet;
using clink::clickhouse::native::ServerIdentity;
using clink::clickhouse::native::SinkOptions;
using clink::clickhouse::native::Token;
using clink::clickhouse::native::TokenSource;
using clink::clickhouse::native::testing::FakeServer;
using clink::clickhouse::native::testing::FakeTable;
using clink::clickhouse::native::testing::FakeTransport;
using clink::clickhouse::native::testing::Fault;
using clink::clickhouse::native::testing::LandedBlock;
using clink::clickhouse::native::testing::ReceivedInsert;
using clink::clickhouse::native::testing::Step;
namespace native = clink::clickhouse::native;
namespace fake = clink::clickhouse::native::testing;
using FtClock = std::chrono::steady_clock;
using FtRows = std::vector<std::vector<std::string>>;

constexpr std::chrono::seconds kFtBudget{1};
const Endpoint kFtEp1{"ch-1", 9000};
const Endpoint kFtEp2{"ch-2", 9000};
const Endpoint kFtEp3{"ch-3", 9000};

FakeTable ft_events(std::size_t window = 0) {
    FakeTable t;
    t.database = "db";
    t.name = "events";
    t.engine = "MergeTree";
    t.engine_full = "MergeTree ORDER BY id SETTINGS index_granularity = 8192";
    t.columns = {{"id", "Int64", DefaultKind::None, 1}, {"s", "String", DefaultKind::None, 2}};
    t.dedup_window = window;
    return t;
}

std::shared_ptr<FakeServer> ft_server_with(const FakeTable& table) {
    auto server = std::make_shared<FakeServer>();
    server->add_table(table);
    return server;
}

TokenSource ft_tokens(std::uint8_t seed = 1) {
    std::array<std::uint8_t, 16> nonce{};
    for (std::size_t i = 0; i < nonce.size(); ++i) {
        nonce[i] = static_cast<std::uint8_t>(seed + i);
    }
    return TokenSource(nonce);
}

// The statement the sink sends, from the real builder, for `table` in `db`.
std::string ft_insert_sql(const Token& token,
                          const std::string& table = "events",
                          const std::string& column_list = "(`id`, `s`)") {
    ColumnPlan plan;
    plan.column_list_sql = column_list;
    InsertText in;
    in.database = "db";
    in.table = table;
    in.plan = &plan;
    in.token = token;
    in.sink_id = "fake-test";
    return native::insert_statement(in);
}

ch::Block ft_block(const std::vector<std::int64_t>& ids, const std::vector<std::string>& ss) {
    auto id = std::make_shared<ch::ColumnInt64>();
    auto s = std::make_shared<ch::ColumnString>();
    for (const auto v : ids) {
        id->Append(v);
    }
    for (const auto& v : ss) {
        s->Append(v);
    }
    ch::Block block;
    block.AppendColumn("id", id);
    block.AppendColumn("s", s);
    block.RefreshRowCount();
    return block;
}

ch::Block ft_ids(std::int64_t from, std::int64_t to) {
    std::vector<std::int64_t> ids;
    std::vector<std::string> ss;
    for (std::int64_t v = from; v < to; ++v) {
        ids.push_back(v);
        ss.push_back("v" + std::to_string(v));
    }
    return ft_block(ids, ss);
}

// One whole INSERT: begin, every block, end.
void ft_insert(InsertTransport& t, const std::string& sql, const std::vector<ch::Block>& blocks) {
    (void)t.begin_insert(sql);
    for (const auto& b : blocks) {
        t.send_block(b);
    }
    t.end_insert();
}

std::vector<std::string> ft_tokens_of(const std::vector<LandedBlock>& landed) {
    std::vector<std::string> out;
    for (const auto& b : landed) {
        out.push_back(b.token);
    }
    return out;
}

template <typename F>
std::optional<int> ft_errno(F&& f) {
    try {
        f();
    } catch (const std::system_error& e) {
        return e.code().value();
    }
    return std::nullopt;
}

struct FtServerError {
    int code{0};
    std::string text;
};

template <typename F>
std::optional<FtServerError> ft_server_error(F&& f) {
    try {
        f();
    } catch (const ch::ServerException& e) {
        return FtServerError{e.GetCode(), e.what()};
    }
    return std::nullopt;
}

std::map<std::string, std::string> ft_as_map(const ResultSet& rs) {
    std::map<std::string, std::string> out;
    for (const auto& row : rs.rows) {
        out[row.at(0)] = row.at(1);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Metadata

TEST(NativeFakeServer, NewServerIsA268LineListingEverySettingTheSinkReads) {
    auto server = std::make_shared<FakeServer>();
    FakeTransport t(server);
    t.connect(kFtEp1);

    const ServerIdentity& id = t.server();
    EXPECT_EQ(id.display_name, server->display_name());
    EXPECT_TRUE(id.display_name.starts_with("fake-")) << id.display_name;
    EXPECT_EQ(id.major, 26u);
    EXPECT_EQ(id.minor, 8u);
    EXPECT_EQ(id.patch, 1u);
    EXPECT_EQ(id.endpoint, kFtEp1);

    const std::string sql = native::select_server_settings(kFtBudget);
    const ResultSet rs = t.select(MetaQuery::ServerSettings, sql);
    const auto got = ft_as_map(rs);
    for (const auto& name : native::required_settings()) {
        EXPECT_TRUE(got.contains(name)) << name;
    }
    for (const auto& name : native::line_conditional_settings()) {
        EXPECT_TRUE(got.contains(name)) << name;
    }
    EXPECT_EQ(got.at("min_insert_block_size_rows"), "1048449");
    EXPECT_EQ(got.at("min_insert_block_size_bytes"), "268402944");
    EXPECT_EQ(got.at("deduplicate_insert"), "enable");
    EXPECT_EQ(got.at("use_strict_insert_block_limits"), "0");
    EXPECT_EQ(got.at("insert_deduplicate"), "1");
    EXPECT_EQ(got.at("insert_quorum"), "0");
    EXPECT_EQ(got.at("max_partitions_per_insert_block"), "100");
    EXPECT_EQ(rs.rows.size(), 17u);
    EXPECT_EQ(rs.columns, (std::vector<std::string>{"name", "value"}));
    EXPECT_EQ(server->statements(), std::vector<std::string>{sql});
}

TEST(NativeFakeServer, SettingsListingFollowsTheConfigurationAndTheNamesAskedFor) {
    auto server = std::make_shared<FakeServer>();
    server->remove_setting("use_strict_insert_block_limits");
    server->set_setting("insert_quorum", "2");
    server->set_setting("not_asked_for", "1");
    FakeTransport t(server);
    t.connect(kFtEp1);

    auto got =
        ft_as_map(t.select(MetaQuery::ServerSettings, native::select_server_settings(kFtBudget)));
    EXPECT_FALSE(got.contains("use_strict_insert_block_limits"));
    EXPECT_FALSE(got.contains("not_asked_for"));
    EXPECT_EQ(got.at("insert_quorum"), "2");
    EXPECT_EQ(got.size(), 16u);

    server->set_settings({{"async_insert", "0"}, {"log_comment", ""}});
    const ResultSet rs =
        t.select(MetaQuery::ServerSettings, native::select_server_settings(kFtBudget));
    EXPECT_EQ(rs.rows, (FtRows{{"async_insert", "0"}, {"log_comment", ""}}));
}

TEST(NativeFakeServer, AnswersTableAndColumnQueriesFromTheConfiguredTable) {
    FakeTable table;
    table.database = "db";
    table.name = "events";
    table.engine = "ReplicatedMergeTree";
    table.engine_full =
        "ReplicatedMergeTree('/t/events', '{replica}') ORDER BY id SETTINGS async_insert = 1";
    table.columns = {{"id", "Int64", DefaultKind::None, 0},
                     {"ts", "DateTime64(3)", DefaultKind::Default, 0},
                     {"m", "Int64", DefaultKind::Materialized, 0},
                     {"e", "String", DefaultKind::Ephemeral, 0},
                     {"a", "String", DefaultKind::Alias, 0}};
    auto server = ft_server_with(table);
    FakeTable other = ft_events();
    other.name = "positions";
    other.columns = {{"second", "String", DefaultKind::None, 2},
                     {"first", "Int32", DefaultKind::None, 1}};
    server->add_table(other);
    FakeTransport t(server);
    t.connect(kFtEp1);

    EXPECT_EQ(t.select(MetaQuery::Table, native::select_table("db", "events", kFtBudget)).rows,
              (FtRows{{"ReplicatedMergeTree", table.engine_full}}));
    EXPECT_TRUE(
        t.select(MetaQuery::Table, native::select_table("db", "missing", kFtBudget)).rows.empty());
    EXPECT_TRUE(t.select(MetaQuery::Table, native::select_table("other", "events", kFtBudget))
                    .rows.empty());

    EXPECT_EQ(t.select(MetaQuery::Columns, native::select_columns("db", "events", kFtBudget)).rows,
              (FtRows{{"id", "Int64", "", "1"},
                      {"ts", "DateTime64(3)", "DEFAULT", "2"},
                      {"m", "Int64", "MATERIALIZED", "3"},
                      {"e", "String", "EPHEMERAL", "4"},
                      {"a", "String", "ALIAS", "5"}}));
    // Rows come in position order, as ORDER BY position asks.
    EXPECT_EQ(
        t.select(MetaQuery::Columns, native::select_columns("db", "positions", kFtBudget)).rows,
        (FtRows{{"first", "Int32", "", "1"}, {"second", "String", "", "2"}}));
}

TEST(NativeFakeServer, MetadataTextIsReadThroughTheStatementsQuoting) {
    FakeTable table = ft_events();
    table.database = "it's";
    table.name = "back\\slash`tick";
    auto server = ft_server_with(table);
    FakeTransport t(server);
    t.connect(kFtEp1);
    EXPECT_EQ(
        t.select(MetaQuery::Table, native::select_table(table.database, table.name, kFtBudget))
            .rows.size(),
        1u);
}

TEST(NativeFakeServer, MergeTreeSettingsAreTheLineDefaultsUntilConfigured) {
    auto server = std::make_shared<FakeServer>();
    FakeTransport t(server);
    t.connect(kFtEp1);
    const std::string sql = native::select_merge_tree_settings(kFtBudget);

    EXPECT_EQ(t.select(MetaQuery::MergeTreeSettings, sql).rows,
              (FtRows{{"async_insert", "0"},
                      {"non_replicated_deduplication_window", "0"},
                      {"replicated_deduplication_window", "10000"}}));

    server->set_merge_tree_setting("async_insert", "1");
    server->remove_merge_tree_setting("non_replicated_deduplication_window");
    server->set_merge_tree_setting("max_parts_in_total", "100000");  // not asked for
    EXPECT_EQ(t.select(MetaQuery::MergeTreeSettings, sql).rows,
              (FtRows{{"async_insert", "1"}, {"replicated_deduplication_window", "10000"}}));
}

TEST(NativeFakeServer, ReplicatedMergeTreeSettingsOverlayTheMergeTreeDefaults) {
    auto server = std::make_shared<FakeServer>();
    FakeTransport t(server);
    t.connect(kFtEp1);
    const std::string sql = native::select_replicated_merge_tree_settings(kFtBudget);

    server->set_merge_tree_setting("async_insert", "1");
    EXPECT_EQ(ft_as_map(t.select(MetaQuery::ReplicatedMergeTreeSettings, sql)).at("async_insert"),
              "1");

    server->set_replicated_merge_tree_setting("async_insert", "0");
    server->set_replicated_merge_tree_setting("replicated_deduplication_window", "50");
    const auto replicated = ft_as_map(t.select(MetaQuery::ReplicatedMergeTreeSettings, sql));
    EXPECT_EQ(replicated.at("async_insert"), "0");
    EXPECT_EQ(replicated.at("replicated_deduplication_window"), "50");
    EXPECT_EQ(ft_as_map(t.select(MetaQuery::MergeTreeSettings,
                                 native::select_merge_tree_settings(kFtBudget)))
                  .at("async_insert"),
              "1");

    server->set_replicated_merge_tree_settings_table(false);
    const auto err =
        ft_server_error([&] { (void)t.select(MetaQuery::ReplicatedMergeTreeSettings, sql); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::UNKNOWN_TABLE);
    EXPECT_EQ(err->text, "Table system.replicated_merge_tree_settings does not exist");
}

struct FtCluster {
    std::shared_ptr<FakeServer> initiator = std::make_shared<FakeServer>();
    std::shared_ptr<FakeServer> r1 = std::make_shared<FakeServer>();
    std::shared_ptr<FakeServer> r2 = std::make_shared<FakeServer>();
    std::shared_ptr<FakeServer> r3 = std::make_shared<FakeServer>();
    FakeTable local = [] {
        FakeTable t = ft_events();
        t.name = "events_local";
        return t;
    }();

    FtCluster() {
        r1->add_table(local);
        FakeTable second = local;
        second.engine = "ReplicatedMergeTree";
        second.engine_full = "ReplicatedMergeTree('/t', 'r2') ORDER BY id";
        r2->add_table(second);
        // r3 has no such table.
        r2->set_merge_tree_setting("async_insert", "1");
        initiator->add_cluster("c", {r1.get(), r2.get(), r3.get()});
    }
};

TEST(NativeFakeServer, ClusterReadsAskEveryReplicaUnderItsNameAndUuid) {
    FtCluster c;
    FakeTransport t(c.initiator);
    t.connect(kFtEp1);

    EXPECT_EQ(t.select(MetaQuery::ClusterReplicaCount,
                       native::select_cluster_replica_count("c", kFtBudget))
                  .rows,
              (FtRows{{"3"}}));
    EXPECT_EQ(t.select(MetaQuery::ClusterReplicaCount,
                       native::select_cluster_replica_count("nope", kFtBudget))
                  .rows,
              (FtRows{{"0"}}));

    // The replica without the table contributes no row.
    EXPECT_EQ(
        t.select(MetaQuery::ClusterTables,
                 native::select_cluster_tables("c", "db", "events_local", kFtBudget))
            .rows,
        (FtRows{
            {c.r1->display_name() + ":9000", c.r1->server_uuid(), "MergeTree", c.local.engine_full},
            {c.r2->display_name() + ":9000",
             c.r2->server_uuid(),
             "ReplicatedMergeTree",
             "ReplicatedMergeTree('/t', 'r2') ORDER BY id"}}));

    const ResultSet mts = t.select(MetaQuery::ClusterMergeTreeSettings,
                                   native::select_cluster_merge_tree_settings("c", kFtBudget));
    ASSERT_EQ(mts.rows.size(), 9u);
    EXPECT_EQ(
        mts.rows[0],
        (std::vector<std::string>{c.r1->replica_name(), c.r1->server_uuid(), "async_insert", "0"}));
    EXPECT_EQ(
        mts.rows[3],
        (std::vector<std::string>{c.r2->replica_name(), c.r2->server_uuid(), "async_insert", "1"}));
    EXPECT_EQ(
        mts.rows[6],
        (std::vector<std::string>{c.r3->replica_name(), c.r3->server_uuid(), "async_insert", "0"}));

    const auto err = ft_server_error([&] {
        (void)t.select(MetaQuery::ClusterTables,
                       native::select_cluster_tables("nope", "db", "t", kFtBudget));
    });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::CLUSTER_DOESNT_EXIST);
}

TEST(NativeFakeServer, AnUnreadableReplicaFailsEveryClusterReadWith279UntilCleared) {
    FtCluster c;
    FakeTransport t(c.initiator);
    t.connect(kFtEp1);
    c.initiator->set_unreadable_replica("c", 1);

    for (const auto& [kind, sql] : std::vector<std::pair<MetaQuery, std::string>>{
             {MetaQuery::ClusterTables,
              native::select_cluster_tables("c", "db", "events_local", kFtBudget)},
             {MetaQuery::ClusterMergeTreeSettings,
              native::select_cluster_merge_tree_settings("c", kFtBudget)},
             {MetaQuery::ClusterReplicatedMergeTreeSettings,
              native::select_cluster_replicated_merge_tree_settings("c", kFtBudget)}}) {
        const auto err = ft_server_error([&] { (void)t.select(kind, sql); });
        ASSERT_TRUE(err.has_value()) << sql;
        EXPECT_EQ(err->code, ch::ALL_CONNECTION_TRIES_FAILED);
        EXPECT_NE(err->text.find(c.r2->display_name()), std::string::npos) << err->text;
    }
    // system.clusters is the initiator's own table, so it still reads.
    EXPECT_EQ(t.select(MetaQuery::ClusterReplicaCount,
                       native::select_cluster_replica_count("c", kFtBudget))
                  .rows,
              (FtRows{{"3"}}));

    c.initiator->set_unreadable_replica("c", 1, false);
    EXPECT_EQ(t.select(MetaQuery::ClusterTables,
                       native::select_cluster_tables("c", "db", "events_local", kFtBudget))
                  .rows.size(),
              2u);
}

TEST(NativeFakeServer, ClusterReplicatedSettingsFailWhereAReplicaLacksThatTable) {
    FtCluster c;
    c.r1->set_replicated_merge_tree_setting("async_insert", "1");
    FakeTransport t(c.initiator);
    t.connect(kFtEp1);
    const std::string sql = native::select_cluster_replicated_merge_tree_settings("c", kFtBudget);

    const ResultSet rs = t.select(MetaQuery::ClusterReplicatedMergeTreeSettings, sql);
    ASSERT_EQ(rs.rows.size(), 9u);
    EXPECT_EQ(
        rs.rows[0],
        (std::vector<std::string>{c.r1->replica_name(), c.r1->server_uuid(), "async_insert", "1"}));
    EXPECT_EQ(rs.columns, (std::vector<std::string>{"host", "uuid", "name", "value"}));

    c.r3->set_replicated_merge_tree_settings_table(false);
    const auto err = ft_server_error(
        [&] { (void)t.select(MetaQuery::ClusterReplicatedMergeTreeSettings, sql); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::UNKNOWN_TABLE);
}

TEST(NativeFakeServer, AClusterThatIncludesTheInitiatorReadsItToo) {
    auto server = ft_server_with(ft_events());
    server->add_cluster("self", {server.get()});
    FakeTransport t(server);
    t.connect(kFtEp1);
    EXPECT_EQ(t.select(MetaQuery::ClusterTables,
                       native::select_cluster_tables("self", "db", "events", kFtBudget))
                  .rows,
              (FtRows{{server->replica_name(),
                       server->server_uuid(),
                       "MergeTree",
                       ft_events().engine_full}}));
}

// Two instances on one machine: hostName() is the same text on both, so only
// the port and the server UUID tell them apart.
TEST(NativeFakeServer, ReplicasSharingAHostNameKeepTheirOwnUuid) {
    FtCluster c;
    c.r1->set_host_name("box");
    c.r2->set_host_name("box");
    c.r2->set_tcp_port(9001);
    EXPECT_EQ(c.r1->replica_name(), "box:9000");
    EXPECT_EQ(c.r2->replica_name(), "box:9001");
    EXPECT_NE(c.r1->server_uuid(), c.r2->server_uuid());
    EXPECT_EQ(c.r1->server_uuid().size(), 36u);
    FakeTransport t(c.initiator);
    t.connect(kFtEp1);
    const ResultSet rs =
        t.select(MetaQuery::ClusterTables,
                 native::select_cluster_tables("c", "db", "events_local", kFtBudget));
    ASSERT_EQ(rs.rows.size(), 2u);
    EXPECT_EQ(rs.rows[0][0], "box:9000");
    EXPECT_EQ(rs.rows[0][1], c.r1->server_uuid());
    EXPECT_EQ(rs.rows[1][0], "box:9001");
    EXPECT_EQ(rs.rows[1][1], c.r2->server_uuid());
    // The display name, which identifies the server in ServerIdentity, is
    // unchanged.
    EXPECT_NE(c.r1->display_name(), "box");
}

TEST(NativeFakeServer, SystemTablesListTheReplicatedSettingsTableWhereItExists) {
    FtCluster c;
    c.r2->set_replicated_merge_tree_settings_table(false);
    FakeTransport t(c.initiator);
    t.connect(kFtEp1);
    const ResultSet rs = t.select(
        MetaQuery::ClusterTables,
        native::select_cluster_tables("c", "system", "replicated_merge_tree_settings", kFtBudget));
    ASSERT_EQ(rs.rows.size(), 2u);
    EXPECT_EQ(rs.rows[0][1], c.r1->server_uuid());
    EXPECT_EQ(rs.rows[1][1], c.r3->server_uuid());
}

TEST(NativeFakeServer, TheMacrosReadListsTheServersMacros) {
    auto server = std::make_shared<FakeServer>();
    FakeTransport t(server);
    t.connect(kFtEp1);
    EXPECT_TRUE(
        t.select(MetaQuery::ClusterReplicaCount, native::select_macros(kFtBudget)).rows.empty());
    server->set_macro("cluster", "main");
    server->set_macro("shard", "01");
    server->set_macro("cluster", "prod");
    EXPECT_EQ(t.select(MetaQuery::ClusterReplicaCount, native::select_macros(kFtBudget)).rows,
              (FtRows{{"cluster", "prod"}, {"shard", "01"}}));
}

// The server's skip_unavailable_shards decides what an unreachable replica
// does to a cluster read: with 0 the read fails, with 1 the replica is left
// out. A statement's own SETTINGS win over the profile.
TEST(NativeFakeServer, SkipUnavailableShardsLeavesAnUnreachableReplicaOut) {
    FtCluster c;
    c.initiator->set_unreadable_replica("c", 0);
    c.initiator->set_setting("skip_unavailable_shards", "1");
    FakeTransport t(c.initiator);
    t.connect(kFtEp1);
    // The probe's statements without their pin, as they were before it.
    const auto unpinned = [](std::string sql) {
        const std::string pin = ", skip_unavailable_shards=0";
        sql.erase(sql.find(pin), pin.size());
        return sql;
    };
    const std::string tables = native::select_cluster_tables("c", "db", "events_local", kFtBudget);
    const ResultSet skipped = t.select(MetaQuery::ClusterTables, unpinned(tables));
    ASSERT_EQ(skipped.rows.size(), 1u);
    EXPECT_EQ(skipped.rows[0][1], c.r2->server_uuid());

    // Pinned to 0, the replica fails the read.
    const auto err = ft_server_error([&] { (void)t.select(MetaQuery::ClusterTables, tables); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::ALL_CONNECTION_TRIES_FAILED);

    // Under the skip, a replica whose read fails for a missing table is left
    // out too; pinned to 0, it fails the read.
    c.initiator->set_unreadable_replica("c", 0, false);
    c.r3->set_replicated_merge_tree_settings_table(false);
    const std::string replicated =
        native::select_cluster_replicated_merge_tree_settings("c", kFtBudget);
    const ResultSet partial =
        t.select(MetaQuery::ClusterReplicatedMergeTreeSettings, unpinned(replicated));
    ASSERT_EQ(partial.rows.size(), 6u);
    EXPECT_EQ(partial.rows[0][1], c.r1->server_uuid());
    EXPECT_EQ(partial.rows[5][1], c.r2->server_uuid());
    const auto missing = ft_server_error(
        [&] { (void)t.select(MetaQuery::ClusterReplicatedMergeTreeSettings, replicated); });
    ASSERT_TRUE(missing.has_value());
    EXPECT_EQ(missing->code, ch::UNKNOWN_TABLE);
}

// ---------------------------------------------------------------------------
// The INSERT

TEST(NativeFakeTransport, HeaderFollowsTheColumnListInTheClientsSpelling) {
    FakeTable table = ft_events();
    table.columns = {{"flag", "Bool", DefaultKind::None, 1},
                     {"amount", "Decimal32(2)", DefaultKind::None, 2},
                     {"ts", "DateTime('UTC')", DefaultKind::None, 3},
                     {"tag", "LowCardinality(String)", DefaultKind::None, 4},
                     {"pair", "Tuple(a Int32, b String)", DefaultKind::None, 5},
                     {"id", "Int64", DefaultKind::None, 6},
                     {"later", "Int64", DefaultKind::Default, 7}};
    auto server = ft_server_with(table);
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();

    const auto header = t.begin_insert(
        ft_insert_sql(tokens.next(), "events", "(`id`, `flag`, `amount`, `ts`, `tag`, `pair`)"));
    ASSERT_EQ(header.size(), 6u);
    const std::vector<std::pair<std::string, std::string>> expected = {
        {"id", "Int64"},
        {"flag", "UInt8"},
        {"amount", "Decimal(9,2)"},
        {"ts", "DateTime('UTC')"},
        {"tag", "LowCardinality(String)"},
        {"pair", "Tuple(a Int32, b String)"}};
    for (std::size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(header[i].name, expected[i].first);
        EXPECT_EQ(header[i].type, expected[i].second);
    }
}

TEST(NativeFakeTransport, BeginInsertRefusesWhatTheServerOrTheClientWould) {
    FakeTable table = ft_events();
    table.columns.push_back({"m", "Int64", DefaultKind::Materialized, 3});
    table.columns.push_back({"wide", "Int256", DefaultKind::None, 4});
    auto server = ft_server_with(table);
    FakeTransport t(server);
    auto tokens = ft_tokens();
    // A refused begin leaves the client inserting, so each attempt gets a
    // new client.
    const auto fresh = [&]() -> FakeTransport& {
        t.abandon();
        t.connect(kFtEp1);
        return t;
    };

    auto err =
        ft_server_error([&] { (void)fresh().begin_insert(ft_insert_sql(tokens.next(), "nope")); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::UNKNOWN_TABLE);
    EXPECT_EQ(err->text, "Table db.nope does not exist");

    err = ft_server_error(
        [&] { (void)fresh().begin_insert(ft_insert_sql(tokens.next(), "events", "(`id`, `x`)")); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::NO_SUCH_COLUMN_IN_TABLE);
    EXPECT_EQ(err->text, "No such column x in table db.events");

    err = ft_server_error(
        [&] { (void)fresh().begin_insert(ft_insert_sql(tokens.next(), "events", "(`id`, `m`)")); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::ILLEGAL_COLUMN);
    EXPECT_EQ(err->text, "Cannot insert column m, because it is MATERIALIZED column");

    try {
        (void)fresh().begin_insert(ft_insert_sql(tokens.next(), "events", "(`id`, `wide`)"));
        ADD_FAILURE() << "an unbuildable header type must throw";
    } catch (const ch::UnimplementedError& e) {
        EXPECT_STREQ(e.what(), "unsupported column type: Int256");
    }

    err = ft_server_error([&] { (void)fresh().begin_insert("SELECT 1"); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::SYNTAX_ERROR);

    // None of those opened an INSERT: abandoning counts nothing, and a new
    // client can begin one.
    EXPECT_NO_THROW((void)fresh().begin_insert(ft_insert_sql(tokens.next())));
    t.abandon();
    EXPECT_EQ(server->abandoned_mid_insert(), 1u);
    EXPECT_EQ(server->inserts("events").size(), 1u);
    EXPECT_EQ(server->destroyed_mid_insert(), 0u);
}

// BeginInsert marks the client Inserting before it sends the query and does
// not undo that when anything fails, so a client whose begin failed runs no
// other query until it is replaced.
TEST(NativeFakeTransport, AFailedBeginLeavesTheClientInsertingUntilAbandon) {
    FakeTable table = ft_events();
    table.columns.push_back({"wide", "Int256", DefaultKind::None, 3});
    auto server = ft_server_with(table);
    auto tokens = ft_tokens();
    const std::string good = ft_insert_sql(tokens.next());
    const std::string settings_sql = native::select_server_settings(kFtBudget);
    const auto begin_fault = [&](Fault::Kind kind, int code) {
        Fault f;
        f.step = Step::Begin;
        f.kind = kind;
        f.code = code;
        f.message = "scripted";
        server->inject(f);
    };
    struct Case {
        std::string what;
        std::function<void()> arm;
        std::string sql;
    };
    const std::vector<Case> cases = {
        {"an unknown table", [] {}, ft_insert_sql(tokens.next(), "missing")},
        {"a header the client cannot build",
         [] {},
         ft_insert_sql(tokens.next(), "events", "(`id`, `wide`)")},
        {"a server error", [&] { begin_fault(Fault::Kind::ServerError, 202); }, good},
        {"a retryable 279", [&] { begin_fault(Fault::Kind::ServerError, 279); }, good},
        {"a broken connection", [&] { begin_fault(Fault::Kind::SystemError, 0); }, good},
        {"a protocol error", [&] { begin_fault(Fault::Kind::ProtocolError, 0); }, good},
    };
    FakeTransport t(server);
    for (const auto& c : cases) {
        SCOPED_TRACE(c.what);
        t.connect(kFtEp1);
        c.arm();
        EXPECT_ANY_THROW((void)t.begin_insert(c.sql));
        try {
            (void)t.begin_insert(good);
            ADD_FAILURE() << "a client whose begin failed must not begin again";
        } catch (const ch::ValidationError& e) {
            EXPECT_STREQ(e.what(), "cannot execute query while executing another operation");
        }
        EXPECT_THROW((void)t.select(MetaQuery::ServerSettings, settings_sql), ch::ValidationError);
        t.abandon();
    }
    EXPECT_EQ(server->abandoned_mid_insert(), 0u);
    EXPECT_TRUE(server->inserts("events").empty());

    // A new client begins at once.
    t.connect(kFtEp1);
    ft_insert(t, good, {ft_ids(0, 2)});
    EXPECT_EQ(server->rows("events"), 2u);
    EXPECT_EQ(server->destroyed_mid_insert(), 0u);
}

TEST(NativeFakeTransport, AfterAFailedBeginBlocksGoNowhereAndTheEndTimesOut) {
    auto server = ft_server_with(ft_events(10));
    auto tokens = ft_tokens();
    FakeTransport t(server);
    t.connect(kFtEp1);
    EXPECT_TRUE(ft_server_error([&] {
                    (void)t.begin_insert(ft_insert_sql(tokens.next(), "missing"));
                }).has_value());

    // The client still writes a block; the server, which already answered
    // that INSERT, throws it away.
    const auto before = t.counters().bytes_written;
    EXPECT_NO_THROW(t.send_block(ft_ids(0, 3)));
    EXPECT_GT(t.counters().bytes_written, before);
    EXPECT_EQ(ft_errno([&] { t.end_insert(); }), ETIMEDOUT);
    EXPECT_EQ(server->rows("events"), 0u);
    EXPECT_TRUE(server->inserts("events").empty());
    // Still inserting, and the connection is now dead.
    EXPECT_THROW((void)t.begin_insert(ft_insert_sql(tokens.next())), ch::ValidationError);
    EXPECT_EQ(ft_errno([&] { t.send_block(ft_ids(0, 1)); }), ECONNRESET);
    t.abandon();
    EXPECT_EQ(server->abandoned_mid_insert(), 0u);
    EXPECT_EQ(server->destroyed_mid_insert(), 0u);
}

TEST(NativeFakeTransport, AClientDestroyedAfterAFailedBeginIsCounted) {
    auto server = ft_server_with(ft_events());
    auto tokens = ft_tokens();
    {
        FakeTransport t(server);
        t.connect(kFtEp1);
        EXPECT_TRUE(ft_server_error([&] {
                        (void)t.begin_insert(ft_insert_sql(tokens.next(), "missing"));
                    }).has_value());
    }
    // Nothing could commit, but the destroy broke the rule that a client is
    // dropped only through abandon().
    EXPECT_EQ(server->destroyed_mid_insert(), 1u);
    EXPECT_EQ(server->rows("events"), 0u);

    {
        FakeTransport t(server);
        t.connect(kFtEp1);
        EXPECT_TRUE(ft_server_error([&] {
                        (void)t.begin_insert(ft_insert_sql(tokens.next(), "missing"));
                    }).has_value());
        t.abandon();
    }
    EXPECT_EQ(server->destroyed_mid_insert(), 1u);
}

TEST(NativeFakeTransport, BlocksAreBufferedUntilEndInsertThenLandSquashed) {
    auto server = ft_server_with(ft_events());
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    const Token token = tokens.next();
    const std::string sql = ft_insert_sql(token);

    (void)t.begin_insert(sql);
    t.send_block(ft_block({1, 2}, {"a", "b"}));
    t.send_block(ft_block({3}, {"c"}));
    EXPECT_EQ(server->rows("events"), 0u);
    EXPECT_TRUE(server->landed("db.events").empty());
    t.end_insert();

    const auto landed = server->landed("events");
    ASSERT_EQ(landed.size(), 1u);
    EXPECT_EQ(landed[0].token, token.text() + "_0");
    EXPECT_EQ(landed[0].rows, 3u);
    EXPECT_EQ(landed[0].values, (FtRows{{"1", "a"}, {"2", "b"}, {"3", "c"}}));
    EXPECT_EQ(server->rows("db.events"), 3u);

    const auto inserts = server->inserts("events");
    ASSERT_EQ(inserts.size(), 1u);
    EXPECT_EQ(inserts[0].token, token.text());
    EXPECT_EQ(inserts[0].sql, sql);
    EXPECT_EQ(inserts[0].outcome, ReceivedInsert::Outcome::Committed);
    ASSERT_EQ(inserts[0].blocks.size(), 2u);
    EXPECT_EQ(inserts[0].blocks[0].rows, 2u);
    EXPECT_EQ(inserts[0].blocks[1].rows, 1u);
    EXPECT_EQ(inserts[0].blocks[1].values, (FtRows{{"3", "c"}}));
    EXPECT_EQ(server->statements(), std::vector<std::string>{sql});

    EXPECT_THROW((void)server->landed("nope"), std::invalid_argument);
    EXPECT_THROW((void)server->rows("db.nope"), std::invalid_argument);
}

TEST(NativeFakeTransport, AnIdenticalResendSerialisesToTheSameBytes) {
    auto server = ft_server_with(ft_events(10));
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    const std::string sql = ft_insert_sql(tokens.next());
    ft_insert(t, sql, {ft_ids(0, 3), ft_ids(3, 5)});
    ft_insert(t, sql, {ft_ids(0, 3), ft_ids(3, 5)});
    ft_insert(t, sql, {ft_ids(0, 2), ft_ids(2, 5)});

    const auto inserts = server->inserts("events");
    ASSERT_EQ(inserts.size(), 3u);
    EXPECT_EQ(inserts[0].blocks[0].bytes, inserts[1].blocks[0].bytes);
    EXPECT_EQ(inserts[0].blocks[1].bytes, inserts[1].blocks[1].bytes);
    // The same rows cut elsewhere are different blocks.
    EXPECT_NE(inserts[0].blocks[0].bytes, inserts[2].blocks[0].bytes);
    EXPECT_FALSE(inserts[0].blocks[0].bytes.empty());
}

TEST(NativeFakeTransport, DedupFifoDropsAKnownResendAndForgetsPastTheWindow) {
    auto server = ft_server_with(ft_events(2));
    auto tokens = ft_tokens();
    const Token a = tokens.next();
    const Token b = tokens.next();
    const Token c = tokens.next();
    FakeTransport t(server);
    t.connect(kFtEp1);

    ft_insert(t, ft_insert_sql(a), {ft_ids(0, 2)});
    ft_insert(t, ft_insert_sql(b), {ft_ids(2, 3)});
    ft_insert(t, ft_insert_sql(a), {ft_ids(0, 2)});  // known: lands nothing, succeeds
    EXPECT_EQ(server->rows("events"), 3u);
    ft_insert(t, ft_insert_sql(c), {ft_ids(3, 4)});  // pushes a out of the window
    ft_insert(t, ft_insert_sql(a), {ft_ids(0, 2)});  // forgotten: lands again
    ft_insert(t, ft_insert_sql(c), {ft_ids(3, 4)});  // still known
    EXPECT_EQ(ft_tokens_of(server->landed("events")),
              (std::vector<std::string>{
                  a.text() + "_0", b.text() + "_0", c.text() + "_0", a.text() + "_0"}));
    EXPECT_EQ(server->rows("events"), 6u);
    EXPECT_EQ(server->inserts("events").size(), 6u);
}

TEST(NativeFakeTransport, WithoutADedupLogEveryResendLands) {
    auto server = ft_server_with(ft_events(0));
    auto tokens = ft_tokens();
    const Token a = tokens.next();
    FakeTransport t(server);
    t.connect(kFtEp1);
    ft_insert(t, ft_insert_sql(a), {ft_ids(0, 2)});
    ft_insert(t, ft_insert_sql(a), {ft_ids(0, 2)});
    EXPECT_EQ(server->rows("events"), 4u);

    // A statement without a token is never deduplicated either.
    auto windowed = ft_server_with(ft_events(10));
    FakeTransport w(windowed);
    w.connect(kFtEp1);
    const std::string untokened = "INSERT INTO `db`.`events` (`id`, `s`) VALUES";
    ft_insert(w, untokened, {ft_ids(0, 2)});
    ft_insert(w, untokened, {ft_ids(0, 2)});
    EXPECT_EQ(ft_tokens_of(windowed->landed("events")), (std::vector<std::string>{"", ""}));
    EXPECT_EQ(windowed->rows("events"), 4u);
}

FakeTable ft_partitioned(std::size_t window) {
    FakeTable t = ft_events(window);
    t.partition_of = [](std::int64_t v) { return v % 4; };
    return t;
}

TEST(NativeFakeTransport, FirstPartitionsLandsAPrefixAndTheResendFillsInTheRest) {
    auto server = ft_server_with(ft_partitioned(100));
    auto tokens = ft_tokens();
    const Token token = tokens.next();
    const std::string sql = ft_insert_sql(token);
    Fault fault;
    fault.step = Step::End;
    fault.kind = Fault::Kind::SystemError;
    fault.landing = Fault::Landing::FirstPartitions;
    fault.landed_partitions = 2;
    server->inject(fault);

    FakeTransport t(server);
    t.connect(kFtEp1);
    (void)t.begin_insert(sql);
    t.send_block(ft_ids(0, 8));
    EXPECT_EQ(ft_errno([&] { t.end_insert(); }), ECONNRESET);

    auto landed = server->landed("events");
    ASSERT_EQ(landed.size(), 2u);
    EXPECT_EQ(landed[0].token, token.text() + "_0");
    EXPECT_EQ(landed[0].values, (FtRows{{"0", "v0"}, {"4", "v4"}}));
    EXPECT_EQ(landed[1].token, token.text() + "_1");
    EXPECT_EQ(landed[1].values, (FtRows{{"1", "v1"}, {"5", "v5"}}));
    EXPECT_EQ(server->rows("events"), 4u);

    // The connection is dead now; the client abandons it and resends.
    EXPECT_EQ(ft_errno([&] { t.send_block(ft_ids(0, 1)); }), ECONNRESET);
    t.abandon();
    t.connect(kFtEp1);
    ft_insert(t, sql, {ft_ids(0, 8)});

    EXPECT_EQ(
        ft_tokens_of(server->landed("events")),
        (std::vector<std::string>{
            token.text() + "_0", token.text() + "_1", token.text() + "_2", token.text() + "_3"}));
    EXPECT_EQ(server->rows("events"), 8u);
    EXPECT_EQ(server->abandoned_mid_insert(), 1u);
    const auto inserts = server->inserts("events");
    ASSERT_EQ(inserts.size(), 2u);
    EXPECT_EQ(inserts[0].outcome, ReceivedInsert::Outcome::Failed);
    EXPECT_EQ(inserts[1].outcome, ReceivedInsert::Outcome::Committed);
}

TEST(NativeFakeTransport, FirstPartitionsWithoutALogDuplicatesTheLandedPrefixOnResend) {
    auto server = ft_server_with(ft_partitioned(0));
    auto tokens = ft_tokens();
    const std::string sql = ft_insert_sql(tokens.next());
    Fault fault;
    fault.step = Step::End;
    fault.kind = Fault::Kind::SystemError;
    fault.landing = Fault::Landing::FirstPartitions;
    fault.landed_partitions = 2;
    server->inject(fault);

    FakeTransport t(server);
    t.connect(kFtEp1);
    EXPECT_EQ(ft_errno([&] { ft_insert(t, sql, {ft_ids(0, 8)}); }), ECONNRESET);
    t.abandon();
    t.connect(kFtEp1);
    ft_insert(t, sql, {ft_ids(0, 8)});
    EXPECT_EQ(server->rows("events"), 12u);
}

TEST(NativeFakeTransport, PartitionsAreOrderedByTheRowThatFirstReachesThem) {
    auto server = ft_server_with(ft_partitioned(10));
    auto tokens = ft_tokens();
    const Token token = tokens.next();
    FakeTransport t(server);
    t.connect(kFtEp1);
    ft_insert(t, ft_insert_sql(token), {ft_block({7, 2, 11, 6}, {"a", "b", "c", "d"})});
    const auto landed = server->landed("events");
    ASSERT_EQ(landed.size(), 2u);
    EXPECT_EQ(landed[0].values, (FtRows{{"7", "a"}, {"11", "c"}}));
    EXPECT_EQ(landed[1].values, (FtRows{{"2", "b"}, {"6", "d"}}));
}

TEST(NativeFakeTransport, EverythingLandedThenFailedIsDeduplicatedOnResend) {
    auto server = ft_server_with(ft_events(10));
    auto tokens = ft_tokens();
    const std::string sql = ft_insert_sql(tokens.next());
    Fault fault;
    fault.step = Step::End;
    fault.kind = Fault::Kind::SystemError;
    fault.landing = Fault::Landing::Everything;
    server->inject(fault);

    FakeTransport t(server);
    t.connect(kFtEp1);
    EXPECT_EQ(ft_errno([&] { ft_insert(t, sql, {ft_ids(0, 5)}); }), ECONNRESET);
    EXPECT_EQ(server->rows("events"), 5u);
    t.abandon();
    t.connect(kFtEp1);
    ft_insert(t, sql, {ft_ids(0, 5)});
    EXPECT_EQ(server->rows("events"), 5u);
    ft_insert(t, ft_insert_sql(tokens.next()), {ft_ids(0, 5)});
    EXPECT_EQ(server->rows("events"), 10u);
}

TEST(NativeFakeTransport, AFailedEndLandsNothingAndRaisesTheServersError) {
    auto server = ft_server_with(ft_events(10));
    auto tokens = ft_tokens();
    Fault fault;
    fault.step = Step::End;
    fault.kind = Fault::Kind::ServerError;
    fault.code = 241;
    fault.message = "Memory limit (total) exceeded";
    server->inject(fault);

    FakeTransport t(server);
    t.connect(kFtEp1);
    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    t.send_block(ft_ids(0, 3));
    try {
        t.end_insert();
        ADD_FAILURE() << "End must fail";
    } catch (const ch::ServerException& e) {
        EXPECT_EQ(e.GetCode(), 241);
        EXPECT_STREQ(e.what(), "Memory limit (total) exceeded");
        EXPECT_EQ(e.GetException().name, "DB::Exception");
    }
    EXPECT_EQ(server->rows("events"), 0u);
    // The INSERT is still open on the client, and it can land nothing more.
    EXPECT_THROW(t.end_insert(), ch::ProtocolError);
    t.abandon();
    EXPECT_EQ(server->abandoned_mid_insert(), 1u);
    EXPECT_EQ(server->inserts("events").at(0).outcome, ReceivedInsert::Outcome::Failed);
}

// Every kind raises the client library's own type with the scripted text.
TEST(NativeFakeTransport, EachFaultKindRaisesTheClientsOwnExceptionType) {
    auto server = std::make_shared<FakeServer>();
    FakeTransport t(server);
    t.connect(kFtEp1);
    const std::string sql = native::select_server_settings(kFtBudget);
    const auto fire = [&](Fault::Kind kind, int code) {
        Fault f;
        f.step = Step::Select;
        f.kind = kind;
        f.code = code;
        f.message = "scripted";
        server->inject(f);
        std::exception_ptr caught;
        try {
            (void)t.select(MetaQuery::ServerSettings, sql);
        } catch (...) {
            caught = std::current_exception();
        }
        // A system error leaves the connection dead; start each kind afresh.
        t.abandon();
        t.connect(kFtEp1);
        return caught;
    };
    const auto what_of = [](const std::exception_ptr& p) -> std::string {
        try {
            std::rethrow_exception(p);
        } catch (const std::exception& e) {
            return e.what();
        }
        return {};
    };

    auto p = fire(Fault::Kind::ServerError, 202);
    try {
        std::rethrow_exception(p);
    } catch (const ch::ServerException& e) {
        EXPECT_EQ(e.GetCode(), 202);
        EXPECT_STREQ(e.what(), "scripted");
    }

    p = fire(Fault::Kind::SystemError, 0);
    try {
        std::rethrow_exception(p);
    } catch (const std::system_error& e) {
        EXPECT_EQ(e.code().value(), ECONNRESET);
        EXPECT_EQ(&e.code().category(), &std::system_category());
    }
    p = fire(Fault::Kind::SystemError, ECONNREFUSED);
    try {
        std::rethrow_exception(p);
    } catch (const std::system_error& e) {
        EXPECT_EQ(e.code().value(), ECONNREFUSED);
    }

    p = fire(Fault::Kind::TlsError, 0);
    EXPECT_THROW(std::rethrow_exception(p), ch::OpenSSLError);
    EXPECT_EQ(what_of(p), "scripted");
    p = fire(Fault::Kind::ProtocolError, 0);
    EXPECT_THROW(std::rethrow_exception(p), ch::ProtocolError);
    EXPECT_EQ(what_of(p), "scripted");
    p = fire(Fault::Kind::CompressionError, 0);
    EXPECT_THROW(std::rethrow_exception(p), ch::CompressionError);
    p = fire(Fault::Kind::Unimplemented, 0);
    EXPECT_THROW(std::rethrow_exception(p), ch::UnimplementedError);
    p = fire(Fault::Kind::ValidationError, 0);
    EXPECT_THROW(std::rethrow_exception(p), ch::ValidationError);
    p = fire(Fault::Kind::BadOptionalAccess, 0);
    EXPECT_THROW(std::rethrow_exception(p), std::bad_optional_access);

    p = fire(Fault::Kind::Other, 0);
    try {
        std::rethrow_exception(p);
    } catch (const ch::Error&) {
        ADD_FAILURE() << "Other must not be one of the client's own errors";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "scripted");
    }
}

TEST(NativeFakeTransport, FaultsOfOneStepFireInArmOrderOnTheirNthCall) {
    auto server = ft_server_with(ft_events());
    const auto armed = [&](Step step, int code, std::size_t nth) {
        Fault f;
        f.step = step;
        f.kind = Fault::Kind::ServerError;
        f.code = code;
        f.nth = nth;
        server->inject(f);
    };
    armed(Step::Select, 1, 2);
    armed(Step::Select, 2, 1);
    armed(Step::End, 3, 1);

    FakeTransport t(server);
    t.connect(kFtEp1);
    const std::string sql = native::select_server_settings(kFtBudget);
    const auto select_code = [&]() -> int {
        const auto err = ft_server_error([&] { (void)t.select(MetaQuery::ServerSettings, sql); });
        return err ? err->code : 0;
    };
    EXPECT_EQ(select_code(), 0);
    EXPECT_EQ(select_code(), 1);
    EXPECT_EQ(select_code(), 2);  // counted from when the first one fired
    EXPECT_EQ(select_code(), 0);

    // The End fault saw none of those calls and fires on the first End.
    auto tokens = ft_tokens();
    const auto err =
        ft_server_error([&] { ft_insert(t, ft_insert_sql(tokens.next()), {ft_ids(0, 1)}); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, 3);
}

TEST(NativeFakeTransport, InjectRefusesAZerothCallAndALandingOffEnd) {
    FakeServer server;
    Fault zeroth;
    zeroth.nth = 0;
    EXPECT_THROW(server.inject(zeroth), std::invalid_argument);
    Fault off_end;
    off_end.step = Step::Send;
    off_end.landing = Fault::Landing::Everything;
    EXPECT_THROW(server.inject(off_end), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// The counters sink tests assert on

TEST(NativeFakeTransport, AbandonedMidInsertCountsEachAbandonOfAnOpenInsert) {
    auto server = ft_server_with(ft_events(10));
    auto tokens = ft_tokens();
    const std::string sql = ft_insert_sql(tokens.next());
    FakeTransport t(server);

    t.connect(kFtEp1);
    t.abandon();  // idle
    EXPECT_EQ(server->abandoned_mid_insert(), 0u);

    Fault begin_fails;
    begin_fails.step = Step::Begin;
    begin_fails.code = 202;
    server->inject(begin_fails);
    t.connect(kFtEp1);
    EXPECT_TRUE(ft_server_error([&] { (void)t.begin_insert(sql); }).has_value());
    t.abandon();  // a failed begin opened nothing
    EXPECT_EQ(server->abandoned_mid_insert(), 0u);

    // End fails k = 3 times, each attempt dropped and resent.
    for (int k = 0; k < 3; ++k) {
        Fault end_fails;
        end_fails.step = Step::End;
        end_fails.code = 202;
        server->inject(end_fails);
    }
    for (int k = 0; k < 3; ++k) {
        t.connect(kFtEp1);
        EXPECT_TRUE(ft_server_error([&] { ft_insert(t, sql, {ft_ids(0, 4)}); }).has_value());
        t.abandon();
    }
    EXPECT_EQ(server->abandoned_mid_insert(), 3u);
    EXPECT_EQ(server->rows("events"), 0u);

    t.connect(kFtEp1);
    ft_insert(t, sql, {ft_ids(0, 4)});
    t.abandon();  // after a successful End there is nothing open
    EXPECT_EQ(server->abandoned_mid_insert(), 3u);
    EXPECT_EQ(server->rows("events"), 4u);

    // Abandoned between begin and the first block, and after some blocks.
    t.connect(kFtEp1);
    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    t.abandon();
    t.connect(kFtEp1);
    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    t.send_block(ft_ids(10, 12));
    t.abandon();
    EXPECT_EQ(server->abandoned_mid_insert(), 5u);
    EXPECT_EQ(server->rows("events"), 4u);
    EXPECT_EQ(server->destroyed_mid_insert(), 0u);

    std::vector<ReceivedInsert::Outcome> outcomes;
    for (const auto& in : server->inserts("events")) {
        outcomes.push_back(in.outcome);
    }
    using O = ReceivedInsert::Outcome;
    EXPECT_EQ(outcomes,
              (std::vector<O>{
                  O::Failed, O::Failed, O::Failed, O::Committed, O::Abandoned, O::Abandoned}));
}

TEST(NativeFakeTransport, ADestroyedClientCommitsItsOpenInsertAndIsCounted) {
    auto server = ft_server_with(ft_events(10));
    auto tokens = ft_tokens();

    {
        FakeTransport t(server);
        t.connect(kFtEp1);
        (void)t.begin_insert(ft_insert_sql(tokens.next()));
        t.send_block(ft_ids(0, 3));
    }
    EXPECT_EQ(server->destroyed_mid_insert(), 1u);
    EXPECT_EQ(server->abandoned_mid_insert(), 0u);
    EXPECT_EQ(server->rows("events"), 3u);
    EXPECT_EQ(server->inserts("events").at(0).outcome, ReceivedInsert::Outcome::Committed);

    {
        // Nothing to commit after the server failed the INSERT, but the
        // destroy is still the defect it counts.
        Fault fault;
        fault.step = Step::End;
        fault.code = 202;
        server->inject(fault);
        FakeTransport t(server);
        t.connect(kFtEp1);
        (void)t.begin_insert(ft_insert_sql(tokens.next()));
        t.send_block(ft_ids(10, 12));
        EXPECT_TRUE(ft_server_error([&] { t.end_insert(); }).has_value());
    }
    {
        // A poisoned socket cannot carry the end-of-data marker.
        FakeTransport t(server);
        t.connect(kFtEp1);
        (void)t.begin_insert(ft_insert_sql(tokens.next()));
        t.send_block(ft_ids(20, 22));
        t.interrupt();
    }
    EXPECT_EQ(server->destroyed_mid_insert(), 3u);
    EXPECT_EQ(server->rows("events"), 3u);

    {
        FakeTransport t(server);
        t.connect(kFtEp1);
        (void)t.begin_insert(ft_insert_sql(tokens.next()));
        t.send_block(ft_ids(30, 32));
        t.abandon();
    }
    {
        FakeTransport t(server);
        t.connect(kFtEp1);
    }
    EXPECT_EQ(server->destroyed_mid_insert(), 3u);
    EXPECT_EQ(server->rows("events"), 3u);
}

// ---------------------------------------------------------------------------
// Interrupt, release and blocking

TEST(NativeFakeTransport, InterruptIsStickyAcrossAbandonAndEveryLaterConnect) {
    auto server = std::make_shared<FakeServer>();
    FakeTransport t(server);
    t.connect(kFtEp1);
    t.interrupt();
    EXPECT_EQ(ft_errno([&] { (void)t.select(MetaQuery::ServerSettings, "SELECT 1"); }),
              ECONNABORTED);
    t.abandon();
    EXPECT_FALSE(t.connected());
    EXPECT_EQ(ft_errno([&] { t.connect(kFtEp1); }), ECONNABORTED);
    EXPECT_EQ(ft_errno([&] { t.connect(kFtEp2); }), ECONNABORTED);
    EXPECT_FALSE(t.connected());
    EXPECT_EQ(server->connects(), 1u);

    FakeTransport early(server);
    early.interrupt();
    EXPECT_EQ(ft_errno([&] { early.connect(kFtEp1); }), ECONNABORTED);
    EXPECT_EQ(server->connects(), 1u);  // never reached the server
}

TEST(NativeFakeTransport, InterruptFailsAHangAtOnce) {
    auto server = ft_server_with(ft_events());
    Fault hang;
    hang.step = Step::End;
    hang.kind = Fault::Kind::Hang;
    server->inject(hang);
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    t.send_block(ft_ids(0, 2));

    std::thread canceller([&] {
        std::this_thread::sleep_for(100ms);
        t.interrupt();
    });
    const auto start = FtClock::now();
    const auto err = ft_errno([&] { t.end_insert(); });
    const auto elapsed = FtClock::now() - start;
    canceller.join();
    EXPECT_EQ(err, ECONNABORTED);
    EXPECT_GE(elapsed, 90ms);
    EXPECT_LT(elapsed, 600ms);
    EXPECT_EQ(server->rows("events"), 0u);
}

TEST(NativeFakeTransport, AnAbandonFromAnotherThreadEndsAHang) {
    auto server = ft_server_with(ft_events());
    Fault hang;
    hang.step = Step::Select;
    hang.kind = Fault::Kind::Hang;
    server->inject(hang);
    FakeTransport t(server);
    t.connect(kFtEp1);

    std::thread dropper([&] {
        std::this_thread::sleep_for(100ms);
        t.abandon();
    });
    const auto start = FtClock::now();
    const auto err = ft_errno([&] { (void)t.select(MetaQuery::ServerSettings, "SELECT 1"); });
    const auto elapsed = FtClock::now() - start;
    dropper.join();
    EXPECT_EQ(err, ECONNABORTED);
    EXPECT_LT(elapsed, 600ms);
    EXPECT_FALSE(t.connected());
}

TEST(NativeFakeTransport, UninterruptibleIgnoresInterruptUntilRelease) {
    auto server = std::make_shared<FakeServer>();
    Fault hold;
    hold.step = Step::Connect;
    hold.kind = Fault::Kind::Uninterruptible;
    server->inject(hold);
    FakeTransport t(server);

    std::atomic<bool> done{false};
    std::optional<int> err;
    std::thread connector([&] {
        err = ft_errno([&] { t.connect(kFtEp1); });
        done = true;
    });
    std::this_thread::sleep_for(100ms);
    t.interrupt();
    std::this_thread::sleep_for(300ms);
    EXPECT_FALSE(done.load()) << "an interrupt must not end an uninterruptible call";
    server->release();
    connector.join();
    EXPECT_EQ(err, ECONNABORTED);  // released, but interrupted meanwhile
    EXPECT_FALSE(t.connected());
}

TEST(NativeFakeTransport, AReleasedCallGoesAheadWhenNothingInterruptedIt) {
    auto server = std::make_shared<FakeServer>();
    Fault hold;
    hold.step = Step::Select;
    hold.kind = Fault::Kind::Uninterruptible;
    server->inject(hold);
    FakeTransport t(server);
    t.connect(kFtEp1);

    auto result = std::async(std::launch::async, [&] {
        return t.select(MetaQuery::MergeTreeSettings,
                        native::select_merge_tree_settings(kFtBudget));
    });
    EXPECT_EQ(result.wait_for(200ms), std::future_status::timeout);
    server->release();
    EXPECT_EQ(result.get().rows.size(), 3u);
}

// ---------------------------------------------------------------------------
// Outages

TEST(NativeFakeTransport, DownWithoutBreakingKeepsLiveConnectionsAndRefusesNewOnes) {
    auto server = ft_server_with(ft_events());
    auto tokens = ft_tokens();
    FakeTransport live(server);
    live.connect(kFtEp1);
    server->set_down(true, false);

    EXPECT_NO_THROW(
        (void)live.select(MetaQuery::ServerSettings, native::select_server_settings(kFtBudget)));
    ft_insert(live, ft_insert_sql(tokens.next()), {ft_ids(0, 2)});
    EXPECT_EQ(server->rows("events"), 2u);

    FakeTransport fresh(server);
    try {
        fresh.connect(kFtEp1);
        ADD_FAILURE() << "a down server must refuse";
    } catch (const std::system_error& e) {
        EXPECT_EQ(e.code().value(), ECONNREFUSED);
        EXPECT_NE(std::string(e.what()).find("is down"), std::string::npos) << e.what();
    }
    EXPECT_EQ(server->connects(), 2u);  // the refused attempt counts
    server->set_down(false);
    EXPECT_NO_THROW(fresh.connect(kFtEp1));
    EXPECT_EQ(fresh.counters().connects, 1u);
}

TEST(NativeFakeTransport, DownBreakingLiveConnectionsResetsThemAndDropsTheirInsert) {
    auto server = ft_server_with(ft_events());
    auto tokens = ft_tokens();
    const std::string sql = ft_insert_sql(tokens.next());
    FakeTransport writer(server);
    FakeTransport idle(server);
    writer.connect(kFtEp1);
    idle.connect(kFtEp1);
    (void)writer.begin_insert(sql);
    writer.send_block(ft_ids(0, 3));

    server->set_down(true);
    EXPECT_EQ(ft_errno([&] { writer.end_insert(); }), ECONNRESET);
    EXPECT_EQ(server->rows("events"), 0u);
    writer.abandon();
    EXPECT_EQ(ft_errno([&] { writer.connect(kFtEp1); }), ECONNREFUSED);

    server->set_down(false);
    // A connection made before the switch stays broken until it is dropped.
    EXPECT_EQ(ft_errno([&] { (void)idle.select(MetaQuery::ServerSettings, "SELECT 1"); }),
              ECONNRESET);
    writer.connect(kFtEp1);
    ft_insert(writer, sql, {ft_ids(0, 3)});
    EXPECT_EQ(server->rows("events"), 3u);
    EXPECT_EQ(server->abandoned_mid_insert(), 1u);
}

TEST(NativeFakeTransport, ABreakWakesAHungCallWithConnectionReset) {
    auto server = ft_server_with(ft_events());
    Fault hang;
    hang.step = Step::End;
    hang.kind = Fault::Kind::Hang;
    server->inject(hang);
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    t.send_block(ft_ids(0, 2));

    std::thread outage([&] {
        std::this_thread::sleep_for(100ms);
        server->set_down(true, true);
    });
    const auto start = FtClock::now();
    const auto err = ft_errno([&] { t.end_insert(); });
    const auto elapsed = FtClock::now() - start;
    outage.join();
    EXPECT_EQ(err, ECONNRESET);
    EXPECT_LT(elapsed, 600ms);
    EXPECT_EQ(server->rows("events"), 0u);
}

// ---------------------------------------------------------------------------
// Foreign inserts and on_fire

TEST(NativeFakeTransport, ForeignInsertsEnterTheDedupFifo) {
    auto tokens = ft_tokens();
    const Token a = tokens.next();
    for (const std::size_t window : {std::size_t{2}, std::size_t{10}}) {
        auto server = ft_server_with(ft_events(window));
        FakeTransport t(server);
        t.connect(kFtEp1);
        ft_insert(t, ft_insert_sql(a), {ft_ids(0, 2)});
        server->land_foreign("events", 3);

        const auto landed = server->landed("events");
        ASSERT_EQ(landed.size(), 4u);
        std::set<std::string> foreign;
        for (std::size_t i = 1; i < landed.size(); ++i) {
            EXPECT_TRUE(landed[i].token.starts_with("foreign-")) << landed[i].token;
            EXPECT_TRUE(landed[i].token.ends_with("_0")) << landed[i].token;
            EXPECT_EQ(landed[i].rows, 0u);
            foreign.insert(landed[i].token);
        }
        EXPECT_EQ(foreign.size(), 3u);
        EXPECT_EQ(server->rows("events"), 2u);

        ft_insert(t, ft_insert_sql(a), {ft_ids(0, 2)});
        // Window 2: three foreign blocks pushed the token out, so the resend
        // lands again. Window 10: it is still known.
        EXPECT_EQ(server->rows("events"), window == 2 ? 4u : 2u) << "window " << window;
    }
}

TEST(NativeFakeTransport, OnFireRunsAfterTheLandingAndBeforeTheOutcome) {
    auto server = ft_server_with(ft_events(2));
    auto tokens = ft_tokens();
    const Token token = tokens.next();
    std::uint64_t rows_seen = 0;
    bool fired = false;
    Fault fault;
    fault.step = Step::End;
    fault.kind = Fault::Kind::SystemError;
    fault.landing = Fault::Landing::Everything;
    fault.on_fire = [&] {
        rows_seen = server->rows("events");
        fired = true;
        server->land_foreign("events", 3);
    };
    server->inject(fault);

    FakeTransport t(server);
    t.connect(kFtEp1);
    try {
        ft_insert(t, ft_insert_sql(token), {ft_ids(0, 4)});
        ADD_FAILURE() << "End must fail";
    } catch (const std::system_error& e) {
        EXPECT_TRUE(fired);
        EXPECT_EQ(e.code().value(), ECONNRESET);
    }
    EXPECT_EQ(rows_seen, 4u);
    t.abandon();
    t.connect(kFtEp1);
    ft_insert(t, ft_insert_sql(token), {ft_ids(0, 4)});
    // The window expired behind the foreign inserts, so the resend's
    // duplicate lands: at least once, nothing lost.
    EXPECT_EQ(server->rows("events"), 8u);
}

TEST(NativeFakeTransport, OnFireMayCallBackIntoTheTransport) {
    auto server = std::make_shared<FakeServer>();
    FakeTransport t(server);
    Fault fault;
    fault.step = Step::Connect;
    fault.kind = Fault::Kind::Delay;
    fault.on_fire = [&] { t.interrupt(); };
    server->inject(fault);
    EXPECT_EQ(ft_errno([&] { t.connect(kFtEp1); }), ECONNABORTED);
    EXPECT_FALSE(t.connected());
}

// ---------------------------------------------------------------------------
// Routing and identity

TEST(NativeFakeTransport, AnEndpointMapRoutesEachConnectToItsServer) {
    auto s1 = std::make_shared<FakeServer>();
    auto s2 = std::make_shared<FakeServer>();
    s2->set_version(26, 3, 4);
    const std::map<Endpoint, std::shared_ptr<FakeServer>> routes{{kFtEp1, s1}, {kFtEp2, s2}};
    const auto factory = fake::fake_factory(routes);
    const auto t = factory(SinkOptions{});

    t->connect(kFtEp1);
    EXPECT_EQ(t->server().display_name, s1->display_name());
    EXPECT_EQ(t->server().endpoint, kFtEp1);
    EXPECT_EQ(t->server().minor, 8u);
    (void)t->select(MetaQuery::ServerSettings, "SELECT 'one'");
    t->abandon();

    t->connect(kFtEp2);
    EXPECT_EQ(t->server().display_name, s2->display_name());
    EXPECT_NE(s1->display_name(), s2->display_name());
    EXPECT_EQ(t->server().endpoint, kFtEp2);
    EXPECT_EQ(t->server().major, 26u);
    EXPECT_EQ(t->server().minor, 3u);
    EXPECT_EQ(t->server().patch, 4u);
    (void)t->select(MetaQuery::ServerSettings, "SELECT 'two'");
    t->abandon();

    try {
        t->connect(kFtEp3);
        ADD_FAILURE() << "an endpoint outside the map must refuse";
    } catch (const std::system_error& e) {
        EXPECT_EQ(e.code().value(), ECONNREFUSED);
        EXPECT_STREQ(e.what(), "no fake server at ch-3:9000: Connection refused");
    }
    EXPECT_EQ(s1->statements(), std::vector<std::string>{"SELECT 'one'"});
    EXPECT_EQ(s2->statements(), std::vector<std::string>{"SELECT 'two'"});
    EXPECT_EQ(s1->connects(), 1u);
    EXPECT_EQ(s2->connects(), 1u);
    EXPECT_EQ(t->counters().connects, 2u);
}

TEST(NativeFakeTransport, OneServerBehindTwoEndpointsDiffersOnlyByEndpoint) {
    auto server = std::make_shared<FakeServer>();
    const auto single = fake::fake_factory(server);
    const auto t = single(SinkOptions{});
    t->connect(kFtEp2);
    const ServerIdentity first = t->server();
    t->abandon();
    t->connect(kFtEp3);
    EXPECT_EQ(t->server().display_name, first.display_name);
    EXPECT_EQ(first.endpoint, kFtEp2);
    EXPECT_EQ(t->server().endpoint, kFtEp3);
    EXPECT_EQ(server->connects(), 2u);

    // A version change shows on the next connection, not on a live one.
    server->set_version(25, 8, 2);
    EXPECT_EQ(t->server().minor, 8u);
    t->abandon();
    t->connect(kFtEp1);
    EXPECT_EQ(t->server().major, 25u);
    EXPECT_EQ(t->server().patch, 2u);
}

// ---------------------------------------------------------------------------
// The deadline

TEST(NativeFakeTransport, ADelayPastTheDeadlineTimesOutAtTheDeadline) {
    auto server = ft_server_with(ft_events());
    Fault slow;
    slow.step = Step::End;
    slow.kind = Fault::Kind::Delay;
    slow.delay = 2000ms;
    server->inject(slow);
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    t.send_block(ft_ids(0, 2));

    const auto start = FtClock::now();
    t.set_deadline(start + 150ms);
    const auto err = ft_errno([&] { t.end_insert(); });
    const auto elapsed = FtClock::now() - start;
    t.set_deadline(std::nullopt);
    EXPECT_EQ(err, ETIMEDOUT);
    EXPECT_GE(elapsed, 140ms);
    EXPECT_LT(elapsed, 1000ms);
    EXPECT_EQ(server->rows("events"), 0u);
}

TEST(NativeFakeTransport, AHangTimesOutAtTheDeadline) {
    auto server = std::make_shared<FakeServer>();
    Fault hang;
    hang.step = Step::Select;
    hang.kind = Fault::Kind::Hang;
    server->inject(hang);
    FakeTransport t(server);
    t.connect(kFtEp1);
    const auto start = FtClock::now();
    t.set_deadline(start + 150ms);
    const auto err = ft_errno([&] { (void)t.select(MetaQuery::ServerSettings, "SELECT 1"); });
    const auto elapsed = FtClock::now() - start;
    EXPECT_EQ(err, ETIMEDOUT);
    EXPECT_GE(elapsed, 140ms);
    EXPECT_LT(elapsed, 1000ms);
}

TEST(NativeFakeTransport, ADelayInsideTheDeadlineSucceedsAfterItsDelay) {
    auto server = ft_server_with(ft_events());
    Fault slow;
    slow.step = Step::End;
    slow.kind = Fault::Kind::Delay;
    slow.delay = 150ms;
    server->inject(slow);
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    t.send_block(ft_ids(0, 2));
    const auto start = FtClock::now();
    t.set_deadline(start + 5s);
    t.end_insert();
    EXPECT_GE(FtClock::now() - start, 140ms);
    EXPECT_EQ(server->rows("events"), 2u);
}

TEST(NativeFakeTransport, ACallStartedPastTheDeadlineFailsBeforeReachingTheServer) {
    auto server = std::make_shared<FakeServer>();
    Fault scripted;
    scripted.step = Step::Select;
    scripted.code = 202;
    server->inject(scripted);
    FakeTransport t(server);
    t.connect(kFtEp1);
    t.set_deadline(FtClock::now() - 1ms);
    EXPECT_EQ(ft_errno([&] { (void)t.select(MetaQuery::ServerSettings, "SELECT 1"); }), ETIMEDOUT);
    EXPECT_TRUE(server->statements().empty());

    // The fault was not spent on it.
    t.set_deadline(std::nullopt);
    t.abandon();
    t.connect(kFtEp1);
    const auto err =
        ft_server_error([&] { (void)t.select(MetaQuery::ServerSettings, "SELECT 1"); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, 202);
}

// ---------------------------------------------------------------------------
// The header and the client's call order

TEST(NativeFakeTransport, AlterColumnTypeChangesTheNextHeader) {
    auto server = ft_server_with(ft_events());
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    auto header = t.begin_insert(ft_insert_sql(tokens.next()));
    EXPECT_EQ(header.at(0).type, "Int64");
    t.abandon();

    server->alter_column_type("events", "id", "Nullable(Int64)");
    t.connect(kFtEp1);
    header = t.begin_insert(ft_insert_sql(tokens.next()));
    EXPECT_EQ(header.at(0).type, "Nullable(Int64)");
    EXPECT_THROW(server->alter_column_type("events", "nope", "Int8"), std::invalid_argument);
    EXPECT_THROW(server->alter_column_type("nope", "id", "Int8"), std::invalid_argument);
}

TEST(NativeFakeTransport, ABlockThatDoesNotMatchTheHeaderFailsTheInsertAtEnd) {
    auto server = ft_server_with(ft_events());
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    (void)t.begin_insert(ft_insert_sql(tokens.next()));

    auto id = std::make_shared<ch::ColumnInt32>();
    id->Append(1);
    auto s = std::make_shared<ch::ColumnString>();
    s->Append("x");
    ch::Block wrong;
    wrong.AppendColumn("id", id);
    wrong.AppendColumn("s", s);
    wrong.RefreshRowCount();
    EXPECT_NO_THROW(t.send_block(wrong));

    const auto err = ft_server_error([&] { t.end_insert(); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, ch::TYPE_MISMATCH);
    EXPECT_EQ(err->text,
              "fake server: a block does not match the INSERT header: column 1 is `id` Int32 and "
              "the INSERT has `id` Int64");
    EXPECT_EQ(server->rows("events"), 0u);
}

// What the server makes of a column whose type is not the header's spelling,
// with type conversion off as the sink sends every INSERT. Empty columns are
// enough, since only the types are compared.
TEST(NativeFakeTransport, AColumnIsCheckedAgainstTheTableTypeAsTheServerComparesTypes) {
    struct Case {
        std::string table_type;
        std::string sent;  // a client type name; "Bool" means the client's ColumnBool
        bool takes;
        std::uint64_t minor{8};
    };
    const std::vector<Case> cases = {
        // The pinned client reads a Bool header as UInt8; to the server they
        // are one type.
        {"Bool", "Bool", true},
        {"Bool", "UInt8", true},
        {"UInt8", "Bool", true},
        // A time zone is display metadata.
        {"DateTime('UTC')", "DateTime", true},
        {"DateTime", "DateTime('Europe/London')", true},
        {"DateTime64(3, 'UTC')", "DateTime64(3)", true},
        {"DateTime64(3)", "DateTime64(6)", false},
        // LowCardinality is added or removed by the server itself.
        {"LowCardinality(String)", "String", true},
        {"LowCardinality(Nullable(String))", "Nullable(String)", true},
        {"LowCardinality(FixedString(2))", "FixedString(2)", true},
        {"String", "LowCardinality(String)", true},
        {"LowCardinality(Nullable(String))", "String", false},
        {"LowCardinality(String)", "Nullable(String)", false},
        {"Array(LowCardinality(String))", "Array(String)", true},
        {"Tuple(a LowCardinality(String), b Int64)", "Tuple(String, Int64)", true},
        {"Tuple(a String, b Int64)", "Tuple(String)", false},
        // Only lines from 26.8 look inside a Map.
        {"Map(LowCardinality(String), Int64)", "Map(String, Int64)", true, 8},
        {"Map(LowCardinality(String), Int64)", "Map(String, Int64)", false, 3},
        {"Map(String, Int64)", "Map(String, Int64)", true, 3},
        // Only an Enum's numbers travel.
        {"Enum8('b' = 1, 'c' = 2)", "Enum8('a' = 1)", true},
        {"Enum8('a' = 1)", "Enum16('a' = 1)", false},
        // A Decimal is its width and scale.
        {"Decimal(18, 2)", "Decimal(10,2)", true},
        {"Decimal(10, 2)", "Decimal(9,2)", false},
        {"Decimal(10, 2)", "Decimal(10,3)", false},
        // No other conversion happens.
        {"Int64", "Int32", false},
        {"Int64", "Nullable(Int64)", false},
        {"String", "FixedString(2)", false},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.sent + " into " + c.table_type + " on 26." + std::to_string(c.minor));
        FakeTable table = ft_events();
        table.columns = {{"c", c.table_type, DefaultKind::None, 1}};
        auto server = ft_server_with(table);
        server->set_version(26, c.minor, 1);
        FakeTransport t(server);
        t.connect(kFtEp1);
        auto tokens = ft_tokens();
        (void)t.begin_insert(ft_insert_sql(tokens.next(), "events", "(`c`)"));
        ch::Block block;
        block.AppendColumn(
            "c",
            c.sent == "Bool" ? std::make_shared<ch::ColumnBool>() : ch::CreateColumnByType(c.sent));
        block.RefreshRowCount();
        t.send_block(block);
        const auto err = ft_server_error([&] { t.end_insert(); });
        EXPECT_EQ(!err.has_value(), c.takes) << (err ? err->text : "taken");
        if (err) {
            EXPECT_EQ(err->code, ch::TYPE_MISMATCH);
            EXPECT_NE(err->text.find("and the INSERT has `c` " + c.table_type), std::string::npos)
                << err->text;
            t.abandon();
        }
    }
}

// BlockBuilder's output for the targets the sink sends as another type of
// column lands, as it does on the server.
TEST(NativeFakeTransport, WhatTheBlockBuilderSendsLandsForEveryTargetTheServerConverts) {
    struct Case {
        std::string declared;
        std::string target;
        std::string json;
        FtRows landed;
    };
    const std::vector<Case> cases = {
        {"BOOLEAN", "Bool", "[true, false]", {{"true"}, {"false"}}},
        {"BOOLEAN", "UInt8", "[true, false]", {{"1"}, {"0"}}},
        {"VARCHAR", "LowCardinality(String)", R"(["a", "b"])", {{"a"}, {"b"}}},
        {"VARCHAR", "LowCardinality(Nullable(String))", R"(["a", null])", {{"a"}, {"NULL"}}},
        {"VARCHAR",
         "LowCardinality(FixedString(2))",
         R"(["ab", "c"])",
         {{"ab"}, {std::string("c\0", 2)}}},
        {"VARCHAR ARRAY",
         "Array(LowCardinality(String))",
         R"([["a", "b"], []])",
         {{"['a','b']"}, {"[]"}}},
        {"MAP<VARCHAR, BIGINT>",
         "Map(LowCardinality(String), Int64)",
         R"([[["k", 1]], []])",
         {{"{'k':1}"}, {"{}"}}},
        {"TIMESTAMP(3)",
         "DateTime('UTC')",
         "[1700000000000, 0]",
         {{"2023-11-14 22:13:20"}, {"1970-01-01 00:00:00"}}},
        {"TIMESTAMP(3)",
         "DateTime64(3, 'UTC')",
         "[1700000000123, 0]",
         {{"2023-11-14 22:13:20.123"}, {"1970-01-01 00:00:00.000"}}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.declared + " into " + c.target);
        FakeTable table = ft_events();
        table.columns = {{"c", c.target, DefaultKind::None, 1}};
        auto server = ft_server_with(table);
        const std::string spec = "c:" + c.declared;
        const auto declared = native::parse_sql_column_types(spec);
        auto planned = native::compile_column_plan(declared, table.columns);
        ASSERT_TRUE(planned.plan.has_value())
            << (planned.problems.empty() ? "" : planned.problems.front().message);
        const auto type = native::arrow_type_for(declared.front().type);
        ASSERT_NE(type, nullptr);
        const auto values = arrow::json::ArrayFromJSONString(type, c.json).ValueOrDie();
        const auto chunk = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("c", type)}), values->length(), {values});
        native::BlockBuilder builder(*planned.plan);
        builder.append(*chunk, 0, chunk->num_rows());
        const ch::Block block = builder.take();

        FakeTransport t(server);
        t.connect(kFtEp1);
        auto tokens = ft_tokens();
        (void)t.begin_insert(ft_insert_sql(tokens.next(), "events", planned.plan->column_list_sql));
        t.send_block(block);
        const auto err = ft_server_error([&] { t.end_insert(); });
        ASSERT_FALSE(err.has_value())
            << "sent " << block[0]->Type()->GetName() << ": " << err->text;
        const auto landed = server->landed("events");
        ASSERT_EQ(landed.size(), 1u);
        EXPECT_EQ(landed[0].values, c.landed);
    }
}

TEST(NativeFakeTransport, CallOrderMirrorsTheClient) {
    auto server = ft_server_with(ft_events());
    FakeTransport t(server);
    auto tokens = ft_tokens();

    EXPECT_EQ(ft_errno([&] { (void)t.select(MetaQuery::ServerSettings, "SELECT 1"); }), ENOTCONN);
    EXPECT_EQ(ft_errno([&] { (void)t.begin_insert(ft_insert_sql(tokens.next())); }), ENOTCONN);
    EXPECT_EQ(ft_errno([&] { t.send_block(ft_ids(0, 1)); }), ENOTCONN);
    EXPECT_EQ(ft_errno([&] { t.end_insert(); }), ENOTCONN);
    EXPECT_FALSE(t.connected());
    EXPECT_TRUE(t.server().display_name.empty());

    t.connect(kFtEp1);
    t.connect(kFtEp1);  // already connected: nothing happens
    EXPECT_EQ(server->connects(), 1u);
    EXPECT_EQ(t.counters().connects, 1u);

    try {
        t.send_block(ft_ids(0, 1));
        ADD_FAILURE() << "a block outside an INSERT must throw";
    } catch (const ch::ValidationError& e) {
        EXPECT_STREQ(e.what(), "illegal to send insert data without first calling BeginInsert");
    }
    EXPECT_NO_THROW(t.end_insert());  // outside an INSERT it does nothing

    (void)t.begin_insert(ft_insert_sql(tokens.next()));
    try {
        (void)t.begin_insert(ft_insert_sql(tokens.next()));
        ADD_FAILURE() << "a second INSERT must throw";
    } catch (const ch::ValidationError& e) {
        EXPECT_STREQ(e.what(), "cannot execute query while executing another operation");
    }
    EXPECT_THROW((void)t.select(MetaQuery::ServerSettings, "SELECT 1"), ch::ValidationError);
    t.send_block(ft_ids(0, 2));
    t.end_insert();
    EXPECT_EQ(server->rows("events"), 2u);

    const auto counters = t.counters();
    EXPECT_GT(counters.bytes_written, 0u);
    EXPECT_GT(counters.bytes_read, 0u);
}

// ---------------------------------------------------------------------------
// Rendering

TEST(NativeFakeTransport, RendersEveryValueKindTheSinkSends) {
    FakeTable table = ft_events();
    table.columns = {
        {"i8", "Int8", DefaultKind::None, 0},
        {"u64", "UInt64", DefaultKind::None, 0},
        {"i128", "Int128", DefaultKind::None, 0},
        {"u128", "UInt128", DefaultKind::None, 0},
        {"f32", "Float32", DefaultKind::None, 0},
        {"f64", "Float64", DefaultKind::None, 0},
        {"flag", "Bool", DefaultKind::None, 0},
        {"s", "String", DefaultKind::None, 0},
        {"fs", "FixedString(4)", DefaultKind::None, 0},
        {"d", "Date", DefaultKind::None, 0},
        {"d32", "Date32", DefaultKind::None, 0},
        {"dt", "DateTime", DefaultKind::None, 0},
        {"dt64", "DateTime64(3)", DefaultKind::None, 0},
        {"dec", "Decimal(10, 2)", DefaultKind::None, 0},
        {"uuid", "UUID", DefaultKind::None, 0},
        {"ip4", "IPv4", DefaultKind::None, 0},
        {"ip6", "IPv6", DefaultKind::None, 0},
        {"e8", "Enum8('red' = 1, 'green' = 2)", DefaultKind::None, 0},
        {"n", "Nullable(Int32)", DefaultKind::None, 0},
        {"lc", "LowCardinality(String)", DefaultKind::None, 0},
        {"arr", "Array(String)", DefaultKind::None, 0},
        {"dates", "Array(Date)", DefaultKind::None, 0},
        {"m", "Map(String, Int64)", DefaultKind::None, 0},
        {"tup", "Tuple(Int32, String)", DefaultKind::None, 0},
    };
    auto server = ft_server_with(table);
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    std::string list = "(";
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        list += (i > 0 ? ", `" : "`") + table.columns[i].name + "`";
    }
    list += ")";
    (void)t.begin_insert(ft_insert_sql(tokens.next(), "events", list));

    ch::Block block;
    const auto add = [&](const std::string& name, ch::ColumnRef column) {
        block.AppendColumn(name, column);
    };
    auto i8 = std::make_shared<ch::ColumnInt8>();
    i8->Append(-5);
    i8->Append(0);
    add("i8", i8);
    auto u64 = std::make_shared<ch::ColumnUInt64>();
    u64->Append(18446744073709551615ULL);
    u64->Append(0);
    add("u64", u64);
    auto i128 = std::make_shared<ch::ColumnInt128>();
    i128->Append(ch::Int128(-123456789012345678LL));
    i128->Append(absl::MakeInt128(-2, 0));  // -2^65
    add("i128", i128);
    auto u128 = std::make_shared<ch::ColumnUInt128>();
    u128->Append(absl::MakeUint128(1, 0));  // 2^64
    u128->Append(ch::UInt128(7));
    add("u128", u128);
    auto f32 = std::make_shared<ch::ColumnFloat32>();
    f32->Append(1.5F);
    f32->Append(-0.25F);
    add("f32", f32);
    auto f64 = std::make_shared<ch::ColumnFloat64>();
    f64->Append(0.1);
    f64->Append(1e21);
    add("f64", f64);
    auto flag = std::make_shared<ch::ColumnUInt8>();
    flag->Append(1);
    flag->Append(0);
    add("flag", flag);
    auto s = std::make_shared<ch::ColumnString>();
    s->Append(std::string_view("it's"));
    s->Append(std::string_view(""));
    add("s", s);
    auto fs = std::make_shared<ch::ColumnFixedString>(4);
    fs->Append(std::string_view("ab"));
    fs->Append(std::string_view("abcd"));
    add("fs", fs);
    auto d = std::make_shared<ch::ColumnDate>();
    d->AppendRaw(19675);
    d->AppendRaw(0);
    add("d", d);
    auto d32 = std::make_shared<ch::ColumnDate32>();
    d32->AppendRaw(-1);
    d32->AppendRaw(-25567);
    add("d32", d32);
    auto dt = std::make_shared<ch::ColumnDateTime>();
    dt->AppendRaw(1700000000);
    dt->AppendRaw(0);
    add("dt", dt);
    auto dt64 = std::make_shared<ch::ColumnDateTime64>(3);
    dt64->Append(1700000000123);
    dt64->Append(-1);
    add("dt64", dt64);
    auto dec = std::make_shared<ch::ColumnDecimal>(10, 2);
    dec->Append(ch::Int128(-12345));
    dec->Append(ch::Int128(5));
    add("dec", dec);
    auto uuid = std::make_shared<ch::ColumnUUID>();
    uuid->Append(ch::UUID(0xbb6a8c699ab2414cULL, 0x86697b7fd27f0825ULL));
    uuid->Append(ch::UUID(0, 1));
    add("uuid", uuid);
    auto ip4 = std::make_shared<ch::ColumnIPv4>();
    ip4->Append(std::string("192.168.0.1"));
    ip4->Append(std::string("10.0.0.255"));
    add("ip4", ip4);
    auto ip6 = std::make_shared<ch::ColumnIPv6>();
    ip6->Append(std::string_view("2001:db8::1"));
    ip6->Append(std::string_view("::ffff:1.2.3.4"));
    add("ip6", ip6);
    auto e8 = std::make_shared<ch::ColumnEnum8>(ch::Type::CreateEnum8({{"red", 1}, {"green", 2}}));
    e8->Append(std::int8_t{2});
    e8->Append(std::int8_t{1});
    add("e8", e8);
    auto n = std::make_shared<ch::ColumnNullableT<ch::ColumnInt32>>();
    n->Append(std::optional<std::int32_t>{});
    n->Append(std::optional<std::int32_t>{7});
    add("n", n);
    auto lc = std::make_shared<ch::ColumnLowCardinalityT<ch::ColumnString>>();
    lc->Append(std::string_view("x"));
    lc->Append(std::string_view("y"));
    add("lc", lc);
    auto arr = std::make_shared<ch::ColumnArrayT<ch::ColumnString>>();
    arr->Append(std::vector<std::string>{"a", "b'c"});
    arr->Append(std::vector<std::string>{});
    add("arr", arr);
    auto dates = std::make_shared<ch::ColumnArray>(std::make_shared<ch::ColumnDate>());
    {
        auto first = std::make_shared<ch::ColumnDate>();
        first->AppendRaw(19675);
        dates->AppendAsColumn(first);
        dates->AppendAsColumn(std::make_shared<ch::ColumnDate>());
    }
    add("dates", dates);
    auto m = std::make_shared<ch::ColumnMapT<ch::ColumnString, ch::ColumnInt64>>(
        std::make_shared<ch::ColumnString>(), std::make_shared<ch::ColumnInt64>());
    m->Append(std::vector<std::tuple<std::string_view, std::int64_t>>{{"k", 1}, {"j", -2}});
    m->Append(std::vector<std::tuple<std::string_view, std::int64_t>>{});
    add("m", m);
    auto tup = std::make_shared<ch::ColumnTupleT<ch::ColumnInt32, ch::ColumnString>>(
        std::make_tuple(std::make_shared<ch::ColumnInt32>(), std::make_shared<ch::ColumnString>()));
    tup->Append(std::make_tuple(std::int32_t{1}, std::string_view("x")));
    tup->Append(std::make_tuple(std::int32_t{-1}, std::string_view("")));
    add("tup", tup);
    block.RefreshRowCount();

    t.send_block(block);
    t.end_insert();
    const auto landed = server->landed("events");
    ASSERT_EQ(landed.size(), 1u);
    ASSERT_EQ(landed[0].values.size(), 2u);
    EXPECT_EQ(landed[0].values[0],
              (std::vector<std::string>{"-5",
                                        "18446744073709551615",
                                        "-123456789012345678",
                                        "18446744073709551616",
                                        "1.5",
                                        "0.1",
                                        "1",
                                        "it's",
                                        std::string("ab\0\0", 4),
                                        "2023-11-14",
                                        "1969-12-31",
                                        "2023-11-14 22:13:20",
                                        "2023-11-14 22:13:20.123",
                                        "-123.45",
                                        "bb6a8c69-9ab2-414c-8669-7b7fd27f0825",
                                        "192.168.0.1",
                                        "2001:db8::1",
                                        "green",
                                        "NULL",
                                        "x",
                                        "['a','b\\'c']",
                                        "['2023-11-14']",
                                        "{'k':1,'j':-2}",
                                        "(1,'x')"}));
    EXPECT_EQ(landed[0].values[1],
              (std::vector<std::string>{"0",
                                        "0",
                                        "-36893488147419103232",
                                        "7",
                                        "-0.25",
                                        "1e+21",
                                        "0",
                                        "",
                                        "abcd",
                                        "1970-01-01",
                                        "1900-01-01",
                                        "1970-01-01 00:00:00",
                                        "1969-12-31 23:59:59.999",
                                        "0.05",
                                        "00000000-0000-0000-0000-000000000001",
                                        "10.0.0.255",
                                        "::ffff:1.2.3.4",
                                        "red",
                                        "7",
                                        "y",
                                        "[]",
                                        "[]",
                                        "{}",
                                        "(-1,'')"}));
}

TEST(NativeFakeTransport, RendersTheClientsOwnBoolColumnAsTrueOrFalse) {
    // The pinned client builds a Bool header as UInt8, but the server takes
    // a ColumnBool for a Bool column all the same.
    FakeTable table = ft_events();
    table.columns = {{"flag", "Bool", DefaultKind::None, 1}};
    auto server = ft_server_with(table);
    FakeTransport t(server);
    t.connect(kFtEp1);
    auto tokens = ft_tokens();
    (void)t.begin_insert(ft_insert_sql(tokens.next(), "events", "(`flag`)"));
    auto flag = std::make_shared<ch::ColumnBool>();
    flag->Append(true);
    flag->Append(false);
    ch::Block block;
    block.AppendColumn("flag", flag);
    block.RefreshRowCount();
    t.send_block(block);
    EXPECT_EQ(server->inserts("events").at(0).blocks.at(0).values, (FtRows{{"true"}, {"false"}}));
    t.end_insert();
    const auto landed = server->landed("events");
    ASSERT_EQ(landed.size(), 1u);
    EXPECT_EQ(landed[0].values, (FtRows{{"true"}, {"false"}}));
}

// ---------------------------------------------------------------------------
// faulty()

TEST(NativeFaulty, LandingNothingLeavesTheInsertOpenOnTheInnerTransport) {
    auto server = ft_server_with(ft_events(10));
    auto faults = std::make_shared<std::deque<Fault>>();
    Fault fault;
    fault.step = Step::End;
    fault.kind = Fault::Kind::SystemError;
    faults->push_back(fault);
    auto t = fake::faulty(std::make_unique<FakeTransport>(server), faults);
    auto tokens = ft_tokens();

    t->connect(kFtEp1);
    EXPECT_EQ(t->server().display_name, server->display_name());
    (void)t->begin_insert(ft_insert_sql(tokens.next()));
    t->send_block(ft_ids(0, 3));
    EXPECT_EQ(ft_errno([&] { t->end_insert(); }), ECONNRESET);
    EXPECT_TRUE(faults->empty());
    EXPECT_EQ(server->rows("events"), 0u);
    t->abandon();
    EXPECT_EQ(server->abandoned_mid_insert(), 1u);
    EXPECT_EQ(server->rows("events"), 0u);
    EXPECT_EQ(t->counters().connects, 1u);
}

TEST(NativeFaulty, LandingEverythingDelegatesTheEndBeforeFailing) {
    auto server = ft_server_with(ft_events(10));
    auto faults = std::make_shared<std::deque<Fault>>();
    bool fired = false;
    Fault fault;
    fault.step = Step::End;
    fault.kind = Fault::Kind::ServerError;
    fault.code = 209;
    fault.landing = Fault::Landing::Everything;
    fault.on_fire = [&] { fired = server->rows("events") == 3; };
    faults->push_back(fault);
    auto t = fake::faulty(std::make_unique<FakeTransport>(server), faults);
    auto tokens = ft_tokens();

    t->connect(kFtEp1);
    (void)t->begin_insert(ft_insert_sql(tokens.next()));
    t->send_block(ft_ids(0, 3));
    const auto err = ft_server_error([&] { t->end_insert(); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, 209);
    EXPECT_TRUE(fired);
    EXPECT_EQ(server->rows("events"), 3u);
    t->abandon();
    EXPECT_EQ(server->abandoned_mid_insert(), 0u);  // the inner INSERT had ended
}

TEST(NativeFaulty, CountsCallsPerStepAndHonoursInterruptAndTheDeadline) {
    auto server = std::make_shared<FakeServer>();
    auto faults = std::make_shared<std::deque<Fault>>();
    Fault second;
    second.step = Step::Select;
    second.code = 285;
    second.nth = 2;
    faults->push_back(second);
    Fault hang;
    hang.step = Step::Select;
    hang.kind = Fault::Kind::Hang;
    faults->push_back(hang);
    Fault hang_again = hang;
    faults->push_back(hang_again);
    auto t = fake::faulty(std::make_unique<FakeTransport>(server), faults);
    t->connect(kFtEp1);
    const std::string sql = native::select_merge_tree_settings(kFtBudget);

    EXPECT_EQ(t->select(MetaQuery::MergeTreeSettings, sql).rows.size(), 3u);
    const auto err = ft_server_error([&] { (void)t->select(MetaQuery::MergeTreeSettings, sql); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, 285);

    auto start = FtClock::now();
    t->set_deadline(start + 150ms);
    EXPECT_EQ(ft_errno([&] { (void)t->select(MetaQuery::MergeTreeSettings, sql); }), ETIMEDOUT);
    EXPECT_GE(FtClock::now() - start, 140ms);
    t->set_deadline(std::nullopt);

    std::thread canceller([&] {
        std::this_thread::sleep_for(100ms);
        t->interrupt();
    });
    start = FtClock::now();
    EXPECT_EQ(ft_errno([&] { (void)t->select(MetaQuery::MergeTreeSettings, sql); }), ECONNABORTED);
    EXPECT_LT(FtClock::now() - start, 600ms);
    canceller.join();
    // The interrupt reached the inner transport too, and stays.
    t->abandon();
    EXPECT_EQ(ft_errno([&] { t->connect(kFtEp1); }), ECONNABORTED);
}

TEST(NativeFaulty, FirstPartitionsNeedsAFakeServer) {
    auto server = ft_server_with(ft_events());
    auto faults = std::make_shared<std::deque<Fault>>();
    Fault fault;
    fault.step = Step::End;
    fault.landing = Fault::Landing::FirstPartitions;
    fault.landed_partitions = 1;
    faults->push_back(fault);
    auto t = fake::faulty(std::make_unique<FakeTransport>(server), faults);
    auto tokens = ft_tokens();
    t->connect(kFtEp1);
    (void)t->begin_insert(ft_insert_sql(tokens.next()));
    t->send_block(ft_ids(0, 2));
    EXPECT_THROW(t->end_insert(), std::logic_error);
    EXPECT_THROW((void)fake::faulty(nullptr, faults), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Thread safety

TEST(NativeFakeServer, ConcurrentWritersAndObserversShareOneServer) {
    auto server = ft_server_with(ft_events(1000));
    constexpr int kWriters = 4;
    constexpr int kInserts = 25;
    std::atomic<bool> stop{false};
    std::thread observer([&] {
        while (!stop.load()) {
            (void)server->rows("events");
            (void)server->landed("events");
            (void)server->statements();
            (void)server->abandoned_mid_insert();
        }
    });
    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            auto tokens = ft_tokens(static_cast<std::uint8_t>(10 * (w + 1)));
            FakeTransport t(server);
            t.connect(kFtEp1);
            for (int i = 0; i < kInserts; ++i) {
                ft_insert(t, ft_insert_sql(tokens.next()), {ft_ids(i, i + 2)});
            }
        });
    }
    for (auto& w : writers) {
        w.join();
    }
    stop = true;
    observer.join();
    EXPECT_EQ(server->rows("events"), static_cast<std::uint64_t>(kWriters * kInserts * 2));
    EXPECT_EQ(server->inserts("events").size(), static_cast<std::size_t>(kWriters * kInserts));
}

}  // namespace
