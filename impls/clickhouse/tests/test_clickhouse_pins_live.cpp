// The server pins for the native ClickHouse sink: what the sink's design takes
// as given about the server, checked against a live server on every line it
// supports (26.3 and 26.8 as of this writing). Each test drives the native
// protocol directly, through the same BeginInsert / SendInsertBlock / EndInsert
// shape the sink uses, and asserts the behaviour the design depends on. A pin
// that fails changes the design, not the expectation.
//
// Skipped unless CLINK_CLICKHOUSE_TEST_HOST names a server (native port from
// CLINK_CLICKHOUSE_TEST_PORT, default 9000). scripts/clickhouse-pins.sh starts
// each pinned line with the configuration the pins need (an embedded Keeper for
// ReplicatedMergeTree) and runs this suite against it. P18 needs a server with a
// <replicated_merge_tree> async_insert default, named by
// CLINK_CLICKHOUSE_TEST_RMT_ASYNC_DEFAULT=1; scripts/clickhouse-live.sh runs it
// against the two-replica profile, which has one.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <clickhouse/base/socket.h>
#include <clickhouse/client.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <gtest/gtest.h>

#if defined(CLINK_CLICKHOUSE_NATIVE)
#include "native/clickhouse_transport.hpp"
#include "native/insert_transport.hpp"
#include "native/sink_options.hpp"
#include "native/statements.hpp"
#endif

namespace {

using clickhouse::Block;
using clickhouse::Client;
using clickhouse::ClientOptions;
using clickhouse::ColumnInt64;
using clickhouse::ColumnNullable;
using clickhouse::ColumnString;
using clickhouse::ColumnUInt64;
using clickhouse::ColumnUInt8;

std::optional<ClientOptions> server_options() {
    const char* host = std::getenv("CLINK_CLICKHOUSE_TEST_HOST");
    if (host == nullptr || *host == '\0') {
        return std::nullopt;
    }
    ClientOptions o;
    o.SetHost(host);
    const char* port = std::getenv("CLINK_CLICKHOUSE_TEST_PORT");
    o.SetPort(port != nullptr && *port != '\0' ? static_cast<std::uint16_t>(std::stoi(port))
                                               : std::uint16_t{9000});
    if (const char* user = std::getenv("CLINK_CLICKHOUSE_TEST_USER"); user != nullptr) {
        o.SetUser(user);
    }
    if (const char* pw = std::getenv("CLINK_CLICKHOUSE_TEST_PASSWORD"); pw != nullptr) {
        o.SetPassword(pw);
    }
    o.SetRethrowException(true);
    return o;
}

std::string nonce() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    return std::to_string(gen());
}

class ClickHousePins : public ::testing::Test {
protected:
    void SetUp() override {
        const auto opts = server_options();
        if (!opts.has_value()) {
            GTEST_SKIP() << "set CLINK_CLICKHOUSE_TEST_HOST to run the server pins";
        }
        client_ = std::make_unique<Client>(*opts);
        const auto& info = client_->GetServerInfo();
        line_ = std::to_string(info.version_major) + "." + std::to_string(info.version_minor);
        RecordProperty("clickhouse_line", line_);
        db_ = "clink_pins_" + std::to_string(::getpid()) + "_" + nonce().substr(0, 8);
        client_->Execute("CREATE DATABASE " + db_);
    }

    void TearDown() override {
        if (client_ != nullptr && !db_.empty()) {
            client_->Execute("DROP DATABASE IF EXISTS " + db_ + " SYNC");
        }
    }

    std::uint64_t count(const std::string& sql) {
        std::uint64_t out = 0;
        client_->Select(sql, [&](const Block& b) {
            if (b.GetRowCount() > 0) {
                out = b[0]->As<ColumnUInt64>()->At(0);
            }
        });
        return out;
    }

    // The first column of every row, which the SELECT renders as a String.
    std::vector<std::string> strings(const std::string& sql) {
        std::vector<std::string> out;
        client_->Select(sql, [&](const Block& b) {
            for (std::size_t i = 0; i < b.GetRowCount(); ++i) {
                out.emplace_back(b[0]->As<ColumnString>()->At(i));
            }
        });
        return out;
    }

    bool has_setting(const std::string& name) {
        return count("SELECT count() FROM system.settings WHERE name = '" + name + "'") > 0;
    }

    // The deduplication setting the sink sends: deduplicate_insert where the
    // server has it, insert_deduplicate otherwise.
    std::string dedup_setting() {
        return has_setting("deduplicate_insert") ? "deduplicate_insert='enable'"
                                                 : "insert_deduplicate=1";
    }

    // The settings every sink INSERT carries, plus `extra`.
    std::string insert_settings(const std::string& extra = {}) {
        std::string s = "async_insert=0, wait_for_async_insert=1, " + dedup_setting() +
                        ", input_format_native_allow_types_conversion=0, "
                        "input_format_null_as_default=0, "
                        "throw_on_max_partitions_per_insert_block=1";
        if (has_setting("use_strict_insert_block_limits")) {
            s += ", use_strict_insert_block_limits=0";
        }
        if (!extra.empty()) {
            s += ", " + extra;
        }
        return s;
    }

    // One native INSERT of `blocks`, each built by `fill`, into (k, p) of
    // `table`: k from `first + row`, p from `partition_of(k)`.
    void insert_kp(const std::string& table,
                   const std::vector<std::pair<std::int64_t, std::int64_t>>& blocks,
                   const std::function<std::int64_t(std::int64_t)>& partition_of,
                   const std::string& extra_settings = {}) {
        client_->BeginInsert("INSERT INTO " + db_ + "." + table + " (k, p) SETTINGS " +
                             insert_settings(extra_settings) + " VALUES");
        for (const auto& [first, rows] : blocks) {
            auto k = std::make_shared<ColumnInt64>();
            auto p = std::make_shared<ColumnInt64>();
            for (std::int64_t i = 0; i < rows; ++i) {
                k->Append(first + i);
                p->Append(partition_of(first + i));
            }
            Block b;
            b.AppendColumn("k", k);
            b.AppendColumn("p", p);
            client_->SendInsertBlock(b);
        }
        client_->EndInsert();
    }

    std::uint64_t rows(const std::string& table) {
        return count("SELECT count() FROM " + db_ + "." + table);
    }

    // The server's line as major * 100 + minor, for line-conditional checks.
    [[nodiscard]] int line_number() const {
        const auto& info = client_->GetServerInfo();
        return static_cast<int>((info.version_major * 100) + info.version_minor);
    }

    std::unique_ptr<Client> client_;
    std::string db_;
    std::string line_;
};

// A MergeTree that keeps a deduplication log, as a Replicated table does by default.
std::string dedup_table(const std::string& db, const std::string& name, bool partitioned) {
    return "CREATE TABLE " + db + "." + name + " (k Int64, p Int64) ENGINE = MergeTree ORDER BY k" +
           (partitioned ? " PARTITION BY p" : "") +
           " SETTINGS non_replicated_deduplication_window = 1000";
}

}  // namespace

// P1: SETTINGS written into the BeginInsert text reach the server. No insert
// overload takes settings on 2.6.x, so this is the only way the sink sets them.
TEST_F(ClickHousePins, P1SettingsInTheInsertTextTakeEffect) {
    client_->Execute(dedup_table(db_, "t", false));
    const auto tag = "clink-p1-" + nonce();
    insert_kp("t", {{1, 10}}, [](std::int64_t) { return 0; }, "log_comment='" + tag + "'");
    client_->Execute("SYSTEM FLUSH LOGS");
    EXPECT_EQ(count("SELECT count() FROM system.query_log WHERE log_comment = '" + tag +
                    "' AND type = 'QueryFinish' AND query_kind = 'Insert' AND "
                    "Settings['async_insert'] = '0'"),
              1U)
        << "the INSERT's SETTINGS clause did not reach the server on " << line_;
}

// P2: a token deduplicates its own resend; distinct tokens on identical data both land.
TEST_F(ClickHousePins, P2ATokenDeduplicatesItsResendAndDistinctTokensBothLand) {
    client_->Execute(dedup_table(db_, "t", false));
    auto zero = [](std::int64_t) { return 0; };
    insert_kp("t", {{1, 10}}, zero, "insert_deduplication_token='clink1-a-1'");
    insert_kp("t", {{1, 10}}, zero, "insert_deduplication_token='clink1-a-1'");
    EXPECT_EQ(rows("t"), 10U) << "the resend under the same token landed again";
    insert_kp("t", {{1, 10}}, zero, "insert_deduplication_token='clink1-a-2'");
    EXPECT_EQ(rows("t"), 20U) << "identical data under a new token was dropped as a duplicate";
}

// P3: without a token, two identical but separate blocks deduplicate against each
// other on a ReplicatedMergeTree: the legacy sink's defect, and why every native
// INSERT carries a token.
TEST_F(ClickHousePins, P3TokenlessIdenticalBlocksDeduplicateOnReplicatedMergeTree) {
    if (count("SELECT count() FROM system.zookeeper_connection") == 0) {
        GTEST_SKIP() << "needs a server with Keeper (docker/clickhouse/pins/keeper.xml)";
    }
    client_->Execute("CREATE TABLE " + db_ +
                     ".t (k Int64, p Int64) ENGINE = ReplicatedMergeTree("
                     "'/clickhouse/tables/" +
                     db_ + "/t', 'r1') ORDER BY k");
    auto zero = [](std::int64_t) { return 0; };
    insert_kp("t", {{1, 10}}, zero);
    insert_kp("t", {{1, 10}}, zero);
    EXPECT_EQ(rows("t"), 10U) << "token-less identical blocks no longer deduplicate on " << line_
                              << "; the token rule still holds, but say so";
}

// P4: a multi-partition INSERT whose first partitions landed before it failed,
// retried whole under the same token, lands each partition exactly once. The
// partial landing is reproduced by inserting the prefix of the same block's
// partitions under the token first; a real failure lands the partitions written
// before it, in the same order.
TEST_F(ClickHousePins, P4APartialMultiPartitionInsertRetriedWithItsTokenLandsEachPartitionOnce) {
    client_->Execute(dedup_table(db_, "t", true));
    auto part_of = [](std::int64_t k) { return (k - 1) / 5; };  // 5 rows per partition
    insert_kp("t", {{1, 10}}, part_of, "insert_deduplication_token='clink1-p4-1'");
    insert_kp("t", {{1, 15}}, part_of, "insert_deduplication_token='clink1-p4-1'");
    EXPECT_EQ(rows("t"), 15U);
    for (int p = 0; p < 3; ++p) {
        EXPECT_EQ(count("SELECT count() FROM " + db_ + ".t WHERE p = " + std::to_string(p)), 5U)
            << "partition " << p << " did not land exactly once on " << line_;
    }
}

// P5: several SendInsertBlock calls inside one INSERT squash into one part, and a
// multi-block INSERT retried with its token lands exactly once.
TEST_F(ClickHousePins, P5BlocksOfOneInsertSquashAndARetryLandsOnce) {
    client_->Execute(dedup_table(db_, "t", false));
    auto zero = [](std::int64_t) { return 0; };
    const std::vector<std::pair<std::int64_t, std::int64_t>> blocks{
        {1, 1000}, {1001, 1000}, {2001, 1000}, {3001, 1000}};
    insert_kp("t", blocks, zero, "insert_deduplication_token='clink1-p5-1'");
    EXPECT_EQ(rows("t"), 4000U);
    EXPECT_EQ(count("SELECT count() FROM system.parts WHERE database = '" + db_ +
                    "' AND table = 't' AND active"),
              1U)
        << "four blocks of one INSERT made more than one part on " << line_;
    insert_kp("t", blocks, zero, "insert_deduplication_token='clink1-p5-1'");
    EXPECT_EQ(rows("t"), 4000U) << "the multi-block resend landed again";
}

// P6: a table-level async_insert is ORed into the decision, so a query that says
// async_insert=0 still goes asynchronous on such a table. The sink therefore reads
// the target's effective setting at open and refuses one that is not 0.
TEST_F(ClickHousePins, P6ATablesAsyncInsertOverridesTheQuerys) {
    client_->Execute("CREATE TABLE " + db_ +
                     ".t (k Int64, p Int64) ENGINE = MergeTree ORDER BY k "
                     "SETTINGS async_insert = 1");
    insert_kp("t", {{1, 10}}, [](std::int64_t) { return 0; });
    client_->Execute("SYSTEM FLUSH LOGS");
    EXPECT_GT(count("SELECT count() FROM system.asynchronous_insert_log WHERE database = '" + db_ +
                    "' AND table = 't'"),
              0U)
        << "a table with async_insert=1 took a query-level async_insert=0 insert "
           "synchronously on "
        << line_ << "; the open-time refusal can be relaxed";
}

// P6, behind a Distributed table: the decision is made on the shard against the
// shard-local table, so the open-time check must read the local tables. The
// shard here is this server, so prefer_localhost_replica=0 makes the Distributed
// table send over a connection, as it does to a real remote shard; with the
// default it writes the local table in-process and the shard's own decision
// never runs.
TEST_F(ClickHousePins, P6AShardLocalAsyncInsertAppliesBehindADistributedTable) {
    client_->Execute("CREATE TABLE " + db_ +
                     ".local (k Int64, p Int64) ENGINE = MergeTree ORDER BY k "
                     "SETTINGS async_insert = 1");
    client_->Execute("CREATE TABLE " + db_ +
                     ".dist (k Int64, p Int64) ENGINE = Distributed(clink_pins_local, " + db_ +
                     ", local)");
    insert_kp(
        "dist",
        {{1, 10}},
        [](std::int64_t) { return 0; },
        "distributed_foreground_insert=1, prefer_localhost_replica=0");
    client_->Execute("SYSTEM FLUSH LOGS");
    EXPECT_GT(count("SELECT count() FROM system.asynchronous_insert_log WHERE database = '" + db_ +
                    "' AND table = 'local'"),
              0U)
        << "the shard-local table's async_insert did not apply behind the Distributed "
           "table on "
        << line_;
}

// P6, from the server-wide <merge_tree> default: needs a server configured with
// it, named by CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT=1.
TEST_F(ClickHousePins, P6AServerWideMergeTreeAsyncInsertDefaultApplies) {
    const char* configured = std::getenv("CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT");
    if (configured == nullptr || std::string(configured) != "1") {
        GTEST_SKIP() << "needs a server whose <merge_tree> sets async_insert=1 "
                        "(docker/clickhouse/pins/merge-tree-async.xml)";
    }
    client_->Execute("CREATE TABLE " + db_ + ".t (k Int64, p Int64) ENGINE = MergeTree ORDER BY k");
    insert_kp("t", {{1, 10}}, [](std::int64_t) { return 0; });
    client_->Execute("SYSTEM FLUSH LOGS");
    EXPECT_GT(count("SELECT count() FROM system.asynchronous_insert_log WHERE database = '" + db_ +
                    "' AND table = 't'"),
              0U)
        << "the server-wide merge_tree async_insert default did not apply on " << line_;
}

// P7: with conversion off, a Nullable column against a non-Nullable target is
// refused (the sink must unwrap and fail on a null itself), while String into
// LowCardinality(String) is accepted.
TEST_F(ClickHousePins, P7NullableAgainstNonNullableIsRefusedAndLowCardinalityAcceptsString) {
    client_->Execute("CREATE TABLE " + db_ +
                     ".t (k Int64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY k");
    {
        client_->BeginInsert("INSERT INTO " + db_ + ".t (k, s) SETTINGS " + insert_settings() +
                             " VALUES");
        auto inner = std::make_shared<ColumnInt64>();
        auto nulls = std::make_shared<ColumnUInt8>();
        inner->Append(1);
        nulls->Append(0);
        auto s = std::make_shared<ColumnString>();
        s->Append("a");
        Block b;
        b.AppendColumn("k", std::make_shared<ColumnNullable>(inner, nulls));
        b.AppendColumn("s", s);
        bool refused = false;
        try {
            client_->SendInsertBlock(b);
            client_->EndInsert();
        } catch (const std::exception& e) {
            refused = true;
            RecordProperty("p7_nullable_refusal", e.what());
            client_->ResetConnection();
        }
        EXPECT_TRUE(refused) << "a Nullable(Int64) block was accepted into an Int64 column "
                                "with conversion off on "
                             << line_;
    }
    client_->BeginInsert("INSERT INTO " + db_ + ".t (k, s) SETTINGS " + insert_settings() +
                         " VALUES");
    auto k = std::make_shared<ColumnInt64>();
    auto s = std::make_shared<ColumnString>();
    k->Append(2);
    s->Append("b");
    Block b;
    b.AppendColumn("k", k);
    b.AppendColumn("s", s);
    client_->SendInsertBlock(b);
    client_->EndInsert();
    EXPECT_EQ(rows("t"), 1U) << "String into LowCardinality(String) was refused on " << line_;
}

// P8: FINAL drops a row whose latest version is deleted without the cleanup
// setting. And merges never collapse one key across partitions: two live
// versions in different partitions both survive OPTIMIZE ... FINAL, and only a
// query-time FINAL that merges across partitions hides one. Hence the upsert
// sink's rule that the partition key may use only PRIMARY KEY columns.
TEST_F(ClickHousePins, P8FinalHonoursIsDeletedAndMergesKeepPartitionsApart) {
    client_->Execute("CREATE TABLE " + db_ +
                     ".t (k Int64, p Int64, ver UInt64, is_deleted UInt8) "
                     "ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY k PARTITION BY p");
    client_->Execute("INSERT INTO " + db_ + ".t VALUES (1, 0, 1, 0)");
    client_->Execute("INSERT INTO " + db_ + ".t VALUES (1, 0, 2, 1)");
    client_->Execute("INSERT INTO " + db_ + ".t VALUES (2, 0, 1, 0)");
    client_->Execute("INSERT INTO " + db_ + ".t VALUES (2, 1, 2, 0)");
    EXPECT_EQ(count("SELECT count() FROM " + db_ + ".t FINAL WHERE k = 1"), 0U)
        << "FINAL kept a row whose latest version is deleted on " << line_;
    client_->Execute("OPTIMIZE TABLE " + db_ + ".t FINAL");
    EXPECT_EQ(count("SELECT count() FROM " + db_ + ".t WHERE k = 2"), 2U)
        << "a merge collapsed one key across two partitions on " << line_
        << "; the partition-key rule can be relaxed";
    EXPECT_EQ(
        count("SELECT count() FROM " + db_ +
              ".t FINAL WHERE k = 2 SETTINGS do_not_merge_across_partitions_select_final = 1"),
        2U)
        << "FINAL restricted to partitions collapsed one key across two on " << line_;
}

// P9: the cost of the upsert sink's open-time max(ver) read. A measurement, not
// a pass mark: the row count comes from CLINK_CLICKHOUSE_PIN_P9_ROWS (default
// ten million), and the test only fails when the read outruns its own budget.
TEST_F(ClickHousePins, P9TheCostOfMaxVer) {
    std::uint64_t n = 10'000'000;
    if (const char* env = std::getenv("CLINK_CLICKHOUSE_PIN_P9_ROWS"); env != nullptr) {
        n = std::stoull(env);
    }
    client_->Execute("CREATE TABLE " + db_ +
                     ".t (k UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k");
    client_->Execute("INSERT INTO " + db_ + ".t SELECT number, number FROM numbers(" +
                     std::to_string(n) + ")");
    const auto t0 = std::chrono::steady_clock::now();
    const auto max_ver = count("SELECT max(ver) FROM " + db_ +
                               ".t SETTINGS max_execution_time = 60, "
                               "timeout_overflow_mode = 'throw'");
    const auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    EXPECT_EQ(max_ver, n - 1);
    RecordProperty("p9_rows", std::to_string(n));
    RecordProperty("p9_max_ver_ms", std::to_string(ms));
    std::cout << "[pin P9] " << line_ << ": max(ver) over " << n << " rows took " << ms << " ms\n";
}

// P11: the setting names the sink sends exist on this line. The sink checks the
// line-conditional ones (deduplicate_insert, use_strict_insert_block_limits)
// against system.settings at open; the rest are required.
TEST_F(ClickHousePins, P11TheSettingNamesTheSinkSendsExist) {
    for (const char* name : {"async_insert",
                             "wait_for_async_insert",
                             "insert_deduplicate",
                             "insert_deduplication_token",
                             "input_format_native_allow_types_conversion",
                             "input_format_null_as_default",
                             "throw_on_max_partitions_per_insert_block",
                             "max_partitions_per_insert_block",
                             "min_insert_block_size_rows",
                             "min_insert_block_size_bytes",
                             "log_comment",
                             "max_execution_time",
                             "timeout_overflow_mode",
                             "distributed_foreground_insert"}) {
        EXPECT_TRUE(has_setting(name)) << name << " is missing on " << line_;
    }
    RecordProperty("has_deduplicate_insert", has_setting("deduplicate_insert") ? "1" : "0");
    RecordProperty("has_use_strict_insert_block_limits",
                   has_setting("use_strict_insert_block_limits") ? "1" : "0");
    if (line_number() >= 2608) {
        EXPECT_TRUE(has_setting("use_strict_insert_block_limits"))
            << "use_strict_insert_block_limits is expected from 26.8";
    }
}

// P12: distributed_foreground_insert=1 makes an INSERT into a Distributed table
// synchronous: the rows are on the shard when EndInsert returns.
TEST_F(ClickHousePins, P12DistributedForegroundInsertIsSynchronous) {
    client_->Execute("CREATE TABLE " + db_ +
                     ".local (k Int64, p Int64) ENGINE = MergeTree ORDER BY k");
    client_->Execute("CREATE TABLE " + db_ +
                     ".dist (k Int64, p Int64) ENGINE = Distributed(clink_pins_local, " + db_ +
                     ", local)");
    insert_kp(
        "dist", {{1, 100}}, [](std::int64_t) { return 0; }, "distributed_foreground_insert=1");
    EXPECT_EQ(rows("local"), 100U) << "the rows were not on the shard when the INSERT returned";
}

// P13: max_execution_time with timeout_overflow_mode='throw' bounds a SELECT. A
// client receive timeout cannot, because progress packets keep the socket busy.
TEST_F(ClickHousePins, P13MaxExecutionTimeBoundsASelect) {
    const auto t0 = std::chrono::steady_clock::now();
    bool timed_out = false;
    try {
        (void)count(
            "SELECT count() FROM numbers_mt(1000000000000) SETTINGS max_execution_time = 1, "
            "timeout_overflow_mode = 'throw'");
    } catch (const clickhouse::ServerException& e) {
        timed_out = e.GetCode() == 159;  // TIMEOUT_EXCEEDED
        RecordProperty("p13_code", std::to_string(e.GetCode()));
        client_->ResetConnection();
    }
    const auto secs =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - t0)
            .count();
    EXPECT_TRUE(timed_out) << "the SELECT did not end with TIMEOUT_EXCEEDED on " << line_;
    EXPECT_LT(secs, 10) << "the bounded SELECT ran " << secs << " s";
}

// P14: a ColumnBool block goes into a Bool column with conversion off. The
// sink sends BOOLEAN into Bool as ColumnBool, whose type name is Bool, rather
// than as the UInt8 the server stores.
TEST_F(ClickHousePins, P14ColumnBoolIntoBoolIsAcceptedWithConversionOff) {
    client_->Execute("CREATE TABLE " + db_ + ".t (k Int64, b Bool) ENGINE = MergeTree ORDER BY k");
    client_->BeginInsert("INSERT INTO " + db_ + ".t (k, b) SETTINGS " + insert_settings() +
                         " VALUES");
    auto k = std::make_shared<ColumnInt64>();
    auto b = std::make_shared<clickhouse::ColumnBool>();
    k->Append(1);
    b->Append(true);
    k->Append(2);
    b->Append(false);
    Block block;
    block.AppendColumn("k", k);
    block.AppendColumn("b", b);
    client_->SendInsertBlock(block);
    client_->EndInsert();
    EXPECT_EQ(strings("SELECT toString(b) FROM " + db_ + ".t ORDER BY k"),
              (std::vector<std::string>{"true", "false"}))
        << "a ColumnBool block did not land as Bool on " << line_;
}

// P15: the two LowCardinality targets the sink sends as their inner type with
// conversion off, as P7 pins for LowCardinality(String): Nullable(String) into
// LowCardinality(Nullable(String)), and FixedString(N) into
// LowCardinality(FixedString(N)).
TEST_F(ClickHousePins, P15LowCardinalityOfNullableStringAndOfFixedStringTakeTheirInnerType) {
    client_->Execute("CREATE TABLE " + db_ +
                     ".t (k Int64, s LowCardinality(Nullable(String)), "
                     "f LowCardinality(FixedString(3))) ENGINE = MergeTree ORDER BY k");
    client_->BeginInsert("INSERT INTO " + db_ + ".t (k, s, f) SETTINGS " + insert_settings() +
                         " VALUES");
    auto k = std::make_shared<ColumnInt64>();
    auto inner = std::make_shared<ColumnString>();
    auto nulls = std::make_shared<ColumnUInt8>();
    auto f = std::make_shared<clickhouse::ColumnFixedString>(3);
    k->Append(1);
    inner->Append("a");
    nulls->Append(0);
    f->Append("abc");
    k->Append(2);
    inner->Append("");
    nulls->Append(1);
    f->Append("xy");
    Block block;
    block.AppendColumn("k", k);
    block.AppendColumn("s", std::make_shared<ColumnNullable>(inner, nulls));
    block.AppendColumn("f", f);
    client_->SendInsertBlock(block);
    client_->EndInsert();
    EXPECT_EQ(strings("SELECT concat(ifNull(toString(s), 'NULL'), '|', hex(f)) FROM " + db_ +
                      ".t ORDER BY k"),
              (std::vector<std::string>{"a|616263", "NULL|787900"}))
        << "the LowCardinality targets did not take their inner types on " << line_;
}

// P16: the Date32 and DateTime64 ranges the converter checks, 1900-01-01 to
// 2299-12-31 (23:59:59.999 for DateTime64(3)), round-trip exactly at both
// ends through the native format. What one unit past either end reads back as
// is recorded: the converter refuses it because the server documents these
// ranges as its supported ones, whatever the native format happens to keep.
TEST_F(ClickHousePins, P16Date32AndDateTime64BoundsRoundTripAndOneUnitPastIsRecorded) {
    client_->Execute("CREATE TABLE " + db_ +
                     ".t (k Int64, d Date32, ts DateTime64(3, 'UTC')) ENGINE = MergeTree "
                     "ORDER BY k");
    constexpr std::int64_t kMinMs = -2208988800000;  // 1900-01-01 00:00:00.000 UTC
    constexpr std::int64_t kMaxMs = 10413791999999;  // 2299-12-31 23:59:59.999 UTC
    const std::vector<std::pair<std::int32_t, std::int64_t>> values{
        {-25567, kMinMs}, {120529, kMaxMs}, {-25568, kMinMs - 1}, {120530, kMaxMs + 1}};
    client_->BeginInsert("INSERT INTO " + db_ + ".t (k, d, ts) SETTINGS " + insert_settings() +
                         " VALUES");
    auto k = std::make_shared<ColumnInt64>();
    auto d = std::make_shared<clickhouse::ColumnDate32>();
    auto ts = std::make_shared<clickhouse::ColumnDateTime64>(3, "UTC");
    for (std::size_t i = 0; i < values.size(); ++i) {
        k->Append(static_cast<std::int64_t>(i));
        d->AppendRaw(values[i].first);
        ts->Append(values[i].second);
    }
    Block block;
    block.AppendColumn("k", k);
    block.AppendColumn("d", d);
    block.AppendColumn("ts", ts);
    client_->SendInsertBlock(block);
    client_->EndInsert();
    const auto dates = strings("SELECT toString(d) FROM " + db_ + ".t ORDER BY k");
    const auto stamps = strings("SELECT toString(ts) FROM " + db_ + ".t ORDER BY k");
    ASSERT_EQ(dates.size(), 4U);
    ASSERT_EQ(stamps.size(), 4U);
    EXPECT_EQ(dates[0], "1900-01-01") << line_;
    EXPECT_EQ(dates[1], "2299-12-31") << line_;
    EXPECT_EQ(stamps[0], "1900-01-01 00:00:00.000") << line_;
    EXPECT_EQ(stamps[1], "2299-12-31 23:59:59.999") << line_;
    RecordProperty("p16_date32_below", dates[2]);
    RecordProperty("p16_date32_above", dates[3]);
    RecordProperty("p16_datetime64_below", stamps[2]);
    RecordProperty("p16_datetime64_above", stamps[3]);
    std::cout << "[pin P16] " << line_ << ": one past reads back as " << dates[2] << ", "
              << dates[3] << ", " << stamps[2] << ", " << stamps[3] << "\n";
}

// P17: a kept-token resend is safe only if the server squashes it into the
// same blocks as the first attempt, because each block of an INSERT under a
// token is deduplicated by an identity built from the token and the block's
// position. A block cut differently on the resend either matches one that held
// other rows, and is dropped, or matches none, and lands again. The first
// attempt of a three-block INSERT is taken to have landed its first part
// before it failed, which the server does once its squash buffer passes
// min_insert_block_size_rows; the part is reproduced, as in P4, by an INSERT
// of that block alone under the token, because when a part lands mid-INSERT
// varies from run to run. The resend runs as a user whose profile sets a
// different min_insert_block_size_rows. With the thresholds pinned in the
// statement, as the sink sends them, the row count is exact. Without them the
// outcome is recorded, so the hazard is shown to exist or not on each line:
// fewer distinct rows than sent is a loss, more rows a duplicate.
TEST_F(ClickHousePins, P17PinnedSquashThresholdsMakeAKeptTokenResendExact) {
    constexpr std::int64_t kBlock = 1000;
    constexpr std::int64_t kBlocks = 3;
    constexpr auto kSent = static_cast<std::uint64_t>(kBlock * kBlocks);
    const std::string bytes = strings(
                                  "SELECT toString(value) FROM system.settings WHERE name = "
                                  "'min_insert_block_size_bytes'")
                                  .at(0);
    const std::string pinned_thresholds = "min_insert_block_size_rows=" + std::to_string(kBlock) +
                                          ", min_insert_block_size_bytes=" + bytes;
    const std::string resend_user =
        "clink_p17_" + std::to_string(::getpid()) + "_" + nonce().substr(0, 6);
    client_->Execute("CREATE USER " + resend_user +
                     " IDENTIFIED WITH no_password SETTINGS min_insert_block_size_rows = " +
                     std::to_string(kBlock + (kBlock / 2)));
    client_->Execute("GRANT CURRENT GRANTS ON *.* TO " + resend_user);

    auto insert = [this](Client& c,
                         const std::string& table,
                         const std::string& token,
                         const std::string& thresholds,
                         std::int64_t blocks) {
        c.BeginInsert("INSERT INTO " + db_ + "." + table + " (k, p) SETTINGS " +
                      insert_settings("insert_deduplication_token='" + token + "'" +
                                      (thresholds.empty() ? "" : ", " + thresholds)) +
                      " VALUES");
        for (std::int64_t i = 0; i < blocks; ++i) {
            auto k = std::make_shared<ColumnInt64>();
            auto p = std::make_shared<ColumnInt64>();
            for (std::int64_t r = 0; r < kBlock; ++r) {
                k->Append(1 + (i * kBlock) + r);
                p->Append(0);
            }
            Block b;
            b.AppendColumn("k", k);
            b.AppendColumn("p", p);
            c.SendInsertBlock(b);
        }
        c.EndInsert();
    };
    auto run = [&](const std::string& table, const std::string& resend_thresholds) {
        client_->Execute(dedup_table(db_, table, false));
        const std::string token = "clink1-p17-" + table;
        insert(*client_, table, token, pinned_thresholds, 1);
        EXPECT_EQ(rows(table), static_cast<std::uint64_t>(kBlock));
        auto ro = *server_options();
        ro.SetUser(resend_user);
        ro.SetPassword("");
        Client resend(ro);
        insert(resend, table, token, resend_thresholds, kBlocks);
        return std::make_pair(rows(table), count("SELECT uniqExact(k) FROM " + db_ + "." + table));
    };

    const auto [pinned, pinned_uniq] = run("pinned", pinned_thresholds);
    const auto [unpinned, unpinned_uniq] = run("unpinned", "");
    client_->Execute("DROP USER IF EXISTS " + resend_user);
    EXPECT_EQ(pinned, kSent)
        << "a kept-token resend with the thresholds pinned in the statement was not exact on "
        << line_;
    EXPECT_EQ(pinned_uniq, kSent);
    RecordProperty("p17_unpinned_rows", std::to_string(unpinned));
    RecordProperty("p17_unpinned_uniq", std::to_string(unpinned_uniq));
    const char* outcome = unpinned_uniq < kSent ? "rows lost"
                          : unpinned > kSent    ? "rows duplicated"
                                                : "no hazard";
    std::cout << "[pin P17] " << line_ << ": pinned " << pinned << " rows (" << pinned_uniq
              << " distinct) of " << kSent << "; unpinned " << unpinned << " rows ("
              << unpinned_uniq << " distinct): " << outcome << "\n";
}

// P18: a server-wide <replicated_merge_tree> async_insert=1 reaches
// ReplicatedMergeTree tables, so a query that says async_insert=0 still goes
// asynchronous, and system.replicated_merge_tree_settings reports it while
// system.merge_tree_settings does not. The sink therefore reads the former for
// the Replicated family. Needs CLINK_CLICKHOUSE_TEST_RMT_ASYNC_DEFAULT=1.
TEST_F(ClickHousePins, P18AReplicatedMergeTreeAsyncInsertDefaultAppliesToReplicatedTables) {
    const char* configured = std::getenv("CLINK_CLICKHOUSE_TEST_RMT_ASYNC_DEFAULT");
    if (configured == nullptr || std::string(configured) != "1") {
        GTEST_SKIP() << "needs a server whose <replicated_merge_tree> sets async_insert=1 "
                        "(docker/clickhouse/replicated/replicated-merge-tree-async.xml)";
    }
    EXPECT_EQ(strings("SELECT toString(value) FROM system.replicated_merge_tree_settings WHERE "
                      "name = 'async_insert'"),
              std::vector<std::string>{"1"})
        << "system.replicated_merge_tree_settings does not report the override on " << line_;
    EXPECT_EQ(strings("SELECT toString(value) FROM system.merge_tree_settings WHERE name = "
                      "'async_insert'"),
              std::vector<std::string>{"0"});
    client_->Execute("CREATE TABLE " + db_ +
                     ".r (k Int64, p Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/" +
                     db_ + "/r', 'p18') ORDER BY k");
    client_->Execute("CREATE TABLE " + db_ + ".m (k Int64, p Int64) ENGINE = MergeTree ORDER BY k");
    auto zero = [](std::int64_t) { return 0; };
    insert_kp("r", {{1, 10}}, zero);
    insert_kp("m", {{1, 10}}, zero);
    client_->Execute("SYSTEM FLUSH LOGS");
    auto async_inserts = [this](const std::string& table) {
        return count("SELECT count() FROM system.asynchronous_insert_log WHERE database = '" + db_ +
                     "' AND table = '" + table + "'");
    };
    EXPECT_GT(async_inserts("r"), 0U) << "the <replicated_merge_tree> async_insert default did "
                                         "not apply to a ReplicatedMergeTree on "
                                      << line_;
    EXPECT_EQ(async_inserts("m"), 0U)
        << "the <replicated_merge_tree> default reached a plain MergeTree on " << line_;
}

#if defined(CLINK_CLICKHOUSE_NATIVE)
// P19: every metadata SELECT the sink sends at open, as its own statement
// builders write them, returns only String columns on this line, and the real
// transport reads each one. The cluster reads lead with the replica each row
// came from, as host:port, and that server's UUID, both evaluated on the
// replica: here the one-shard cluster is this server, so both must equal what
// this server reports for itself. The macros read returns no rows on a server
// without macros, which these are, so for it only the header's types are
// checked.
TEST_F(ClickHousePins, P19EveryProbeSelectReturnsOnlyStringColumns) {
    namespace native = clink::clickhouse::native;
    client_->Execute("CREATE TABLE " + db_ +
                     ".local (k Int64, p Int64) ENGINE = MergeTree ORDER BY k");
    client_->Execute("CREATE TABLE " + db_ +
                     ".dist (k Int64, p Int64) ENGINE = Distributed(clink_pins_local, " + db_ +
                     ", local)");
    const auto budget = native::metadata_budget(std::chrono::seconds(30));
    struct Probe {
        native::MetaQuery kind;
        std::string sql;
        bool cluster;  // leads with the replica's host:port and serverUUID()
        bool may_be_empty;
    };
    const std::vector<Probe> probes{
        {native::MetaQuery::ServerSettings, native::select_server_settings(budget), false, false},
        {native::MetaQuery::Table, native::select_table(db_, "dist", budget), false, false},
        {native::MetaQuery::Columns, native::select_columns(db_, "dist", budget), false, false},
        {native::MetaQuery::MergeTreeSettings,
         native::select_merge_tree_settings(budget),
         false,
         false},
        {native::MetaQuery::ReplicatedMergeTreeSettings,
         native::select_replicated_merge_tree_settings(budget),
         false,
         false},
        {native::MetaQuery::ClusterReplicaCount,
         native::select_cluster_replica_count("clink_pins_local", budget),
         false,
         false},
        {native::MetaQuery::ClusterReplicaCount, native::select_macros(budget), false, true},
        {native::MetaQuery::ClusterTables,
         native::select_cluster_tables("clink_pins_local", db_, "local", budget),
         true,
         false},
        {native::MetaQuery::ClusterMergeTreeSettings,
         native::select_cluster_merge_tree_settings("clink_pins_local", budget),
         true,
         false},
        {native::MetaQuery::ClusterReplicatedMergeTreeSettings,
         native::select_cluster_replicated_merge_tree_settings("clink_pins_local", budget),
         true,
         false},
    };
    // What this server says of itself, for the cluster reads' leading columns.
    std::string self_endpoint;
    std::string self_uuid;
    client_->Select("SELECT concat(hostName(), ':', toString(tcpPort())), toString(serverUUID())",
                    [&](const Block& b) {
                        if (b.GetRowCount() > 0) {
                            self_endpoint = std::string(b[0]->As<ColumnString>()->At(0));
                            self_uuid = std::string(b[1]->As<ColumnString>()->At(0));
                        }
                    });
    ASSERT_FALSE(self_endpoint.empty());
    ASSERT_NE(self_uuid, "00000000-0000-0000-0000-000000000000")
        << "the server has no UUID on " << line_;

    native::SinkOptions opts;
    const auto o = *server_options();
    opts.endpoints = {native::Endpoint{o.host, o.port}};
    if (const char* user = std::getenv("CLINK_CLICKHOUSE_TEST_USER"); user != nullptr) {
        opts.user = user;
    }
    if (const char* pw = std::getenv("CLINK_CLICKHOUSE_TEST_PASSWORD"); pw != nullptr) {
        opts.password = pw;
    }
    opts.compression = native::Compression::None;
    auto transport = native::make_clickhouse_transport(opts);
    transport->connect(opts.endpoints.front());
    for (const auto& probe : probes) {
        SCOPED_TRACE(probe.sql);
        std::size_t result_rows = 0;
        std::size_t typed_columns = 0;
        client_->Select(probe.sql, [&](const Block& b) {
            result_rows += b.GetRowCount();
            typed_columns = std::max(typed_columns, b.GetColumnCount());
            for (std::size_t c = 0; c < b.GetColumnCount(); ++c) {
                EXPECT_EQ(b[c]->Type()->GetName(), "String")
                    << "column " << b.GetColumnName(c) << " on " << line_;
            }
        });
        EXPECT_GT(typed_columns, 0U) << "no block with columns came back on " << line_;
        if (!probe.may_be_empty) {
            EXPECT_GT(result_rows, 0U) << "the probe read nothing on " << line_;
        }
        const auto rs = transport->select(probe.kind, probe.sql);
        EXPECT_EQ(rs.rows.size(), result_rows);
        if (probe.cluster) {
            ASSERT_GT(rs.columns.size(), 2U);
            for (const auto& row : rs.rows) {
                EXPECT_EQ(row[0], self_endpoint) << "the replica's host:port on " << line_;
                EXPECT_EQ(row[1], self_uuid) << "the replica's serverUUID() on " << line_;
            }
        }
    }
    transport->abandon();
}
#endif

namespace {

// What a client reads, and when: every read of the socket during a call, with
// its time and size, so a pin can see how often the server speaks.
struct ReadLog {
    std::mutex mu;
    bool recording{false};
    std::vector<std::pair<std::chrono::steady_clock::time_point, std::size_t>> reads;
};

class TimedInput final : public clickhouse::InputStream {
public:
    TimedInput(std::unique_ptr<clickhouse::InputStream> inner, std::shared_ptr<ReadLog> log)
        : inner_(std::move(inner)), log_(std::move(log)) {}
    bool Skip(std::size_t bytes) override { return inner_->Skip(bytes); }

protected:
    std::size_t DoRead(void* buf, std::size_t len) override {
        const std::size_t n = inner_->Read(buf, len);
        const std::lock_guard<std::mutex> lock(log_->mu);
        if (log_->recording) {
            log_->reads.emplace_back(std::chrono::steady_clock::now(), n);
        }
        return n;
    }

private:
    std::unique_ptr<clickhouse::InputStream> inner_;
    std::shared_ptr<ReadLog> log_;
};

class TimedSocket final : public clickhouse::SocketBase {
public:
    TimedSocket(std::unique_ptr<clickhouse::SocketBase> inner, std::shared_ptr<ReadLog> log)
        : inner_(std::move(inner)), log_(std::move(log)) {}
    [[nodiscard]] std::unique_ptr<clickhouse::InputStream> makeInputStream() const override {
        return std::make_unique<TimedInput>(inner_->makeInputStream(), log_);
    }
    [[nodiscard]] std::unique_ptr<clickhouse::OutputStream> makeOutputStream() const override {
        return inner_->makeOutputStream();
    }

private:
    std::unique_ptr<clickhouse::SocketBase> inner_;
    std::shared_ptr<ReadLog> log_;
};

class TimedSocketFactory final : public clickhouse::SocketFactory {
public:
    explicit TimedSocketFactory(std::shared_ptr<ReadLog> log) : log_(std::move(log)) {}
    std::unique_ptr<clickhouse::SocketBase> connect(const ClientOptions& opts,
                                                    const clickhouse::Endpoint& endpoint) override {
        return std::make_unique<TimedSocket>(inner_.connect(opts, endpoint), log_);
    }

private:
    clickhouse::NonSecureSocketFactory inner_;
    std::shared_ptr<ReadLog> log_;
};

}  // namespace

// P20: what the server sends while it finishes a slow INSERT, here one whose
// two materialised views sleep, and how long it stays silent meanwhile. A
// measurement, not a pass mark: the sink bounds EndInsert by its own deadline
// whether the server keeps the socket busy or not, and a server silent for
// longer than receive_timeout_ms fails the attempt in doubt, which the
// connector page tells users with slow views to allow for.
TEST_F(ClickHousePins, P20WhatTheServerSendsDuringASlowInsert) {
    client_->Execute("CREATE TABLE " + db_ + ".t (k Int64, p Int64) ENGINE = MergeTree ORDER BY k");
    for (const char* view : {"v1", "v2"}) {
        client_->Execute("CREATE TABLE " + db_ + "." + view +
                         "_dst (k Int64, z UInt8) ENGINE = MergeTree ORDER BY k");
        client_->Execute("CREATE MATERIALIZED VIEW " + db_ + "." + view + " TO " + db_ + "." +
                         view + "_dst AS SELECT k, sleepEachRow(0.25) AS z FROM " + db_ + ".t");
    }
    auto log = std::make_shared<ReadLog>();
    auto opts = *server_options();
    opts.SetConnectionRecvTimeout(std::chrono::seconds(60));
    Client c(opts, std::make_unique<TimedSocketFactory>(log));
    c.BeginInsert("INSERT INTO " + db_ + ".t (k, p) SETTINGS " + insert_settings() + " VALUES");
    auto k = std::make_shared<ColumnInt64>();
    auto p = std::make_shared<ColumnInt64>();
    for (std::int64_t i = 0; i < 10; ++i) {
        k->Append(i);
        p->Append(0);
    }
    Block b;
    b.AppendColumn("k", k);
    b.AppendColumn("p", p);
    c.SendInsertBlock(b);
    {
        const std::lock_guard<std::mutex> lock(log->mu);
        log->recording = true;
    }
    const auto t0 = std::chrono::steady_clock::now();
    c.EndInsert();
    const auto t1 = std::chrono::steady_clock::now();
    std::vector<std::pair<std::chrono::steady_clock::time_point, std::size_t>> reads;
    {
        const std::lock_guard<std::mutex> lock(log->mu);
        log->recording = false;
        reads = log->reads;
    }
    auto ms = [](auto d) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
    };
    std::int64_t longest = 0;
    auto previous = t0;
    std::size_t bytes = 0;
    for (const auto& [at, n] : reads) {
        longest = std::max<std::int64_t>(longest, ms(at - previous));
        previous = at;
        bytes += n;
    }
    longest = std::max<std::int64_t>(longest, ms(t1 - previous));
    const auto total = ms(t1 - t0);
    EXPECT_EQ(rows("t"), 10U);
    EXPECT_GE(total, 4000) << "the views did not slow the INSERT down; the pin measures nothing";
    RecordProperty("p20_end_insert_ms", std::to_string(total));
    RecordProperty("p20_reads", std::to_string(reads.size()));
    RecordProperty("p20_bytes", std::to_string(bytes));
    RecordProperty("p20_longest_silence_ms", std::to_string(longest));
    std::cout << "[pin P20] " << line_ << ": EndInsert took " << total << " ms, " << reads.size()
              << " reads of " << bytes << " bytes, longest silence " << longest << " ms\n";
}
