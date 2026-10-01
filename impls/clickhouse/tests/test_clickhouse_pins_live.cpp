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
// ReplicatedMergeTree) and runs this suite against it.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

#include <clickhouse/client.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <gtest/gtest.h>

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
