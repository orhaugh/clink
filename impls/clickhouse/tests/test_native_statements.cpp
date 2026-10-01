#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "native/column_plan.hpp"
#include "native/sql_text.hpp"
#include "native/statements.hpp"

namespace clink::clickhouse::native {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;

constexpr std::array<std::uint8_t, 16> kStmtCountingNonce = {
    0x00,
    0x01,
    0x02,
    0x03,
    0x04,
    0x05,
    0x06,
    0x07,
    0x08,
    0x09,
    0x0a,
    0x0b,
    0x0c,
    0x0d,
    0x0e,
    0x0f,
};
constexpr std::string_view kStmtCountingHex = "000102030405060708090a0b0c0d0e0f";

// The server defaults on both tested lines, so the pinned values look real.
constexpr std::uint64_t kStmtSquashRows = 1048449;
constexpr std::uint64_t kStmtSquashBytes = 268402944;

ColumnPlan stmt_plan(std::string column_list) {
    ColumnPlan plan;
    plan.column_list_sql = std::move(column_list);
    return plan;
}

Token stmt_token(std::uint64_t seq) {
    return Token{std::string(kStmtCountingHex), seq};
}

InsertText stmt_insert(const ColumnPlan& plan, bool deduplicate_insert, bool strict_limits) {
    InsertText in;
    in.database = "analytics";
    in.table = "events";
    in.plan = &plan;
    in.caps.deduplicate_insert = deduplicate_insert;
    in.caps.use_strict_insert_block_limits = strict_limits;
    in.caps.min_insert_block_size_rows = kStmtSquashRows;
    in.caps.min_insert_block_size_bytes = kStmtSquashBytes;
    in.token = stmt_token(7);
    in.sink_id = "orders_sink";
    in.subtask = 3;
    return in;
}

// The INSERT text for one combination of the two line-conditional settings,
// spelt out in full so that a change of order or spacing shows.
std::string stmt_expected_insert(std::string_view dedup, bool strict_limits) {
    return std::string(
               "INSERT INTO `analytics`.`events` (`id`, `amount`) SETTINGS async_insert=0, "
               "wait_for_async_insert=1, ") +
           std::string(dedup) +
           ", insert_deduplication_token='clink1-000102030405060708090a0b0c0d0e0f-7', "
           "input_format_native_allow_types_conversion=0, input_format_null_as_default=0, "
           "throw_on_max_partitions_per_insert_block=1, distributed_foreground_insert=1, "
           "min_insert_block_size_rows=1048449, min_insert_block_size_bytes=268402944" +
           (strict_limits ? ", use_strict_insert_block_limits=0" : "") +
           ", log_comment='clink:orders_sink:sub3:7' VALUES";
}

bool stmt_contains(const std::string& haystack, std::string_view needle) {
    return haystack.find(needle) != std::string::npos;
}

std::size_t stmt_count(const std::string& haystack, std::string_view needle) {
    std::size_t n = 0;
    for (auto at = haystack.find(needle); at != std::string::npos;
         at = haystack.find(needle, at + needle.size())) {
        ++n;
    }
    return n;
}

// The comma-separated expressions between SELECT and the first FROM, split at
// top-level commas only, so CAST(count() AS String) stays one item.
std::vector<std::string> stmt_select_list(const std::string& sql) {
    std::vector<std::string> items;
    if (!sql.starts_with("SELECT ")) {
        return items;
    }
    const auto from = sql.find(" FROM ");
    if (from == std::string::npos) {
        return items;
    }
    const std::string list = sql.substr(7, from - 7);
    int depth = 0;
    std::string current;
    for (std::size_t i = 0; i < list.size(); ++i) {
        const char c = list[i];
        if (c == '(') {
            ++depth;
        } else if (c == ')') {
            --depth;
        }
        if (c == ',' && depth == 0) {
            items.push_back(current);
            current.clear();
            if (i + 1 < list.size() && list[i + 1] == ' ') {
                ++i;
            }
            continue;
        }
        current.push_back(c);
    }
    items.push_back(current);
    return items;
}

// Every metadata SELECT the open checks and the re-probe run, under one budget.
std::vector<std::pair<std::string, std::string>> stmt_every_select(seconds budget) {
    return {
        {"server settings", select_server_settings(budget)},
        {"table", select_table("analytics", "events", budget)},
        {"columns", select_columns("analytics", "events", budget)},
        {"merge tree settings", select_merge_tree_settings(budget)},
        {"replicated merge tree settings", select_replicated_merge_tree_settings(budget)},
        {"cluster replica count", select_cluster_replica_count("main", budget)},
        {"cluster tables", select_cluster_tables("main", "analytics", "events_local", budget)},
        {"cluster merge tree settings", select_cluster_merge_tree_settings("main", budget)},
        {"cluster replicated merge tree settings",
         select_cluster_replicated_merge_tree_settings("main", budget)},
    };
}

// ---- quoting -------------------------------------------------------------

TEST(NativeSqlText, QuoteIdentifierEscapesBackticksAndBackslashes) {
    EXPECT_EQ(quote_identifier("events"), "`events`");
    EXPECT_EQ(quote_identifier("we`ird"), "`we\\`ird`");
    EXPECT_EQ(quote_identifier("back\\slash"), "`back\\\\slash`");
    EXPECT_EQ(quote_identifier("``"), "`\\`\\``");
    // A single quote means nothing inside backticks and is left alone.
    EXPECT_EQ(quote_identifier("it's"), "`it's`");
    EXPECT_EQ(quote_identifier(""), "``");
}

TEST(NativeSqlText, QuoteStringEscapesQuotesAndBackslashes) {
    EXPECT_EQ(quote_string("plain"), "'plain'");
    EXPECT_EQ(quote_string("o'brien"), "'o\\'brien'");
    EXPECT_EQ(quote_string("c:\\tmp"), "'c:\\\\tmp'");
    // A backslash before a quote must not escape the closing quote.
    EXPECT_EQ(quote_string("\\'"), "'\\\\\\''");
    EXPECT_EQ(quote_string("tail\\"), "'tail\\\\'");
    // A backtick means nothing inside single quotes and is left alone.
    EXPECT_EQ(quote_string("a`b"), "'a`b'");
    EXPECT_EQ(quote_string(""), "''");
}

TEST(NativeSqlText, QuotedTextKeepsEmbeddedNulAndNewlineBytes) {
    const std::string value("a\nb\0c", 5);
    const std::string expected("'a\nb\0c'", 7);
    EXPECT_EQ(quote_string(value), expected);
}

TEST(NativeSqlText, QualifiedTableQuotesBothParts) {
    EXPECT_EQ(qualified_table("analytics", "events"), "`analytics`.`events`");
    EXPECT_EQ(qualified_table("my`db", "t\\1"), "`my\\`db`.`t\\\\1`");
    // A dot inside a name stays inside its quotes rather than adding a level.
    EXPECT_EQ(qualified_table("a.b", "c"), "`a.b`.`c`");
}

// ---- tokens --------------------------------------------------------------

TEST(NativeSqlText, TokenTextIsVersionNonceAndSequence) {
    EXPECT_EQ(stmt_token(42).text(), "clink1-000102030405060708090a0b0c0d0e0f-42");
}

TEST(NativeSqlText, NonceHexIsLowerCaseAndZeroPadded) {
    const TokenSource tokens({0xde,
                              0xad,
                              0xbe,
                              0xef,
                              0x00,
                              0x01,
                              0xff,
                              0xa0,
                              0x0a,
                              0x10,
                              0x7f,
                              0x80,
                              0xc3,
                              0x3c,
                              0x05,
                              0x50});
    EXPECT_EQ(tokens.nonce_hex(), "deadbeef0001ffa00a107f80c33c0550");
}

TEST(NativeSqlText, SequenceStartsAtOneAndRisesByOne) {
    TokenSource tokens(kStmtCountingNonce);
    EXPECT_EQ(tokens.nonce_hex(), kStmtCountingHex);
    const Token first = tokens.next();
    EXPECT_EQ(first.seq, 1U);
    EXPECT_EQ(first.nonce_hex, kStmtCountingHex);
    EXPECT_EQ(first.text(), "clink1-000102030405060708090a0b0c0d0e0f-1");
    std::uint64_t previous = first.seq;
    for (int i = 0; i < 10000; ++i) {
        const Token t = tokens.next();
        ASSERT_EQ(t.seq, previous + 1);
        ASSERT_EQ(t.nonce_hex, kStmtCountingHex);
        previous = t.seq;
    }
    EXPECT_EQ(previous, 10001U);
}

TEST(NativeSqlText, RandomSourcesNeverShareANonceAcross10000Opens) {
    std::unordered_set<std::string> nonces;
    std::unordered_set<std::string> first_tokens;
    for (int i = 0; i < 10000; ++i) {
        TokenSource tokens = TokenSource::random();
        const std::string& hex = tokens.nonce_hex();
        ASSERT_EQ(hex.size(), 32U) << hex;
        for (const char c : hex) {
            ASSERT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) << hex;
        }
        const Token first = tokens.next();
        ASSERT_EQ(first.seq, 1U);
        ASSERT_EQ(first.nonce_hex, hex);
        nonces.insert(hex);
        first_tokens.insert(first.text());
    }
    EXPECT_EQ(nonces.size(), 10000U);
    EXPECT_EQ(first_tokens.size(), 10000U);
}

TEST(NativeSqlText, RandomNoncesUseAll128Bits) {
    // Across 10000 draws every bit of every byte is set at least once and
    // clear at least once; a nonce filled from fewer random bits than it
    // has, or with a stuck byte, fails this. A false failure needs a bit to
    // stay put 10000 times running.
    std::array<std::uint8_t, 16> ever_set{};
    std::array<std::uint8_t, 16> ever_clear{};
    for (int i = 0; i < 10000; ++i) {
        const std::string hex = TokenSource::random().nonce_hex();
        ASSERT_EQ(hex.size(), 32U);
        for (std::size_t b = 0; b < 16; ++b) {
            const auto byte =
                static_cast<std::uint8_t>(std::stoul(hex.substr(b * 2, 2), nullptr, 16));
            ever_set[b] = static_cast<std::uint8_t>(ever_set[b] | byte);
            ever_clear[b] =
                static_cast<std::uint8_t>(ever_clear[b] | static_cast<std::uint8_t>(~byte));
        }
    }
    for (std::size_t b = 0; b < 16; ++b) {
        EXPECT_EQ(static_cast<unsigned>(ever_set[b]), 0xFFU) << "byte " << b;
        EXPECT_EQ(static_cast<unsigned>(ever_clear[b]), 0xFFU) << "byte " << b;
    }
}

// ---- legacy prefix -------------------------------------------------------

TEST(NativeSqlText, LegacyPrefixCarriesTheSettingsAndTheToken) {
    TokenSource tokens(kStmtCountingNonce);
    const Token first = tokens.next();
    EXPECT_EQ(legacy_insert_prefix("analytics", "events", first, "JSONEachRow"),
              "INSERT INTO `analytics`.`events` SETTINGS async_insert=0, wait_for_async_insert=1, "
              "insert_deduplication_token='clink1-000102030405060708090a0b0c0d0e0f-1' FORMAT "
              "JSONEachRow");
    const Token second = tokens.next();
    EXPECT_EQ(legacy_insert_prefix("analytics", "events", second, "TSV"),
              "INSERT INTO `analytics`.`events` SETTINGS async_insert=0, wait_for_async_insert=1, "
              "insert_deduplication_token='clink1-000102030405060708090a0b0c0d0e0f-2' FORMAT TSV");
}

TEST(NativeSqlText, LegacyPrefixQuotesTheTableName) {
    EXPECT_EQ(legacy_insert_prefix("my`db", "it's\\t", stmt_token(5), "TSV"),
              "INSERT INTO `my\\`db`.`it's\\\\t` SETTINGS async_insert=0, wait_for_async_insert=1, "
              "insert_deduplication_token='clink1-000102030405060708090a0b0c0d0e0f-5' FORMAT TSV");
}

TEST(NativeSqlText, LegacyPrefixRefusesATokenNoSourceIssued) {
    EXPECT_THROW((void)legacy_insert_prefix("db", "t", Token{}, "TSV"), std::invalid_argument);
    EXPECT_THROW((void)legacy_insert_prefix("db", "t", stmt_token(0), "TSV"),
                 std::invalid_argument);
    EXPECT_THROW(
        (void)legacy_insert_prefix("db", "t", Token{"000102030405060708090A0B0C0D0E0F", 1}, "TSV"),
        std::invalid_argument);
    EXPECT_THROW((void)legacy_insert_prefix("db", "t", Token{"0001", 1}, "TSV"),
                 std::invalid_argument);
}

TEST(NativeSqlText, LegacyPrefixRefusesAFormatThatIsNotAName) {
    EXPECT_THROW((void)legacy_insert_prefix("db", "t", stmt_token(1), ""), std::invalid_argument);
    EXPECT_THROW((void)legacy_insert_prefix("db", "t", stmt_token(1), "TSV\nSELECT 1"),
                 std::invalid_argument);
    EXPECT_THROW((void)legacy_insert_prefix("db", "t", stmt_token(1), "TSV SETTINGS x=1"),
                 std::invalid_argument);
}

// ---- INSERT --------------------------------------------------------------

TEST(NativeStatements, InsertWithDeduplicateInsertAndStrictLimits) {
    const ColumnPlan plan = stmt_plan("(`id`, `amount`)");
    EXPECT_EQ(insert_statement(stmt_insert(plan, true, true)),
              stmt_expected_insert("deduplicate_insert='enable'", true));
}

TEST(NativeStatements, InsertWithDeduplicateInsertOnly) {
    const ColumnPlan plan = stmt_plan("(`id`, `amount`)");
    EXPECT_EQ(insert_statement(stmt_insert(plan, true, false)),
              stmt_expected_insert("deduplicate_insert='enable'", false));
}

TEST(NativeStatements, InsertWithStrictLimitsOnly) {
    const ColumnPlan plan = stmt_plan("(`id`, `amount`)");
    EXPECT_EQ(insert_statement(stmt_insert(plan, false, true)),
              stmt_expected_insert("insert_deduplicate=1", true));
}

TEST(NativeStatements, InsertWithNeitherLineConditionalSetting) {
    const ColumnPlan plan = stmt_plan("(`id`, `amount`)");
    EXPECT_EQ(insert_statement(stmt_insert(plan, false, false)),
              stmt_expected_insert("insert_deduplicate=1", false));
}

TEST(NativeStatements, EveryInsertCarriesTheFixedSettingsAndAToken) {
    const ColumnPlan plan = stmt_plan("(`id`)");
    for (const bool dedup : {false, true}) {
        for (const bool strict : {false, true}) {
            InsertText in = stmt_insert(plan, dedup, strict);
            in.caps.min_insert_block_size_rows = 4096;
            in.caps.min_insert_block_size_bytes = 1234567;
            const std::string sql = insert_statement(in);
            SCOPED_TRACE(sql);
            EXPECT_TRUE(sql.starts_with("INSERT INTO `analytics`.`events` (`id`) SETTINGS "));
            EXPECT_TRUE(sql.ends_with("' VALUES"));
            for (const std::string_view setting : {"wait_for_async_insert=1",
                                                   "input_format_native_allow_types_conversion=0",
                                                   "input_format_null_as_default=0",
                                                   "throw_on_max_partitions_per_insert_block=1",
                                                   "distributed_foreground_insert=1",
                                                   "min_insert_block_size_rows=4096",
                                                   "min_insert_block_size_bytes=1234567"}) {
                EXPECT_EQ(stmt_count(sql, std::string(", ") + std::string(setting) + ","), 1U)
                    << setting;
            }
            EXPECT_EQ(stmt_count(sql, "SETTINGS async_insert=0, "), 1U);
            EXPECT_EQ(stmt_count(sql,
                                 "insert_deduplication_token='clink1-" +
                                     std::string(kStmtCountingHex) + "-7'"),
                      1U);
            EXPECT_EQ(stmt_contains(sql, "deduplicate_insert='enable'"), dedup);
            EXPECT_EQ(stmt_contains(sql, "insert_deduplicate=1"), !dedup);
            EXPECT_EQ(stmt_contains(sql, "use_strict_insert_block_limits=0"), strict);
            // async_insert is set once, to 0, and nothing later overrides it.
            EXPECT_EQ(stmt_count(sql, " async_insert="), 1U);
        }
    }
}

TEST(NativeStatements, InsertSendsThePinnedSquashThresholdsAsGiven) {
    const ColumnPlan plan = stmt_plan("(`id`)");
    InsertText in = stmt_insert(plan, true, false);
    in.caps.min_insert_block_size_rows = 0;
    in.caps.min_insert_block_size_bytes = 18446744073709551615ULL;
    const std::string sql = insert_statement(in);
    EXPECT_TRUE(stmt_contains(
        sql,
        ", min_insert_block_size_rows=0, min_insert_block_size_bytes=18446744073709551615, "
        "log_comment="))
        << sql;
}

TEST(NativeStatements, DistinctTokensGiveDistinctInsertsThatCorrelateInTheLog) {
    const ColumnPlan plan = stmt_plan("(`id`)");
    TokenSource tokens(kStmtCountingNonce);
    InsertText in = stmt_insert(plan, true, true);
    in.token = tokens.next();
    const std::string first = insert_statement(in);
    in.token = tokens.next();
    const std::string second = insert_statement(in);
    EXPECT_NE(first, second);
    EXPECT_TRUE(stmt_contains(
        first, "insert_deduplication_token='clink1-" + std::string(kStmtCountingHex) + "-1'"));
    EXPECT_TRUE(stmt_contains(first, "log_comment='clink:orders_sink:sub3:1' VALUES"));
    EXPECT_TRUE(stmt_contains(
        second, "insert_deduplication_token='clink1-" + std::string(kStmtCountingHex) + "-2'"));
    EXPECT_TRUE(stmt_contains(second, "log_comment='clink:orders_sink:sub3:2' VALUES"));
}

TEST(NativeStatements, InsertQuotesTheTableAndUsesTheColumnListVerbatim) {
    const ColumnPlan plan = stmt_plan("(`we\\`ird`, `back\\\\slash`)");
    InsertText in = stmt_insert(plan, false, false);
    in.database = "o'db";
    in.table = "t`1\\";
    const std::string sql = insert_statement(in);
    EXPECT_TRUE(sql.starts_with(
        "INSERT INTO `o'db`.`t\\`1\\\\` (`we\\`ird`, `back\\\\slash`) SETTINGS async_insert=0, "))
        << sql;
}

TEST(NativeStatements, InsertSanitisesTheSinkIdInTheLogComment) {
    const ColumnPlan plan = stmt_plan("(`id`)");
    InsertText in = stmt_insert(plan, false, false);
    in.sink_id = "Sink: orders/eu 1 'x' \\ \xc3\xa9.v-2";
    in.subtask = 12;
    const std::string sql = insert_statement(in);
    EXPECT_TRUE(
        sql.ends_with(", log_comment='clink:Sink__orders_eu_1__x______.v-2:sub12:7' VALUES"))
        << sql;
}

TEST(NativeStatements, SanitiseSinkIdKeepsOnlyTheSafeCharacters) {
    EXPECT_EQ(sanitise_sink_id("orders_sink.v2-a"), "orders_sink.v2-a");
    EXPECT_EQ(sanitise_sink_id("ABCxyz019"), "ABCxyz019");
    EXPECT_EQ(sanitise_sink_id("a b:c/d'e\\f`g"), "a_b_c_d_e_f_g");
    EXPECT_EQ(sanitise_sink_id(std::string("x\0y\n", 4)), "x_y_");
    // Each byte of a multi-byte character is replaced, so the id stays ASCII.
    EXPECT_EQ(sanitise_sink_id("caf\xc3\xa9"), "caf__");
    EXPECT_EQ(sanitise_sink_id(""), "");
    EXPECT_EQ(sanitise_sink_id(sanitise_sink_id("a b")), "a_b");
}

TEST(NativeStatements, InsertRefusesAMissingPlanOrColumnList) {
    InsertText in = stmt_insert(stmt_plan("(`id`)"), false, false);
    in.plan = nullptr;
    EXPECT_THROW((void)insert_statement(in), std::invalid_argument);
    const ColumnPlan empty = stmt_plan("");
    in.plan = &empty;
    EXPECT_THROW((void)insert_statement(in), std::invalid_argument);
}

TEST(NativeStatements, InsertRefusesATokenNoSourceIssued) {
    const ColumnPlan plan = stmt_plan("(`id`)");
    InsertText in = stmt_insert(plan, false, false);
    in.token = Token{};
    EXPECT_THROW((void)insert_statement(in), std::invalid_argument);
    in.token = stmt_token(0);
    EXPECT_THROW((void)insert_statement(in), std::invalid_argument);
    in.token = Token{"000102030405060708090a0b0c0d0e0", 1};
    EXPECT_THROW((void)insert_statement(in), std::invalid_argument);
    in.token = Token{"000102030405060708090a0b0c0d0e0g", 1};
    EXPECT_THROW((void)insert_statement(in), std::invalid_argument);
}

// ---- bounded reads -------------------------------------------------------

TEST(NativeStatements, MetadataBudgetIsTheReceiveTimeoutCappedAtTenSecondsRoundedUp) {
    EXPECT_EQ(metadata_budget(milliseconds{30000}), seconds{10});
    EXPECT_EQ(metadata_budget(milliseconds{10000}), seconds{10});
    EXPECT_EQ(metadata_budget(milliseconds{10001}), seconds{10});
    EXPECT_EQ(metadata_budget(milliseconds{9001}), seconds{10});
    EXPECT_EQ(metadata_budget(milliseconds{9000}), seconds{9});
    EXPECT_EQ(metadata_budget(milliseconds{2500}), seconds{3});
    EXPECT_EQ(metadata_budget(milliseconds{1001}), seconds{2});
    EXPECT_EQ(metadata_budget(milliseconds{1000}), seconds{1});
    EXPECT_EQ(metadata_budget(milliseconds{1}), seconds{1});
    EXPECT_EQ(metadata_budget(milliseconds{0}), seconds{1});
    EXPECT_EQ(metadata_budget(milliseconds{-5000}), seconds{1});
}

TEST(NativeStatements, BoundedSettingsThrowOnOverflow) {
    EXPECT_EQ(bounded_settings(seconds{10}),
              "SETTINGS max_execution_time=10, timeout_overflow_mode='throw'");
    EXPECT_EQ(bounded_settings(seconds{1}),
              "SETTINGS max_execution_time=1, timeout_overflow_mode='throw'");
}

TEST(NativeStatements, BoundedSettingsNeverSendAnUnlimitedBudget) {
    // max_execution_time=0 means no limit on the server.
    EXPECT_EQ(bounded_settings(seconds{0}),
              "SETTINGS max_execution_time=1, timeout_overflow_mode='throw'");
    EXPECT_EQ(bounded_settings(seconds{-3}),
              "SETTINGS max_execution_time=1, timeout_overflow_mode='throw'");
}

// ---- metadata SELECTs ----------------------------------------------------

TEST(NativeStatements, EverySelectCastsEachExpressionToStringAndEndsBounded) {
    const seconds budget{4};
    const std::string tail = " SETTINGS max_execution_time=4, timeout_overflow_mode='throw'";
    for (const auto& [what, sql] : stmt_every_select(budget)) {
        SCOPED_TRACE(what + ": " + sql);
        EXPECT_TRUE(sql.ends_with(tail));
        EXPECT_EQ(stmt_count(sql, "SETTINGS "), 1U);
        const auto items = stmt_select_list(sql);
        ASSERT_FALSE(items.empty());
        for (const auto& item : items) {
            EXPECT_TRUE(item.starts_with("CAST(") && item.ends_with(" AS String)")) << item;
        }
        EXPECT_EQ(stmt_count(sql, "CAST("), items.size());
    }
}

TEST(NativeStatements, SelectServerSettingsListsEveryProbedSetting) {
    EXPECT_EQ(
        select_server_settings(seconds{10}),
        "SELECT CAST(name AS String), CAST(value AS String) FROM system.settings WHERE name IN "
        "('async_insert', 'wait_for_async_insert', 'insert_deduplication_token', "
        "'input_format_native_allow_types_conversion', 'input_format_null_as_default', "
        "'throw_on_max_partitions_per_insert_block', 'log_comment', 'max_execution_time', "
        "'timeout_overflow_mode', 'distributed_foreground_insert', 'min_insert_block_size_rows', "
        "'min_insert_block_size_bytes', 'deduplicate_insert', 'use_strict_insert_block_limits', "
        "'insert_deduplicate', 'max_partitions_per_insert_block', 'insert_quorum') SETTINGS "
        "max_execution_time=10, timeout_overflow_mode='throw'");
}

TEST(NativeStatements, SelectServerSettingsCoversBothSettingLists) {
    const std::string sql = select_server_settings(seconds{1});
    for (const auto& name : required_settings()) {
        EXPECT_EQ(stmt_count(sql, "'" + name + "'"), 1U) << name;
    }
    for (const auto& name : line_conditional_settings()) {
        EXPECT_EQ(stmt_count(sql, "'" + name + "'"), 1U) << name;
    }
}

TEST(NativeStatements, RequiredSettingsAreTheOnesEveryInsertSends) {
    const std::vector<std::string> expected = {
        "async_insert",
        "wait_for_async_insert",
        "insert_deduplication_token",
        "input_format_native_allow_types_conversion",
        "input_format_null_as_default",
        "throw_on_max_partitions_per_insert_block",
        "log_comment",
        "max_execution_time",
        "timeout_overflow_mode",
        "distributed_foreground_insert",
        "min_insert_block_size_rows",
        "min_insert_block_size_bytes",
    };
    EXPECT_EQ(required_settings(), expected);

    // Every setting the INSERT and the bounded SELECTs send unconditionally
    // is one the gate requires, so the gate cannot pass a server that would
    // reject the statement.
    const ColumnPlan plan = stmt_plan("(`id`)");
    const std::string insert = insert_statement(stmt_insert(plan, false, false));
    const std::string bounded = bounded_settings(seconds{1});
    const std::set<std::string> listed(expected.begin(), expected.end());
    for (const std::string_view sent : {"async_insert",
                                        "wait_for_async_insert",
                                        "insert_deduplication_token",
                                        "input_format_native_allow_types_conversion",
                                        "input_format_null_as_default",
                                        "throw_on_max_partitions_per_insert_block",
                                        "distributed_foreground_insert",
                                        "min_insert_block_size_rows",
                                        "min_insert_block_size_bytes",
                                        "log_comment"}) {
        EXPECT_TRUE(stmt_contains(insert, std::string(sent) + "=")) << sent;
        EXPECT_EQ(listed.count(std::string(sent)), 1U) << sent;
    }
    for (const std::string_view sent : {"max_execution_time", "timeout_overflow_mode"}) {
        EXPECT_TRUE(stmt_contains(bounded, std::string(sent) + "=")) << sent;
        EXPECT_EQ(listed.count(std::string(sent)), 1U) << sent;
    }
}

TEST(NativeStatements, LineConditionalSettingsNeverOverlapTheRequiredOnes) {
    const std::vector<std::string> expected = {"deduplicate_insert",
                                               "use_strict_insert_block_limits"};
    EXPECT_EQ(line_conditional_settings(), expected);
    const auto& required = required_settings();
    const std::set<std::string> required_set(required.begin(), required.end());
    for (const auto& name : line_conditional_settings()) {
        EXPECT_EQ(required_set.count(name), 0U) << name;
    }
    // The older deduplication switch is the alternative to deduplicate_insert,
    // so neither list makes it mandatory.
    EXPECT_EQ(required_set.count("insert_deduplicate"), 0U);
}

TEST(NativeStatements, SelectTableReadsEngineAndEngineFull) {
    EXPECT_EQ(select_table("analytics", "events", seconds{10}),
              "SELECT CAST(engine AS String), CAST(engine_full AS String) FROM system.tables "
              "WHERE database = 'analytics' AND name = 'events' SETTINGS max_execution_time=10, "
              "timeout_overflow_mode='throw'");
}

TEST(NativeStatements, SelectColumnsReadsInPositionOrder) {
    EXPECT_EQ(select_columns("analytics", "events", seconds{2}),
              "SELECT CAST(name AS String), CAST(type AS String), CAST(default_kind AS String), "
              "CAST(position AS String) FROM system.columns WHERE database = 'analytics' AND "
              "table = 'events' ORDER BY position SETTINGS max_execution_time=2, "
              "timeout_overflow_mode='throw'");
}

TEST(NativeStatements, SelectLiteralsAreQuotedStrings) {
    const std::string table = select_table("o'brien\\db", "t`1'", seconds{1});
    EXPECT_TRUE(stmt_contains(table, "WHERE database = 'o\\'brien\\\\db' AND name = 't`1\\'' "))
        << table;
    const std::string columns = select_columns("o'brien\\db", "t`1'", seconds{1});
    EXPECT_TRUE(
        stmt_contains(columns, "WHERE database = 'o\\'brien\\\\db' AND table = 't`1\\'' ORDER BY"))
        << columns;
    const std::string count = select_cluster_replica_count("it's", seconds{1});
    EXPECT_TRUE(stmt_contains(count, "WHERE cluster = 'it\\'s' SETTINGS")) << count;
    const std::string tables = select_cluster_tables("it's", "d'b", "t\\", seconds{1});
    EXPECT_TRUE(stmt_contains(tables,
                              "FROM clusterAllReplicas('it\\'s', system.tables) WHERE database = "
                              "'d\\'b' AND name = 't\\\\' SETTINGS"))
        << tables;
    const std::string settings = select_cluster_merge_tree_settings("it's", seconds{1});
    EXPECT_TRUE(
        stmt_contains(settings, "FROM clusterAllReplicas('it\\'s', system.merge_tree_settings)"))
        << settings;
}

TEST(NativeStatements, SelectMergeTreeSettingsReadsAsyncInsertAndBothWindows) {
    EXPECT_EQ(select_merge_tree_settings(seconds{10}),
              "SELECT CAST(name AS String), CAST(value AS String) FROM system.merge_tree_settings "
              "WHERE name IN ('async_insert', 'non_replicated_deduplication_window', "
              "'replicated_deduplication_window') SETTINGS max_execution_time=10, "
              "timeout_overflow_mode='throw'");
}

TEST(NativeStatements, SelectReplicatedMergeTreeSettingsReadsTheReplicatedTable) {
    EXPECT_EQ(select_replicated_merge_tree_settings(seconds{10}),
              "SELECT CAST(name AS String), CAST(value AS String) FROM "
              "system.replicated_merge_tree_settings WHERE name IN ('async_insert', "
              "'non_replicated_deduplication_window', 'replicated_deduplication_window') SETTINGS "
              "max_execution_time=10, timeout_overflow_mode='throw'");
}

TEST(NativeStatements, ReplicatedSettingsSelectsMatchTheirMergeTreeCounterparts) {
    // The replicated reads are the same query against the other table, so a
    // row from either answers the same question.
    const seconds budget{3};
    std::string plain = select_merge_tree_settings(budget);
    std::string replicated = select_replicated_merge_tree_settings(budget);
    const std::string from = "system.merge_tree_settings";
    const std::string to = "system.replicated_merge_tree_settings";
    ASSERT_EQ(stmt_count(plain, from), 1U);
    plain.replace(plain.find(from), from.size(), to);
    EXPECT_EQ(plain, replicated);

    std::string cluster_plain = select_cluster_merge_tree_settings("main", budget);
    const std::string cluster_replicated =
        select_cluster_replicated_merge_tree_settings("main", budget);
    ASSERT_EQ(stmt_count(cluster_plain, from), 1U);
    cluster_plain.replace(cluster_plain.find(from), from.size(), to);
    EXPECT_EQ(cluster_plain, cluster_replicated);
}

TEST(NativeStatements, SelectClusterReplicaCountCountsTheClusterRows) {
    EXPECT_EQ(select_cluster_replica_count("main", seconds{10}),
              "SELECT CAST(count() AS String) FROM system.clusters WHERE cluster = 'main' "
              "SETTINGS max_execution_time=10, timeout_overflow_mode='throw'");
}

TEST(NativeStatements, SelectClusterTablesNamesEachReplica) {
    EXPECT_EQ(select_cluster_tables("main", "analytics", "events_local", seconds{10}),
              "SELECT CAST(hostName() AS String), CAST(engine AS String), CAST(engine_full AS "
              "String) FROM clusterAllReplicas('main', system.tables) WHERE database = "
              "'analytics' AND name = 'events_local' SETTINGS max_execution_time=10, "
              "timeout_overflow_mode='throw'");
}

TEST(NativeStatements, SelectClusterMergeTreeSettingsNamesEachReplica) {
    EXPECT_EQ(select_cluster_merge_tree_settings("main", seconds{10}),
              "SELECT CAST(hostName() AS String), CAST(name AS String), CAST(value AS String) "
              "FROM clusterAllReplicas('main', system.merge_tree_settings) WHERE name IN "
              "('async_insert', 'non_replicated_deduplication_window', "
              "'replicated_deduplication_window') SETTINGS max_execution_time=10, "
              "timeout_overflow_mode='throw'");
}

TEST(NativeStatements, SelectClusterReplicatedMergeTreeSettingsNamesEachReplica) {
    EXPECT_EQ(select_cluster_replicated_merge_tree_settings("main", seconds{10}),
              "SELECT CAST(hostName() AS String), CAST(name AS String), CAST(value AS String) "
              "FROM clusterAllReplicas('main', system.replicated_merge_tree_settings) WHERE name "
              "IN ('async_insert', 'non_replicated_deduplication_window', "
              "'replicated_deduplication_window') SETTINGS max_execution_time=10, "
              "timeout_overflow_mode='throw'");
}

}  // namespace
}  // namespace clink::clickhouse::native
