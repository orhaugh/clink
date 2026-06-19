// Unit tests for the pure parsing helpers extracted from
// postgres_cdc_source.cpp into clink::pg. Runs without libpq, so the
// entire test_decoding parse path is now exercised on every CI build,
// not just the Docker-gated integration suite.

#include <cstdint>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_decoder.hpp"

using namespace clink;

// ----- Big-endian readers -----

TEST(PostgresDecoder, ReadBe16) {
    const char buf[2] = {static_cast<char>(0xCA), static_cast<char>(0xFE)};
    EXPECT_EQ(pg::read_be16(buf), 0xCAFE);
}

TEST(PostgresDecoder, ReadBe32) {
    const char buf[4] = {0x01, 0x02, 0x03, 0x04};
    EXPECT_EQ(pg::read_be32(buf), 0x01020304u);
}

TEST(PostgresDecoder, ReadBe64) {
    const char buf[8] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    EXPECT_EQ(pg::read_be64(buf), 0x0102030405060708ULL);
}

TEST(PostgresDecoder, WriteBe64IsInverseOfRead) {
    char buf[8] = {};
    const std::uint64_t v = 0xAABBCCDD11223344ULL;
    pg::write_be64(buf, v);
    EXPECT_EQ(pg::read_be64(buf), v);
    // First byte is highest-order: 0xAA.
    EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0xAA);
    EXPECT_EQ(static_cast<unsigned char>(buf[7]), 0x44);
}

// ----- format_lsn -----

TEST(PostgresDecoder, FormatLsnMatchesPostgresFormat) {
    EXPECT_EQ(pg::format_lsn(0), "0/0");
    EXPECT_EQ(pg::format_lsn(0x16E2A38ULL), "0/16E2A38");
    // hi=1 lo=0
    EXPECT_EQ(pg::format_lsn(0x100000000ULL), "1/0");
    // both halves non-zero
    EXPECT_EQ(pg::format_lsn(0xABCD0000DEADBEEFULL), "ABCD0000/DEADBEEF");
}

// ----- read_cstring -----

TEST(PostgresDecoder, ReadCstringConsumesNul) {
    std::string raw{"public"};
    raw.push_back('\0');
    raw.append("users");
    raw.push_back('\0');

    std::string_view cursor(raw.data(), raw.size());
    EXPECT_EQ(pg::read_cstring(cursor), "public");
    EXPECT_EQ(pg::read_cstring(cursor), "users");
    EXPECT_TRUE(cursor.empty());
}

TEST(PostgresDecoder, ReadCstringHandlesUnterminatedInput) {
    // No NUL - the function should consume the whole cursor and return
    // an empty string rather than reading past the end.
    std::string raw{"abc"};  // no trailing NUL
    std::string_view cursor(raw.data(), raw.size());
    EXPECT_EQ(pg::read_cstring(cursor), "");
    EXPECT_TRUE(cursor.empty());
}

// ----- starts_with -----

TEST(PostgresDecoder, StartsWith) {
    EXPECT_TRUE(pg::starts_with("BEGIN 123", "BEGIN "));
    EXPECT_TRUE(pg::starts_with("BEGIN", "BEGIN"));
    EXPECT_FALSE(pg::starts_with("BEGI", "BEGIN"));
    EXPECT_FALSE(pg::starts_with("", "x"));
    EXPECT_TRUE(pg::starts_with("anything", ""));
}

// ----- lookup_builtin_type_name -----

TEST(PostgresDecoder, LookupBuiltinKnownOids) {
    EXPECT_STREQ(pg::lookup_builtin_type_name(16), "bool");
    EXPECT_STREQ(pg::lookup_builtin_type_name(23), "int4");
    EXPECT_STREQ(pg::lookup_builtin_type_name(25), "text");
    EXPECT_STREQ(pg::lookup_builtin_type_name(1184), "timestamptz");
    EXPECT_STREQ(pg::lookup_builtin_type_name(2950), "uuid");
}

TEST(PostgresDecoder, LookupBuiltinUnknownReturnsNullptr) {
    EXPECT_EQ(pg::lookup_builtin_type_name(99999), nullptr);
    EXPECT_EQ(pg::lookup_builtin_type_name(0), nullptr);
}

// ----- parse_xid -----

TEST(PostgresDecoder, ParseXidExtractsNumber) {
    EXPECT_EQ(pg::parse_xid("BEGIN 12345"), 12345);
    EXPECT_EQ(pg::parse_xid("COMMIT 9876"), 9876);
}

TEST(PostgresDecoder, ParseXidReturnsZeroOnGarbage) {
    EXPECT_EQ(pg::parse_xid("BEGIN"), 0);      // no space
    EXPECT_EQ(pg::parse_xid("BEGIN abc"), 0);  // non-numeric
    EXPECT_EQ(pg::parse_xid(""), 0);
}

// ----- parse_test_decoding (BEGIN/COMMIT) -----

TEST(PostgresDecoder, ParseBeginEvent) {
    auto ev = pg::parse_test_decoding("BEGIN 100", "0/1A");
    EXPECT_EQ(ev.op, CdcEvent::Op::Begin);
    EXPECT_EQ(ev.xid, 100);
    EXPECT_EQ(ev.lsn, "0/1A");
    EXPECT_EQ(ev.table, "");
    EXPECT_TRUE(ev.values.empty());
}

TEST(PostgresDecoder, ParseCommitEvent) {
    auto ev = pg::parse_test_decoding("COMMIT 100", "0/1B");
    EXPECT_EQ(ev.op, CdcEvent::Op::Commit);
    EXPECT_EQ(ev.xid, 100);
    EXPECT_EQ(ev.lsn, "0/1B");
}

// ----- parse_test_decoding (INSERT) -----

TEST(PostgresDecoder, ParseInsertWithMultipleColumns) {
    auto ev = pg::parse_test_decoding(
        "table public.users: INSERT: id[int4]:1 name[text]:'alice' age[int4]:30", "0/100");
    EXPECT_EQ(ev.op, CdcEvent::Op::Insert);
    EXPECT_EQ(ev.table, "public.users");
    EXPECT_EQ(ev.lsn, "0/100");
    ASSERT_EQ(ev.values.size(), 3u);
    EXPECT_EQ(ev.values[0].name, "id");
    EXPECT_EQ(ev.values[0].type, "int4");
    EXPECT_EQ(ev.values[0].value, "1");
    EXPECT_FALSE(ev.values[0].is_null);

    EXPECT_EQ(ev.values[1].name, "name");
    EXPECT_EQ(ev.values[1].type, "text");
    EXPECT_EQ(ev.values[1].value, "alice");

    EXPECT_EQ(ev.values[2].name, "age");
    EXPECT_EQ(ev.values[2].value, "30");
}

TEST(PostgresDecoder, ParseInsertWithNullValue) {
    auto ev =
        pg::parse_test_decoding("table public.t: INSERT: id[int4]:1 nick[text]:null", "0/200");
    ASSERT_EQ(ev.values.size(), 2u);
    EXPECT_FALSE(ev.values[0].is_null);
    EXPECT_TRUE(ev.values[1].is_null);
    EXPECT_EQ(ev.values[1].value, "");
}

TEST(PostgresDecoder, ParseInsertWithEscapedQuotes) {
    // SQL escapes a literal apostrophe by doubling it.
    auto ev = pg::parse_test_decoding("table public.q: INSERT: text[text]:'it''s mine'", "0/300");
    ASSERT_EQ(ev.values.size(), 1u);
    EXPECT_EQ(ev.values[0].value, "it's mine");
}

TEST(PostgresDecoder, ParseInsertWithEmptyStringValue) {
    auto ev = pg::parse_test_decoding("table public.e: INSERT: s[text]:''", "0/400");
    ASSERT_EQ(ev.values.size(), 1u);
    EXPECT_EQ(ev.values[0].value, "");
    EXPECT_FALSE(ev.values[0].is_null);
}

// ----- parse_test_decoding (UPDATE / DELETE) -----

TEST(PostgresDecoder, ParseUpdateEvent) {
    auto ev =
        pg::parse_test_decoding("table public.users: UPDATE: id[int4]:1 name[text]:'bob'", "0/500");
    EXPECT_EQ(ev.op, CdcEvent::Op::Update);
    EXPECT_EQ(ev.table, "public.users");
    ASSERT_EQ(ev.values.size(), 2u);
    EXPECT_EQ(ev.values[1].value, "bob");
}

TEST(PostgresDecoder, ParseDeleteEvent) {
    auto ev = pg::parse_test_decoding("table public.users: DELETE: id[int4]:1", "0/600");
    EXPECT_EQ(ev.op, CdcEvent::Op::Delete);
    EXPECT_EQ(ev.table, "public.users");
    ASSERT_EQ(ev.values.size(), 1u);
    EXPECT_EQ(ev.values[0].name, "id");
    EXPECT_EQ(ev.values[0].value, "1");
}

// ----- parse_test_decoding (malformed input) -----

TEST(PostgresDecoder, ParseUnknownPrefixLandsAsUnknown) {
    auto ev = pg::parse_test_decoding("VACUUM verbose", "0/700");
    EXPECT_EQ(ev.op, CdcEvent::Op::Unknown);
    ASSERT_EQ(ev.values.size(), 1u);
    EXPECT_EQ(ev.values[0].name, "raw");
    EXPECT_EQ(ev.values[0].value, "VACUUM verbose");
}

TEST(PostgresDecoder, ParseTableLineWithoutOpIsUnknown) {
    auto ev = pg::parse_test_decoding("table public.x", "0/800");
    EXPECT_EQ(ev.op, CdcEvent::Op::Unknown);
    ASSERT_FALSE(ev.values.empty());
    EXPECT_EQ(ev.values[0].name, "raw");
}

TEST(PostgresDecoder, ParseTableLineWithUnknownOpYieldsUnknown) {
    auto ev = pg::parse_test_decoding("table public.x: VACUUM: id[int4]:1", "0/900");
    EXPECT_EQ(ev.op, CdcEvent::Op::Unknown);
    EXPECT_EQ(ev.table, "public.x");
}

TEST(PostgresDecoder, ParseEmptyPayloadIsUnknown) {
    auto ev = pg::parse_test_decoding("", "0/A00");
    EXPECT_EQ(ev.op, CdcEvent::Op::Unknown);
    EXPECT_EQ(ev.lsn, "0/A00");
    ASSERT_FALSE(ev.values.empty());
    EXPECT_EQ(ev.values[0].name, "raw");
}

// ----- parse_test_decoding_fields edge cases -----

TEST(PostgresDecoder, ParseFieldsHandlesMissingTypeBracket) {
    // No '[' - parser bails cleanly without overrunning.
    auto fields = pg::parse_test_decoding_fields("name");
    EXPECT_TRUE(fields.empty());
}

TEST(PostgresDecoder, ParseFieldsHandlesMissingColon) {
    auto fields = pg::parse_test_decoding_fields("name[text] value_without_colon");
    EXPECT_TRUE(fields.empty());
}

TEST(PostgresDecoder, ParseFieldsHandlesUnterminatedQuotedString) {
    // Parser consumes up to end of input on unterminated quote - no
    // segfault, but the produced value is the partial read.
    auto fields = pg::parse_test_decoding_fields("s[text]:'unterminated");
    ASSERT_EQ(fields.size(), 1u);
    EXPECT_EQ(fields[0].value, "unterminated");
}

TEST(PostgresDecoder, ParseFieldsHandlesMultipleSpaces) {
    auto fields = pg::parse_test_decoding_fields("  a[int4]:1   b[text]:'x'");
    ASSERT_EQ(fields.size(), 2u);
    EXPECT_EQ(fields[0].name, "a");
    EXPECT_EQ(fields[1].name, "b");
}
