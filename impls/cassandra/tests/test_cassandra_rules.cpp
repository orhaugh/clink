// The Cassandra sinks' rules, without a server: names, consistency levels, the
// key types the upsert sink accepts, how a changelog record becomes an insert or
// a delete, and the redaction of server messages.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cassandra/cassandra_rules.hpp"
#include "clink/cassandra/connection_params.hpp"

using clink::cassandra::CassandraConsistency;
using clink::cassandra::CassandraKeyColumn;
using clink::cassandra::CassandraRecordError;
using clink::cassandra::cql_identifier;
using clink::cassandra::cql_key_type;
using clink::cassandra::CqlKeyType;
using clink::cassandra::kCqlMaxTableNameLength;
using clink::cassandra::parse_cassandra_consistency;
using clink::cassandra::prepare_cassandra_upsert_record;
using clink::cassandra::redact_cassandra_message;

namespace {

std::string refusal(const std::string& record, const std::vector<CassandraKeyColumn>& key) {
    try {
        (void)prepare_cassandra_upsert_record(record, key);
    } catch (const CassandraRecordError& e) {
        return e.what();
    }
    return "";
}

const std::vector<CassandraKeyColumn> kIntKey = {{"id", CqlKeyType::Int}};

// A JSON \u escape, built at run time so that no tool on the way to the
// compiler can turn the escape into the character it names.
std::string esc(const char* hex) {
    return std::string{"\\"} + "u" + hex;
}

}  // namespace

TEST(CassandraRules, NamesAreUnquotedCqlIdentifiersFoldedToLowerCase) {
    EXPECT_EQ(cql_identifier("Sensor_Out2", kCqlMaxTableNameLength), "sensor_out2");
    EXPECT_EQ(cql_identifier("t", kCqlMaxTableNameLength), "t");
    for (const char* bad : {"", "2t", "_t", "my-table", "my table", "ks.t", "t\"", "é"}) {
        std::string why;
        EXPECT_FALSE(cql_identifier(bad, kCqlMaxTableNameLength, &why)) << bad;
        EXPECT_FALSE(why.empty()) << bad;
    }
    EXPECT_TRUE(cql_identifier(std::string(48, 'a'), kCqlMaxTableNameLength));
    EXPECT_FALSE(cql_identifier(std::string(49, 'a'), kCqlMaxTableNameLength));
}

TEST(CassandraRules, ConsistencyLevelsAreAClosedSetIgnoringCase) {
    EXPECT_EQ(parse_cassandra_consistency("LOCAL_QUORUM"), CassandraConsistency::LocalQuorum);
    EXPECT_EQ(parse_cassandra_consistency("each_quorum"), CassandraConsistency::EachQuorum);
    EXPECT_EQ(parse_cassandra_consistency("One"), CassandraConsistency::One);
    for (const char* refused : {"any", "serial", "local_serial", "", "quorum2", "local-one"}) {
        EXPECT_FALSE(parse_cassandra_consistency(refused)) << refused;
    }
}

TEST(CassandraRules, KeyTypesAreTheNativeScalarTypes) {
    EXPECT_EQ(cql_key_type("varchar"), CqlKeyType::Text);
    EXPECT_EQ(cql_key_type("timeuuid"), CqlKeyType::TimeUuid);
    for (const char* refused :
         {"list", "set", "map", "tuple", "udt", "custom", "duration", "counter"}) {
        EXPECT_FALSE(cql_key_type(refused)) << refused;
    }
}

TEST(CassandraRules, AnInsertKeepsTheRecordsTextAndDigits) {
    const std::string rec = R"({"id": 7, "d": 12345678901234567890.123456789, "v": "a"})";
    const auto r = prepare_cassandra_upsert_record(rec, kIntKey);
    EXPECT_FALSE(r.is_delete);
    EXPECT_EQ(r.insert_json, rec);  // no __row_kind: byte for byte
    ASSERT_EQ(r.key_json.size(), 1u);
    EXPECT_EQ(r.key_json[0], "7");
}

TEST(CassandraRules, TheRowKindIsRemovedAndEveryOtherMemberKeptAsWritten) {
    const std::string e = esc("00e9");
    const auto r = prepare_cassandra_upsert_record(
        R"({"id":7,"__row_kind":"update_after","d":1.10,"s":")" + e + R"(\n"})", kIntKey);
    EXPECT_FALSE(r.is_delete);
    EXPECT_EQ(r.insert_json, R"({"id":7,"d":1.10,"s":")" + e + R"(\n"})");
}

TEST(CassandraRules, DeletesAndUpdateBeforesAreDeletes) {
    for (const char* kind : {"delete", "update_before"}) {
        const auto r = prepare_cassandra_upsert_record(
            std::string{R"({"id":7,"__row_kind":")"} + kind + "\"}", kIntKey);
        EXPECT_TRUE(r.is_delete) << kind;
        EXPECT_TRUE(r.insert_json.empty()) << kind;
        EXPECT_EQ(r.key_json, std::vector<std::string>{"7"}) << kind;
    }
    EXPECT_FALSE(
        prepare_cassandra_upsert_record(R"({"id":7,"__row_kind":"insert"})", kIntKey).is_delete);
}

TEST(CassandraRules, AnUnknownRowKindIsRefusedNotTakenForAnInsert) {
    for (const char* rec : {R"({"id":1,"__row_kind":"upsert"})",
                            R"({"id":1,"__row_kind":1})",
                            R"({"id":1,"__row_kind":null})",
                            R"({"id":1,"__row_kind":"DELETE"})",
                            R"({"id":1,"__ROW_KIND":"remove"})"}) {
        EXPECT_NE(refusal(rec, kIntKey).find("__row_kind"), std::string::npos) << rec;
    }
}

TEST(CassandraRules, KeyColumnsMustBePresentNotNullAndOfTheirType) {
    EXPECT_NE(refusal(R"({"v":"a"})", kIntKey).find("'id' is missing"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":null})", kIntKey).find("'id' is null"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":"1"})", kIntKey).find("int and needs a JSON integer"),
              std::string::npos);
    EXPECT_NE(refusal(R"({"id":1.0})", kIntKey).find("int"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":2147483648})", kIntKey).find("within 32 bits"), std::string::npos);
    EXPECT_EQ(refusal(R"({"id":-2147483648})", kIntKey), "");
    EXPECT_NE(refusal("[1]", kIntKey).find("not a JSON object"), std::string::npos);
    EXPECT_NE(refusal("{\"id\":1", kIntKey).find("not valid JSON"), std::string::npos);
}

TEST(CassandraRules, NumericKeysKeepTheirDigitsAndRespectTheirRanges) {
    const std::vector<CassandraKeyColumn> big = {{"id", CqlKeyType::BigInt}};
    // Past 2^53 a double would name another row.
    EXPECT_EQ(prepare_cassandra_upsert_record(R"({"id":9007199254740993})", big).key_json[0],
              "9007199254740993");
    EXPECT_EQ(refusal(R"({"id":9223372036854775807})", big), "");
    EXPECT_NE(refusal(R"({"id":9223372036854775808})", big).find("within 64 bits"),
              std::string::npos);
    const std::vector<CassandraKeyColumn> var = {{"id", CqlKeyType::Varint}};
    // Up to 2^64 - 1 the digits pass through as written; the reader that checks
    // the record refuses an integer past that, and the refusal says so.
    EXPECT_EQ(prepare_cassandra_upsert_record(R"({"id":18446744073709551615})", var).key_json[0],
              "18446744073709551615");
    EXPECT_NE(refusal(R"({"id":1234567890123456789012345678901234567890})", var)
                  .find("a number the sink's JSON reader cannot read (BIGINT_ERROR"),
              std::string::npos);
    const std::vector<CassandraKeyColumn> dec = {{"id", CqlKeyType::Decimal}};
    EXPECT_EQ(prepare_cassandra_upsert_record(R"({"id":0.10000000000000000001})", dec).key_json[0],
              "0.10000000000000000001");
    EXPECT_EQ(refusal(R"({"id":"12.50"})", dec), "");
    const std::vector<CassandraKeyColumn> tiny = {{"id", CqlKeyType::TinyInt}};
    EXPECT_EQ(refusal(R"({"id":-128})", tiny), "");
    EXPECT_NE(refusal(R"({"id":128})", tiny), "");
}

TEST(CassandraRules, TextKeysKeepUnicodeAndControlCharactersAsWritten) {
    const std::vector<CassandraKeyColumn> text = {{"k", CqlKeyType::Text}};
    const std::string raw = "\"caf" + esc("00e9") + " " + esc("0000") + R"(\t\"q\" )" +
                            esc("d83d") + esc("de00") + "\"";
    const auto r = prepare_cassandra_upsert_record("{\"k\":" + raw + "}", text);
    EXPECT_EQ(r.key_json[0], raw);
    // Two spellings of one string are two identities; the sink then writes both
    // in record order, so the later one wins at the server.
    const auto a = prepare_cassandra_upsert_record(R"({"k":"é"})", text);
    const auto b = prepare_cassandra_upsert_record("{\"k\":\"" + esc("00e9") + "\"}", text);
    EXPECT_NE(a.identity, b.identity);
    const std::vector<CassandraKeyColumn> ascii = {{"k", CqlKeyType::Ascii}};
    EXPECT_EQ(refusal(R"({"k":"plain \u0001"})", ascii), "");
    EXPECT_NE(refusal(R"({"k":"café"})", ascii).find("ASCII"), std::string::npos);
}

TEST(CassandraRules, UuidBooleanAndBlobKeysAreChecked) {
    const std::vector<CassandraKeyColumn> uuid = {{"k", CqlKeyType::Uuid}};
    EXPECT_EQ(refusal(R"({"k":"550E8400-e29b-41d4-a716-446655440000"})", uuid), "");
    EXPECT_NE(refusal(R"({"k":"550e8400e29b41d4a716446655440000"})", uuid), "");
    const std::vector<CassandraKeyColumn> tu = {{"k", CqlKeyType::TimeUuid}};
    EXPECT_EQ(refusal(R"({"k":"d1a3e0f0-9c7a-11ee-8c90-0242ac120002"})", tu), "");
    EXPECT_NE(refusal(R"({"k":"550e8400-e29b-41d4-a716-446655440000"})", tu).find("version-1"),
              std::string::npos);
    const std::vector<CassandraKeyColumn> flag = {{"k", CqlKeyType::Boolean}};
    EXPECT_EQ(refusal(R"({"k":false})", flag), "");
    EXPECT_NE(refusal(R"({"k":"false"})", flag), "");
    const std::vector<CassandraKeyColumn> blob = {{"k", CqlKeyType::Blob}};
    EXPECT_EQ(refusal(R"({"k":"0xCAFE"})", blob), "");
    EXPECT_NE(refusal(R"({"k":"0xCAF"})", blob), "");
    EXPECT_NE(refusal(R"({"k":"cafe"})", blob), "");
}

TEST(CassandraRules, CompositeKeysAreMatchedIgnoringCaseInKeyOrder) {
    const std::vector<CassandraKeyColumn> key = {{"tenant", CqlKeyType::Text},
                                                 {"seq", CqlKeyType::BigInt}};
    const auto r = prepare_cassandra_upsert_record(R"({"SEQ":5,"v":"x","Tenant":"acme"})", key);
    EXPECT_EQ(r.key_json, (std::vector<std::string>{"\"acme\"", "5"}));
    EXPECT_EQ(r.identity, "6:\"acme\"1:5");
}

TEST(CassandraRules, FieldsNamingOneColumnTwiceAreRefused) {
    EXPECT_NE(refusal(R"({"id":1,"ID":2})", kIntKey).find("same column"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":1,"v":"a","v":"b"})", kIntKey).find("same column"),
              std::string::npos);
}

TEST(CassandraRules, RefusalsNameTheRuleAndColumnButNoValue) {
    const std::string secret = "s3cr3t-value-0042";
    const std::vector<CassandraKeyColumn> uuid = {{"k", CqlKeyType::Uuid}};
    const auto why = refusal("{\"k\":\"" + secret + "\",\"v\":\"" + secret + "\"}", uuid);
    EXPECT_NE(why.find("'k'"), std::string::npos);
    EXPECT_EQ(why.find(secret), std::string::npos) << why;
}

TEST(CassandraRules, ServerMessagesLoseQuotedValuesAndParserExcerpts) {
    EXPECT_EQ(redact_cassandra_message("Unable to make int from 'secret-42'"),
              "Unable to make int from '...'");
    EXPECT_EQ(
        redact_cassandra_message("Could not decode JSON string as a map: Unexpected character "
                                 "[Source: {\"id\": \"secret\"}; line: 1]"),
        "Could not decode JSON string as a map: Unexpected character");
    EXPECT_EQ(redact_cassandra_message("Undefined column name v2 in table ks.t"),
              "Undefined column name v2 in table ks.t");
    EXPECT_EQ(redact_cassandra_message("line one\nline \"two"), "line one line \"...");
    EXPECT_LE(redact_cassandra_message(std::string(1000, 'x')).size(), 300u);
}
