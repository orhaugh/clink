// Offline tests for the typed SQL row of a change event (sql_row.hpp): each
// supported BSON-to-SQL pair, the refusal of every value a declared type cannot
// hold exactly, the metadata columns, and the declared-types rules. The events
// are hand-built canonical Extended JSON, as the source reads them. No live
// MongoDB.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"

#ifdef CLINK_HAS_MONGODB
#include "sql_row.hpp"
#endif

#ifdef CLINK_HAS_MONGODB

using clink::mongodb::ChangeEventError;
using clink::mongodb::FullDocument;
using clink::mongodb::sql::change_event_to_sql_row;
using clink::mongodb::sql::ConversionError;
using clink::mongodb::sql::Kind;
using clink::mongodb::sql::parse_declared_types;
using clink::mongodb::sql::parse_sink_columns;
using clink::mongodb::sql::sql_row_to_document;

namespace {

// An insert of `doc` (canonical Extended JSON fields, without _id) into
// shop.orders, with _id ObjectId 650000000000000000000001.
std::string insert_of(const std::string& fields) {
    return R"({"operationType":"insert","ns":{"db":"shop","coll":"orders"},)"
           R"("documentKey":{"_id":{"$oid":"650000000000000000000001"}},)"
           R"("fullDocument":{"_id":{"$oid":"650000000000000000000001"})" +
           (fields.empty() ? "" : "," + fields) + "}}";
}

clink::config::JsonObject row_of(const std::string& event,
                                 const std::string& declared,
                                 FullDocument mode = FullDocument::UpdateLookup) {
    const auto text =
        change_event_to_sql_row(clink::config::parse(event), mode, parse_declared_types(declared));
    return clink::config::parse(text).as_object();
}

// The raw row text, for checks on exact number tokens.
std::string text_of(const std::string& event, const std::string& declared) {
    return change_event_to_sql_row(
        clink::config::parse(event), FullDocument::UpdateLookup, parse_declared_types(declared));
}

std::string refusal(const std::string& event, const std::string& declared) {
    try {
        (void)text_of(event, declared);
    } catch (const ConversionError& e) {
        return e.what();
    }
    return "";
}

bool contains(const std::string& s, const std::string& needle) {
    return s.find(needle) != std::string::npos;
}

}  // namespace

TEST(MongoSqlRow, IntegersFillTheIntegerTypesWithinTheirRanges) {
    const auto o =
        row_of(insert_of(R"("a":{"$numberInt":"7"},"b":{"$numberLong":"9007199254740993"},)"
                         R"("c":{"$numberLong":"-2147483648"},"d":{"$numberInt":"-32768"})"),
               "a:BIGINT;b:BIGINT;c:INTEGER;d:SMALLINT");
    EXPECT_EQ(o.at("a").as_int(), 7);
    EXPECT_TRUE(o.at("b").is_integral_number());
    EXPECT_EQ(o.at("b").as_int(), 9007199254740993LL);
    EXPECT_EQ(o.at("c").as_int(), -2147483648LL);
    EXPECT_EQ(o.at("d").as_int(), -32768);

    EXPECT_TRUE(contains(refusal(insert_of(R"("c":{"$numberLong":"2147483648"})"), "c:INTEGER"),
                         "outside its range"));
    EXPECT_TRUE(contains(refusal(insert_of(R"("d":{"$numberInt":"32768"})"), "d:SMALLINT"),
                         "outside its range"));
}

// Neither a double nor a decimal goes into an integer column: a fraction would
// be cut, and a whole double is still a double in MongoDB.
TEST(MongoSqlRow, AFloatingOrDecimalValueIsRefusedForAnIntegerColumn) {
    const auto d = refusal(insert_of(R"("n":{"$numberDouble":"3.0"})"), "n:BIGINT");
    EXPECT_TRUE(contains(d, "field 'n' is a double")) << d;
    EXPECT_TRUE(contains(d, "column n (BIGINT)")) << d;
    EXPECT_TRUE(contains(d, "declare it DOUBLE")) << d;
    EXPECT_TRUE(contains(d, "shop.orders")) << d;
    EXPECT_TRUE(contains(d, "ObjectId(650000000000000000000001)")) << d;
    EXPECT_TRUE(
        contains(refusal(insert_of(R"("n":{"$numberDecimal":"3"})"), "n:BIGINT"), "is a decimal"));
}

TEST(MongoSqlRow, DoublesAreHeldExactlyAndIntegersOnlyWhereADoubleHoldsThem) {
    const auto text =
        text_of(insert_of(R"("x":{"$numberDouble":"0.1"},"y":{"$numberDouble":"2.0"},)"
                          R"("z":{"$numberLong":"9007199254740992"})"),
                "x:DOUBLE;y:DOUBLE;z:DOUBLE");
    const auto o = clink::config::parse(text).as_object();
    EXPECT_EQ(o.at("x").as_number(), 0.1);
    // A whole double stays a double, so arithmetic on it stays floating.
    EXPECT_FALSE(o.at("y").is_integral_number()) << text;
    EXPECT_FALSE(o.at("z").is_integral_number()) << text;
    EXPECT_EQ(o.at("z").as_number(), 9007199254740992.0);

    EXPECT_TRUE(
        contains(refusal(insert_of(R"("z":{"$numberLong":"9007199254740993"})"), "z:DOUBLE"),
                 "outside its range"));
    EXPECT_TRUE(
        contains(refusal(insert_of(R"("x":{"$numberDouble":"NaN"})"), "x:DOUBLE"), "not finite"));
    EXPECT_TRUE(contains(refusal(insert_of(R"("x":{"$numberDouble":"-Infinity"})"), "x:DOUBLE"),
                         "not finite"));
}

TEST(MongoSqlRow, ARealHoldsOnlyWhatAFloatHoldsExactly) {
    const auto o = row_of(insert_of(R"("r":{"$numberDouble":"0.5"},"i":{"$numberInt":"16777216"})"),
                          "r:REAL;i:REAL");
    EXPECT_EQ(o.at("r").as_number(), 0.5);
    EXPECT_EQ(o.at("i").as_number(), 16777216.0);
    const auto m = refusal(insert_of(R"("r":{"$numberDouble":"0.1"})"), "r:REAL");
    EXPECT_TRUE(contains(m, "not a float")) << m;
    EXPECT_TRUE(contains(m, "declare it DOUBLE")) << m;
    EXPECT_TRUE(contains(refusal(insert_of(R"("i":{"$numberInt":"16777217"})"), "i:REAL"),
                         "outside its range"));
}

// A decimal keeps its digits: written as a plain numeral, its trailing zeros
// kept, and refused where the declared precision or scale would round it.
TEST(MongoSqlRow, DecimalsKeepTheirDigitsAndAreRefusedRatherThanRounded) {
    const auto text = text_of(
        insert_of(R"("p":{"$numberDecimal":"12.50"},"q":{"$numberDecimal":"1.5E+2"},)"
                  R"("r":{"$numberDecimal":"-1E-3"},"s":{"$numberLong":"42"},)"
                  R"("t":{"$numberDecimal":"-0.00"})"),
        "p:DECIMAL(10, 2);q:DECIMAL(10, 2);r:DECIMAL(10, 3);s:DECIMAL(5, 0);t:DECIMAL(4, 2)");
    EXPECT_TRUE(contains(text, R"("p":12.50)")) << text;
    EXPECT_TRUE(contains(text, R"("q":150)")) << text;
    EXPECT_TRUE(contains(text, R"("r":-0.001)")) << text;
    EXPECT_TRUE(contains(text, R"("s":42)")) << text;
    EXPECT_TRUE(contains(text, R"("t":0.00)")) << text;

    EXPECT_TRUE(
        contains(refusal(insert_of(R"("p":{"$numberDecimal":"12.345"})"), "p:DECIMAL(10, 2)"),
                 "without rounding"));
    EXPECT_TRUE(
        contains(refusal(insert_of(R"("p":{"$numberDecimal":"123456789.5"})"), "p:DECIMAL(10, 2)"),
                 "without rounding"));
    EXPECT_TRUE(contains(refusal(insert_of(R"("p":{"$numberDecimal":"NaN"})"), "p:DECIMAL(10, 2)"),
                         "not finite"));
    // A double's binary fraction is not a decimal one.
    const auto m = refusal(insert_of(R"("p":{"$numberDouble":"0.1"})"), "p:DECIMAL(10, 2)");
    EXPECT_TRUE(contains(m, "is a double")) << m;
}

TEST(MongoSqlRow, DatesFillTimestampsAndMidnightDatesFillDates) {
    const auto o = row_of(insert_of(R"("at":{"$date":{"$numberLong":"1700000000123"}},)"
                                    R"("on":{"$date":{"$numberLong":"1699920000000"}},)"
                                    R"("old":{"$date":{"$numberLong":"-86400000"}})"),
                          "at:TIMESTAMP(3);on:DATE;old:DATE");
    EXPECT_EQ(o.at("at").as_int(), 1700000000123LL);
    EXPECT_EQ(o.at("on").as_string(), "2023-11-14");
    EXPECT_EQ(o.at("old").as_string(), "1969-12-31");
    const auto m =
        refusal(insert_of(R"("on":{"$date":{"$numberLong":"1700000000123"}})"), "on:DATE");
    EXPECT_TRUE(contains(m, "time of day")) << m;
    EXPECT_TRUE(contains(m, "declare it TIMESTAMP")) << m;
    EXPECT_TRUE(
        contains(refusal(insert_of(R"("at":"2023-11-14")"), "at:TIMESTAMP(3)"), "is a string"));
}

TEST(MongoSqlRow, VarcharHoldsStringsObjectIdsIntegersAndNestedValuesAsText) {
    const auto o = row_of(insert_of(R"("s":"café \"x\"","o":{"$oid":"650000000000000000000009"},)"
                                    R"("n":{"$numberLong":"12"},"doc":{"a":{"$numberInt":"1"}},)"
                                    R"("arr":[{"$numberInt":"1"},"two"])"),
                          "s:VARCHAR;o:VARCHAR;n:VARCHAR;doc:VARCHAR;arr:VARCHAR");
    EXPECT_EQ(o.at("s").as_string(), "café \"x\"");
    EXPECT_EQ(o.at("o").as_string(), "650000000000000000000009");
    EXPECT_EQ(o.at("n").as_string(), "12");
    // Nested values are their canonical Extended JSON, which keeps BSON types.
    EXPECT_EQ(o.at("doc").as_string(), R"({"a":{"$numberInt":"1"}})");
    EXPECT_EQ(o.at("arr").as_string(), R"([{"$numberInt":"1"},"two"])");
    for (const auto* field : {R"("v":{"$numberDouble":"1.5"})",
                              R"("v":{"$numberDecimal":"1.5"})",
                              R"("v":{"$date":{"$numberLong":"0"}})",
                              R"("v":true)"}) {
        EXPECT_TRUE(contains(refusal(insert_of(field), "v:VARCHAR"), "cannot hold")) << field;
    }
}

TEST(MongoSqlRow, BsonTypesWithNoSqlTypeAreRefusedForEveryColumn) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"("v":{"$binary":{"base64":"AQI=","subType":"00"}})", "binary data"},
        {R"("v":{"$timestamp":{"t":1,"i":2}})", "a BSON timestamp"},
        {R"("v":{"$regularExpression":{"pattern":"a","options":""}})", "a regular expression"},
        {R"("v":{"$minKey":1})", "a minKey"},
    };
    for (const auto& [field, what] : cases) {
        for (const auto* type : {"VARCHAR", "BIGINT", "TIMESTAMP(3)"}) {
            const auto m = refusal(insert_of(field), std::string("v:") + type);
            EXPECT_TRUE(contains(m, "is " + what)) << m;
            EXPECT_TRUE(contains(m, "no SQL type holds it")) << m;
        }
    }
}

TEST(MongoSqlRow, NullAndMissingFieldsAreSqlNullForEveryType) {
    const auto o = row_of(insert_of(R"("a":null)"), "a:BIGINT;b:DECIMAL(5, 2);c:VARCHAR;d:DATE");
    for (const auto* k : {"a", "b", "c", "d"}) {
        EXPECT_TRUE(o.at(k).is_null()) << k;
    }
}

TEST(MongoSqlRow, BooleansFillOnlyBooleanColumns) {
    const auto o = row_of(insert_of(R"("ok":true)"), "ok:BOOLEAN");
    EXPECT_TRUE(o.at("ok").as_bool());
    EXPECT_TRUE(
        contains(refusal(insert_of(R"("ok":{"$numberInt":"1"})"), "ok:BOOLEAN"), "32-bit integer"));
}

// The metadata columns are filled from the event, __id converted for its
// declared type like any field, and in declared order.
TEST(MongoSqlRow, MetadataColumnsCarryTheChangeAndTheKey) {
    const auto text = text_of(insert_of(R"("qty":{"$numberInt":"5"})"),
                              "__id:VARCHAR;__op:VARCHAR;__ns:VARCHAR;__image:VARCHAR;qty:INTEGER");
    EXPECT_EQ(text,
              R"({"__id":"650000000000000000000001","__op":"insert","__ns":"shop.orders",)"
              R"("__image":"full","qty":5})");
    const auto del = row_of(R"({"operationType":"delete","ns":{"db":"shop","coll":"orders"},)"
                            R"("documentKey":{"_id":{"$numberLong":"9"}}})",
                            "__id:BIGINT;__op:VARCHAR;__image:VARCHAR;qty:INTEGER");
    EXPECT_EQ(del.at("__id").as_int(), 9);
    EXPECT_EQ(del.at("__op").as_string(), "delete");
    EXPECT_EQ(del.at("__image").as_string(), "key");
    EXPECT_TRUE(del.at("qty").is_null());
}

// An update read without a document has only the fields it set; the others are
// NULL, and __image says so.
TEST(MongoSqlRow, APartialUpdateFillsOnlyTheFieldsItSet) {
    const auto o = row_of(
        R"({"operationType":"update","ns":{"db":"shop","coll":"orders"},)"
        R"("documentKey":{"_id":{"$numberInt":"3"}},)"
        R"("updateDescription":{"updatedFields":{"qty":{"$numberInt":"6"}},"removedFields":[]}})",
        "__id:INTEGER;__image:VARCHAR;qty:INTEGER;item:VARCHAR;_id:BIGINT",
        FullDocument::Off);
    EXPECT_EQ(o.at("__image").as_string(), "partial");
    EXPECT_EQ(o.at("qty").as_int(), 6);
    EXPECT_TRUE(o.at("item").is_null());
    // _id cannot change, so the key fills it.
    EXPECT_EQ(o.at("_id").as_int(), 3);
}

TEST(MongoSqlRow, ADocumentFieldNamedLikeTheMetadataIsRefused) {
    EXPECT_THROW((void)text_of(insert_of(R"("__op":"x")"), "qty:INTEGER"), ChangeEventError);
}

TEST(MongoSqlRow, DeclaredTypesAreReadAndUnsupportedOnesRefused) {
    const auto cols = parse_declared_types(
        "a:BIGINT;b:INTEGER;c:SMALLINT;d:DOUBLE;e:REAL;f:DECIMAL(12, 4);g:BOOLEAN;h:DATE;"
        "i:TIMESTAMP(3);j:TIMESTAMP(3) WITH TIME ZONE;k:VARCHAR;__id:BIGINT;__op:VARCHAR");
    ASSERT_EQ(cols.size(), 13U);
    EXPECT_EQ(cols[5].kind, Kind::Decimal);
    EXPECT_EQ(cols[5].precision, 12);
    EXPECT_EQ(cols[5].scale, 4);
    EXPECT_EQ(cols[9].kind, Kind::Timestamp);
    EXPECT_TRUE(cols[11].is_id);
    EXPECT_EQ(cols[11].kind, Kind::BigInt);
    EXPECT_EQ(cols[12].kind, Kind::MetaOp);

    for (const auto* bad : {"v:BYTEA",
                            "v:TIME",
                            "v:TINYINT",
                            "v:VARCHAR ARRAY",
                            "v:MAP<VARCHAR, BIGINT>",
                            "v:ROW<a BIGINT>"}) {
        try {
            (void)parse_declared_types(bad);
            ADD_FAILURE() << bad << " was accepted";
        } catch (const ConversionError& e) {
            EXPECT_TRUE(contains(e.what(), "is not one the MongoDB connector maps to BSON"))
                << e.what();
        }
    }
    EXPECT_THROW((void)parse_declared_types("__op:BIGINT"), ConversionError);
    EXPECT_THROW((void)parse_declared_types("__lsn:VARCHAR"), ConversionError);
    EXPECT_THROW((void)parse_declared_types("novalue"), ConversionError);
}

// --- The sink side: a SQL row as the document written ---

namespace {

clink::mongodb::sql::SinkDocument doc_of(const std::string& row, const std::string& declared) {
    return sql_row_to_document(row, parse_sink_columns(declared));
}

std::string sink_refusal(const std::string& row, const std::string& declared) {
    try {
        (void)doc_of(row, declared);
    } catch (const ConversionError& e) {
        return e.what();
    }
    return "";
}

}  // namespace

// Each column is written as the BSON type its SQL type maps to, in declared
// order: canonical Extended JSON says which.
TEST(MongoSqlSink, EachColumnIsWrittenAsTheBsonTypeItsSqlTypeMapsTo) {
    const auto d = doc_of(
        R"({"n":5,"i":7,"s":-2,"x":2,"r":0.5,"ok":true,"name":"café","at":1700000000123,)"
        R"("on":"2023-11-14","gone":null})",
        "n:BIGINT;i:INTEGER;s:SMALLINT;x:DOUBLE;r:REAL;ok:BOOLEAN;name:VARCHAR;at:TIMESTAMP(3);"
        "on:DATE;gone:VARCHAR;absent:BIGINT");
    EXPECT_EQ(d.kind, "insert");
    EXPECT_EQ(d.json,
              R"({"n":{"$numberLong":"5"},"i":{"$numberInt":"7"},"s":{"$numberInt":"-2"},)"
              R"("x":{"$numberDouble":"2.0"},"r":{"$numberDouble":"0.5"},"ok":true,)"
              "\"name\":\"café\","
              R"("at":{"$date":{"$numberLong":"1700000000123"}},)"
              R"("on":{"$date":{"$numberLong":"1699920000000"}},"gone":null,"absent":null})");
}

// A decimal is written with the row's own digits, past what a double holds.
TEST(MongoSqlSink, ADecimalKeepsTheRowsDigits) {
    const auto d = doc_of(R"({"amount":12345678901234567890.123450,"small":0.10})",
                          "amount:DECIMAL(38, 6);small:DECIMAL(4, 2)");
    EXPECT_EQ(d.json,
              R"({"amount":{"$numberDecimal":"12345678901234567890.123450"},)"
              R"("small":{"$numberDecimal":"0.10"}})");
}

// The bridge writes NaN and the infinities in the tagged form for this sink,
// and they are written as the BSON doubles themselves.
TEST(MongoSqlSink, NonFiniteDoublesAreWrittenAsThemselves) {
    const auto d = doc_of(R"({"a":"\u0002NaN","b":"\u0002-Infinity"})", "a:DOUBLE;b:DOUBLE");
    EXPECT_EQ(d.json, R"({"a":{"$numberDouble":"NaN"},"b":{"$numberDouble":"-Infinity"}})");
}

// The change kind is read and not written; nor is anything undeclared.
TEST(MongoSqlSink, TheRowKindIsReadAndNotWritten) {
    const auto d =
        doc_of(R"({"__row_kind":"delete","id":"k1","v":3,"__key":"x"})", "id:VARCHAR;v:BIGINT");
    EXPECT_EQ(d.kind, "delete");
    EXPECT_EQ(d.json, R"({"id":"k1","v":{"$numberLong":"3"}})");
    // A name starting with __ is an ordinary field on this side.
    EXPECT_EQ(doc_of(R"({"__op":"update"})", "__op:VARCHAR").json, R"({"__op":"update"})");
    EXPECT_THROW((void)parse_sink_columns("__row_kind:VARCHAR"), ConversionError);
}

TEST(MongoSqlSink, AValueItsBsonTypeCannotHoldIsRefused) {
    EXPECT_TRUE(contains(sink_refusal(R"({"n":1.5})", "n:BIGINT"), "is not an integer"));
    EXPECT_TRUE(
        contains(sink_refusal(R"({"i":2147483648})", "i:INTEGER"), "is not a 32-bit integer"));
    EXPECT_TRUE(contains(sink_refusal(R"({"on":"2023-02-30"})", "on:DATE"), "YYYY-MM-DD"));
    EXPECT_TRUE(contains(sink_refusal(R"({"on":"14/11/2023"})", "on:DATE"), "YYYY-MM-DD"));
    EXPECT_TRUE(
        contains(sink_refusal(R"({"at":"2023-11-14"})", "at:TIMESTAMP(3)"), "epoch milliseconds"));
    EXPECT_TRUE(contains(sink_refusal(R"({"name":5})", "name:VARCHAR"), "is not a string"));
    EXPECT_TRUE(contains(sink_refusal(R"({"amount":"1.5"})", "amount:DECIMAL(5, 2)"),
                         "is not a decimal numeral"));
    const auto m = sink_refusal(R"({"n":1.5})", "n:BIGINT");
    EXPECT_TRUE(contains(m, "column n (BIGINT)")) << m;
}

TEST(MongoSqlSink, TypesWithNoBsonMappingAreRefused) {
    for (const auto* bad : {"v:BYTEA", "v:TIME", "v:VARCHAR ARRAY", "v:MAP<VARCHAR, BIGINT>"}) {
        EXPECT_THROW((void)parse_sink_columns(bad), ConversionError) << bad;
    }
}

#endif  // CLINK_HAS_MONGODB
