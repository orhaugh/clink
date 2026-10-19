// The SQL CDC contract's row form (cdc_row_encoder.hpp): each declared column
// converted from PostgreSQL's text form to the value its declared type holds,
// a value the type cannot hold exactly refused, and the change metadata and
// changelog kind laid out as the planner and the sinks read them.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/connectors/cdc_event.hpp"

#include "cdc_row_encoder.hpp"

using clink::CdcEvent;
using clink::CdcField;
namespace cdc = clink::postgres::cdc;

namespace {

cdc::ColumnPlan column(const std::string& spec) {
    auto plan = cdc::plan_columns(spec);
    EXPECT_EQ(plan.size(), 1U);
    return plan.at(0);
}

std::string convert(const std::string& pg_type, const std::string& text, const std::string& spec) {
    return cdc::convert_value(pg_type, text, column(spec));
}

CdcEvent event(CdcEvent::Op op, std::vector<CdcField> values, const std::string& lsn = "0/1A2B") {
    CdcEvent ev;
    ev.op = op;
    ev.table = "public.orders";
    ev.lsn = lsn;
    ev.xid = 77;
    ev.values = std::move(values);
    return ev;
}

clink::config::JsonObject parse_row(const std::string& json) {
    return clink::config::parse(json).as_object();
}

}  // namespace

TEST(CdcRowEncoder, PlansTheDeclaredTypes) {
    const auto plan = cdc::plan_columns(
        "id:BIGINT;n:INTEGER;s:SMALLINT;d:DOUBLE;r:REAL;b:BOOLEAN;day:DATE;"
        "at:TIMESTAMP(3);atz:TIMESTAMP(6) WITH TIME ZONE;amount:DECIMAL(12, 2);name:VARCHAR;"
        "__version:BIGINT;__deleted:BOOLEAN;__op:VARCHAR;__lsn:VARCHAR;__table:VARCHAR;__xid:"
        "BIGINT");
    ASSERT_EQ(plan.size(), 17U);
    EXPECT_EQ(plan[0].kind, cdc::Kind::BigInt);
    EXPECT_EQ(plan[7].kind, cdc::Kind::Timestamp);
    EXPECT_EQ(plan[8].kind, cdc::Kind::TimestampTz);
    EXPECT_EQ(plan[9].kind, cdc::Kind::Decimal);
    EXPECT_EQ(plan[9].precision, 12);
    EXPECT_EQ(plan[9].scale, 2);
    EXPECT_EQ(plan[11].kind, cdc::Kind::MetaVersion);
    EXPECT_EQ(plan[12].kind, cdc::Kind::MetaDeleted);
}

TEST(CdcRowEncoder, RefusesTypesTheContractCannotCarry) {
    EXPECT_THROW(cdc::plan_columns("blob:BYTEA"), cdc::EncodeError);
    EXPECT_THROW(cdc::plan_columns("t:TIME"), cdc::EncodeError);
    EXPECT_THROW(cdc::plan_columns("tags:VARCHAR ARRAY"), cdc::EncodeError);
    // Reserved names are the metadata columns only, each with its one type.
    EXPECT_THROW(cdc::plan_columns("__row_kind:VARCHAR"), cdc::EncodeError);
    EXPECT_THROW(cdc::plan_columns("__mine:VARCHAR"), cdc::EncodeError);
    EXPECT_THROW(cdc::plan_columns("__version:VARCHAR"), cdc::EncodeError);
    EXPECT_THROW(cdc::plan_columns("__deleted:BIGINT"), cdc::EncodeError);
}

TEST(CdcRowEncoder, TypePairsFollowTheDocumentedTable) {
    EXPECT_FALSE(cdc::type_pair_problem("int8", column("x:BIGINT")));
    EXPECT_FALSE(cdc::type_pair_problem("int4", column("x:BIGINT")));
    EXPECT_TRUE(cdc::type_pair_problem("int8", column("x:INTEGER")));  // could overflow
    EXPECT_TRUE(cdc::type_pair_problem("numeric", column("x:DOUBLE")));
    EXPECT_FALSE(cdc::type_pair_problem("numeric", column("x:DECIMAL(10, 2)")));
    EXPECT_TRUE(cdc::type_pair_problem("timestamptz", column("x:TIMESTAMP(3)")));
    EXPECT_TRUE(cdc::type_pair_problem("timestamp", column("x:TIMESTAMP(3) WITH TIME ZONE")));
    EXPECT_TRUE(cdc::type_pair_problem("text", column("x:BIGINT")));
    // VARCHAR takes any type's text form, user-defined types included.
    EXPECT_FALSE(cdc::type_pair_problem("public.mood", column("x:VARCHAR")));
    EXPECT_FALSE(cdc::type_pair_problem("jsonb", column("x:VARCHAR")));
    const auto msg = cdc::type_pair_problem("text", column("qty:BIGINT"));
    ASSERT_TRUE(msg.has_value());
    EXPECT_NE(msg->find("qty"), std::string::npos) << *msg;
    EXPECT_NE(msg->find("VARCHAR"), std::string::npos) << *msg;
}

TEST(CdcRowEncoder, ConvertsNumbersExactlyAndRefusesWhatTheTypeCannotHold) {
    EXPECT_EQ(convert("int8", "9223372036854775807", "x:BIGINT"), "9223372036854775807");
    EXPECT_EQ(convert("int4", "-42", "x:INTEGER"), "-42");
    EXPECT_EQ(convert("float8", "1.5e+30", "x:DOUBLE"), "1.5e+30");
    EXPECT_THROW(convert("float8", "NaN", "x:DOUBLE"), cdc::EncodeError);
    EXPECT_THROW(convert("float8", "Infinity", "x:DOUBLE"), cdc::EncodeError);
    EXPECT_THROW(convert("float4", "-Infinity", "x:REAL"), cdc::EncodeError);
    // DECIMAL: exact digits through, never rounded.
    EXPECT_EQ(convert("numeric", "12345.67", "x:DECIMAL(12, 2)"), "12345.67");
    EXPECT_EQ(convert("numeric", "-0.10", "x:DECIMAL(4, 2)"), "-0.10");
    EXPECT_EQ(convert("numeric", "1.500", "x:DECIMAL(4, 2)"), "1.500");  // trailing zeros fit
    EXPECT_THROW(convert("numeric", "1.125", "x:DECIMAL(10, 2)"), cdc::EncodeError);
    EXPECT_THROW(convert("numeric", "123456789.5", "x:DECIMAL(10, 2)"), cdc::EncodeError);
    EXPECT_THROW(convert("numeric", "NaN", "x:DECIMAL(10, 2)"), cdc::EncodeError);
    EXPECT_EQ(convert("int4", "99", "x:DECIMAL(4, 2)"), "99");
    EXPECT_THROW(convert("int4", "100", "x:DECIMAL(4, 2)"), cdc::EncodeError);
}

TEST(CdcRowEncoder, ConvertsBooleansDatesAndText) {
    EXPECT_EQ(convert("bool", "t", "x:BOOLEAN"), "true");
    EXPECT_EQ(convert("bool", "f", "x:BOOLEAN"), "false");
    EXPECT_EQ(convert("date", "2026-02-28", "x:DATE"), "\"2026-02-28\"");
    EXPECT_THROW(convert("date", "2026-02-30", "x:DATE"), cdc::EncodeError);
    EXPECT_THROW(convert("date", "infinity", "x:DATE"), cdc::EncodeError);
    EXPECT_THROW(convert("date", "0044-03-15 BC", "x:DATE"), cdc::EncodeError);
    EXPECT_EQ(convert("text", "say \"hi\"\n\\", "x:VARCHAR"), "\"say \\\"hi\\\"\\n\\\\\"");
    EXPECT_EQ(convert("int4", "7", "x:VARCHAR"), "\"7\"");
}

TEST(CdcRowEncoder, TimestampsBecomeEpochMillisecondsAndRefuseFinerDigits) {
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("1970-01-01 00:00:00", false), 0);
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("2026-10-19 10:00:00.123", false), 1792404000123);
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("2026-10-19 10:00:00.123+00", true), 1792404000123);
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("2026-10-19 15:30:00.5+05:30", true), 1792404000500);
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("1969-12-31 23:59:59.999", false), -1);
    // Zero digits below the millisecond are still exact.
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("2026-10-19 10:00:00.123000+00", true), 1792404000123);
    EXPECT_THROW(cdc::timestamp_to_epoch_ms("2026-10-19 10:00:00.123456+00", true),
                 cdc::EncodeError);
    EXPECT_THROW(cdc::timestamp_to_epoch_ms("infinity", true), cdc::EncodeError);
    EXPECT_THROW(cdc::timestamp_to_epoch_ms("0001-01-01 00:00:00 BC", false), cdc::EncodeError);
    EXPECT_THROW(cdc::timestamp_to_epoch_ms("2026-10-19 10:00:00", true),
                 cdc::EncodeError);  // no offset
    EXPECT_THROW(cdc::timestamp_to_epoch_ms("10/19/2026 10:00:00", false), cdc::EncodeError);
    EXPECT_EQ(convert("timestamptz", "2026-10-19 10:00:00+00", "x:TIMESTAMP(3) WITH TIME ZONE"),
              "1792404000000");
}

TEST(CdcRowEncoder, SubMillisecondDigitsAreDroppedOnlyWhereAllowed) {
    bool truncated = false;
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("2026-10-19 10:00:00.123456+00", true, true, &truncated),
              1792404000123);
    EXPECT_TRUE(truncated);
    // Rounded down before 1970 as after: -0.5 ms and -999.9 ms.
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("1969-12-31 23:59:59.9995", false, true), -1);
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("1969-12-31 23:59:59.0001", false, true), -1000);
    // Through a whole-second offset the instant still rounds down.
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("2026-10-19 15:30:00.0009+05:30", true, true),
              1792404000000);
    // Zeros below the millisecond lose nothing, so nothing is reported.
    truncated = false;
    EXPECT_EQ(cdc::timestamp_to_epoch_ms("2026-10-19 10:00:00.123000+00", true, true, &truncated),
              1792404000123);
    EXPECT_FALSE(truncated);

    // The option reaches every TIMESTAMP column but the key's.
    auto plan = cdc::plan_columns(
        "at:TIMESTAMP(3);seen:TIMESTAMP(3) WITH TIME ZONE;id:BIGINT;name:VARCHAR");
    cdc::truncate_sub_millisecond_outside_key(plan, {"at"});
    EXPECT_FALSE(plan[0].truncate_sub_millisecond);
    EXPECT_TRUE(plan[1].truncate_sub_millisecond);
    EXPECT_FALSE(plan[2].truncate_sub_millisecond);
    EXPECT_FALSE(plan[3].truncate_sub_millisecond);

    const cdc::RowEncoder enc(plan, cdc::ChangelogMode::Append);
    std::vector<std::string> lost;
    const std::vector<CdcField> row = {
        {.name = "at", .value = "2026-10-19 10:00:00.123", .type = "timestamp"},
        {.name = "seen", .value = "2026-10-19 10:00:00.123456+00", .type = "timestamptz"},
        {.name = "id", .value = "5", .type = "int8"},
        {.name = "name", .value = "a", .type = "text"}};
    auto out = parse_row(*enc.encode(event(CdcEvent::Op::Insert, row), false, &lost));
    EXPECT_EQ(out.at("seen").as_int(), 1792404000123);
    EXPECT_EQ(lost, std::vector<std::string>{"seen"});
    // The key column still refuses: truncating it could make two keys one.
    auto keyed = row;
    keyed[0].value = "2026-10-19 10:00:00.123456";
    EXPECT_THROW((void)enc.encode(event(CdcEvent::Op::Insert, keyed), false, &lost),
                 cdc::EncodeError);
}

TEST(CdcRowEncoder, ParsesLsnText) {
    EXPECT_EQ(cdc::parse_lsn("0/16E2A38"), 0x16E2A38U);
    EXPECT_EQ(cdc::parse_lsn("1/0"), std::uint64_t{1} << 32);
    EXPECT_FALSE(cdc::parse_lsn("16E2A38").has_value());
    EXPECT_FALSE(cdc::parse_lsn("0/xyz").has_value());
}

TEST(CdcRowEncoder, UpsertModeTagsEachRowWithItsChangelogKind) {
    const cdc::RowEncoder enc(cdc::plan_columns("id:BIGINT;name:VARCHAR"),
                              cdc::ChangelogMode::Upsert);
    const std::vector<CdcField> row = {{.name = "id", .value = "5", .type = "int8"},
                                       {.name = "name", .value = "a", .type = "text"}};
    auto snap = parse_row(*enc.encode(event(CdcEvent::Op::Insert, row), /*snapshot_row=*/true));
    EXPECT_EQ(snap.at("__row_kind").as_string(), "insert");
    EXPECT_EQ(snap.at("id").as_int(), 5);
    auto upd = parse_row(*enc.encode(event(CdcEvent::Op::Update, row), false));
    EXPECT_EQ(upd.at("__row_kind").as_string(), "update_after");
    // A delete under REPLICA IDENTITY DEFAULT carries the key, other columns null.
    const std::vector<CdcField> key_only = {
        {.name = "id", .value = "5", .type = "int8"},
        {.name = "name", .value = "", .type = "text", .is_null = true}};
    auto del = parse_row(*enc.encode(event(CdcEvent::Op::Delete, key_only), false));
    EXPECT_EQ(del.at("__row_kind").as_string(), "delete");
    EXPECT_TRUE(del.at("name").is_null());
    // Transaction markers carry no row.
    EXPECT_FALSE(enc.encode(event(CdcEvent::Op::Begin, {}), false).has_value());
    EXPECT_FALSE(enc.encode(event(CdcEvent::Op::Commit, {}), false).has_value());
}

TEST(CdcRowEncoder, AppendModeWritesTheChangeMetadataAndNoChangelogKind) {
    const cdc::RowEncoder enc(
        cdc::plan_columns("id:BIGINT;__op:VARCHAR;__version:BIGINT;__deleted:BOOLEAN;__lsn:VARCHAR;"
                          "__table:VARCHAR;__xid:BIGINT"),
        cdc::ChangelogMode::Append);
    const std::vector<CdcField> row = {{.name = "id", .value = "5", .type = "int8"}};
    auto snap = parse_row(*enc.encode(event(CdcEvent::Op::Insert, row, "0/500"), true));
    EXPECT_EQ(snap.count("__row_kind"), 0U);
    EXPECT_EQ(snap.at("__op").as_string(), "snapshot");
    EXPECT_EQ(snap.at("__version").as_int(), 0);
    EXPECT_FALSE(snap.at("__deleted").as_bool());
    EXPECT_EQ(snap.at("__xid").as_int(), 0);
    auto del = parse_row(*enc.encode(event(CdcEvent::Op::Delete, row, "0/1A2B"), false));
    EXPECT_EQ(del.at("__op").as_string(), "delete");
    EXPECT_EQ(del.at("__version").as_int(), 0x1A2B);
    EXPECT_TRUE(del.at("__deleted").as_bool());
    EXPECT_EQ(del.at("__lsn").as_string(), "0/1A2B");
    EXPECT_EQ(del.at("__table").as_string(), "public.orders");
    EXPECT_EQ(del.at("__xid").as_int(), 77);
}

// keep_rows: the rows feed the stage that keeps every key's latest row, so
// each carries its kind and position, an unchanged out-of-line value is left
// for that stage to fill in, and a TRUNCATE is a row of its own.
TEST(CdcRowEncoder, KeepRowsWritesTheFieldsTheRowKeepingStageReads) {
    const cdc::RowEncoder enc(cdc::plan_columns("id:BIGINT;body:VARCHAR;__op:VARCHAR;__version:"
                                                "BIGINT;__deleted:BOOLEAN"),
                              cdc::ChangelogMode::Upsert,
                              /*keep_rows=*/true);
    const std::vector<CdcField> row = {{.name = "id", .value = "5", .type = "int8"},
                                       {.name = "body",
                                        .value = "",
                                        .type = std::string{cdc::kUnchangedToastType},
                                        .is_null = false}};
    auto upd = parse_row(*enc.encode(event(CdcEvent::Op::Update, row, "0/1A2B"), false));
    EXPECT_EQ(upd.at("__cdc_kind").as_string(), "update");
    EXPECT_EQ(upd.at("__cdc_version").as_int(), 0x1A2B);
    EXPECT_TRUE(upd.at("body").is_null());
    ASSERT_TRUE(upd.at("__cdc_unchanged").is_array());
    ASSERT_EQ(upd.at("__cdc_unchanged").as_array().size(), 1U);
    EXPECT_EQ(upd.at("__cdc_unchanged").as_array()[0].as_string(), "body");
    // The stage sets the changelog kind, not the source.
    EXPECT_EQ(upd.count("__row_kind"), 0U);

    const std::vector<CdcField> full = {{.name = "id", .value = "5", .type = "int8"},
                                        {.name = "body", .value = "b", .type = "text"}};
    auto snap = parse_row(*enc.encode(event(CdcEvent::Op::Insert, full), /*snapshot_row=*/true));
    EXPECT_EQ(snap.at("__cdc_kind").as_string(), "snapshot");
    EXPECT_EQ(snap.at("__cdc_version").as_int(), 0);
    EXPECT_EQ(snap.count("__cdc_unchanged"), 0U);

    // A change a snapshot taken fresh may already hold is marked so.
    auto conv = parse_row(*enc.encode(event(CdcEvent::Op::Update, full), false, nullptr, true));
    EXPECT_TRUE(conv.at("__cdc_converging").as_bool());
    EXPECT_EQ(upd.count("__cdc_converging"), 0U);

    CdcEvent truncate = event(CdcEvent::Op::Truncate, {}, "0/2000");
    auto t = parse_row(*enc.encode(truncate, false));
    EXPECT_EQ(t.at("__cdc_kind").as_string(), "truncate");
    EXPECT_EQ(t.at("__cdc_version").as_int(), 0x2000);
    EXPECT_TRUE(t.at("id").is_null());
    EXPECT_EQ(t.at("__op").as_string(), "delete");
    EXPECT_TRUE(t.at("__deleted").as_bool());

    // Without keep_rows a TRUNCATE is no row, and nothing is marked.
    const cdc::RowEncoder plain(cdc::plan_columns("id:BIGINT;body:VARCHAR"),
                                cdc::ChangelogMode::Upsert);
    EXPECT_FALSE(plain.encode(truncate, false).has_value());
    auto p = parse_row(*plain.encode(event(CdcEvent::Op::Update, full), false));
    EXPECT_EQ(p.count("__cdc_kind"), 0U);
    EXPECT_EQ(p.at("__row_kind").as_string(), "update_after");
}

TEST(CdcRowEncoder, AColumnMissingFromTheChangeIsRefusedNotNulled) {
    const cdc::RowEncoder enc(cdc::plan_columns("id:BIGINT;name:VARCHAR"),
                              cdc::ChangelogMode::Upsert);
    const std::vector<CdcField> row = {{.name = "id", .value = "5", .type = "int8"}};
    try {
        (void)enc.encode(event(CdcEvent::Op::Update, row), false);
        FAIL() << "a change without a declared column was encoded";
    } catch (const cdc::EncodeError& e) {
        EXPECT_NE(std::string{e.what()}.find("name"), std::string::npos) << e.what();
    }
}

TEST(CdcRowEncoder, AnUnrepresentableValueNamesItsColumnAndPosition) {
    const cdc::RowEncoder enc(cdc::plan_columns("id:BIGINT;at:TIMESTAMP(3) WITH TIME ZONE"),
                              cdc::ChangelogMode::Append);
    const std::vector<CdcField> row = {
        {.name = "id", .value = "5", .type = "int8"},
        {.name = "at", .value = "2026-10-19 10:00:00.123456+00", .type = "timestamptz"}};
    try {
        (void)enc.encode(event(CdcEvent::Op::Insert, row, "0/99"), false);
        FAIL() << "a sub-millisecond timestamp was encoded";
    } catch (const cdc::EncodeError& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("column at"), std::string::npos) << what;
        EXPECT_NE(what.find("0/99"), std::string::npos) << what;
        EXPECT_NE(what.find("millisecond"), std::string::npos) << what;
    }
}
