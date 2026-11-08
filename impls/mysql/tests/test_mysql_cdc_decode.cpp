// Offline tests for the MySQL row-image decoder. The headline test feeds the
// EXACT bytes captured from a live MySQL 8.0 binlog (TABLE_MAP column_types +
// metadata + WRITE_ROWS row_data, for a table covering every common type) and
// asserts each decoded column - validating the whole decoder against ground
// truth with no server. Plus targeted jsonb tests.

#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/mysql/mysql_json.hpp"
#include "clink/mysql/mysql_row_decode.hpp"

using clink::CdcEvent;
using clink::mysql::CdcColumn;
using clink::mysql::CdcTableSchema;
using clink::mysql::ColumnMeta;
using clink::mysql::decode_rows_payload;
using clink::mysql::parse_table_metadata;

namespace {

// Parse "03 03 fe ..." into raw bytes.
std::vector<std::uint8_t> hx(const std::string& s) {
    std::vector<std::uint8_t> out;
    for (std::size_t i = 0; i < s.size();) {
        if (s[i] == ' ') {
            ++i;
            continue;
        }
        out.push_back(static_cast<std::uint8_t>(std::strtoul(s.substr(i, 2).c_str(), nullptr, 16)));
        i += 2;
    }
    return out;
}

CdcColumn col(std::string name, bool uns = false, std::vector<std::string> labels = {}) {
    CdcColumn c;
    c.name = std::move(name);
    c.is_unsigned = uns;
    c.enum_set_labels = std::move(labels);
    return c;
}

}  // namespace

// Ground truth captured from a live MySQL 8.0 master for:
//   (id INT, u INT UNSIGNED, c CHAR(4), v VARCHAR(300), t TEXT, dt DATETIME(3),
//    ts TIMESTAMP(0), d DATE, worker TIME(2), dc DECIMAL(10,2), dbl DOUBLE, fl FLOAT,
//    en ENUM('a','b','c'), st SET('x','y','z'), js JSON, bl BLOB, bt BIT(8), yr YEAR)
//   INSERT (-5, 4000000000, 'abcd', 'hi there', 'hello world',
//           '2026-06-27 14:05:09.123', '2026-06-27 14:05:09', '2026-06-27',
//           '12:34:56.78', 12345.67, 3.141592653589793, 1.5, 'b', 'x,z',
//           '{"a":1,"b":"hi"}', 'blobby', b'10100101', 2026)
TEST(MysqlRowDecode, DecodesEveryCommonTypeFromLiveBytes) {
    const auto types = hx("03 03 fe 0f fc 12 11 0a 13 f6 05 04 fe fe f5 fc 10 0d");
    const auto meta = hx("fe 10 b0 04 02 03 00 02 0a 02 08 04 f7 01 f8 01 04 02 00 01");
    const auto row = hx(
        "00 00 00 fb ff ff ff 00 28 6b ee 04 61 62 63 64 08 00 68 69 20 74 68 65 72 65 0b 00 68 65 "
        "6c 6c 6f 20 77 6f 72 6c 64 99 ba 36 e1 49 04 ce 6a 3f d8 95 db d4 0f 80 c8 b8 4e 80 00 30 "
        "39 43 18 2d 44 54 fb 21 09 40 00 00 c0 3f 02 05 18 00 00 00 00 02 00 17 00 12 00 01 00 13 "
        "00 01 00 05 01 00 0c 14 00 61 62 02 68 69 06 00 62 6c 6f 62 62 79 a5 7e");

    std::vector<ColumnMeta> metas =
        parse_table_metadata(types.data(), 18, meta.data(), meta.size());
    ASSERT_EQ(metas.size(), 18u);

    CdcTableSchema schema;
    schema.db = "test";
    schema.table = "dd";
    schema.columns = {col("id"),
                      col("u", /*uns=*/true),
                      col("c"),
                      col("v"),
                      col("t"),
                      col("dt"),
                      col("ts"),
                      col("d"),
                      col("worker"),
                      col("dc"),
                      col("dbl"),
                      col("fl"),
                      col("en", false, {"a", "b", "c"}),
                      col("st", false, {"x", "y", "z"}),
                      col("js"),
                      col("bl"),
                      col("bt"),
                      col("yr")};

    auto res = decode_rows_payload(
        row.data(), row.size(), clink::mysql::RowOp::Insert, metas, schema, "binlog:1", 0);
    ASSERT_EQ(res.dropped, 0u);
    ASSERT_EQ(res.events.size(), 1u);
    const auto& f = res.events[0].values;
    ASSERT_EQ(f.size(), 18u);
    EXPECT_EQ(f[0].value, "-5");                                    // INT signed
    EXPECT_EQ(f[1].value, "4000000000");                            // INT UNSIGNED
    EXPECT_EQ(f[2].value, "abcd");                                  // CHAR(4)
    EXPECT_EQ(f[3].value, "hi there");                              // VARCHAR(300)
    EXPECT_EQ(f[4].value, "hello world");                           // TEXT
    EXPECT_EQ(f[5].value, "2026-06-27 14:05:09.123000");            // DATETIME(3)
    EXPECT_EQ(f[6].value, "2026-06-27 14:05:09");                   // TIMESTAMP (UTC)
    EXPECT_EQ(f[7].value, "2026-06-27");                            // DATE
    EXPECT_EQ(f[8].value, "12:34:56.780000");                       // TIME(2)
    EXPECT_EQ(f[9].value, "12345.67");                              // DECIMAL(10,2)
    EXPECT_NEAR(std::stod(f[10].value), 3.141592653589793, 1e-15);  // DOUBLE
    EXPECT_EQ(f[11].value, "1.5");                                  // FLOAT
    EXPECT_EQ(f[12].value, "b");                                    // ENUM label
    EXPECT_EQ(f[13].value, "x,z");                                  // SET labels
    EXPECT_EQ(f[14].value, R"({"a":1,"b":"hi"})");                  // JSON
    EXPECT_EQ(f[15].value, "blobby");                               // BLOB
    EXPECT_EQ(f[16].value, "165");                                  // BIT(8) = 0xA5
    EXPECT_EQ(f[17].value, "2026");                                 // YEAR
}

TEST(MysqlRowDecode, NullsAndUpdateAfterImage) {
    // Simple table (a INT, b VARCHAR(20)). Build two rows by hand.
    const auto types = hx("03 0f");
    const auto meta = hx("14 00");  // varchar max length 20
    std::vector<ColumnMeta> metas = parse_table_metadata(types.data(), 2, meta.data(), meta.size());
    CdcTableSchema schema;
    schema.db = "d";
    schema.table = "t";
    schema.columns = {col("a"), col("b")};

    // INSERT (1, NULL): null bitmap 0x02 (bit1 set), then a=1 (4 bytes LE).
    auto ins = hx("02 01 00 00 00");
    auto r1 = decode_rows_payload(
        ins.data(), ins.size(), clink::mysql::RowOp::Insert, metas, schema, "l", 0);
    ASSERT_EQ(r1.events.size(), 1u);
    EXPECT_EQ(r1.events[0].op, CdcEvent::Op::Insert);
    EXPECT_EQ(r1.events[0].values[0].value, "1");
    EXPECT_TRUE(r1.events[0].values[1].is_null);

    // UPDATE before (1,'x') after (1,'yy'): leading after-bitmap (1 byte) + two
    // images; event uses the AFTER image.
    auto upd = hx("03 00 01 00 00 00 01 78 00 01 00 00 00 02 79 79");
    auto r2 = decode_rows_payload(
        upd.data(), upd.size(), clink::mysql::RowOp::Update, metas, schema, "l", 0);
    ASSERT_EQ(r2.events.size(), 1u);
    EXPECT_EQ(r2.events[0].op, CdcEvent::Op::Update);
    EXPECT_EQ(r2.events[0].values[1].value, "yy");
}

TEST(MysqlRowDecode, ColumnCountMismatchDrops) {
    const auto types = hx("03 03 03");  // 3 INT columns
    std::vector<ColumnMeta> metas = parse_table_metadata(types.data(), 3, nullptr, 0);
    CdcTableSchema schema;
    schema.columns = {col("a"), col("b")};  // schema says 2 -> mismatch
    auto row = hx("00 01 00 00 00 02 00 00 00 03 00 00 00");
    auto res = decode_rows_payload(
        row.data(), row.size(), clink::mysql::RowOp::Insert, metas, schema, "l", 0);
    EXPECT_TRUE(res.events.empty());
    EXPECT_EQ(res.dropped, 1u);
}

namespace {

// One column of the given binlog type and metadata, decoded from `row` (its
// null bitmap first): the value, or nullopt when the row was refused.
std::optional<std::string> one_value(const std::string& type,
                                     const std::string& meta,
                                     const std::string& row,
                                     std::vector<std::string> labels = {}) {
    const auto types = hx(type);
    const auto m = hx(meta);
    const auto metas = parse_table_metadata(types.data(), 1, m.data(), m.size());
    CdcTableSchema schema;
    schema.db = "d";
    schema.table = "t";
    schema.columns = {col("c", false, std::move(labels))};
    const auto data = hx(row);
    auto res = decode_rows_payload(
        data.data(), data.size(), clink::mysql::RowOp::Insert, metas, schema, "l", 0);
    if (res.dropped > 0 || res.events.size() != 1) {
        return std::nullopt;
    }
    return res.events[0].values[0].value;
}

}  // namespace

// A width or precision outside MySQL's limits marks a corrupt TABLE_MAP: the
// row is refused rather than read with it.
TEST(MysqlRowDecode, MetadataOutsideMysqlsLimitsRefusesTheRow) {
    EXPECT_FALSE(one_value("f6", "00 00", "00 80").has_value());        // DECIMAL(0,0)
    EXPECT_FALSE(one_value("f6", "05 07", "00 80 00 00").has_value());  // scale over precision
    EXPECT_FALSE(one_value("fc", "09", "00 01 00 00 00 00 00 00 00 00 41").has_value());  // BLOB
    EXPECT_FALSE(one_value("f5", "05", "00 00 00 00 00 00").has_value());  // JSON length width
    EXPECT_FALSE(one_value("10", "00 09", "00 01 02 03 04 05 06 07 08 09").has_value());  // BIT
    EXPECT_FALSE(one_value("12", "07", "00 99 b8 b6 a0 00 00 00 00").has_value());  // DATETIME2(7)
    // Within the limits they decode.
    EXPECT_EQ(one_value("fc", "01", "00 02 68 69"), "hi");
    EXPECT_EQ(one_value("10", "00 01", "00 05"), "5");
}

// Binary JSON that does not parse is refused, not turned into null; an empty
// value is MySQL's JSON null.
TEST(MysqlRowDecode, MalformedJsonRefusesTheRowAndEmptyJsonIsNull) {
    EXPECT_FALSE(one_value("f5", "04", "00 02 00 00 00 7f 7f").has_value());
    EXPECT_EQ(one_value("f5", "04", "00 00 00 00 00"), "null");
}

// ENUM index 0 is MySQL's error value, the empty string; an index or a SET bit
// past the members the definition lists is refused.
TEST(MysqlRowDecode, EnumAndSetValuesOutsideTheirMembersAreRefused) {
    EXPECT_EQ(one_value("fe", "f7 01", "00 02", {"a", "b"}), "b");
    EXPECT_EQ(one_value("fe", "f7 01", "00 00", {"a", "b"}), "");
    EXPECT_FALSE(one_value("fe", "f7 01", "00 03", {"a", "b"}).has_value());
    EXPECT_EQ(one_value("fe", "f8 01", "00 03", {"x", "y"}), "x,y");
    EXPECT_FALSE(one_value("fe", "f8 01", "00 04", {"x", "y"}).has_value());
}

TEST(MysqlJsonb, DecodesObjectArrayStringLiterals) {
    auto bytes = [](std::initializer_list<int> b) {
        std::string s;
        for (int x : b) {
            s.push_back(static_cast<char>(x));
        }
        return s;
    };
    EXPECT_EQ(clink::mysql::jsonb::decode(bytes(
                  {0x00, 0x01, 0x00, 0x0C, 0x00, 0x0B, 0x00, 0x01, 0x00, 0x05, 0x01, 0x00, 0x61})),
              std::optional<std::string>(R"({"a":1})"));
    EXPECT_EQ(
        clink::mysql::jsonb::decode(bytes(
            {0x02, 0x03, 0x00, 0x0D, 0x00, 0x04, 0x01, 0x00, 0x04, 0x02, 0x00, 0x04, 0x00, 0x00})),
        std::optional<std::string>("[true,false,null]"));
    EXPECT_EQ(clink::mysql::jsonb::decode(bytes({0x00,
                                                 0x01,
                                                 0x00,
                                                 0x0E,
                                                 0x00,
                                                 0x0B,
                                                 0x00,
                                                 0x01,
                                                 0x00,
                                                 0x0C,
                                                 0x0C,
                                                 0x00,
                                                 0x6B,
                                                 0x01,
                                                 0x76})),
              std::optional<std::string>(R"({"k":"v"})"));
    EXPECT_FALSE(clink::mysql::jsonb::decode(bytes({0x00, 0x05, 0x00})).has_value());
}
