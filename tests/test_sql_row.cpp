#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/sql/row.hpp"
#include "clink/sql/row_kind.hpp"

namespace clink::sql {

TEST(SqlRow, JsonCodecRoundTripsScalars) {
    Row r;
    r.values["user_id"] = clink::config::JsonValue{static_cast<std::int64_t>(42)};
    r.values["url"] = clink::config::JsonValue{std::string{"http://x"}};
    r.values["active"] = clink::config::JsonValue{true};

    auto codec = row_json_codec();
    auto bytes = codec.encode(r);
    auto decoded = codec.decode({bytes.data(), bytes.size()});
    ASSERT_TRUE(decoded.has_value());

    EXPECT_EQ(decoded->get_string("url"), std::optional<std::string>{"http://x"});
    EXPECT_EQ(decoded->get_string("user_id"), std::optional<std::string>{"42"});
    EXPECT_EQ(decoded->get_string("active"), std::optional<std::string>{"true"});
}

// The row-list codec backing the async/disaggregated INNER join: a per-key entry
// list must round-trip through the remote pool (encode -> bytes -> decode) with
// every row's fields intact, order preserved. A bug here silently corrupts joins.
TEST(SqlRow, RowListJsonCodecRoundTrips) {
    auto mk = [](std::int64_t id, std::int64_t v, const std::string& s) {
        Row r;
        r.values["id"] = clink::config::JsonValue{id};
        r.values["v"] = clink::config::JsonValue{v};
        r.values["s"] = clink::config::JsonValue{s};
        return r;
    };
    std::vector<Row> rows = {mk(1, 10, "a"), mk(1, 11, "b"), mk(1, 12, "c")};

    auto codec = row_list_json_codec();
    auto bytes = codec.encode(rows);
    auto decoded = codec.decode({bytes.data(), bytes.size()});
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->size(), rows.size());
    for (std::size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ((*decoded)[i].get_string("id"), rows[i].get_string("id")) << "row " << i;
        EXPECT_EQ((*decoded)[i].get_string("v"), rows[i].get_string("v")) << "row " << i;
        EXPECT_EQ((*decoded)[i].get_string("s"), rows[i].get_string("s")) << "row " << i;
    }

    // Empty list round-trips to an empty list (not nullopt).
    auto empty_bytes = codec.encode({});
    auto empty = codec.decode({empty_bytes.data(), empty_bytes.size()});
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->empty());
}

// The Row codecs now populate encode_into (the zero-alloc keyed-state path).
// It MUST append bytes byte-identical to encode() to a caller-cleared buffer -
// a divergence would silently corrupt SQL keyed state on restore. (Both share
// one body, so this is byte-identical by construction; the test guards the
// append contract against a future refactor.)
TEST(SqlRow, RowCodecsEncodeIntoMatchesEncodeAndAppends) {
    Row r;
    r.values["user_id"] = clink::config::JsonValue{static_cast<std::int64_t>(42)};
    r.values["url"] = clink::config::JsonValue{std::string{"http://x"}};
    r.values["active"] = clink::config::JsonValue{true};

    auto check = [](const auto& codec, const auto& value) {
        ASSERT_TRUE(static_cast<bool>(codec.encode_into)) << "encode_into not populated";
        const auto canonical = codec.encode(value);
        // (1) into empty buffer == encode().
        std::vector<std::byte> buf;
        codec.encode_into(value, buf);
        EXPECT_EQ(buf, canonical);
        // (2) APPENDS to a non-empty buffer.
        std::vector<std::byte> pref{std::byte{0xAB}};
        codec.encode_into(value, pref);
        ASSERT_EQ(pref.size(), 1 + canonical.size());
        EXPECT_EQ(pref[0], std::byte{0xAB});
        EXPECT_TRUE(std::equal(canonical.begin(), canonical.end(), pref.begin() + 1));
        // (3) reuse + still decodes back.
        buf.clear();
        codec.encode_into(value, buf);
        EXPECT_EQ(buf, canonical);
        EXPECT_TRUE(codec.decode({buf.data(), buf.size()}).has_value());
    };

    check(row_json_codec(), r);
    std::vector<Row> rows = {r, r};
    check(row_list_json_codec(), rows);
}

// The row codec keeps a number's kind: a double with an integral value comes back
// a double and an integer comes back an integer, at the top level and nested in
// an array or object (ARRAY, MAP and ROW values). The evaluator picks integer or
// floating arithmetic by kind, so a double decoded as an integer changes the
// answer of `x / 2` after any hop that crosses this codec.
TEST(SqlRow, JsonCodecKeepsNumberKind) {
    using clink::config::JsonArray;
    using clink::config::JsonObject;
    using clink::config::JsonValue;
    Row r;
    r.values["d"] = JsonValue{9.0};
    r.values["neg"] = JsonValue{-4.0};
    r.values["zero"] = JsonValue{0.0};
    r.values["negzero"] = JsonValue{-0.0};
    r.values["big"] = JsonValue{1e18};
    r.values["huge"] = JsonValue{1e300};
    r.values["frac"] = JsonValue{2.5};
    r.values["i"] = JsonValue{std::int64_t{9}};
    r.values["imax"] = JsonValue{std::int64_t{9223372036854775807}};
    r.values["arr"] = JsonValue{JsonArray{JsonValue{1.0}, JsonValue{std::int64_t{2}}}};
    r.values["obj"] = JsonValue{JsonObject::from_entries({{"a", JsonValue{3.0}}})};

    auto check = [](const Row& got) {
        auto kind = [&](const char* name) { return got.values.find(name)->second.type(); };
        using T = JsonValue::Type;
        EXPECT_EQ(kind("d"), T::Number);
        EXPECT_EQ(kind("neg"), T::Number);
        EXPECT_EQ(kind("zero"), T::Number);
        EXPECT_EQ(kind("negzero"), T::Number);
        EXPECT_TRUE(std::signbit(got.values.find("negzero")->second.as_number()));
        EXPECT_EQ(kind("big"), T::Number);
        EXPECT_EQ(got.values.find("big")->second.as_number(), 1e18);
        EXPECT_EQ(kind("huge"), T::Number);
        EXPECT_EQ(got.values.find("huge")->second.as_number(), 1e300);
        EXPECT_EQ(kind("frac"), T::Number);
        EXPECT_EQ(kind("i"), T::Int);
        EXPECT_EQ(kind("imax"), T::Int);
        EXPECT_EQ(got.values.find("imax")->second.as_int(), 9223372036854775807);
        const auto& arr = got.values.find("arr")->second.as_array();
        ASSERT_EQ(arr.size(), 2u);
        EXPECT_EQ(arr[0].type(), T::Number);
        EXPECT_EQ(arr[1].type(), T::Int);
        EXPECT_EQ(got.values.find("obj")->second.at("a").type(), T::Number);
    };

    auto codec = row_json_codec();
    auto bytes = codec.encode(r);
    auto decoded = codec.decode({bytes.data(), bytes.size()});
    ASSERT_TRUE(decoded.has_value());
    check(*decoded);

    auto list_codec = row_list_json_codec();
    auto list_bytes = list_codec.encode({r});
    auto list = list_codec.decode({list_bytes.data(), list_bytes.size()});
    ASSERT_TRUE(list.has_value());
    ASSERT_EQ(list->size(), 1u);
    check(list->front());
}

// Compatibility both ways. The frame is JSON text, so the new writer's frame must
// be plain JSON any earlier reader accepts (every release decodes it with the
// generic JSON parse), and a frame an earlier writer produced, with an integral
// double written as a bare integer, must still decode as it always did.
TEST(SqlRow, JsonCodecFramesStayReadableBothWays) {
    using clink::config::JsonValue;
    Row r;
    r.values["d"] = JsonValue{9.0};
    r.values["i"] = JsonValue{std::int64_t{9}};
    auto codec = row_json_codec();
    auto bytes = codec.encode(r);
    const std::string text(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    EXPECT_EQ(text, R"({"d":9.0,"i":9})");
    auto generic = clink::config::parse(text);
    EXPECT_EQ(generic.at("d").type(), JsonValue::Type::Number);
    EXPECT_EQ(generic.at("d").as_number(), 9.0);
    EXPECT_EQ(generic.at("i").type(), JsonValue::Type::Int);

    // An earlier writer printed the double 9.0 bare. Its frame decodes as it
    // always did: the bare token is an integer, at the top level and nested, and
    // a fractional double stays a double.
    const std::string earlier = R"({"d":9,"f":2.5,"arr":[9,2.5],"obj":{"a":9}})";
    auto old_frame =
        codec.decode({reinterpret_cast<const std::byte*>(earlier.data()), earlier.size()});
    ASSERT_TRUE(old_frame.has_value());
    const auto& old = old_frame->values;
    EXPECT_EQ(old.find("d")->second.type(), JsonValue::Type::Int);
    EXPECT_EQ(old.find("d")->second.as_int(), 9);
    EXPECT_EQ(old.find("f")->second.type(), JsonValue::Type::Number);
    EXPECT_EQ(old.find("f")->second.as_number(), 2.5);
    const auto& old_arr = old.find("arr")->second.as_array();
    ASSERT_EQ(old_arr.size(), 2u);
    EXPECT_EQ(old_arr[0].type(), JsonValue::Type::Int);
    EXPECT_EQ(old_arr[1].type(), JsonValue::Type::Number);
    EXPECT_EQ(old.find("obj")->second.at("a").type(), JsonValue::Type::Int);
}

// External output is unchanged: the NDJSON sink still prints an integral double
// as a bare integer. Only the row codec between operators carries the kind.
TEST(SqlRow, JsonTextFormatStillPrintsIntegralDoubleBare) {
    Row r;
    r.values["d"] = clink::config::JsonValue{7.0};
    EXPECT_EQ(row_json_text_format().encode(r), R"({"d":7})");
    EXPECT_EQ(clink::config::JsonValue{7.0}.serialize(0), "7");
}

TEST(SqlRow, GetStringStringifiesNumbersAndBools) {
    Row r;
    r.values["i"] = clink::config::JsonValue{static_cast<std::int64_t>(100)};
    r.values["b"] = clink::config::JsonValue{false};
    r.values["s"] = clink::config::JsonValue{std::string{"hi"}};
    r.values["n"] = clink::config::JsonValue{nullptr};

    EXPECT_EQ(r.get_string("i"), std::optional<std::string>{"100"});
    EXPECT_EQ(r.get_string("b"), std::optional<std::string>{"false"});
    EXPECT_EQ(r.get_string("s"), std::optional<std::string>{"hi"});
    EXPECT_EQ(r.get_string("n"), std::nullopt);
    EXPECT_EQ(r.get_string("missing"), std::nullopt);
}

TEST(SqlRow, JsonTextFormatLineEncoding) {
    auto fmt = row_json_text_format();

    Row r;
    r.values["a"] = clink::config::JsonValue{std::string{"hello"}};
    r.values["b"] = clink::config::JsonValue{static_cast<std::int64_t>(7)};

    auto line = fmt.encode(r);
    EXPECT_NE(line.find("\"a\":\"hello\""), std::string::npos);
    EXPECT_NE(line.find("\"b\":7"), std::string::npos);
    EXPECT_EQ(line.find('\n'), std::string::npos);  // no trailing newline

    auto decoded = fmt.decode(line);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->get_string("a"), std::optional<std::string>{"hello"});
    EXPECT_EQ(decoded->get_string("b"), std::optional<std::string>{"7"});
}

TEST(SqlRow, JsonTextFormatSkipsMalformedLines) {
    auto fmt = row_json_text_format();
    EXPECT_FALSE(fmt.decode("not json").has_value());
    EXPECT_FALSE(fmt.decode("").has_value());
    EXPECT_FALSE(fmt.decode("[1, 2]").has_value());  // arrays aren't Row objects
}

// --- changelog wire convention --------------------------

TEST(SqlRow, RowKindHelpersRoundTrip) {
    Row r;
    EXPECT_FALSE(has_row_kind(r));
    EXPECT_EQ(row_kind_of(r), std::string{kRowKindInsert});  // unmarked == insert
    set_row_kind(r, kRowKindDelete);
    EXPECT_TRUE(has_row_kind(r));
    EXPECT_EQ(row_kind_of(r), std::string{kRowKindDelete});
}

TEST(SqlRow, CopyRowKindPropagatesWhenSet) {
    Row src;
    set_row_kind(src, kRowKindDelete);
    Row dst;
    copy_row_kind(src, dst);
    EXPECT_EQ(row_kind_of(dst), std::string{kRowKindDelete});
}

TEST(SqlRow, CopyRowKindNoOpsWhenUnset) {
    Row src;
    Row dst;
    copy_row_kind(src, dst);
    EXPECT_FALSE(has_row_kind(dst));
}

// --- update_before / update_after classifiers -----------

TEST(SqlRow, IsInsertLikeMatchesInsertAndUpdateAfter) {
    EXPECT_TRUE(is_insert_like(kRowKindInsert));
    EXPECT_TRUE(is_insert_like(kRowKindUpdateAfter));
    EXPECT_FALSE(is_insert_like(kRowKindDelete));
    EXPECT_FALSE(is_insert_like(kRowKindUpdateBefore));
}

TEST(SqlRow, IsDeleteLikeMatchesDeleteAndUpdateBefore) {
    EXPECT_TRUE(is_delete_like(kRowKindDelete));
    EXPECT_TRUE(is_delete_like(kRowKindUpdateBefore));
    EXPECT_FALSE(is_delete_like(kRowKindInsert));
    EXPECT_FALSE(is_delete_like(kRowKindUpdateAfter));
}

TEST(SqlRow, NewKindsRoundTrip) {
    Row r;
    set_row_kind(r, kRowKindUpdateAfter);
    EXPECT_EQ(row_kind_of(r), std::string{kRowKindUpdateAfter});
    set_row_kind(r, kRowKindUpdateBefore);
    EXPECT_EQ(row_kind_of(r), std::string{kRowKindUpdateBefore});
}

// ----- projected decode (filtered parse) -----

// Construction-path symmetry: decoding with the in-parse keep-list must
// produce exactly what the reference path (full decode, then
// project_row) produces - including the __row_kind marker surviving,
// decimal requantisation on surviving columns, and absent projected
// columns simply not appearing.
TEST(RowProjectedFormat, FilteredDecodeMatchesDecodeThenProject) {
    std::map<std::string, int> decimals{{"price", 2}};
    const std::vector<std::string> projected{"auction", "price"};

    auto filtered = row_json_text_format_projected(decimals, projected);
    auto reference = row_json_text_format_with_decimals(decimals);

    const std::string lines[] = {
        R"({"auction": 7, "bidder": "bob", "price": 1.239, "channel": "web", "ts": 5})",
        R"({"auction": 8, "price": 2.5, "__row_kind": "-U"})",
        R"({"bidder": "eve", "channel": "app"})",  // no projected columns at all
        R"({"auction": 9})",                       // partial row
    };
    for (const auto& line : lines) {
        auto got = filtered.decode(line);
        auto want = reference.decode(line);
        ASSERT_TRUE(want.has_value());
        project_row(*want, std::set<std::string>(projected.begin(), projected.end()));
        ASSERT_TRUE(got.has_value());
        EXPECT_TRUE(got->values == want->values) << line;
    }
    // Malformed / non-object / empty lines are skipped identically.
    EXPECT_FALSE(filtered.decode("{broken").has_value());
    EXPECT_FALSE(filtered.decode("[1,2]").has_value());
    EXPECT_FALSE(filtered.decode("").has_value());
}

TEST(RowProjectedFormat, RetainCompactsInOnePassPreservingOrder) {
    Row r;
    r.values.emplace("a", clink::config::JsonValue{1.0});
    r.values.emplace("b", clink::config::JsonValue{2.0});
    r.values.emplace("c", clink::config::JsonValue{3.0});
    r.values.emplace("d", clink::config::JsonValue{4.0});
    project_row(r, std::set<std::string>{"b", "d"});
    std::string order;
    for (const auto& [k, v] : r.values)
        order += k;
    EXPECT_EQ(order, "bd");
}

}  // namespace clink::sql
