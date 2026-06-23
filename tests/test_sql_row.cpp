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

// --- Phase 21a: changelog wire convention --------------------------

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

// --- Phase 24a: update_before / update_after classifiers -----------

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

}  // namespace clink::sql
