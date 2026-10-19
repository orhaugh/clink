// Unit tests for the pgoutput decoder's silent-data-loss accounting (F1), driven
// through the pg_cdc_test_seam without a live Postgres. These construct raw
// pgoutput message bytes by hand and assert that undecodable I/U/D change events
// are DROPPED and counted (not emitted as corrupt partial rows), while valid
// rows decode cleanly and benign control messages are not counted as drops.
//
// The UpdateTruncatedOldImage case is the regression guard for an adversarial
// review finding (H1): a truncated OLD image on an UPDATE desyncs the cursor and,
// pre-fix, let a structurally-valid-but-WRONG NEW tuple be emitted uncounted.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/pg_cdc_test_seam.hpp"

#ifdef CLINK_HAS_POSTGRES

using clink::CdcEvent;
using clink::pg_cdc_testing::decode_pgoutput_messages;

namespace {

void put_be16(std::string& s, std::uint16_t v) {
    s.push_back(static_cast<char>((v >> 8) & 0xFF));
    s.push_back(static_cast<char>(v & 0xFF));
}
void put_be32(std::string& s, std::uint32_t v) {
    s.push_back(static_cast<char>((v >> 24) & 0xFF));
    s.push_back(static_cast<char>((v >> 16) & 0xFF));
    s.push_back(static_cast<char>((v >> 8) & 0xFF));
    s.push_back(static_cast<char>(v & 0xFF));
}
void put_cstr(std::string& s, const std::string& v) {
    s += v;
    s.push_back('\0');
}

// pgoutput Relation message: registers a relation so a later I/U/D referencing
// its oid can be decoded.
std::string rel_msg(std::uint32_t id,
                    const std::string& ns,
                    const std::string& name,
                    const std::vector<std::pair<std::string, std::uint32_t>>& cols) {
    std::string m;
    m.push_back('R');
    put_be32(m, id);
    put_cstr(m, ns);
    put_cstr(m, name);
    m.push_back('d');  // replica identity: default
    put_be16(m, static_cast<std::uint16_t>(cols.size()));
    for (const auto& [cname, oid] : cols) {
        m.push_back('\0');  // flags
        put_cstr(m, cname);
        put_be32(m, oid);
        put_be32(m, 0xFFFFFFFFu);  // type modifier -1
    }
    return m;
}

// TupleData with all-text columns.
std::string text_tuple(const std::vector<std::string>& vals) {
    std::string t;
    put_be16(t, static_cast<std::uint16_t>(vals.size()));
    for (const auto& v : vals) {
        t.push_back('t');
        put_be32(t, static_cast<std::uint32_t>(v.size()));
        t += v;
    }
    return t;
}

std::string insert_msg(std::uint32_t rel_id, const std::string& tuple) {
    std::string m;
    m.push_back('I');
    put_be32(m, rel_id);
    m.push_back('N');
    m += tuple;
    return m;
}

std::string begin_msg() {
    std::string m;
    m.push_back('B');
    m.append(20, '\0');  // final_lsn(8) + commit_ts(8) + xid(4)
    return m;
}
std::string commit_msg() {
    std::string m;
    m.push_back('C');
    m.append(25, '\0');  // flags(1) + commit_lsn(8) + end_lsn(8) + commit_ts(8)
    return m;
}

}  // namespace

// A well-formed Insert decodes to exactly one event and is NOT counted as a drop
// (guards against the truncation logic false-positiving on valid tuples).
TEST(PostgresCdcDecode, ValidInsertNotDropped) {
    auto res = decode_pgoutput_messages({
        rel_msg(1, "public", "t", {{"id", 23}}),
        insert_msg(1, text_tuple({"42"})),
    });
    ASSERT_EQ(res.events.size(), 1u);
    EXPECT_EQ(res.events[0].op, CdcEvent::Op::Insert);
    EXPECT_EQ(res.events[0].table, "public.t");
    EXPECT_EQ(res.dropped, 0u);
}

// An Insert for a relation never announced by a Relation message (a missed 'R')
// is dropped and counted.
TEST(PostgresCdcDecode, UnknownRelationInsertDropped) {
    auto res = decode_pgoutput_messages({insert_msg(99, text_tuple({"1"}))});
    EXPECT_TRUE(res.events.empty());
    EXPECT_EQ(res.dropped, 1u);
}

// An Insert message shorter than its fixed header is dropped, not decoded.
TEST(PostgresCdcDecode, ShortInsertDropped) {
    std::string m;
    m.push_back('I');
    put_be32(m, 1);  // 4 bytes after 'I' -> rest < 5 -> drop
    auto res = decode_pgoutput_messages({m});
    EXPECT_TRUE(res.events.empty());
    EXPECT_EQ(res.dropped, 1u);
}

// A tuple that declares more columns than it carries is dropped rather than
// emitted as a column-missing (corrupt) row.
TEST(PostgresCdcDecode, TruncatedTupleDropped) {
    std::string trunc;
    put_be16(trunc, 2);  // declares 2 columns
    trunc.push_back('t');
    put_be32(trunc, 1);
    trunc += "x";  // only 1 column present -> truncated mid-loop
    auto res = decode_pgoutput_messages({
        rel_msg(1, "public", "t", {{"a", 23}, {"b", 23}}),
        insert_msg(1, trunc),
    });
    EXPECT_TRUE(res.events.empty());
    EXPECT_EQ(res.dropped, 1u);
}

// H1 regression: an UPDATE whose discarded OLD image is truncated must DROP.
// The OLD tuple claims a column length far larger than the remaining bytes, so
// parse_tuple bails at the length-underrun WITHOUT consuming the value, leaving
// the cursor on the stray 'N' marker of a perfectly valid NEW tuple. Pre-fix the
// 'N' guard passed and a corrupt NEW Update was emitted uncounted; post-fix the
// truncated OLD image is treated as unrecoverable framing loss and dropped.
TEST(PostgresCdcDecode, UpdateTruncatedOldImageDropped) {
    std::string m;
    m.push_back('U');
    put_be32(m, 1);
    m.push_back('O');  // old image follows
    put_be16(m, 1);
    m.push_back('t');
    put_be32(m, 0x7FFFFFFFu);  // claimed length, never satisfied -> truncated
    m.push_back('N');          // stray valid-looking NEW marker
    put_be16(m, 1);
    m.push_back('n');  // a structurally-valid NEW null column

    auto res = decode_pgoutput_messages({
        rel_msg(1, "public", "t", {{"id", 23}}),
        m,
    });
    EXPECT_TRUE(res.events.empty()) << "a corrupt NEW tuple was emitted from a truncated OLD image";
    EXPECT_EQ(res.dropped, 1u);
}

// Begin/Relation/Commit are benign metadata: they may emit (Begin/Commit) or
// register state (Relation) but must NEVER be counted as dropped data events.
TEST(PostgresCdcDecode, BenignControlMessagesNotDropped) {
    auto res = decode_pgoutput_messages({
        begin_msg(),
        rel_msg(1, "public", "t", {{"id", 23}}),
        commit_msg(),
    });
    EXPECT_EQ(res.dropped, 0u);
}

// ---- strict decoding (the SQL CDC contract) --------------------------------

namespace {

using clink::pg_cdc_testing::PgDecodeConfig;

// A TupleData cell: 't' with a value, 'n' null, 'u' unchanged TOAST.
struct TupleCell {
    char kind;
    std::string value;
};

std::string tuple(const std::vector<TupleCell>& cells) {
    std::string t;
    put_be16(t, static_cast<std::uint16_t>(cells.size()));
    for (const auto& c : cells) {
        t.push_back(c.kind);
        if (c.kind == 't') {
            put_be32(t, static_cast<std::uint32_t>(c.value.size()));
            t += c.value;
        }
    }
    return t;
}

// UPDATE with an optional old image ('K' key only, 'O' whole row, 0 none).
std::string update_msg(std::uint32_t rel_id,
                       char old_kind,
                       const std::string& old_tuple,
                       const std::string& new_tuple) {
    std::string m;
    m.push_back('U');
    put_be32(m, rel_id);
    if (old_kind != 0) {
        m.push_back(old_kind);
        m += old_tuple;
    }
    m.push_back('N');
    m += new_tuple;
    return m;
}

std::string truncate_msg(std::uint32_t rel_id) {
    std::string m;
    m.push_back('T');
    put_be32(m, 1);     // one relation
    m.push_back('\0');  // flags
    put_be32(m, rel_id);
    return m;
}

// public.orders (id int8 key, status text, body text) and public.other (id).
std::vector<std::string> relations() {
    return {rel_msg(1, "public", "orders", {{"id", 20}, {"status", 25}, {"body", 25}}),
            rel_msg(2, "public", "other", {{"id", 20}})};
}

PgDecodeConfig strict_orders(std::vector<std::string> needed = {"id", "status", "body"}) {
    return PgDecodeConfig{.table = "public.orders",
                          .key_columns = {"id"},
                          .needed_columns = std::move(needed),
                          .strict = true};
}

std::string value_of(const CdcEvent& ev, const std::string& name) {
    for (const auto& f : ev.values) {
        if (f.name == name) {
            return f.is_null ? "<null>" : f.value;
        }
    }
    return "<absent>";
}

}  // namespace

TEST(PostgresCdcDecodeStrict, ChangesToOtherTablesAreSkippedNotDropped) {
    auto msgs = relations();
    msgs.push_back(insert_msg(2, tuple({{'t', "9"}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    EXPECT_TRUE(res.events.empty());
    EXPECT_EQ(res.dropped, 0U);
}

TEST(PostgresCdcDecodeStrict, AnUnchangedToastValueIsTakenFromTheWholeOldRow) {
    // REPLICA IDENTITY FULL: the old row carries body inline.
    auto msgs = relations();
    msgs.push_back(update_msg(1,
                              'O',
                              tuple({{'t', "1"}, {'t', "new"}, {'t', "long body"}}),
                              tuple({{'t', "1"}, {'t', "paid"}, {'u', ""}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    ASSERT_EQ(res.events.size(), 1U);
    EXPECT_EQ(res.events[0].op, CdcEvent::Op::Update);
    EXPECT_EQ(value_of(res.events[0], "status"), "paid");
    EXPECT_EQ(value_of(res.events[0], "body"), "long body");
    EXPECT_EQ(res.dropped, 0U);
}

TEST(PostgresCdcDecodeStrict, AnUnchangedToastValueWithNoOldRowStopsTheChange) {
    // REPLICA IDENTITY DEFAULT: no old row, so body's value is not in the change.
    auto msgs = relations();
    msgs.push_back(update_msg(1, 0, "", tuple({{'t', "1"}, {'t', "paid"}, {'u', ""}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    EXPECT_TRUE(res.events.empty());
    ASSERT_EQ(res.errors.size(), 1U);
    EXPECT_NE(res.errors[0].find("REPLICA IDENTITY FULL"), std::string::npos) << res.errors[0];
    EXPECT_NE(res.errors[0].find("body"), std::string::npos) << res.errors[0];
}

TEST(PostgresCdcDecodeStrict, AnUnchangedToastValueTheConsumerDoesNotReadIsLeftOut) {
    auto msgs = relations();
    msgs.push_back(update_msg(1, 0, "", tuple({{'t', "1"}, {'t', "paid"}, {'u', ""}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders({"id", "status"}));
    ASSERT_EQ(res.events.size(), 1U);
    EXPECT_EQ(value_of(res.events[0], "body"), "<absent>");
    EXPECT_EQ(value_of(res.events[0], "status"), "paid");
}

// An UPDATE that brings a row into a publication's row filter is published
// as an INSERT, which under REPLICA IDENTITY DEFAULT marks the out-of-line
// values it left unchanged and does not carry them.
TEST(PostgresCdcDecodeStrict, AnInsertWithAnUnchangedToastValueStopsTheChange) {
    auto msgs = relations();
    msgs.push_back(insert_msg(1, tuple({{'t', "1"}, {'t', "paid"}, {'u', ""}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    EXPECT_TRUE(res.events.empty());
    ASSERT_EQ(res.errors.size(), 1U);
    EXPECT_NE(res.errors[0].find("REPLICA IDENTITY FULL"), std::string::npos) << res.errors[0];
    EXPECT_NE(res.errors[0].find("body"), std::string::npos) << res.errors[0];
    // A column the consumer does not read is left out instead.
    res = decode_pgoutput_messages(msgs, strict_orders({"id", "status"}));
    ASSERT_EQ(res.events.size(), 1U);
    EXPECT_EQ(value_of(res.events[0], "body"), "<absent>");
}

// keep_rows: the consumer holds every row, so an unchanged out-of-line value
// is passed on marked for it to fill in, and a TRUNCATE is passed on.
TEST(PostgresCdcDecodeStrict, WhenTheConsumerKeepsRowsUnchangedValuesAndTruncatesPass) {
    auto config = strict_orders();
    config.consumer_keeps_rows = true;
    auto msgs = relations();
    msgs.push_back(update_msg(1, 0, "", tuple({{'t', "1"}, {'t', "paid"}, {'u', ""}})));
    auto res = decode_pgoutput_messages(msgs, config);
    ASSERT_EQ(res.events.size(), 1U);
    EXPECT_TRUE(res.errors.empty());
    bool marked = false;
    for (const auto& f : res.events[0].values) {
        if (f.name == "body") {
            marked = f.type == "(unchanged toast)" && !f.is_null;
        }
    }
    EXPECT_TRUE(marked) << "the unchanged value was not marked";

    // TRUNCATE of the table: an event naming the table, not a stop.
    msgs = relations();
    msgs.push_back(truncate_msg(1));
    res = decode_pgoutput_messages(msgs, config);
    ASSERT_EQ(res.events.size(), 1U);
    EXPECT_EQ(res.events[0].op, CdcEvent::Op::Truncate);
    EXPECT_EQ(res.events[0].table, "public.orders");
    EXPECT_TRUE(res.errors.empty());
}

TEST(PostgresCdcDecodeStrict, LegacyDecodingKeepsTheEmptyUnchangedToastValue) {
    auto msgs = relations();
    msgs.push_back(update_msg(1, 0, "", tuple({{'t', "1"}, {'t', "paid"}, {'u', ""}})));
    auto res = decode_pgoutput_messages(msgs);
    ASSERT_EQ(res.events.size(), 1U);
    EXPECT_EQ(value_of(res.events[0], "body"), "");
}

TEST(PostgresCdcDecodeStrict, AKeyChangingUpdateIsADeleteOfTheOldKeyAndTheNewRow) {
    auto msgs = relations();
    // REPLICA IDENTITY DEFAULT sends the old key ('K') only when it changed.
    msgs.push_back(update_msg(1,
                              'K',
                              tuple({{'t', "1"}, {'n', ""}, {'n', ""}}),
                              tuple({{'t', "2"}, {'t', "paid"}, {'t', "b"}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    ASSERT_EQ(res.events.size(), 2U);
    EXPECT_EQ(res.events[0].op, CdcEvent::Op::Delete);
    EXPECT_EQ(value_of(res.events[0], "id"), "1");
    EXPECT_EQ(res.events[1].op, CdcEvent::Op::Update);
    EXPECT_EQ(value_of(res.events[1], "id"), "2");
}

TEST(PostgresCdcDecodeStrict, AnUpdateKeepingItsKeyUnderFullIdentityIsOneUpdate) {
    auto msgs = relations();
    msgs.push_back(update_msg(1,
                              'O',
                              tuple({{'t', "1"}, {'t', "new"}, {'t', "b"}}),
                              tuple({{'t', "1"}, {'t', "paid"}, {'t', "b"}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    ASSERT_EQ(res.events.size(), 1U);
    EXPECT_EQ(res.events[0].op, CdcEvent::Op::Update);
}

TEST(PostgresCdcDecodeStrict, ATruncateOfTheTableStopsTheStream) {
    auto msgs = relations();
    msgs.push_back(truncate_msg(1));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    EXPECT_TRUE(res.events.empty());
    ASSERT_EQ(res.errors.size(), 1U);
    EXPECT_NE(res.errors[0].find("TRUNCATE"), std::string::npos) << res.errors[0];
    // A TRUNCATE of another table is not this source's business.
    auto other = relations();
    other.push_back(truncate_msg(2));
    EXPECT_TRUE(decode_pgoutput_messages(other, strict_orders()).errors.empty());
}

TEST(PostgresCdcDecodeStrict, AColumnTheJobReadsThatTheTableNoLongerHasStopsTheChange) {
    std::vector<std::string> msgs = {rel_msg(1, "public", "orders", {{"id", 20}, {"status", 25}})};
    msgs.push_back(insert_msg(1, tuple({{'t', "1"}, {'t', "new"}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    EXPECT_TRUE(res.events.empty());
    ASSERT_EQ(res.errors.size(), 1U);
    EXPECT_NE(res.errors[0].find("body"), std::string::npos) << res.errors[0];
}

TEST(PostgresCdcDecodeStrict, StreamedTransactionMessagesAreAProtocolError) {
    auto msgs = relations();
    msgs.push_back(std::string{"S"} + std::string(4, '\0') + std::string(1, '\1'));
    EXPECT_THROW(decode_pgoutput_messages(msgs, strict_orders()), std::runtime_error);
}

TEST(PostgresCdcDecodeStrict, ChangesCarryTheirTransactionId) {
    std::string b;
    b.push_back('B');
    b.append(16, '\0');  // final_lsn + commit_ts
    put_be32(b, 4242);   // xid
    auto msgs = relations();
    msgs.push_back(b);
    msgs.push_back(insert_msg(1, tuple({{'t', "1"}, {'t', "new"}, {'t', "b"}})));
    auto res = decode_pgoutput_messages(msgs, strict_orders());
    ASSERT_EQ(res.events.size(), 2U);
    EXPECT_EQ(res.events[1].xid, 4242);
}

#endif  // CLINK_HAS_POSTGRES
