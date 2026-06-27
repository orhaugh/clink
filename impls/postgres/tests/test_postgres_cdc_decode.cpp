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

#endif  // CLINK_HAS_POSTGRES
