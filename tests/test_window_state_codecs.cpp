// Durable-window codecs (FOUND-2): session_row_codec + record_codec +
// buffer_entry_codec. A codec that silently mis-decodes = a wrong restore, so
// these pin round-trip fidelity (incl. the agg/value-as-remainder framing under
// vector_codec), the optional event_time, the version byte, and rejection of
// truncated input.

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/window_state.hpp"

using namespace clink;

namespace {

template <typename T>
std::optional<T> roundtrip(const Codec<T>& c, const T& v) {
    return c.decode(c.encode(v));
}

}  // namespace

TEST(WindowStateCodecs, SessionRowRoundTrip) {
    auto c = session_row_codec<std::int64_t>(int64_codec());
    for (bool fired : {false, true}) {
        SessionRow<std::int64_t> sr{
            .start = 1234567890123LL, .end = 9876543210987LL, .fired = fired, .agg = 42};
        auto got = roundtrip(c, sr);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(got->start, sr.start);
        EXPECT_EQ(got->end, sr.end);
        EXPECT_EQ(got->fired, fired);
        EXPECT_EQ(got->agg, 42);
    }
}

TEST(WindowStateCodecs, SessionRowVectorOfNUnderVectorCodec) {
    auto c = vector_codec<SessionRow<std::int64_t>>(session_row_codec<std::int64_t>(int64_codec()));
    for (std::size_t n : {std::size_t{0}, std::size_t{1}, std::size_t{5}}) {
        std::vector<SessionRow<std::int64_t>> v;
        for (std::size_t i = 0; i < n; ++i) {
            v.push_back({.start = static_cast<std::int64_t>(i * 100),
                         .end = static_cast<std::int64_t>(i * 100 + 50),
                         .fired = (i % 2 == 0),
                         .agg = static_cast<std::int64_t>(i)});
        }
        auto got = roundtrip(c, v);
        ASSERT_TRUE(got.has_value());
        ASSERT_EQ(got->size(), n);
        for (std::size_t i = 0; i < n; ++i) {
            EXPECT_EQ((*got)[i].start, v[i].start);
            EXPECT_EQ((*got)[i].end, v[i].end);
            EXPECT_EQ((*got)[i].fired, v[i].fired);
            EXPECT_EQ((*got)[i].agg, v[i].agg);
        }
    }
}

TEST(WindowStateCodecs, SessionRowRejectsTruncatedAndBadVersion) {
    auto c = session_row_codec<std::int64_t>(int64_codec());
    SessionRow<std::int64_t> sr{.start = 1, .end = 2, .fired = true, .agg = 3};
    auto bytes = c.encode(sr);
    // Truncated header.
    EXPECT_FALSE(
        c.decode(typename Codec<SessionRow<std::int64_t>>::BytesView(bytes.data(), 5)).has_value());
    // Bad version byte.
    auto bad = bytes;
    bad[0] = static_cast<std::byte>(2);
    EXPECT_FALSE(c.decode(bad).has_value());
}

TEST(WindowStateCodecs, RecordCodecWithAndWithoutEventTime) {
    auto c = record_codec<std::string>(string_codec());

    Record<std::string> with{std::string{"hello"}};
    with.set_event_time(EventTime{777});
    auto g1 = roundtrip(c, with);
    ASSERT_TRUE(g1.has_value());
    EXPECT_EQ(g1->value(), "hello");
    ASSERT_TRUE(g1->event_time().has_value());
    EXPECT_EQ(g1->event_time()->millis(), 777);
    EXPECT_FALSE(g1->pane().has_value());  // PaneInfo deliberately not serialised

    Record<std::string> without{std::string{"plain"}};
    auto g2 = roundtrip(c, without);
    ASSERT_TRUE(g2.has_value());
    EXPECT_EQ(g2->value(), "plain");
    EXPECT_FALSE(g2->event_time().has_value());
}

TEST(WindowStateCodecs, BufferEntryRoundTrip) {
    auto c = buffer_entry_codec<std::string>(string_codec());
    for (std::size_t n : {std::size_t{0}, std::size_t{1}, std::size_t{4}}) {
        BufferEntry<std::string> be;
        be.fired = (n % 2 == 1);
        be.next_pane_index = static_cast<std::int64_t>(n) + 3;
        for (std::size_t i = 0; i < n; ++i) {
            Record<std::string> r{"v" + std::to_string(i)};
            if (i % 2 == 0) {
                r.set_event_time(EventTime{static_cast<std::int64_t>(i) * 10});
            }
            be.records.push_back(std::move(r));
        }
        auto got = roundtrip(c, be);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(got->fired, be.fired);
        EXPECT_EQ(got->next_pane_index, be.next_pane_index);
        ASSERT_EQ(got->records.size(), n);
        for (std::size_t i = 0; i < n; ++i) {
            EXPECT_EQ(got->records[i].value(), "v" + std::to_string(i));
            EXPECT_EQ(got->records[i].event_time().has_value(), (i % 2 == 0));
        }
    }
}

TEST(WindowStateCodecs, BufferEntryRejectsTruncatedAndBadVersion) {
    auto c = buffer_entry_codec<std::string>(string_codec());
    BufferEntry<std::string> be;
    be.records.push_back(Record<std::string>{"x"});
    auto bytes = c.encode(be);
    EXPECT_FALSE(
        c.decode(typename Codec<BufferEntry<std::string>>::BytesView(bytes.data(), 4)).has_value());
    auto bad = bytes;
    bad[0] = static_cast<std::byte>(9);
    EXPECT_FALSE(c.decode(bad).has_value());
}
