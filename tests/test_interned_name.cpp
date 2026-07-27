// InternedName: the 8-byte column-name handle behind a Row's column map.
//
// A Row used to store its own copy of every column name, so a six-column nexmark
// bid row spent 144 of its 424 bytes on names byte-identical in every row of the
// stream - paid again for every row an operator RETAINS. Interning replaces the
// key with a pointer to one shared copy.
//
// The table is lock-free on lookup and takes a mutex only to publish a name not
// seen before, because interning sits on the row-decode path that every worker
// thread runs at once. That makes concurrency the thing worth testing: the
// claims below are that identity is genuinely shared across threads, that the
// table survives growth without losing or duplicating a name, and that a handle
// stays valid after the table has grown underneath it.

#include <atomic>
#include <cstddef>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/interned_name.hpp"
#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

namespace {

using clink::config::InternedName;

}  // namespace

TEST(InternedName, SameTextGivesTheSameHandle) {
    const InternedName a{"auction"};
    const InternedName b{std::string("auction")};
    const InternedName c{std::string_view("auction")};
    EXPECT_EQ(a, b);
    EXPECT_EQ(b, c);
    // Identity, not just equality: the whole point is one shared copy.
    EXPECT_EQ(&a.str(), &b.str());
    EXPECT_EQ(&b.str(), &c.str());
}

TEST(InternedName, DifferentTextGivesDifferentHandles) {
    const InternedName a{"auction"};
    const InternedName b{"bidder"};
    EXPECT_NE(a, b);
    EXPECT_NE(&a.str(), &b.str());
}

TEST(InternedName, StaysPointerSized) {
    EXPECT_EQ(sizeof(InternedName), sizeof(void*));
    // The reason the type exists: the Row column pair must shrink.
    EXPECT_EQ(sizeof(clink::sql::RowColumns::value_type),
              sizeof(InternedName) + sizeof(clink::config::JsonValue));
    EXPECT_LT(sizeof(clink::sql::RowColumns::value_type),
              sizeof(clink::config::JsonObject::value_type))
        << "an interned key must be smaller than the std::string key it replaced";
}

// Ordering compares CONTENT, not pointer value. Row iteration is sorted by name
// and the engine relies on that for stable serialisation; ordering by intern
// order would make a snapshot's byte layout depend on which names a process
// happened to see first.
TEST(InternedName, OrdersByContentNotByInternOrder) {
    const InternedName z{"zzz_last"};  // interned first, on purpose
    const InternedName a{"aaa_first"};
    EXPECT_LT(a, z);
    EXPECT_GT(z, a);
    EXPECT_FALSE(a > z);
}

TEST(InternedName, ComparesAgainstAStringView) {
    const InternedName a{"price"};
    EXPECT_TRUE(a == std::string_view{"price"});
    EXPECT_FALSE(a == std::string_view{"prices"});
    EXPECT_LT(a, std::string_view{"q"});
    EXPECT_GT(a, std::string_view{"a"});
}

TEST(InternedName, DefaultIsEmptyAndInternsEmptyToTheSameHandle) {
    const InternedName d;
    EXPECT_TRUE(d.empty());
    EXPECT_EQ(d.str(), "");
    EXPECT_EQ(d, InternedName{""});
    EXPECT_EQ(d, InternedName{std::string_view{}});
}

TEST(InternedName, RoundTripsThroughARowColumnMap) {
    clink::sql::Row r;
    r.values[InternedName{"bidder"}] = clink::config::JsonValue{std::int64_t{7}};
    r.values["price"] = clink::config::JsonValue{std::int64_t{9}};  // string_view overload
    ASSERT_EQ(r.values.size(), 2u);
    // Found by either key form, and iteration is name-sorted.
    ASSERT_NE(r.values.find(InternedName{"bidder"}), r.values.end());
    ASSERT_NE(r.values.find("bidder"), r.values.end());
    EXPECT_EQ(r.values.at("bidder").as_number(), 7);
    std::vector<std::string> order;
    for (const auto& [k, v] : r.values) {
        order.push_back(k.str());
    }
    EXPECT_EQ(order, (std::vector<std::string>{"bidder", "price"}));
}

// Growth is the risky part: the table rehashes into a bigger slot array while
// other threads may be probing the old one. Interning many distinct names forces
// several growths; afterwards every name must resolve to exactly one handle, and
// handles taken BEFORE the growth must still be the ones returned after it.
TEST(InternedName, SurvivesTableGrowthWithoutLosingOrDuplicatingAName) {
    constexpr int kNames = 4000;  // several doublings past the 1024 initial slots
    std::vector<InternedName> early;
    early.reserve(64);
    for (int i = 0; i < 64; ++i) {
        early.emplace_back("grow_early_" + std::to_string(i));
    }
    for (int i = 0; i < kNames; ++i) {
        (void)InternedName{"grow_filler_" + std::to_string(i)};
    }
    // A handle taken before the growth is still the handle for that text.
    for (int i = 0; i < 64; ++i) {
        const InternedName again{"grow_early_" + std::to_string(i)};
        EXPECT_EQ(early[static_cast<std::size_t>(i)], again)
            << "handle for grow_early_" << i << " changed across a table growth";
        EXPECT_EQ(early[static_cast<std::size_t>(i)].str(), "grow_early_" + std::to_string(i));
    }
    // And every filler resolves to one distinct handle - no collision merged two
    // names, no growth dropped one.
    std::set<const std::string*> handles;
    for (int i = 0; i < kNames; ++i) {
        handles.insert(&InternedName{"grow_filler_" + std::to_string(i)}.str());
    }
    EXPECT_EQ(handles.size(), static_cast<std::size_t>(kNames));
}

// Concurrent interning of the SAME names from many threads must converge on one
// handle each. Under a torn publish (a slot visible before its string is built)
// this either crashes or hands back two handles for one name.
TEST(InternedName, ConcurrentInterningOfTheSameNamesConvergesOnOneHandle) {
    constexpr int kThreads = 8;
    constexpr int kDistinct = 200;
    std::vector<std::string> names;
    names.reserve(kDistinct);
    for (int i = 0; i < kDistinct; ++i) {
        names.push_back("concurrent_" + std::to_string(i));
    }

    std::vector<std::vector<const std::string*>> seen(kThreads);
    std::atomic<int> ready{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            ready.fetch_add(1);
            while (ready.load() < kThreads) {
                // Spin so the threads race on the same unpublished names.
            }
            auto& out = seen[static_cast<std::size_t>(t)];
            out.reserve(names.size());
            for (const auto& n : names) {
                out.push_back(&InternedName{n}.str());
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    for (int t = 1; t < kThreads; ++t) {
        EXPECT_EQ(seen[static_cast<std::size_t>(t)], seen[0])
            << "thread " << t << " got different handles for the same names";
    }
    for (std::size_t i = 0; i < names.size(); ++i) {
        EXPECT_EQ(*seen[0][i], names[i]);
    }
}

// Concurrent interning of DISTINCT names, which is the growth path under
// contention: every thread inserts, so the mutex-protected publish and the
// lock-free readers overlap continuously.
TEST(InternedName, ConcurrentInterningOfDistinctNamesKeepsThemAllDistinct) {
    constexpr int kThreads = 8;
    constexpr int kPerThread = 500;
    std::vector<std::vector<const std::string*>> seen(kThreads);
    std::atomic<int> ready{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            ready.fetch_add(1);
            while (ready.load() < kThreads) {
            }
            auto& out = seen[static_cast<std::size_t>(t)];
            out.reserve(kPerThread);
            for (int i = 0; i < kPerThread; ++i) {
                out.push_back(
                    &InternedName{"race_" + std::to_string(t) + "_" + std::to_string(i)}.str());
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    std::set<const std::string*> all;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kPerThread; ++i) {
            all.insert(seen[static_cast<std::size_t>(t)][static_cast<std::size_t>(i)]);
            EXPECT_EQ(*seen[static_cast<std::size_t>(t)][static_cast<std::size_t>(i)],
                      "race_" + std::to_string(t) + "_" + std::to_string(i));
        }
    }
    EXPECT_EQ(all.size(), static_cast<std::size_t>(kThreads * kPerThread))
        << "two distinct names ended up sharing a handle";
}
