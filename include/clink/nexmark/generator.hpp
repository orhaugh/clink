#pragma once

// Nexmark event generator (https://github.com/nexmark/nexmark) as a clink Row
// stream, for the Nexmark-on-clink benchmark.
//
// Nexmark models ONE event stream carrying three interleaved record types -
// Person (new users), Auction (new listings), Bid (bids on listings) - in the
// standard 1:3:46 (Person:Auction:Bid) ratio per 50-event epoch. clink SQL joins
// require BASE TABLES, so each type is exposed as its own table backed by a
// nexmark_source filtered to that type (nexmark_type='person'|'auction'|'bid').
// Every instance runs the SAME deterministic stream (same seed) and advances its
// state over EVERY event - returning only the requested type - so the shared
// counters/RNG stay in lock-step and foreign keys resolve across the per-type
// tables (an auction's seller is a person id the person table also produced; a
// bid's auction is an auction id the auction table produced).
//
// Deterministic: fixed-seed mt19937 + a monotonic event counter, reproducible.
// `dateTime` advances by 1000/tps ms per event so event-time windows are
// meaningful. Bounded when events_num > 0 (the source drains so the harness can
// take a final metrics reading).

#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/sql/row.hpp"

namespace clink::nexmark {

struct NexmarkConfig {
    std::int64_t events_num = 1'000'000;            // total events across all types; 0 => unbounded
    std::int64_t tps = 1'000'000;                   // event rate -> dateTime spacing (1000/tps ms)
    std::int64_t base_time_ms = 1'000'000'000'000;  // event-time origin
    std::uint64_t seed = 0x9E3779B97F4A7C15ULL;
    int type_filter = -1;  // -1 = all; 0 = person, 1 = auction, 2 = bid
};

// Standard Nexmark per-epoch proportions (Person:Auction:Bid = 1:3:46 of 50).
inline constexpr std::int64_t kPersonProportion = 1;
inline constexpr std::int64_t kAuctionProportion = 3;
inline constexpr std::int64_t kEventsPerEpoch = 50;  // 1 + 3 + 46
inline constexpr std::int64_t kNumCategories = 5;    // auction categories 10..14 (q3 filters =10)

class NexmarkGenerator {
public:
    explicit NexmarkGenerator(NexmarkConfig cfg) : cfg_(cfg), rng_(cfg.seed) {}

    // Next event of the configured type (any type if type_filter < 0), or nullopt
    // once events_num is reached. Skipped events still advance the shared state so
    // per-type instances stay in lock-step.
    std::optional<Record<clink::sql::Row>> next() {
        while (cfg_.events_num <= 0 || event_id_ < cfg_.events_num) {
            const std::int64_t e = event_id_++;
            const std::int64_t dt = cfg_.base_time_ms + (cfg_.tps > 0 ? (e * 1000) / cfg_.tps : e);
            clink::sql::Row r;
            put(r, "dateTime", dt);
            const std::int64_t slot = e % kEventsPerEpoch;
            int type;
            if (slot < kPersonProportion) {
                gen_person(r);
                type = 0;
            } else if (slot < kPersonProportion + kAuctionProportion) {
                gen_auction(r, dt);
                type = 1;
            } else {
                gen_bid(r);
                type = 2;
            }
            if (cfg_.type_filter < 0 || type == cfg_.type_filter) {
                return Record<clink::sql::Row>{std::move(r)};
            }
            // else: discard (state already advanced, RNG consumed identically)
        }
        return std::nullopt;
    }

private:
    static void put(clink::sql::Row& r, const char* k, std::int64_t v) {
        r.values[k] = clink::config::JsonValue{v};
    }
    static void put(clink::sql::Row& r, const char* k, const std::string& v) {
        r.values[k] = clink::config::JsonValue{v};
    }
    std::int64_t pick(std::int64_t count) {
        return count > 0 ? static_cast<std::int64_t>(rng_() % static_cast<std::uint64_t>(count))
                         : 0;
    }

    void gen_person(clink::sql::Row& r) {
        const std::int64_t id = person_count_++;
        put(r, "id", id);
        put(r, "name", "Person_" + std::to_string(id));
        put(r, "emailAddress", "p" + std::to_string(id) + "@nexmark.dev");
        put(r, "city", kCities[rng_() % kCities.size()]);
        put(r, "state", kStates[rng_() % kStates.size()]);
    }

    void gen_auction(clink::sql::Row& r, std::int64_t dt) {
        const std::int64_t id = auction_count_++;
        put(r, "id", id);
        put(r, "itemName", "Item_" + std::to_string(id));
        put(r, "initialBid", static_cast<std::int64_t>(rng_() % 1000));
        put(r, "reserve", static_cast<std::int64_t>(rng_() % 10000));
        put(r, "expires", dt + 10'000);  // 10s lifetime
        put(r, "seller", pick(person_count_));
        put(r, "category", 10 + static_cast<std::int64_t>(rng_() % kNumCategories));
    }

    void gen_bid(clink::sql::Row& r) {
        put(r, "auction", pick(auction_count_));
        put(r, "bidder", pick(person_count_));
        put(r, "price", static_cast<std::int64_t>(rng_() % 100'000));
        put(r, "channel", kChannels[rng_() % kChannels.size()]);
        put(r, "url", "https://nexmark.dev/" + std::to_string(rng_() % 10'000));
    }

    NexmarkConfig cfg_;
    std::mt19937_64 rng_;
    std::int64_t event_id_ = 0;
    std::int64_t person_count_ = 0;
    std::int64_t auction_count_ = 0;

    inline static const std::vector<std::string> kStates{"OR", "ID", "CA", "WY", "WA", "NY"};
    inline static const std::vector<std::string> kCities{
        "Portland", "Boise", "LA", "Seattle", "Phoenix", "Denver"};
    inline static const std::vector<std::string> kChannels{
        "Google", "Apple", "Facebook", "Baidu", "Amazon"};
};

}  // namespace clink::nexmark
