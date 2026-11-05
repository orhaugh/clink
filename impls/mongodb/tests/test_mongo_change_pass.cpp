// The change-stream source's checkpoint-token rule, against a cursor that
// stands in for the driver: a pass hands over events one at a time and never
// reads one it does not hand over, so the token a checkpoint records after it
// covers exactly what the pass emitted - at the per-pass cap, at a cancel, on an
// idle stream, and not at all when an event stops the source. Also the source's
// refusal to start at the current time when it is restored without a token. No
// live MongoDB; MongoSourceRecoveryLive holds the driver to the same rule.

#include <cstddef>
#include <deque>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_HAS_MONGODB
#include "clink/mongodb/mongo_cdc_source.hpp"

#include "change_pass.hpp"
#endif

#ifdef CLINK_HAS_MONGODB

using clink::mongodb::ChangeStreamStop;
using clink::mongodb::FullDocument;
using clink::mongodb::detail::read_pass;

namespace {

// What the driver hands the source, one fetch at a time: an event with the
// token that covers it, or an empty fetch with the stream's position after it.
class FakeCursor {
public:
    void add_event(int id) {
        queue_.push_back({R"({"_id":{"_data":"t)" + std::to_string(id) +
                              R"("},"operationType":"insert","ns":{"db":"d","coll":"c"},)"
                              R"("documentKey":{"_id":)" +
                              std::to_string(id) + R"(},"fullDocument":{"_id":)" +
                              std::to_string(id) + "}}",
                          "t" + std::to_string(id)});
    }
    void add_raw(std::string event, std::string token) {
        queue_.push_back({std::move(event), std::move(token)});
    }
    void add_empty_fetch(std::string position) { queue_.push_back({std::nullopt, position}); }

    std::optional<std::string> next() {
        ++reads_;
        if (queue_.empty()) {
            return std::nullopt;
        }
        auto item = queue_.front();
        queue_.pop_front();
        token_ = item.token;
        return item.event;
    }
    std::optional<std::string> resume_token() { return token_; }
    [[nodiscard]] std::size_t reads() const noexcept { return reads_; }

private:
    struct Item {
        std::optional<std::string> event;
        std::string token;
    };
    std::deque<Item> queue_;
    std::string token_{"opened"};
    std::size_t reads_{0};
};

std::vector<int> ids_of(const std::vector<std::string>& rows) {
    std::vector<int> ids;
    for (const auto& r : rows) {
        ids.push_back(static_cast<int>(clink::config::parse(r).as_object().at("__id").as_number()));
    }
    return ids;
}

bool never() {
    return false;
}

std::string envelope(const clink::config::JsonValue& event) {
    return clink::mongodb::change_event_to_json(event, FullDocument::UpdateLookup);
}

}  // namespace

// The pass stops at the cap having read exactly the cap's worth, so the token is
// the last emitted event's, and the next pass starts with the event after it.
TEST(MongoChangePass, AtTheCapTheTokenIsTheLastEmittedEvents) {
    FakeCursor c;
    for (int i = 1; i <= 5; ++i) {
        c.add_event(i);
    }
    c.add_empty_fetch("after-5");
    auto first = read_pass(c, envelope, 3, never);
    EXPECT_EQ(ids_of(first.rows), (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(first.token, "t3") << "the token covers an event the pass did not emit";
    EXPECT_EQ(c.reads(), 3U) << "the pass read past the cap";
    auto second = read_pass(c, envelope, 3, never);
    EXPECT_EQ(ids_of(second.rows), (std::vector<int>{4, 5}));
    EXPECT_EQ(second.token, "after-5");
}

// A stop - a cancel, or the pass's time budget spent while changes keep coming -
// is checked before each read, so no event is read and dropped.
TEST(MongoChangePass, AStopBeforeAReadLeavesTheTokenAtTheLastEmittedEvent) {
    FakeCursor c;
    for (int i = 1; i <= 4; ++i) {
        c.add_event(i);
    }
    std::size_t asked = 0;
    auto pass = read_pass(c, envelope, 100, [&] { return ++asked > 2; });
    EXPECT_TRUE(pass.stopped);
    EXPECT_EQ(ids_of(pass.rows), (std::vector<int>{1, 2}));
    EXPECT_EQ(pass.token, "t2");
    EXPECT_EQ(c.reads(), 2U);
}

// An idle stream still moves: the empty fetch's position becomes the token, and
// it covers nothing the source has not seen.
TEST(MongoChangePass, AnIdlePassTakesTheStreamsPosition) {
    FakeCursor c;
    c.add_empty_fetch("idle-1");
    auto pass = read_pass(c, envelope, 100, never);
    EXPECT_TRUE(pass.rows.empty());
    EXPECT_EQ(pass.token, "idle-1");
}

// An event that stops the source throws out of the pass before its token is
// taken: nothing of the failed pass, the events before the stop included, can
// reach a checkpoint, so a restore reads them, and the stop, again.
TEST(MongoChangePass, AnEventThatStopsTheSourceFailsThePassBeforeItsToken) {
    FakeCursor c;
    c.add_event(1);
    c.add_raw(R"({"_id":{"_data":"t2"},"operationType":"drop","ns":{"db":"d","coll":"c"}})", "t2");
    EXPECT_THROW(read_pass(c, envelope, 100, never), ChangeStreamStop);
}

// Restored from a checkpoint whose state holds no token, the source would start
// at the current time and skip the changes since; it refuses before it connects.
TEST(MongoSourceRestore, ARestoreWithoutATokenRefusesToStartAtTheCurrentTime) {
    clink::InMemoryStateBackend backend;
    clink::RuntimeContext ctx(clink::OperatorId{1}, "src", &backend, nullptr);
    ctx.set_restore_from_checkpoint_id(7);
    clink::mongodb::MongoCdcOptions o;
    o.uri = "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100";
    o.database = "d";
    o.collection = "c";
    auto src = clink::mongodb::make_mongo_cdc_source(o);
    src->attach_runtime(&ctx);
    EXPECT_FALSE(src->restore_offset(backend, clink::OperatorId{1}));
    try {
        src->open();
        ADD_FAILURE() << "a restore without a token started";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("restored from checkpoint 7"), std::string::npos)
            << e.what();
        EXPECT_NE(std::string(e.what()).find("does not start at the current time"),
                  std::string::npos)
            << e.what();
    }
}

#endif  // CLINK_HAS_MONGODB
