// Offline tests for the pure MongoDB change-event -> emitted-JSON mapping
// (mongo_event.hpp): the document image for each op type, the __op/__ns/__id
// metadata, reserved-key precedence, and skipping non-row events. No live MongoDB.

#include <string>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"

#ifdef CLINK_HAS_MONGODB
#include "clink/mongodb/mongo_event.hpp"
#endif

#ifdef CLINK_HAS_MONGODB

using clink::mongodb::change_event_to_json;

namespace {
clink::config::JsonValue ev_of(const std::string& s) {
    return clink::config::parse(s);
}
clink::config::JsonObject emitted(const std::string& change) {
    auto js = change_event_to_json(ev_of(change));
    EXPECT_TRUE(js.has_value());
    return clink::config::parse(*js).as_object();
}
}  // namespace

TEST(MongoEvent, InsertEmitsFullDocumentWithMetadata) {
    auto o = emitted(
        R"({"operationType":"insert","ns":{"db":"shop","coll":"orders"},
            "documentKey":{"_id":7},"fullDocument":{"_id":7,"item":"abc","qty":5}})");
    EXPECT_EQ(o.at("item").as_string(), "abc");
    EXPECT_EQ(o.at("qty").as_number(), 5);
    EXPECT_EQ(o.at("__op").as_string(), "insert");
    EXPECT_EQ(o.at("__ns").as_string(), "shop.orders");
    EXPECT_EQ(o.at("__id").as_number(), 7);
}

TEST(MongoEvent, UpdateWithFullDocumentUsesPostImage) {
    auto o = emitted(
        R"({"operationType":"update","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "updateDescription":{"updatedFields":{"qty":6},"removedFields":[]},
            "fullDocument":{"_id":1,"item":"abc","qty":6}})");
    EXPECT_EQ(o.at("__op").as_string(), "update");
    EXPECT_EQ(o.at("item").as_string(), "abc") << "updateLookup post-image is the full doc";
    EXPECT_EQ(o.at("qty").as_number(), 6);
}

TEST(MongoEvent, UpdateWithoutFullDocumentUsesUpdatedFields) {
    auto o = emitted(
        R"({"operationType":"update","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "updateDescription":{"updatedFields":{"qty":6},"removedFields":[]}})");
    EXPECT_EQ(o.at("__op").as_string(), "update");
    EXPECT_EQ(o.at("qty").as_number(), 6);
    EXPECT_EQ(o.find("item"), o.end()) << "no post-image: only the changed fields";
}

TEST(MongoEvent, ReplaceEmitsFullDocument) {
    auto o = emitted(
        R"({"operationType":"replace","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "fullDocument":{"_id":1,"item":"new"}})");
    EXPECT_EQ(o.at("__op").as_string(), "replace");
    EXPECT_EQ(o.at("item").as_string(), "new");
}

TEST(MongoEvent, DeleteEmitsDocumentKey) {
    auto o = emitted(
        R"({"operationType":"delete","ns":{"db":"d","coll":"c"},"documentKey":{"_id":42}})");
    EXPECT_EQ(o.at("__op").as_string(), "delete");
    EXPECT_EQ(o.at("__id").as_number(), 42);
    EXPECT_EQ(o.at("_id").as_number(), 42) << "delete carries the key as the body";
}

TEST(MongoEvent, NonRowEventsAreSkipped) {
    EXPECT_FALSE(
        change_event_to_json(ev_of(R"({"operationType":"drop","ns":{"db":"d","coll":"c"}})"))
            .has_value());
    EXPECT_FALSE(change_event_to_json(ev_of(R"({"operationType":"invalidate"})")).has_value());
    EXPECT_FALSE(change_event_to_json(ev_of(R"({"operationType":"rename"})")).has_value());
}

TEST(MongoEvent, MissingOrNonObjectReturnsNullopt) {
    EXPECT_FALSE(
        change_event_to_json(ev_of(R"({"ns":{"db":"d"}})")).has_value());  // no operationType
    EXPECT_FALSE(change_event_to_json(ev_of(R"("a string")")).has_value());
    EXPECT_FALSE(change_event_to_json(ev_of("42")).has_value());
}

TEST(MongoEvent, DataFieldNamedLikeMetadataWins) {
    // A document field literally named __op must keep its own value (no-overwrite
    // emplace), matching the Postgres/MySQL CDC reserved-key precedence.
    auto o = emitted(
        R"({"operationType":"insert","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "fullDocument":{"_id":1,"__op":"custom"}})");
    EXPECT_EQ(o.at("__op").as_string(), "custom") << "data field wins over the metadata";
}

#endif  // CLINK_HAS_MONGODB
