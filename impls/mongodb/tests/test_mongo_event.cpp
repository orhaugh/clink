// Offline tests for the pure MongoDB change-event -> emitted-JSON mapping
// (mongo_event.hpp): the document fields and __image for each operation and
// full_document mode, the __op/__ns/__id metadata, the refusal of a data field
// named like the metadata, the events that stop the source, and what a resume
// failure means. No live MongoDB.

#include <string>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"

#ifdef CLINK_HAS_MONGODB
#include "clink/mongodb/mongo_event.hpp"
#endif

#ifdef CLINK_HAS_MONGODB

using clink::mongodb::change_event_to_json;
using clink::mongodb::ChangeEventError;
using clink::mongodb::ChangeStreamStop;
using clink::mongodb::FullDocument;

namespace {
clink::config::JsonValue ev_of(const std::string& s) {
    return clink::config::parse(s);
}
clink::config::JsonObject emitted(const std::string& change,
                                  FullDocument mode = FullDocument::UpdateLookup) {
    return clink::config::parse(change_event_to_json(ev_of(change), mode)).as_object();
}
// The message an event that stops the source carries, or "" if it did not stop.
std::string stop_message(const std::string& change) {
    try {
        (void)change_event_to_json(ev_of(change), FullDocument::UpdateLookup);
    } catch (const ChangeStreamStop& e) {
        return e.what();
    }
    return "";
}
bool contains(const std::string& s, const std::string& needle) {
    return s.find(needle) != std::string::npos;
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
    EXPECT_EQ(o.at("__image").as_string(), "full");
}

// updateLookup reads the document when the server looks it up, after the event,
// so the image is marked as such: it can already carry later changes.
TEST(MongoEvent, AnUpdateReadWithUpdateLookupIsMarkedLookup) {
    auto o = emitted(
        R"({"operationType":"update","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "updateDescription":{"updatedFields":{"qty":6},"removedFields":[]},
            "fullDocument":{"_id":1,"item":"abc","qty":6}})",
        FullDocument::UpdateLookup);
    EXPECT_EQ(o.at("__op").as_string(), "update");
    EXPECT_EQ(o.at("item").as_string(), "abc");
    EXPECT_EQ(o.at("__image").as_string(), "lookup");
}

// A post-image (whenAvailable, required) is the document as this change left it.
TEST(MongoEvent, AnUpdateReadWithAPostImageIsMarkedFull) {
    for (const auto mode : {FullDocument::WhenAvailable, FullDocument::Required}) {
        auto o = emitted(
            R"({"operationType":"update","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
                "updateDescription":{"updatedFields":{"qty":6},"removedFields":[]},
                "fullDocument":{"_id":1,"item":"abc","qty":6}})",
            mode);
        EXPECT_EQ(o.at("__image").as_string(), "full");
    }
}

// An update with no document to read - full_document off, a lookup of a document
// deleted since (null), or whenAvailable without a post-image - carries only the
// fields it set, and says so.
TEST(MongoEvent, AnUpdateWithoutADocumentCarriesTheSetFieldsAndIsMarkedPartial) {
    const std::string without =
        R"({"operationType":"update","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "updateDescription":{"updatedFields":{"qty":6,"a.b":1},"removedFields":["x"]}})";
    const std::string deleted_since =
        R"({"operationType":"update","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "updateDescription":{"updatedFields":{"qty":6},"removedFields":[]},
            "fullDocument":null})";
    for (const auto& [change, mode] : {std::pair{without, FullDocument::Off},
                                       std::pair{deleted_since, FullDocument::UpdateLookup},
                                       std::pair{deleted_since, FullDocument::WhenAvailable}}) {
        auto o = emitted(change, mode);
        EXPECT_EQ(o.at("qty").as_number(), 6);
        EXPECT_EQ(o.find("item"), o.end());
        EXPECT_EQ(o.at("__image").as_string(), "partial");
        EXPECT_EQ(o.at("__id").as_number(), 1);
    }
    auto o = emitted(without, FullDocument::Off);
    EXPECT_EQ(o.at("a.b").as_number(), 1) << "a dotted path stays as the server names it";
    EXPECT_EQ(o.find("x"), o.end()) << "removed fields are not carried";
}

TEST(MongoEvent, ReplaceEmitsFullDocument) {
    auto o = emitted(
        R"({"operationType":"replace","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
            "fullDocument":{"_id":1,"item":"new"}})");
    EXPECT_EQ(o.at("__op").as_string(), "replace");
    EXPECT_EQ(o.at("item").as_string(), "new");
    EXPECT_EQ(o.at("__image").as_string(), "full");
}

TEST(MongoEvent, DeleteEmitsDocumentKey) {
    auto o = emitted(
        R"({"operationType":"delete","ns":{"db":"d","coll":"c"},"documentKey":{"_id":42}})");
    EXPECT_EQ(o.at("__op").as_string(), "delete");
    EXPECT_EQ(o.at("__id").as_number(), 42);
    EXPECT_EQ(o.at("_id").as_number(), 42) << "delete carries the key as the body";
    EXPECT_EQ(o.at("__image").as_string(), "key");
}

// Drop, rename, dropDatabase and invalidate remove or move documents without a
// delete for each, or end the stream: the source stops, naming the event and
// how to continue, rather than skipping it and going quiet.
TEST(MongoEvent, EventsThatAreNotRowChangesStopTheSource) {
    const auto drop = stop_message(R"({"operationType":"drop","ns":{"db":"d","coll":"c"}})");
    EXPECT_TRUE(contains(drop, "d.c was dropped")) << drop;
    EXPECT_TRUE(contains(drop, "without restoring this source's state")) << drop;
    const auto rename = stop_message(
        R"({"operationType":"rename","ns":{"db":"d","coll":"c"},"to":{"db":"d","coll":"c2"}})");
    EXPECT_TRUE(contains(rename, "d.c was renamed to d.c2")) << rename;
    const auto dropdb = stop_message(R"({"operationType":"dropDatabase","ns":{"db":"d"}})");
    EXPECT_TRUE(contains(dropdb, "database d was dropped")) << dropdb;
    const auto invalidate = stop_message(R"({"operationType":"invalidate"})");
    EXPECT_TRUE(contains(invalidate, "invalidated")) << invalidate;
    const auto unknown = stop_message(R"({"operationType":"modify","ns":{"db":"d","coll":"c"}})");
    EXPECT_TRUE(contains(unknown, "'modify'")) << unknown;
}

TEST(MongoEvent, AMalformedEventIsRefused) {
    EXPECT_THROW(change_event_to_json(ev_of(R"({"ns":{"db":"d"}})"), FullDocument::Off),
                 ChangeEventError);  // no operationType
    EXPECT_THROW(change_event_to_json(ev_of(R"("a string")"), FullDocument::Off), ChangeEventError);
    EXPECT_THROW(
        change_event_to_json(
            ev_of(
                R"({"operationType":"insert","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1}})"),
            FullDocument::Off),
        ChangeEventError)
        << "an insert without its document";
}

// A data field named like the metadata would pass for it; the source refuses
// the event instead, naming the field and the document.
TEST(MongoEvent, ADataFieldNamedLikeTheMetadataIsRefused) {
    for (const std::string field : {"__op", "__ns", "__id", "__image"}) {
        try {
            (void)change_event_to_json(
                ev_of(
                    R"({"operationType":"insert","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
                          "fullDocument":{"_id":1,")" +
                    field + R"(":"custom"}})"),
                FullDocument::UpdateLookup);
            ADD_FAILURE() << field << " passed for change metadata";
        } catch (const ChangeEventError& e) {
            EXPECT_TRUE(contains(e.what(), "'" + field + "'")) << e.what();
            EXPECT_TRUE(contains(e.what(), "d.c (_id 1)")) << e.what();
        }
    }
    EXPECT_THROW(
        change_event_to_json(
            ev_of(R"({"operationType":"update","ns":{"db":"d","coll":"c"},"documentKey":{"_id":1},
                      "updateDescription":{"updatedFields":{"__op":"x"},"removedFields":[]}})"),
            FullDocument::Off),
        ChangeEventError)
        << "a partial update setting a field named like the metadata";
}

TEST(MongoEvent, AResumeFailureSaysWhatWasLost) {
    EXPECT_TRUE(contains(clink::mongodb::resume_failure(286).value_or(""), "oplog no longer"));
    EXPECT_TRUE(contains(clink::mongodb::resume_failure(260).value_or(""), "resume token"));
    EXPECT_TRUE(clink::mongodb::resume_failure(280).has_value());
    EXPECT_FALSE(clink::mongodb::resume_failure(11000).has_value());
}

#endif  // CLINK_HAS_MONGODB
