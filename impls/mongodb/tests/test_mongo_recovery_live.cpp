// The MongoDB source's and sink's recovery contract against a real replica set.
// SKIPPED unless CLINK_MONGODB_TEST_URI points at one started with
// enableTestCommands (scripts/mongodb-live.sh starts it; the sink's failure
// cases arm the server's failCommand fail point).
//
// MongoSourceRecoveryLive: a source checkpoints its resume token, is destroyed
// without close() as a crash would, and a second source restored from that
// checkpoint must deliver exactly the events after it - from the per-pass cap,
// an idle pass and a checkpoint taken before the first pass. A token the server
// cannot resume from fails the restore instead of starting at the current time;
// drop, rename and a field named like the metadata stop the source; and
// updateLookup and post-images carry what the connector page says they do.
//
// MongoSinkLive: what the sink refuses before writing (a replacement without
// its key, a key other than _id without a unique index, a write concern below
// majority), and that a write concern error, a dropped connection and a batch
// stopped part way fail the flush, while the replay of the batch converges.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_MONGODB
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/options/index.hpp>

#include "clink/mongodb/mongo_cdc_source.hpp"
#include "clink/mongodb/mongo_client.hpp"
#include "clink/mongodb/mongo_sink.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;
using clink::Batch;
using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::Source;
using clink::StreamElement;
using clink::mongodb::FullDocument;
using clink::mongodb::make_mongo_cdc_source;
using clink::mongodb::make_mongo_sink;
using clink::mongodb::MongoCdcOptions;
using clink::mongodb::MongoSinkOptions;

namespace {

bool recovery_configured() {
    return std::getenv("CLINK_MONGODB_TEST_URI") != nullptr;
}
std::string recovery_uri() {
    return std::getenv("CLINK_MONGODB_TEST_URI");
}

std::string fresh_collection() {
    static int n = 0;
    return "clink_rec_" + std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

// Every record a source emits, parsed.
struct Rows {
    std::vector<clink::config::JsonObject> rows;
    std::vector<int> ids() const {
        std::vector<int> out;
        for (const auto& r : rows) {
            const auto it = r.find("__id");
            out.push_back(it != r.end() && it->second.is_number()
                              ? static_cast<int>(it->second.as_number())
                              : -1);
        }
        return out;
    }
};

void one_pass(Source<std::string>& src, Rows& out) {
    Emitter<std::string> em{[&out](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.rows.push_back(clink::config::parse(r.value()).as_object());
            }
        }
        return true;
    }};
    src.produce(em);
}

void read_until(Source<std::string>& src, Rows& out, std::size_t want, int timeout_ms = 20000) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (out.rows.size() < want && std::chrono::steady_clock::now() < until) {
        one_pass(src, out);
    }
}

// Passes for `ms`, whatever arrives.
void read_for(Source<std::string>& src, Rows& out, int ms) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds{ms};
    while (std::chrono::steady_clock::now() < until) {
        one_pass(src, out);
    }
}

MongoCdcOptions watch(const std::string& coll, FullDocument mode = FullDocument::UpdateLookup) {
    MongoCdcOptions o;
    o.uri = recovery_uri();
    o.database = "test";
    o.collection = coll;
    o.full_document = mode;
    o.max_await = std::chrono::milliseconds{200};
    return o;
}

std::vector<int> range(int from, int to) {
    std::vector<int> out;
    for (int i = from; i <= to; ++i) {
        out.push_back(i);
    }
    return out;
}

std::string what_produce_throws(Source<std::string>& src, int timeout_ms = 20000) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    Rows ignored;
    try {
        while (std::chrono::steady_clock::now() < until) {
            one_pass(src, ignored);
        }
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

bool has(const std::string& text, const std::string& needle) {
    return text.find(needle) != std::string::npos;
}

const OperatorId kOp{1};

}  // namespace

// The pass stops at its cap of 1024 events. The checkpoint taken there must
// cover exactly those: the previous source read one event past the cap into
// the driver and checkpointed a token covering it, so the restore skipped it.
TEST(MongoSourceRecoveryLive, ACheckpointAtThePerPassCapCoversOnlyWhatWasEmitted) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    InMemoryStateBackend backend;
    constexpr int kN = 1100;
    {
        auto src = make_mongo_cdc_source(watch(coll));
        src->open();
        std::vector<bsoncxx::document::value> docs;
        std::vector<bsoncxx::document::view> views;
        for (int i = 1; i <= kN; ++i) {
            docs.push_back(make_document(kvp("_id", i)));
        }
        for (const auto& d : docs) {
            views.push_back(d.view());
        }
        c.insert_many(views);
        Rows first;
        read_until(*src, first, 1);  // the first pass that finds the burst
        ASSERT_EQ(first.rows.size(), 1024U) << "the pass did not stop at its cap";
        EXPECT_EQ(first.ids(), range(1, 1024));
        src->snapshot_offset(backend, kOp, CheckpointId{1});
        // destroyed without close(): a crash
    }
    auto again = make_mongo_cdc_source(watch(coll));
    ASSERT_TRUE(again->restore_offset(backend, kOp));
    again->open();
    Rows rest;
    read_until(*again, rest, kN - 1024);
    read_for(*again, rest, 1000);
    EXPECT_EQ(rest.ids(), range(1025, kN))
        << "the restore did not resume exactly after the last event the checkpoint covered";
    again->close();
    c.drop();
}

// An idle stream's position moves on, and a checkpoint taken after an idle pass
// resumes exactly after it: the events since, and none before.
TEST(MongoSourceRecoveryLive, ACheckpointAfterAnIdlePassResumesExactlyAfterIt) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    InMemoryStateBackend backend;
    {
        auto src = make_mongo_cdc_source(watch(coll));
        src->open();
        for (int i = 1; i <= 3; ++i) {
            c.insert_one(make_document(kvp("_id", i)));
        }
        Rows seen;
        read_until(*src, seen, 3);
        ASSERT_EQ(seen.ids(), range(1, 3));
        read_for(*src, seen, 600);  // idle passes
        ASSERT_EQ(seen.rows.size(), 3U);
        src->snapshot_offset(backend, kOp, CheckpointId{1});
    }
    for (int i = 4; i <= 6; ++i) {
        c.insert_one(make_document(kvp("_id", i)));
    }
    auto again = make_mongo_cdc_source(watch(coll));
    ASSERT_TRUE(again->restore_offset(backend, kOp));
    again->open();
    Rows rest;
    read_until(*again, rest, 3);
    read_for(*again, rest, 1000);
    EXPECT_EQ(rest.ids(), range(4, 6));
    again->close();
    c.drop();
}

// A checkpoint can come before the source's first pass. It must still hold a
// token, or a restore from it would start at the current time and skip the
// changes since; the source takes one when it opens.
TEST(MongoSourceRecoveryLive, ACheckpointBeforeTheFirstPassHoldsAToken) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    InMemoryStateBackend backend;
    {
        auto src = make_mongo_cdc_source(watch(coll));
        src->open();
        src->snapshot_offset(backend, kOp, CheckpointId{1});
        c.insert_one(make_document(kvp("_id", 1)));
        c.insert_one(make_document(kvp("_id", 2)));
    }
    auto again = make_mongo_cdc_source(watch(coll));
    ASSERT_TRUE(again->restore_offset(backend, kOp)) << "the checkpoint holds no token";
    again->open();
    Rows rest;
    read_until(*again, rest, 2);
    EXPECT_EQ(rest.ids(), range(1, 2));
    again->close();
    c.drop();
}

// A token the server cannot resume from fails the restore, and the source never
// starts at the current time instead. The token is a real one with its cluster
// time moved before the oplog's first entry. On a replica set whose oplog has
// never rolled over, MongoDB 7 reports that as a token it cannot find (code 280)
// rather than as lost history (286); both fail the same way, and the wording
// for 286 is MongoEvent.AResumeFailureSaysWhatWasLost's.
TEST(MongoSourceRecoveryLive, ATokenTheServerCannotResumeFromFailsTheRestore) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    InMemoryStateBackend backend;
    {
        auto src = make_mongo_cdc_source(watch(coll));
        src->open();
        c.insert_one(make_document(kvp("_id", 1)));
        Rows seen;
        read_until(*src, seen, 1);
        src->snapshot_offset(backend, kOp, CheckpointId{1});
    }
    std::string token;
    backend.scan_operator_state(
        kOp, [&](auto, auto value) { token.assign(value.data(), value.size()); });
    // A resume token's _data is a KeyString whose first field is the cluster
    // time: type byte 82, then the timestamp's seconds and increment.
    const auto at = token.find("\"82");
    ASSERT_NE(at, std::string::npos) << "an unexpected token layout: " << token;
    token.replace(at + 3, 16, "0000000100000001");
    InMemoryStateBackend stale;
    stale.put_operator_state(
        kOp, "__mongo_cdc_token__", clink::StateBackend::ValueView{token.data(), token.size()});
    auto again = make_mongo_cdc_source(watch(coll));
    ASSERT_TRUE(again->restore_offset(stale, kOp));
    std::string failure;
    try {
        again->open();
        failure = what_produce_throws(*again);
    } catch (const std::exception& e) {
        failure = e.what();
    }
    EXPECT_TRUE(has(failure, "cannot resume the change stream")) << failure;
    EXPECT_TRUE(has(failure, "does not fall back to the current time")) << failure;
    c.drop();
}

// While changes keep arriving the stream always has one more, so a pass bound
// only by its cap of 1,024 events read until the cap filled, holding back the
// checkpoint barriers that go in between passes. At 400 changes a second that
// was two and a half seconds a pass; a trickle stretched it further. A pass now
// ends once max_await (here 200 ms) has passed.
TEST(MongoSourceRecoveryLive, APassEndsWithinItsBudgetWhileChangesKeepArriving) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto src = make_mongo_cdc_source(watch(coll));
    src->open();
    std::atomic<bool> writing{true};
    std::thread writer([&] {
        auto own = clink::mongodb::make_client(recovery_uri());
        auto c = own["test"][coll];
        for (int i = 1; writing.load() && i <= 4000; ++i) {
            c.insert_one(make_document(kvp("_id", i)));
            if (i % 20 == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        }
    });
    std::size_t passes_with_events = 0;
    std::chrono::milliseconds longest{0};
    std::size_t largest = 0;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{4};
    while (std::chrono::steady_clock::now() < until) {
        Rows got;
        const auto t0 = std::chrono::steady_clock::now();
        one_pass(*src, got);
        const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0);
        if (!got.rows.empty()) {
            ++passes_with_events;
            longest = std::max(longest, took);
            largest = std::max(largest, got.rows.size());
        }
    }
    writing.store(false);
    writer.join();
    src->close();
    EXPECT_GE(passes_with_events, 8U) << "too few passes handed changes over";
    EXPECT_LT(longest.count(), 1000)
        << "a pass held back the barriers for " << longest.count() << " ms";
    EXPECT_LT(largest, 1024U) << "a pass ran to its cap while changes kept arriving";
    auto client = clink::mongodb::make_client(recovery_uri());
    client["test"][coll].drop();
}

// A dropped collection's documents vanish without a delete for each, and the
// stream ends; the source stops, naming the drop, rather than going quiet.
TEST(MongoSourceRecoveryLive, ADroppedCollectionStopsTheSource) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    auto src = make_mongo_cdc_source(watch(coll));
    src->open();
    c.insert_one(make_document(kvp("_id", 1)));
    Rows seen;
    read_until(*src, seen, 1);
    ASSERT_EQ(seen.rows.size(), 1U);
    c.drop();
    const auto failure = what_produce_throws(*src);
    EXPECT_TRUE(has(failure, "test." + coll + " was dropped")) << failure;
}

TEST(MongoSourceRecoveryLive, ARenamedCollectionStopsTheSource) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    auto src = make_mongo_cdc_source(watch(coll));
    src->open();
    c.insert_one(make_document(kvp("_id", 1)));
    Rows seen;
    read_until(*src, seen, 1);
    c.rename(coll + "_moved");
    const auto failure = what_produce_throws(*src);
    EXPECT_TRUE(has(failure, "was renamed to test." + coll + "_moved")) << failure;
    client["test"][coll + "_moved"].drop();
}

// updateLookup reads an update's document when the server looks it up, after
// the event: a later change is already in it, and a document deleted since
// leaves only the fields the update set.
TEST(MongoSourceRecoveryLive, UpdateLookupReadsTheDocumentAfterTheEvent) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    auto src = make_mongo_cdc_source(watch(coll));
    src->open();
    c.insert_one(make_document(kvp("_id", 1), kvp("v", 0), kvp("note", "kept")));
    c.update_one(make_document(kvp("_id", 1)),
                 make_document(kvp("$set", make_document(kvp("v", 1)))));
    c.update_one(make_document(kvp("_id", 1)),
                 make_document(kvp("$set", make_document(kvp("v", 2)))));
    c.insert_one(make_document(kvp("_id", 2), kvp("v", 0), kvp("note", "gone")));
    c.update_one(make_document(kvp("_id", 2)),
                 make_document(kvp("$set", make_document(kvp("v", 1)))));
    c.delete_one(make_document(kvp("_id", 2)));
    Rows seen;
    read_until(*src, seen, 6);
    ASSERT_EQ(seen.rows.size(), 6U);
    const auto& first_update = seen.rows[1];
    EXPECT_EQ(first_update.at("__image").as_string(), "lookup");
    EXPECT_EQ(first_update.at("v").as_number(), 2)
        << "the first update's image does not show the later change";
    const auto& deleted_since = seen.rows[4];
    EXPECT_EQ(deleted_since.at("__op").as_string(), "update");
    EXPECT_EQ(deleted_since.at("__image").as_string(), "partial");
    EXPECT_EQ(deleted_since.at("v").as_number(), 1);
    EXPECT_EQ(deleted_since.find("note"), deleted_since.end());
    EXPECT_EQ(seen.rows[5].at("__image").as_string(), "key");
    src->close();
    c.drop();
}

// A post-image is the document as each change left it. full_document='required'
// fails the stream when the collection keeps no post-images.
TEST(MongoSourceRecoveryLive, ARequiredPostImageIsEachChangesOwnAndItsAbsenceFails) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto db = client["test"];
    db.run_command(
        make_document(kvp("create", coll),
                      kvp("changeStreamPreAndPostImages", make_document(kvp("enabled", true)))));
    auto c = db[coll];
    auto src = make_mongo_cdc_source(watch(coll, FullDocument::Required));
    src->open();
    c.insert_one(make_document(kvp("_id", 1), kvp("v", 0)));
    c.update_one(make_document(kvp("_id", 1)),
                 make_document(kvp("$set", make_document(kvp("v", 1)))));
    c.update_one(make_document(kvp("_id", 1)),
                 make_document(kvp("$set", make_document(kvp("v", 2)))));
    Rows seen;
    read_until(*src, seen, 3);
    ASSERT_EQ(seen.rows.size(), 3U);
    EXPECT_EQ(seen.rows[1].at("__image").as_string(), "full");
    EXPECT_EQ(seen.rows[1].at("v").as_number(), 1) << "the post-image of the first update";
    EXPECT_EQ(seen.rows[2].at("v").as_number(), 2);
    src->close();
    c.drop();

    const auto plain = fresh_collection();
    auto p = db[plain];
    auto required = make_mongo_cdc_source(watch(plain, FullDocument::Required));
    required->open();
    p.insert_one(make_document(kvp("_id", 1), kvp("v", 0)));
    p.update_one(make_document(kvp("_id", 1)),
                 make_document(kvp("$set", make_document(kvp("v", 1)))));
    const auto failure = what_produce_throws(*required);
    EXPECT_TRUE(has(failure, "change stream error")) << failure;
    p.drop();
}

TEST(MongoSourceRecoveryLive, ADataFieldNamedLikeTheMetadataStopsTheSource) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    auto src = make_mongo_cdc_source(watch(coll));
    src->open();
    c.insert_one(make_document(kvp("_id", 1), kvp("__op", "mine")));
    const auto failure = what_produce_throws(*src);
    EXPECT_TRUE(has(failure, "top-level field '__op'")) << failure;
    c.drop();
}

namespace {

MongoSinkOptions sink_into(const std::string& coll, bool upsert, std::string uri = {}) {
    MongoSinkOptions o;
    o.uri = uri.empty() ? recovery_uri() : uri;
    o.database = "test";
    o.collection = coll;
    o.upsert = upsert;
    return o;
}

// Writes `records` through a fresh sink and returns why the flush failed, or "".
std::string write_batch(const MongoSinkOptions& o, const std::vector<std::string>& records) {
    auto sink = make_mongo_sink(o);
    try {
        sink->open();
        Batch<std::string> b;
        for (const auto& r : records) {
            b.emplace(std::string{r});
        }
        sink->on_data(b);
        sink->flush();
        sink->close();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

std::string with_param(const std::string& uri, const std::string& param) {
    return uri + (uri.find('?') == std::string::npos ? "/?" : "&") + param;
}

// Arms the server's failCommand fail point for the next `times` commands named.
void fail_next(mongocxx::client& client,
               const std::string& command,
               bsoncxx::document::value data_extra,
               int times = 1) {
    bsoncxx::builder::basic::document data;
    data.append(kvp("failCommands", make_array(command)));
    for (const auto& e : data_extra.view()) {
        data.append(kvp(std::string(e.key()), e.get_value()));
    }
    client["admin"].run_command(make_document(kvp("configureFailPoint", "failCommand"),
                                              kvp("mode", make_document(kvp("times", times))),
                                              kvp("data", data.extract())));
}

void clear_fail_point(mongocxx::client& client) {
    client["admin"].run_command(
        make_document(kvp("configureFailPoint", "failCommand"), kvp("mode", "off")));
}

}  // namespace

// Every record of a replacement batch is checked before anything is written: a
// record without its key would otherwise be inserted beside the document it
// should have replaced.
TEST(MongoSinkLive, AReplacementWithoutItsKeyWritesNothingOfTheBatch) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    const auto missing =
        write_batch(sink_into(coll, true), {R"({"_id":1,"v":"a"})", R"({"v":"no key"})"});
    EXPECT_TRUE(has(missing, "record 2 of 2 cannot replace by key")) << missing;
    EXPECT_TRUE(has(missing, "nothing of the batch was written")) << missing;
    const auto floating = write_batch(sink_into(coll, true), {R"({"_id":1.5,"v":"a"})"});
    EXPECT_TRUE(has(floating, "is a double")) << floating;
    EXPECT_EQ(c.count_documents(make_document()), 0);
    c.drop();
}

// A key other than _id matches at most one document only under a unique index
// on it alone; without one the sink refuses to open.
TEST(MongoSinkLive, AKeyOtherThanIdNeedsAUniqueIndexOnItAlone) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    auto keyed = sink_into(coll, true);
    keyed.key_field = "order_id";
    const auto absent = write_batch(keyed, {R"({"order_id":"a","v":1})"});
    EXPECT_TRUE(has(absent, "needs a unique index on that field alone")) << absent;
    c.create_index(make_document(kvp("order_id", 1)));  // not unique
    const auto plain = write_batch(keyed, {R"({"order_id":"a","v":1})"});
    EXPECT_TRUE(has(plain, "needs a unique index on that field alone")) << plain;
    mongocxx::options::index unique;
    unique.unique(true);
    unique.name("order_id_unique");
    c.indexes().drop_all();
    c.create_index(make_document(kvp("order_id", 1)), unique);
    EXPECT_EQ(write_batch(keyed, {R"({"order_id":"a","v":1})", R"({"order_id":"a","v":2})"}), "");
    EXPECT_EQ(c.count_documents(make_document()), 1);
    EXPECT_EQ(c.find_one(make_document(kvp("order_id", "a")))->view()["v"].get_int32().value, 2);
    c.drop();
}

// Only a majority-acknowledged write survives a failover; a URI asking for
// less is refused when the sink opens, before anything is written.
TEST(MongoSinkLive, AWriteConcernBelowMajorityIsRefused) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    for (const auto* w : {"w=0", "w=1", "w=2"}) {
        const auto refused =
            write_batch(sink_into(coll, false, with_param(recovery_uri(), w)), {R"({"_id":1})"});
        EXPECT_TRUE(has(refused, std::string("asks for ") + w)) << refused;
    }
    EXPECT_EQ(c.count_documents(make_document()), 0);
    EXPECT_EQ(write_batch(sink_into(coll, false, with_param(recovery_uri(), "w=majority")),
                          {R"({"_id":1})"}),
              "");
    c.drop();
}

// The server applies the batch and reports that the write concern was not met:
// the flush fails, so the checkpoint does not complete, and the replay of the
// batch converges on one document per record.
TEST(MongoSinkLive, AWriteConcernErrorFailsTheFlushAndTheReplayConverges) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    const std::vector<std::string> batch = {R"({"_id":1,"v":"a"})", R"({"_id":2,"v":"b"})"};
    for (const bool upsert : {false, true}) {
        c.drop();
        fail_next(
            client,
            upsert ? "update" : "insert",
            make_document(kvp("writeConcernError",
                              make_document(kvp("code", 64),
                                            kvp("errmsg", "waiting for replication timed out")))));
        const auto failed = write_batch(sink_into(coll, upsert), batch);
        clear_fail_point(client);
        EXPECT_TRUE(has(failed, "bulk write failed"))
            << (upsert ? "replace: " : "insert: ") << failed;
        EXPECT_EQ(write_batch(sink_into(coll, upsert), batch), "") << "the replay failed";
        EXPECT_EQ(c.count_documents(make_document()), 2);
    }
    c.drop();
}

// A connection dropped before the reply: with retryable writes (the URI's
// default) the driver sends the batch again under the same transaction number,
// which the server applies once; without them the flush fails and the replay
// converges.
TEST(MongoSinkLive, ADroppedConnectionIsRetriedOrFailsTheFlush) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    const std::vector<std::string> batch = {R"({"_id":1})", R"({"_id":2})", R"({"_id":3})"};
    fail_next(client, "insert", make_document(kvp("closeConnection", true)));
    EXPECT_EQ(write_batch(sink_into(coll, false), batch), "");
    clear_fail_point(client);
    EXPECT_EQ(c.count_documents(make_document()), 3);
    c.drop();

    const auto no_retry = with_param(recovery_uri(), "retryWrites=false");
    fail_next(client, "insert", make_document(kvp("closeConnection", true)));
    const auto failed = write_batch(sink_into(coll, false, no_retry), batch);
    clear_fail_point(client);
    EXPECT_TRUE(has(failed, "bulk write failed")) << failed;
    EXPECT_EQ(write_batch(sink_into(coll, false, no_retry), batch), "") << "the replay failed";
    EXPECT_EQ(c.count_documents(make_document()), 3);
    c.drop();
}

// An ordered replacement batch stops at its first failing record. What came
// before it is written, what came after is not, and the flush fails, so the
// checkpoint does not count any of it as written.
TEST(MongoSinkLive, AReplacementBatchStoppedPartWayFailsTheFlush) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    mongocxx::options::index unique;
    unique.unique(true);
    c.create_index(make_document(kvp("email", 1)), unique);
    const auto failed = write_batch(
        sink_into(coll, true),
        {R"({"_id":1,"email":"a"})", R"({"_id":2,"email":"a"})", R"({"_id":3,"email":"c"})"});
    EXPECT_TRUE(has(failed, "bulk write failed")) << failed;
    EXPECT_TRUE(c.find_one(make_document(kvp("_id", 1))).has_value());
    EXPECT_FALSE(c.find_one(make_document(kvp("_id", 3))).has_value())
        << "an ordered batch went on past its failure";
    c.drop();
}

// Writes to one key keep their order, within a batch and across flushes.
TEST(MongoSinkLive, RepeatedWritesToOneKeyKeepTheirOrder) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    auto o = sink_into(coll, true);
    o.batch_records = 2;  // the five records span three flushes
    EXPECT_EQ(write_batch(o,
                          {R"({"_id":"k","v":1})",
                           R"({"_id":"k","v":2})",
                           R"({"_id":"k","v":3})",
                           R"({"_id":"k","v":4})",
                           R"({"_id":"k","v":5})"}),
              "");
    EXPECT_EQ(c.count_documents(make_document()), 1);
    EXPECT_EQ(c.find_one(make_document(kvp("_id", "k")))->view()["v"].get_int32().value, 5);
    c.drop();
}

// What the sink buffers is bounded by bytes as well as records: past max_bytes
// it writes without waiting for a barrier.
TEST(MongoSinkLive, TheByteBoundWritesWithoutABarrier) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    auto client = clink::mongodb::make_client(recovery_uri());
    auto c = client["test"][coll];
    auto o = sink_into(coll, false);
    o.max_bytes = 200;
    auto sink = make_mongo_sink(o);
    sink->open();
    Batch<std::string> b;
    for (int i = 1; i <= 10; ++i) {
        b.emplace(R"({"_id":)" + std::to_string(i) +
                  R"(,"pad":"0123456789012345678901234567890"})");
    }
    sink->on_data(b);
    EXPECT_GT(c.count_documents(make_document()), 0) << "nothing was written past max_bytes";
    sink->flush();
    sink->close();
    EXPECT_EQ(c.count_documents(make_document()), 10);
    c.drop();
}

// Credentials travel only in the URI; an error about the connection does not
// repeat them.
TEST(MongoSinkLive, AConnectionErrorDoesNotRepeatTheUrisPassword) {
    if (!recovery_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const auto coll = fresh_collection();
    const auto bad_auth = write_batch(
        sink_into(
            coll, false, "mongodb://clink:s3cret-pw@127.0.0.1:1/?serverSelectionTimeoutMS=300"),
        {R"({"_id":1})"});
    EXPECT_FALSE(bad_auth.empty());
    EXPECT_FALSE(has(bad_auth, "s3cret-pw")) << bad_auth;
    const auto malformed = write_batch(sink_into(coll, false, "mongodb://clink:s3cret-pw@"), {});
    EXPECT_FALSE(has(malformed, "s3cret-pw")) << malformed;
}

#endif  // CLINK_HAS_MONGODB
