// MongoDB LIVE integration test. SKIPPED unless CLINK_MONGODB_TEST_URI points at a
// REPLICA SET (change streams require one, e.g. mongo:7 --replSet rs0 + rs.initiate).
// Proves against a real MongoDB: change-stream insert/update/delete produce the
// matching __op events; a resume-token checkpoint resumes without gap; and the sink
// writes documents that the change stream then observes.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_MONGODB
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>

#include "clink/mongodb/mongo_cdc_source.hpp"
#include "clink/mongodb/mongo_client.hpp"
#include "clink/mongodb/mongo_sink.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using clink::Batch;
using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::Source;
using clink::StreamElement;
using clink::mongodb::make_mongo_cdc_source;
using clink::mongodb::make_mongo_sink;
using clink::mongodb::MongoCdcOptions;
using clink::mongodb::MongoSinkOptions;

namespace {

bool mongo_configured() {
    return std::getenv("CLINK_MONGODB_TEST_URI") != nullptr;
}
std::string uri() {
    return std::getenv("CLINK_MONGODB_TEST_URI");
}

std::string unique_coll() {
    static int n = 0;
    return "clink_cdc_" + std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

struct Change {
    std::string op;
    clink::config::JsonValue row;
};
struct Captured {
    std::vector<Change> changes;
};
Emitter<std::string> capturing(Captured& cap) {
    return Emitter<std::string>{[&cap](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                auto j = clink::config::parse(r.value());
                if (!j.is_object()) {
                    continue;
                }
                Change c;
                if (auto it = j.as_object().find("__op");
                    it != j.as_object().end() && it->second.is_string()) {
                    c.op = it->second.as_string();
                }
                c.row = j;
                cap.changes.push_back(std::move(c));
            }
        }
        return true;
    }};
}

void drain(Source<std::string>& src, Captured& cap, std::size_t want, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (cap.changes.size() < want && std::chrono::steady_clock::now() < deadline) {
        auto em = capturing(cap);
        src.produce(em);
    }
}

MongoCdcOptions cdc_opts(const std::string& coll) {
    MongoCdcOptions o;
    o.uri = uri();
    o.database = "test";
    o.collection = coll;
    o.max_await = std::chrono::milliseconds{200};
    return o;
}

int id_of(const clink::config::JsonValue& row) {
    if (row.is_object()) {
        if (auto it = row.as_object().find("_id");
            it != row.as_object().end() && it->second.is_number()) {
            return static_cast<int>(it->second.as_number());
        }
    }
    return -1;
}

}  // namespace

TEST(MongoLive, ChangeStreamInsertUpdateDelete) {
    if (!mongo_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const std::string coll = unique_coll();
    auto client = clink::mongodb::make_client(uri());
    auto c = client["test"][coll];
    c.drop();

    auto src = make_mongo_cdc_source(cdc_opts(coll));
    src->open();  // establishes the change stream; changes below are captured
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    c.insert_one(make_document(kvp("_id", 1), kvp("v", "a")));
    c.insert_one(make_document(kvp("_id", 2), kvp("v", "b")));
    c.update_one(make_document(kvp("_id", 1)),
                 make_document(kvp("$set", make_document(kvp("v", "a2")))));
    c.delete_one(make_document(kvp("_id", 2)));

    Captured cap;
    drain(*src, cap, 4, /*timeout_ms=*/20000);
    src->close();
    c.drop();

    ASSERT_EQ(cap.changes.size(), 4u) << "insert,insert,update,delete";
    EXPECT_EQ(cap.changes[0].op, "insert");
    EXPECT_EQ(cap.changes[1].op, "insert");
    EXPECT_EQ(cap.changes[2].op, "update");
    EXPECT_EQ(cap.changes[3].op, "delete");
    EXPECT_EQ(cap.changes[2].row.as_object().at("v").as_string(), "a2")
        << "update carries the full post-image (updateLookup)";
    EXPECT_EQ(id_of(cap.changes[3].row), 2) << "delete carries the key";
}

TEST(MongoLive, ResumeTokenResumesWithoutGap) {
    if (!mongo_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const std::string coll = unique_coll();
    auto client = clink::mongodb::make_client(uri());
    auto c = client["test"][coll];
    c.drop();

    InMemoryStateBackend backend;
    const OperatorId op{1};

    auto src1 = make_mongo_cdc_source(cdc_opts(coll));
    src1->open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    for (int i = 1; i <= 3; ++i) {
        c.insert_one(make_document(kvp("_id", i), kvp("v", "x")));
    }
    Captured cap1;
    drain(*src1, cap1, 3, /*timeout_ms=*/20000);
    ASSERT_EQ(cap1.changes.size(), 3u);
    src1->snapshot_offset(backend, op, CheckpointId{1});  // persist the resume token
    src1->close();

    for (int i = 4; i <= 6; ++i) {  // changes while detached
        c.insert_one(make_document(kvp("_id", i), kvp("v", "y")));
    }

    auto src2 = make_mongo_cdc_source(cdc_opts(coll));
    ASSERT_TRUE(src2->restore_offset(backend, op)) << "resume token must restore";
    src2->open();
    Captured cap2;
    drain(*src2, cap2, 3, /*timeout_ms=*/20000);
    src2->close();
    c.drop();

    std::set<int> ids2;
    for (const auto& ch : cap2.changes) {
        ids2.insert(id_of(ch.row));
    }
    ASSERT_EQ(cap2.changes.size(), 3u) << "resume delivers exactly the post-checkpoint changes";
    EXPECT_EQ(ids2, (std::set<int>{4, 5, 6})) << "no gap, no replay of 1..3";
}

// Regression for the change-stream batch-cap boundary: a burst larger than the
// per-produce() cap (kMaxBatch=1024) must be delivered across multiple produce()
// calls with NO duplicate at the boundary (begin() does not advance the cursor).
TEST(MongoLive, LargeBurstAcrossBatchCapHasNoDuplicate) {
    if (!mongo_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const std::string coll = unique_coll();
    auto client = clink::mongodb::make_client(uri());
    auto c = client["test"][coll];
    c.drop();

    auto src = make_mongo_cdc_source(cdc_opts(coll));
    src->open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    constexpr int kN = 1100;  // > kMaxBatch (1024): one capped produce() + a continuation
    std::vector<bsoncxx::document::value> docs;
    std::vector<bsoncxx::document::view> views;
    docs.reserve(kN);
    views.reserve(kN);
    for (int i = 1; i <= kN; ++i) {
        docs.push_back(make_document(kvp("_id", i)));
        views.push_back(docs.back().view());
    }
    c.insert_many(views);

    Captured cap;
    drain(*src, cap, kN, /*timeout_ms=*/30000);
    src->close();
    c.drop();

    std::set<int> ids;
    for (const auto& ch : cap.changes) {
        ids.insert(id_of(ch.row));
    }
    EXPECT_EQ(ids.size(), static_cast<std::size_t>(kN)) << "every id delivered exactly once";
    EXPECT_EQ(cap.changes.size(), ids.size()) << "no duplicate at the batch-cap boundary";
}

TEST(MongoLive, SinkWritesDocuments) {
    if (!mongo_configured()) {
        GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI to a replica-set MongoDB";
    }
    const std::string coll = unique_coll();
    auto client = clink::mongodb::make_client(uri());
    auto c = client["test"][coll];
    c.drop();

    MongoSinkOptions o;
    o.uri = uri();
    o.database = "test";
    o.collection = coll;
    o.upsert = true;  // replace_one upsert by _id (idempotent)
    auto sink = make_mongo_sink(o);
    sink->open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"_id":1,"v":"a"})"});
    b.emplace(std::string{R"({"_id":2,"v":"b"})"});
    sink->on_data(b);
    sink->flush();
    // Re-write _id=1 with a new value: upsert must replace, not duplicate.
    Batch<std::string> b2;
    b2.emplace(std::string{R"({"_id":1,"v":"a2"})"});
    sink->on_data(b2);
    sink->flush();
    sink->close();

    EXPECT_EQ(c.count_documents(make_document()), 2) << "upsert replaced, did not duplicate _id=1";
    auto doc = c.find_one(make_document(kvp("_id", 1)));
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(std::string(doc->view()["v"].get_string().value), "a2");
    c.drop();
}

#endif  // CLINK_HAS_MONGODB
