// The MongoDB connector through the SQL frontend (connector='mongodb'): what the
// planner puts on the source and sink ops, the plans and options it refuses,
// and whole SQL pipelines run by the embedded engine against the replica set
// scripts/mongodb-live.sh starts (CLINK_MONGODB_TEST_URI), each checked by
// reading MongoDB through the driver rather than anything clink reports. The
// live cases skip without the URI; the gate requires them to run.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/types.hpp>
#include <gtest/gtest.h>
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/mongodb/install.hpp"
#include "clink/mongodb/mongo_client.hpp"
#include "clink/mongodb/mongo_sink.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/script_runner.hpp"

namespace {

using namespace std::chrono_literals;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

void ensure_installed() {
    static const bool done = [] {
        {
            clink::embed::EngineOptions opts;
            std::ostringstream sink;
            opts.out = &sink;
            opts.err = &sink;
            const clink::embed::EmbeddedEngine first{std::move(opts)};
        }
        clink::plugin::PluginRegistry reg;
        clink::mongodb::install(reg);
        return true;
    }();
    (void)done;
}

// The specs a script compiles to, or the script runner's diagnostics thrown.
std::vector<clink::cluster::JobGraphSpec> compile_script(const std::string& sql,
                                                         std::uint32_t parallelism = 1) {
    ensure_installed();
    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;
    opts.parallelism = parallelism;
    std::ostringstream out;
    std::ostringstream err;
    const clink::sql::ScriptIO io{&out, &err};
    std::vector<clink::cluster::JobGraphSpec> specs;
    const auto submit = [&specs](const clink::cluster::JobGraphSpec& spec, const std::string&) {
        specs.push_back(spec);
        return 0;
    };
    if (clink::sql::run_script(sql, catalog, opts, io, submit) != 0) {
        throw std::runtime_error(err.str());
    }
    return specs;
}

std::string refusal(const std::string& sql) {
    try {
        (void)compile_script(sql);
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

bool contains(const std::string& s, const std::string& needle) {
    return s.find(needle) != std::string::npos;
}

const clink::cluster::OperatorSpec* op_of(const clink::cluster::JobGraphSpec& spec,
                                          const std::string& type) {
    for (const auto& op : spec.ops) {
        if (op.type == type) {
            return &op;
        }
    }
    return nullptr;
}

std::string param(const clink::cluster::OperatorSpec& op, const std::string& key) {
    const auto it = op.params.find(key);
    return it == op.params.end() ? std::string{} : it->second;
}

const char* kSourceDdl =
    "CREATE TABLE changes (__op VARCHAR, __id VARCHAR, item VARCHAR, qty BIGINT, "
    "price DECIMAL(12, 2)) WITH (connector='mongodb', uri='mongodb://db.example:27017', "
    "database='shop', collection='orders'); ";

const char* kFileSinkDdl =
    "CREATE TABLE out (op VARCHAR, id VARCHAR, item VARCHAR, qty BIGINT, price DECIMAL(12, 2)) "
    "WITH (connector='file', format='json', path='/tmp/clink_mongodb_sql_plan_out'); ";

std::string sink_ddl(const std::string& columns, const std::string& extra = "") {
    return "CREATE TABLE dst " + columns +
           " WITH (connector='mongodb', uri='mongodb://db.example:27017', database='shop', "
           "collection='copy'" +
           extra + "); ";
}

}  // namespace

// --- The plans ---

// One reader, its declared columns passed for the strict conversion, and every
// operator behind it that is not keyed on that one subtask too.
TEST(MongoSqlPlan, ASourceTableIsOneTypedReaderWhoseEventsKeepTheirOrder) {
    const auto specs = compile_script(std::string(kSourceDdl) + kFileSinkDdl +
                                          "INSERT INTO out SELECT __op, __id, item, qty, price "
                                          "FROM changes WHERE qty > 0;",
                                      /*parallelism=*/4);
    ASSERT_EQ(specs.size(), 1U);
    const auto& spec = specs.front();
    const auto* src = op_of(spec, "mongo_cdc_source");
    ASSERT_NE(src, nullptr);
    EXPECT_EQ(src->parallelism, 1U);
    EXPECT_EQ(param(*src, std::string{clink::cluster::kForcedSingletonParam}), "true");
    EXPECT_EQ(param(*src, "declared_types"),
              "__op:VARCHAR;__id:VARCHAR;item:VARCHAR;qty:BIGINT;price:DECIMAL(12, 2)");
    EXPECT_EQ(param(*src, "uri"), "mongodb://db.example:27017");
    EXPECT_EQ(param(*src, "collection"), "orders");
    EXPECT_NE(op_of(spec, "json_string_to_row"), nullptr);
    for (const auto& op : spec.ops) {
        EXPECT_EQ(op.parallelism, 1U) << op.type << " fans out behind the single reader";
    }
}

TEST(MongoSqlPlan, AnAppendSinkInsertsAndAnUpsertSinkReplacesByItsPrimaryKey) {
    const auto append =
        compile_script(std::string(kSourceDdl) +
                       sink_ddl("(id VARCHAR, item VARCHAR, qty BIGINT, price DECIMAL(12, 2))") +
                       "INSERT INTO dst SELECT __id, item, qty, price FROM changes;");
    const auto* ins = op_of(append.front(), "mongo_sink");
    ASSERT_NE(ins, nullptr);
    EXPECT_EQ(param(*ins, "sql_column_types"),
              "id:VARCHAR;item:VARCHAR;qty:BIGINT;price:DECIMAL(12, 2)");
    EXPECT_EQ(param(*ins, "key_field"), "");
    const auto* bridge = op_of(append.front(), "row_to_json_string");
    ASSERT_NE(bridge, nullptr);
    // NaN and the infinities reach the sink as the doubles themselves.
    EXPECT_EQ(param(*bridge, "non_finite_form"), "tagged");

    const auto upsert = compile_script(
        std::string(kSourceDdl) +
        sink_ddl("(_id VARCHAR PRIMARY KEY, item VARCHAR, qty BIGINT)", ", mode='upsert'") +
        "INSERT INTO dst SELECT __id AS _id, item, qty FROM changes;");
    const auto* up = op_of(upsert.front(), "mongo_upsert_sink");
    ASSERT_NE(up, nullptr);
    EXPECT_EQ(param(*up, "key_field"), "_id");
    EXPECT_EQ(param(*up, "sql_column_types"), "_id:VARCHAR;item:VARCHAR;qty:BIGINT");
}

TEST(MongoSqlPlan, PlansTheConnectorCannotHonourAreRefused) {
    const std::string upsert_columns = "(a VARCHAR, b VARCHAR, qty BIGINT)";
    struct Case {
        const char* what;
        std::string sql;
        const char* expect;
    };
    const std::vector<Case> cases = {
        {"a composite key",
         std::string(kSourceDdl) + sink_ddl(upsert_columns, ", mode='upsert', primary_key='a,b'") +
             "INSERT INTO dst SELECT __id AS a, item AS b, qty FROM changes;",
         "single-column PRIMARY KEY"},
        {"exactly-once",
         std::string(kSourceDdl) +
             sink_ddl("(id VARCHAR, qty BIGINT)", ", delivery_guarantee='exactly_once'") +
             "INSERT INTO dst SELECT __id, qty FROM changes;",
         "exactly_once"},
        {"changelog='true'",
         std::string(kSourceDdl) + sink_ddl("(id VARCHAR, qty BIGINT)", ", changelog='true'") +
             "INSERT INTO dst SELECT __id, qty FROM changes;",
         "changelog='true'"},
        {"a misspelt option",
         std::string(kSourceDdl) + sink_ddl("(id VARCHAR, qty BIGINT)", ", colection='x'") +
             "INSERT INTO dst SELECT __id, qty FROM changes;",
         "does not read option 'colection'"},
        {"a source option on a sink",
         std::string(kSourceDdl) +
             sink_ddl("(id VARCHAR, qty BIGINT)", ", full_document='required'") +
             "INSERT INTO dst SELECT __id, qty FROM changes;",
         "it is a source option"},
        {"key_field in SQL",
         std::string(kSourceDdl) +
             sink_ddl("(id VARCHAR PRIMARY KEY, qty BIGINT)", ", mode='upsert', key_field='id'") +
             "INSERT INTO dst SELECT __id AS id, qty FROM changes;",
         "takes no 'key_field'"},
        {"json_non_finite",
         std::string(kSourceDdl) +
             sink_ddl("(id VARCHAR, qty BIGINT)", ", json_non_finite='null'") +
             "INSERT INTO dst SELECT __id, qty FROM changes;",
         "json_non_finite does not apply to connector='mongodb'"},
        {"a source without a uri",
         "CREATE TABLE changes (__id VARCHAR, qty BIGINT) WITH (connector='mongodb', "
         "database='shop', collection='orders'); " +
             std::string(kFileSinkDdl) +
             "INSERT INTO out SELECT __id, __id, __id, qty, 1.00 FROM changes;",
         "needs 'uri'"},
        {"a sink without a collection",
         std::string(kSourceDdl) +
             "CREATE TABLE dst (id VARCHAR) WITH (connector='mongodb', uri='mongodb://h:1', "
             "database='shop'); INSERT INTO dst SELECT __id FROM changes;",
         "needs 'collection'"},
        {"a source in upsert mode",
         "CREATE TABLE changes (__id VARCHAR PRIMARY KEY, qty BIGINT) WITH (connector='mongodb', "
         "uri='mongodb://h:1', database='shop', collection='orders', mode='upsert'); " +
             std::string(kFileSinkDdl) +
             "INSERT INTO out SELECT __id, __id, __id, qty, 1.00 FROM changes;",
         "appends one row per change event"},
        {"a sink option on a source",
         "CREATE TABLE changes (__id VARCHAR, qty BIGINT) WITH (connector='mongodb', "
         "uri='mongodb://h:1', database='shop', collection='orders', batch_records='5'); " +
             std::string(kFileSinkDdl) +
             "INSERT INTO out SELECT __id, __id, __id, qty, 1.00 FROM changes;",
         "it is a sink option"},
    };
    for (const auto& c : cases) {
        const auto m = refusal(c.sql);
        EXPECT_TRUE(contains(m, c.expect)) << c.what << ": " << (m.empty() ? "accepted" : m);
    }
}

// A retracting query reaches a MongoDB table only through mode='upsert', which
// applies its deletes by key.
TEST(MongoSqlPlan, AChangelogQueryNeedsTheUpsertSink) {
    const std::string topn =
        "INSERT INTO dst SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY g ORDER BY "
        "v DESC) AS rn FROM t) s WHERE rn = 1;";
    const std::string src =
        "CREATE TABLE t (g VARCHAR, k VARCHAR, v BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/clink_mongodb_sql_plan_in'); ";
    EXPECT_TRUE(contains(refusal(src + sink_ddl("(k VARCHAR, v BIGINT)") + topn), "append-only"));
    const auto specs = compile_script(
        src + sink_ddl("(k VARCHAR PRIMARY KEY, v BIGINT)", ", mode='upsert'") + topn);
    EXPECT_NE(op_of(specs.front(), "mongo_upsert_sink"), nullptr);
}

// What the coordinator's submission-time analyser reports for the two sinks:
// inserts at least once, replacements effectively once on their key with the
// key's caveat, and neither exactly once.
TEST(MongoSqlPlan, TheAnalyserCreditsTheUpsertSinkOnItsKeyAndNeverExactlyOnce) {
    clink::cluster::CheckpointConfig checkpoint;
    checkpoint.checkpoint_dir = "/tmp/clink_mongodb_sql_plan_ckpt";
    checkpoint.interval_ms = 1000;
    const auto report_for = [&](const std::string& sql) {
        clink::connectors::GuaranteeReport report;
        const auto refusal = clink::cluster::check_delivery_guarantee(
            compile_script(sql).front(), checkpoint, &report);
        EXPECT_EQ(refusal, "");
        return report;
    };
    const auto upsert =
        report_for(std::string(kSourceDdl) +
                   sink_ddl("(_id VARCHAR PRIMARY KEY, qty BIGINT)", ", mode='upsert'") +
                   "INSERT INTO dst SELECT __id AS _id, qty FROM changes;");
    EXPECT_EQ(upsert.level,
              clink::connectors::EndToEndGuarantee::EffectivelyOnceRequiresIdempotentKey);
    bool caveat = false;
    for (const auto& w : upsert.warnings) {
        caveat = caveat || contains(w, "mongo_upsert_sink");
    }
    EXPECT_TRUE(caveat) << "no warning about the key";

    const auto insert = report_for(std::string(kSourceDdl) + sink_ddl("(id VARCHAR, qty BIGINT)") +
                                   "INSERT INTO dst SELECT __id, qty FROM changes;");
    EXPECT_EQ(insert.level,
              clink::connectors::EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce);
}

// The key column's SQL type must name one document: a string or an integer.
TEST(MongoSqlPlan, AnUpsertKeyOfAnotherTypeIsRefusedWhenTheSinkIsBuilt) {
    clink::mongodb::MongoSinkOptions o;
    o.database = "shop";
    o.collection = "copy";
    o.upsert = true;
    o.key_field = "k";
    o.sql_column_types = "k:DOUBLE;v:BIGINT";
    try {
        (void)clink::mongodb::make_mongo_sink(o);
        ADD_FAILURE() << "a DOUBLE key was accepted";
    } catch (const std::exception& e) {
        EXPECT_TRUE(
            contains(e.what(), "a document key must be VARCHAR, BIGINT, INTEGER or SMALLINT"))
            << e.what();
    }
    o.sql_column_types = "v:BIGINT";
    EXPECT_THROW((void)clink::mongodb::make_mongo_sink(o), std::exception);
}

// --- Live, against the replica set ---

namespace {

std::string env_or_empty(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

// The connector runs in this process, so the verifier shares its one driver
// instance rather than creating a second, which the driver refuses.
mongocxx::client verifier_client(const std::string& uri) {
    return clink::mongodb::make_client(uri);
}

constexpr const char* kUriVar = "CLINK_MONGODB_SQL_TEST_URI";

class MongoSqlLive : public ::testing::Test {
protected:
    void SetUp() override {
        uri_ = env_or_empty("CLINK_MONGODB_TEST_URI");
        if (uri_.empty()) {
            GTEST_SKIP() << "set CLINK_MONGODB_TEST_URI (scripts/mongodb-live.sh does)";
        }
        ensure_installed();
        // The tables name the URI by reference, as a deployment keeps
        // credentials out of its SQL: the worker reads it from its environment.
        ::setenv(kUriVar, uri_.c_str(), 1);
        static int counter = 0;
        suffix_ = std::to_string(::getpid()) + "_" + std::to_string(++counter);
        src_ = "sqlsrc_" + suffix_;
        dst_ = "sqldst_" + suffix_;
        client_ = std::make_unique<mongocxx::client>(verifier_client(uri_));
    }

    void TearDown() override {
        if (engine_) {
            engine_->cancel_all();
            (void)engine_->await_all();
            engine_.reset();
        }
        if (client_) {
            (*client_)["test"][src_].drop();
            (*client_)["test"][dst_].drop();
        }
        if (!input_.empty()) {
            std::filesystem::remove(input_);
        }
    }

    // Starts `sql` on an embedded engine; the jobs keep running until stop().
    void start(const std::string& sql) {
        clink::embed::EngineOptions opts;
        opts.out = &out_;
        opts.err = &err_;
        engine_ = std::make_unique<clink::embed::EmbeddedEngine>(std::move(opts));
        ASSERT_EQ(engine_->execute_script(sql), 0) << err_.str();
    }

    // Waits for every job to end, cancelling them once `done` holds or after
    // `limit`. True when the jobs ended without an error (a cancel included).
    bool finish(const std::function<bool()>& done, std::chrono::seconds limit = 120s) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        return engine_->await_all(
            [&] { return done() || std::chrono::steady_clock::now() >= deadline; });
    }

    std::string errors() const {
        std::string all = err_.str();
        for (const auto& e : engine_->errors()) {
            all += "\n" + e;
        }
        return all;
    }

    std::int64_t count(const std::string& coll, bsoncxx::document::view_or_value filter = {}) {
        return (*client_)["test"][coll].count_documents(std::move(filter));
    }

    // A change stream delivers only what happens after it opens: writes a
    // marker document until one reaches the destination, so the cases' own
    // changes all happen after it.
    void await_stream_open() {
        auto src = (*client_)["test"][src_];
        const auto deadline = std::chrono::steady_clock::now() + 90s;
        for (std::int64_t n = 1; std::chrono::steady_clock::now() < deadline; ++n) {
            src.insert_one(make_document(kvp("item", "marker"), kvp("qty", n)));
            for (int i = 0; i < 10; ++i) {
                if (count(dst_, make_document(kvp("item", "marker"))) > 0) {
                    markers_ = n;
                    return;
                }
                std::this_thread::sleep_for(100ms);
            }
        }
        FAIL() << "the source never delivered a change: " << errors();
    }

    std::string source_table(const std::string& columns) const {
        return "CREATE TABLE changes " + columns + " WITH (connector='mongodb', uri='env://" +
               kUriVar + "', database='test', collection='" + src_ + "', max_await_ms='200'); ";
    }

    std::string sink_table(const std::string& columns, const std::string& extra = "") const {
        return "CREATE TABLE dst " + columns + " WITH (connector='mongodb', uri='env://" + kUriVar +
               "', database='test', collection='" + dst_ + "', batch_records='1'" + extra + "); ";
    }

    std::string uri_;
    std::string suffix_;
    std::string src_;
    std::string dst_;
    std::filesystem::path input_;
    std::int64_t markers_{0};
    std::ostringstream out_;
    std::ostringstream err_;
    std::unique_ptr<mongocxx::client> client_;
    std::unique_ptr<clink::embed::EmbeddedEngine> engine_;
};

bsoncxx::types::b_date date_at(std::int64_t ms) {
    return bsoncxx::types::b_date{std::chrono::milliseconds{ms}};
}

}  // namespace

// Inserts, an update and a delete, each read as a typed row and written back as
// typed BSON: the copy holds each value as the BSON type its SQL type maps to,
// the decimal's digits and the date's milliseconds exactly.
TEST_F(MongoSqlLive, ChangeEventsArriveAsTypedRowsAndAreWrittenAsTypedDocuments) {
    const std::string cols =
        "(__op VARCHAR, __id VARCHAR, __image VARCHAR, item VARCHAR, qty BIGINT, "
        "price DECIMAL(12, 2), at TIMESTAMP(3), day DATE, ok BOOLEAN, ratio DOUBLE)";
    const std::string out_cols =
        "(op VARCHAR, id VARCHAR, image VARCHAR, item VARCHAR, qty BIGINT, price DECIMAL(12, 2), "
        "at TIMESTAMP(3), day DATE, ok BOOLEAN, ratio DOUBLE)";
    start(source_table(cols) + sink_table(out_cols) +
          "INSERT INTO dst SELECT __op, __id, __image, item, qty, price, at, day, ok, ratio FROM "
          "changes;");
    await_stream_open();

    auto src = (*client_)["test"][src_];
    const bsoncxx::oid apple;
    const bsoncxx::oid pear;
    src.insert_one(make_document(kvp("_id", apple),
                                 kvp("item", "apple"),
                                 kvp("qty", std::int64_t{5}),
                                 kvp("price", bsoncxx::types::b_decimal128{"12.50"}),
                                 kvp("at", date_at(1700000000123)),
                                 kvp("day", date_at(1699920000000)),
                                 kvp("ok", true),
                                 kvp("ratio", 0.25)));
    src.insert_one(make_document(kvp("_id", pear),
                                 kvp("item", "pear"),
                                 kvp("qty", std::int32_t{7}),
                                 kvp("price", bsoncxx::types::b_decimal128{"0.10"})));
    src.update_one(make_document(kvp("_id", apple)),
                   make_document(kvp("$set", make_document(kvp("qty", std::int64_t{6})))));
    src.delete_one(make_document(kvp("_id", pear)));

    ASSERT_TRUE(finish([&] { return count(dst_) >= markers_ + 4; })) << errors();
    ASSERT_EQ(count(dst_), markers_ + 4) << errors();

    auto dst = (*client_)["test"][dst_];
    const auto inserted =
        dst.find_one(make_document(kvp("id", apple.to_string()), kvp("op", "insert")));
    ASSERT_TRUE(inserted);
    const auto v = inserted->view();
    EXPECT_EQ(v["image"].get_string().value, "full");
    EXPECT_EQ(v["item"].get_string().value, "apple");
    EXPECT_EQ(v["qty"].type(), bsoncxx::type::k_int64);
    EXPECT_EQ(v["qty"].get_int64().value, 5);
    ASSERT_EQ(v["price"].type(), bsoncxx::type::k_decimal128);
    EXPECT_EQ(v["price"].get_decimal128().value.to_string(), "12.50");
    ASSERT_EQ(v["at"].type(), bsoncxx::type::k_date);
    EXPECT_EQ(v["at"].get_date().to_int64(), 1700000000123);
    ASSERT_EQ(v["day"].type(), bsoncxx::type::k_date);
    EXPECT_EQ(v["day"].get_date().to_int64(), 1699920000000);
    EXPECT_TRUE(v["ok"].get_bool().value);
    ASSERT_EQ(v["ratio"].type(), bsoncxx::type::k_double);
    EXPECT_EQ(v["ratio"].get_double().value, 0.25);

    // An int32 in a BIGINT column is written as an int64; absent fields are null.
    const auto p = dst.find_one(make_document(kvp("id", pear.to_string()), kvp("op", "insert")));
    ASSERT_TRUE(p);
    EXPECT_EQ(p->view()["qty"].type(), bsoncxx::type::k_int64);
    EXPECT_EQ(p->view()["price"].get_decimal128().value.to_string(), "0.10");
    EXPECT_EQ(p->view()["at"].type(), bsoncxx::type::k_null);

    // updateLookup reads the document after the update.
    const auto u = dst.find_one(make_document(kvp("id", apple.to_string()), kvp("op", "update")));
    ASSERT_TRUE(u);
    EXPECT_EQ(u->view()["image"].get_string().value, "lookup");
    EXPECT_EQ(u->view()["qty"].get_int64().value, 6);
    // A delete carries the key alone.
    const auto d = dst.find_one(make_document(kvp("id", pear.to_string()), kvp("op", "delete")));
    ASSERT_TRUE(d);
    EXPECT_EQ(d->view()["image"].get_string().value, "key");
    EXPECT_EQ(d->view()["item"].type(), bsoncxx::type::k_null);
}

// A value its declared column cannot hold stops the job, naming the document,
// the field and the column; nothing after it is written.
TEST_F(MongoSqlLive, AValueItsColumnCannotHoldFailsTheJobNamingTheDocument) {
    start(source_table("(__id VARCHAR, item VARCHAR, qty BIGINT)") +
          sink_table("(id VARCHAR, item VARCHAR, qty BIGINT)") +
          "INSERT INTO dst SELECT __id, item, qty FROM changes;");
    await_stream_open();
    auto src = (*client_)["test"][src_];
    const bsoncxx::oid bad;
    src.insert_one(make_document(kvp("_id", bad), kvp("item", "half"), kvp("qty", 1.5)));
    src.insert_one(make_document(kvp("item", "after"), kvp("qty", std::int64_t{1})));
    const bool clean = finish([] { return false; }, 60s);
    EXPECT_FALSE(clean) << "the job did not fail";
    const auto all = errors();
    EXPECT_TRUE(contains(all, "field 'qty' is a double")) << all;
    EXPECT_TRUE(contains(all, "column qty (BIGINT)")) << all;
    EXPECT_TRUE(contains(all, "ObjectId(" + bad.to_string() + ")")) << all;
    EXPECT_EQ(count(dst_, make_document(kvp("item", "after"))), 0);
}

// A retracting query into mode='upsert': each group's top row replaces by key,
// and a row that leaves the top is deleted by key, so the collection ends as
// the query's answer. A GROUP BY's decimal sum is written with its digits.
TEST_F(MongoSqlLive, AnUpsertSinkReplacesAndDeletesByItsKey) {
    input_ = std::filesystem::temp_directory_path() / ("clink_mongodb_sql_" + suffix_ + ".json");
    {
        std::ofstream f(input_);
        f << R"({"g":"a","k":"a1","v":1,"amount":0.10})" << "\n"
          << R"({"g":"a","k":"a2","v":5,"amount":0.20})" << "\n"
          << R"({"g":"b","k":"b1","v":3,"amount":1.25})" << "\n"
          << R"({"g":"a","k":"a3","v":4,"amount":2.00})" << "\n"
          << R"({"g":"b","k":"b2","v":9,"amount":0.05})" << "\n";
    }
    const std::string totals = "sqltot_" + suffix_;
    start(
        "CREATE TABLE t (g VARCHAR, k VARCHAR, v BIGINT, amount DECIMAL(10, 2)) WITH "
        "(connector='file', format='json', path='" +
        input_.string() + "'); " +
        sink_table("(_id VARCHAR PRIMARY KEY, g VARCHAR, v BIGINT)", ", mode='upsert'") +
        "CREATE TABLE totals (_id VARCHAR PRIMARY KEY, n BIGINT, total DECIMAL(38, 2)) WITH "
        "(connector='mongodb', uri='env://" +
        kUriVar + "', database='test', collection='" + totals +
        "', mode='upsert'); "
        "INSERT INTO dst SELECT k AS _id, g, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION "
        "BY g ORDER BY v DESC) AS rn FROM t) s WHERE rn = 1; "
        "INSERT INTO totals SELECT g AS _id, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP "
        "BY g;");
    const bool clean = finish([] { return false; });
    EXPECT_TRUE(clean) << errors();

    auto dst = (*client_)["test"][dst_];
    EXPECT_EQ(count(dst_), 2) << errors();
    for (const auto* gone : {"a1", "a3", "b1"}) {
        EXPECT_EQ(count(dst_, make_document(kvp("_id", gone))), 0) << gone << " was not deleted";
    }
    const auto a = dst.find_one(make_document(kvp("_id", "a2")));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->view()["v"].get_int64().value, 5);
    ASSERT_TRUE(dst.find_one(make_document(kvp("_id", "b2"))));

    auto t = (*client_)["test"][totals];
    const auto ta = t.find_one(make_document(kvp("_id", "a")));
    ASSERT_TRUE(ta) << errors();
    EXPECT_EQ(ta->view()["n"].get_int64().value, 3);
    EXPECT_EQ(ta->view()["total"].get_decimal128().value.to_string(), "2.30");
    const auto tb = t.find_one(make_document(kvp("_id", "b")));
    ASSERT_TRUE(tb);
    EXPECT_EQ(tb->view()["total"].get_decimal128().value.to_string(), "1.30");
    t.drop();
}
