// Elasticsearch/OpenSearch _bulk sink, against a model of the _bulk endpoint:
// the NDJSON action+doc body; document_id -> _id and which values may name a
// document; a batch counted as written only when the response confirms every
// operation (a 200 with "errors":true, a malformed or short body, or an HTTP
// error for the whole request is never success, and never dead-letters a good
// record); a resent record going out with the later records of its document,
// so a retry cannot leave the older value stored; and a retry pause that a
// cancelled task cuts short.
//
// The model applies each operation to an in-memory index the way the servers
// do (operations on one _id in order, a failed operation not applied) unless a
// case scripts a call's reply, so the cases check what the index holds, not
// only what was sent.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <httplib.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/bulk_sink_builders.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::http_connector::BatchedHttpBulkSink;
using clink::http_connector::DlqPolicy;
using clink::http_connector::es_bulk_result;
using clink::http_connector::es_document_id;
using clink::http_connector::EsBulkOptions;
using clink::http_connector::HttpResponse;
using clink::http_connector::make_es_bulk_sink;

namespace {

// A model of the _bulk endpoint. Unscripted, each call applies its operations
// in order and answers as the servers do: one item per operation, "errors" true
// exactly when an item failed. A case scripts call N (1-based) to fail some of
// its items (a failed item is not applied) or to answer with its own status and
// body, after applying the operations or not.
class BulkModel {
public:
    struct ItemFault {
        int status;
        std::string type;
    };
    struct Call {
        std::map<std::size_t, ItemFault> item_faults;
        std::optional<int> http_status;  // with body: the reply, verbatim
        std::string body;
        bool apply{false};  // with http_status: apply the operations first
    };

    BulkModel() {
        svr_.Post(R"(/.*)", [this](const httplib::Request& req, httplib::Response& res) {
            handle(req, res);
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~BulkModel() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    BulkModel(const BulkModel&) = delete;
    BulkModel& operator=(const BulkModel&) = delete;

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    void script(int call, Call c) {
        std::lock_guard<std::mutex> lk(mu_);
        script_[call] = std::move(c);
    }
    // Every later call fails each of its items with `status`, or (0) stops.
    void fail_every_item(int status) {
        std::lock_guard<std::mutex> lk(mu_);
        fail_all_ = status;
    }
    std::map<std::string, std::string> docs() {
        std::lock_guard<std::mutex> lk(mu_);
        return docs_;
    }
    std::vector<std::string> bodies() {
        std::lock_guard<std::mutex> lk(mu_);
        return bodies_;
    }
    std::vector<std::string> content_types() {
        std::lock_guard<std::mutex> lk(mu_);
        return content_types_;
    }
    int calls() {
        std::lock_guard<std::mutex> lk(mu_);
        return static_cast<int>(bodies_.size());
    }

private:
    struct Op {
        std::string index;
        std::optional<std::string> id;
        std::string source;
    };

    static std::vector<Op> parse_ops(const std::string& body) {
        std::vector<Op> ops;
        std::size_t pos = 0;
        std::vector<std::string> lines;
        while (pos < body.size()) {
            const auto nl = body.find('\n', pos);
            if (nl == std::string::npos) {
                throw std::runtime_error("the bulk request must be terminated by a newline");
            }
            lines.push_back(body.substr(pos, nl - pos));
            pos = nl + 1;
        }
        if (lines.size() % 2 != 0) {
            throw std::runtime_error("an action without its source");
        }
        for (std::size_t i = 0; i < lines.size(); i += 2) {
            const auto action = clink::config::parse(lines[i]);
            const auto& meta = action.as_object().at("index").as_object();
            Op op;
            op.index = meta.at("_index").as_string();
            if (auto it = meta.find("_id"); it != meta.end()) {
                op.id = it->second.as_string();
            }
            op.source = lines[i + 1];
            ops.push_back(std::move(op));
        }
        return ops;
    }

    void handle(const httplib::Request& req, httplib::Response& res) {
        std::lock_guard<std::mutex> lk(mu_);
        bodies_.push_back(req.body);
        content_types_.push_back(req.get_header_value("Content-Type"));
        const int call = static_cast<int>(bodies_.size());
        Call c;
        if (auto it = script_.find(call); it != script_.end()) {
            c = it->second;
        }
        if (c.http_status) {
            if (c.apply) {
                for (const auto& op : parse_ops(req.body)) {
                    docs_[op.id.value_or("auto-" + std::to_string(next_auto_++))] = op.source;
                }
            }
            res.status = *c.http_status;
            res.set_content(c.body, "application/json");
            return;
        }
        std::vector<Op> ops;
        try {
            ops = parse_ops(req.body);
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(R"({"error":{"type":"illegal_argument_exception","reason":")" +
                                std::string(e.what()) + R"("},"status":400})",
                            "application/json");
            return;
        }
        JsonArray items;
        bool errors = false;
        for (std::size_t i = 0; i < ops.size(); ++i) {
            const auto& op = ops[i];
            const std::string id = op.id.value_or("auto-" + std::to_string(next_auto_++));
            JsonObject r;
            r["_index"] = JsonValue{op.index};
            r["_id"] = JsonValue{id};
            std::optional<ItemFault> fault;
            if (auto f = c.item_faults.find(i); f != c.item_faults.end()) {
                fault = f->second;
            } else if (fail_all_ != 0) {
                fault = ItemFault{fail_all_, "es_rejected_execution_exception"};
            }
            if (fault) {
                errors = true;
                r["status"] = JsonValue{std::int64_t{fault->status}};
                JsonObject err;
                err["type"] = JsonValue{fault->type};
                err["reason"] = JsonValue{std::string{"scripted"}};
                r["error"] = JsonValue{std::move(err)};
            } else {
                const bool existed = docs_.count(id) != 0;
                docs_[id] = op.source;
                r["status"] = JsonValue{std::int64_t{existed ? 200 : 201}};
                r["result"] = JsonValue{std::string{existed ? "updated" : "created"}};
            }
            JsonObject item;
            item["index"] = JsonValue{std::move(r)};
            items.push_back(JsonValue{std::move(item)});
        }
        JsonObject out;
        out["took"] = JsonValue{std::int64_t{1}};
        out["errors"] = JsonValue{errors};
        out["items"] = JsonValue{std::move(items)};
        res.status = 200;
        res.set_content(JsonValue{std::move(out)}.serialize(0), "application/json");
    }

    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::map<int, Call> script_;
    int fail_all_{0};
    std::map<std::string, std::string> docs_;
    std::uint64_t next_auto_{0};
    std::vector<std::string> bodies_;
    std::vector<std::string> content_types_;
};

// Call N fails the items `faults` names; the rest are applied.
BulkModel::Call failing(std::map<std::size_t, BulkModel::ItemFault> faults) {
    BulkModel::Call c;
    c.item_faults = std::move(faults);
    return c;
}

// Call N answers `status` with `body`, after applying its operations when `apply`.
BulkModel::Call replying(int status, std::string body, bool apply) {
    BulkModel::Call c;
    c.http_status = status;
    c.body = std::move(body);
    c.apply = apply;
    return c;
}

Batch<std::string> batch_of(std::vector<std::string> recs) {
    Batch<std::string> b;
    for (auto& r : recs) {
        b.emplace(std::move(r));
    }
    return b;
}

bool contains(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
}

std::uint64_t counter_value(const std::string& name) {
    for (const auto& [k, v] : clink::MetricsRegistry::global().snapshot().counters) {
        if (k == name) {
            return v;
        }
    }
    return 0;
}

const std::string kDropped =
    R"(clink_connector_dropped_records_total{connector="elasticsearch_sink",direction="sink"})";

EsBulkOptions options_for(const BulkModel& m) {
    EsBulkOptions o;
    o.url = m.url();
    o.index = "logs";
    o.batch_records = 100;  // flush via flush(), not auto
    o.max_retries = 3;
    return o;
}

HttpResponse resp(int status, std::string body) {
    HttpResponse r;
    r.status = status;
    r.body = std::move(body);
    return r;
}

// A well-formed response for `statuses`, one 'index' item each.
std::string bulk_reply(const std::vector<int>& statuses, std::optional<bool> errors = {}) {
    std::string items;
    bool failed = false;
    for (std::size_t i = 0; i < statuses.size(); ++i) {
        const int st = statuses[i];
        items += (i == 0 ? "" : ",");
        items += R"({"index":{"_index":"logs","_id":")" + std::to_string(i) + R"(","status":)" +
                 std::to_string(st);
        if (st < 200 || st >= 300) {
            failed = true;
            items += R"(,"error":{"type":"x","reason":"y"})";
        }
        items += "}}";
    }
    const bool e = errors.value_or(failed);
    return std::string{R"({"took":1,"errors":)"} + (e ? "true" : "false") + R"(,"items":[)" +
           items + "]}";
}

}  // namespace

// ---- the request ------------------------------------------------------------

TEST(ElasticsearchSink, BulkBodyIsActionPlusDocNdjson) {
    BulkModel srv;
    auto sink = make_es_bulk_sink(options_for(srv));
    sink->open();
    sink->on_data(batch_of({R"({"a":1})", R"({"a":2})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    // Each item is an action line then the source doc, both newline-terminated.
    EXPECT_EQ(got[0],
              "{\"index\":{\"_index\":\"logs\"}}\n{\"a\":1}\n"
              "{\"index\":{\"_index\":\"logs\"}}\n{\"a\":2}\n");
    // _bulk requires the NDJSON content type.
    ASSERT_FALSE(srv.content_types().empty());
    EXPECT_EQ(srv.content_types()[0], "application/x-ndjson");
    EXPECT_EQ(srv.docs().size(), 2u);
}

TEST(ElasticsearchSink, DocumentIdGoesIntoActionLine) {
    BulkModel srv;
    auto o = options_for(srv);
    o.document_id = "id";  // idempotent: re-delivery overwrites the same _id
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"id":"k1","a":1})", R"({"id":42,"a":2})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    // Key order in the action metadata is the JSON serializer's (sorted), so
    // assert on the presence of both fields rather than exact ordering.
    EXPECT_TRUE(contains(got[0], R"("_index":"logs")")) << got[0];
    EXPECT_TRUE(contains(got[0], R"("_id":"k1")")) << got[0];         // string id
    EXPECT_TRUE(contains(got[0], R"("_id":"42")")) << got[0];         // numeric id stringified
    EXPECT_TRUE(contains(got[0], R"({"id":"k1","a":1})")) << got[0];  // source doc verbatim
    const auto docs = srv.docs();
    EXPECT_EQ(docs.at("k1"), R"({"id":"k1","a":1})");
    EXPECT_EQ(docs.at("42"), R"({"id":42,"a":2})");
}

// With document_id set, a record without the field is refused rather than
// indexed under a generated _id, which a replay would index again beside it.
// Under dlq=drop it is dead-lettered and the others are written. It used to be
// indexed without an _id, so the overwrite document_id promises did not hold
// for it.
TEST(ElasticsearchSink, ARecordWithoutItsDocumentIdIsRefused) {
    {
        BulkModel srv;
        auto o = options_for(srv);
        o.document_id = "id";
        auto sink = make_es_bulk_sink(o);
        sink->open();
        EXPECT_THROW(sink->on_data(batch_of({R"({"a":1})"})), std::runtime_error);
        EXPECT_THROW(sink->on_data(batch_of({R"({"id":null,"a":2})"})), std::runtime_error);
    }
    {
        BulkModel srv;
        auto o = options_for(srv);
        o.document_id = "id";
        o.dlq_policy = DlqPolicy::Drop;
        auto sink = make_es_bulk_sink(o);
        sink->open();
        sink->on_data(batch_of({R"({"a":1})", R"({"id":"k2","a":2})"}));
        sink->flush();
        auto got = srv.bodies();
        ASSERT_EQ(got.size(), 1u);
        EXPECT_TRUE(contains(got[0], R"("_id":"k2")")) << got[0];
        EXPECT_FALSE(contains(got[0], R"({"a":1})")) << got[0];
    }
}

// The _bulk body takes each record as one JSON object on one line. A record
// spanning lines broke the framing, and both servers then failed the whole
// request (HTTP 400), so under dlq=drop every record of the batch was
// dead-lettered with it; one that is not an object failed as a record, and
// Elasticsearch answers that with 500, which was resent until the job failed,
// on every replay. Both are refused before they are sent.
TEST(ElasticsearchSink, ARecordThatIsNotOneJsonObjectOnOneLineIsRefusedAlone) {
    BulkModel srv;
    auto o = options_for(srv);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(kDropped);
    sink->on_data(batch_of(
        {R"({"a":1})", "{\"a\":\n2}", "42", R"("x")", "[1]", "not json at all", R"(  {"a":3})"}));
    sink->flush();
    EXPECT_EQ(counter_value(kDropped) - before, 5u);
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(srv.docs().size(), 2u) << got[0];
    // Under the default policy the refusal fails the sink.
    BulkModel srv2;
    auto sink2 = make_es_bulk_sink(options_for(srv2));
    sink2->open();
    EXPECT_THROW(sink2->on_data(batch_of({"{\"a\":\n1}"})), std::runtime_error);
    EXPECT_EQ(srv2.calls(), 0);
}

// ---- document_id: which values name a document, and as what ----------------

namespace {

// The _id `value` (the text of a record's id field) names, or nullopt when the
// sink refuses it. Reads the record as the sink does, raw number token included.
std::optional<std::string> id_for(const std::string& value) {
    const std::string rec = R"({"id":)" + value + "}";
    JsonValue j;
    try {
        j = clink::config::parse(rec);
    } catch (const std::exception&) {
        return std::nullopt;
    }
    const auto& v = j.as_object().at("id");
    std::string token;
    if (v.is_number() && !v.is_integral_number()) {
        token = clink::config::raw_number_tokens(rec, {{"id", 0}}).at("id");
    }
    auto d = es_document_id(v, token);
    if (!d.refusal.empty()) {
        return std::nullopt;
    }
    return d.id;
}

}  // namespace

// Integers keep their exact digits at any size, however the record writes them.
// A number with a fraction or exponent used to go through a double: 1e19 and
// 2e19 kept distinct ids only by luck, 9007199254740993.0 rounded onto the id of
// 9007199254740992, and above 2^63 the id was the double printed to six
// decimals, so neighbouring values shared one.
TEST(EsDocumentId, IntegersNameTheirExactDigits) {
    EXPECT_EQ(id_for("42"), "42");
    EXPECT_EQ(id_for("-9223372036854775808"), "-9223372036854775808");
    EXPECT_EQ(id_for("9223372036854775807"), "9223372036854775807");
    EXPECT_EQ(id_for("9007199254740992"), "9007199254740992");
    EXPECT_EQ(id_for("9007199254740993"), "9007199254740993");
    EXPECT_EQ(id_for("9007199254740993.0"), "9007199254740993");
    EXPECT_EQ(id_for("9007199254740992.0"), "9007199254740992");
    EXPECT_EQ(id_for("9223372036854775808"), "9223372036854775808");
    EXPECT_EQ(id_for("18446744073709551615"), "18446744073709551615");
    EXPECT_EQ(id_for("18446744073709551614"), "18446744073709551614");
    EXPECT_EQ(id_for("1e19"), "10000000000000000000");
    EXPECT_EQ(id_for("2e19"), "20000000000000000000");
    EXPECT_EQ(id_for("42.0"), "42");
    EXPECT_EQ(id_for("4.2e1"), "42");
    EXPECT_EQ(id_for("4200e-2"), "42");
    EXPECT_EQ(id_for("-0"), "0");
    EXPECT_EQ(id_for("-0.0"), "0");
    EXPECT_EQ(id_for("0e5"), "0");
}

// A fraction keeps the six-decimal form the sink always wrote when that form is
// exact, so replaying older records still overwrites what they wrote; any other
// fraction is refused, since that form rounded it onto another number's id
// (0.1234567 and 0.1234568 both wrote "0.123457").
TEST(EsDocumentId, AFractionKeepsItsSixDecimalFormOnlyWhereExact) {
    EXPECT_EQ(id_for("0.5"), "0.500000");
    EXPECT_EQ(id_for("-0.5"), "-0.500000");
    EXPECT_EQ(id_for("0.1"), "0.100000");
    EXPECT_EQ(id_for("1.25e-1"), "0.125000");
    EXPECT_EQ(id_for("123456.000001"), "123456.000001");
    EXPECT_EQ(id_for("0.000001"), "0.000001");
    EXPECT_EQ(id_for("0.1234567"), std::nullopt);
    EXPECT_EQ(id_for("0.1234568"), std::nullopt);
    EXPECT_EQ(id_for("1e-7"), std::nullopt);
    EXPECT_EQ(id_for("1.0000001"), std::nullopt);
    EXPECT_EQ(id_for("0.10000000000000001"), std::nullopt);
    // Past 2^53 a double cannot hold the fraction to six places.
    EXPECT_EQ(id_for("1234567890123456789.5"), std::nullopt);
}

// Strings name themselves, within the 512 bytes an _id may hold; an empty one
// names nothing. Both servers refuse an empty _id for its record and a longer
// one for the whole request, which failed every record sent with it.
TEST(EsDocumentId, StringsAndBooleansNameThemselvesWithinTheIdLimit) {
    EXPECT_EQ(id_for(R"("k1")"), "k1");
    EXPECT_EQ(id_for(R"("")"), std::nullopt);
    EXPECT_EQ(id_for("\"" + std::string(512, 'x') + "\""), std::string(512, 'x'));
    EXPECT_EQ(id_for("\"" + std::string(513, 'x') + "\""), std::nullopt);
    EXPECT_EQ(id_for("true"), "true");
    EXPECT_EQ(id_for("false"), "false");
}

// An object or array names no single document: its JSON text, the id it used to
// get, depends on how this sink happens to print it.
TEST(EsDocumentId, NullObjectsAndArraysNameNoDocument) {
    EXPECT_EQ(id_for("null"), std::nullopt);
    EXPECT_EQ(id_for(R"({"a":1})"), std::nullopt);
    EXPECT_EQ(id_for("[1,2]"), std::nullopt);
    EXPECT_EQ(id_for("[]"), std::nullopt);
}

// The id is text in one namespace: a number and the string of its id name one
// document. Kept as it was, so documents already written keep their ids.
TEST(EsDocumentId, ANumberAndItsTextNameOneDocument) {
    EXPECT_EQ(id_for("42"), id_for(R"("42")"));
    EXPECT_EQ(id_for("0.5"), id_for(R"("0.500000")"));
    EXPECT_EQ(id_for("true"), id_for(R"("true")"));
}

// Through the sink: refused ids never reach the server, and under dlq=drop the
// rest of the batch is written under the ids the contract gives.
TEST(ElasticsearchSink, DocumentIdsFollowTheContractThroughTheSink) {
    BulkModel srv;
    auto o = options_for(srv);
    o.document_id = "id";
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(kDropped);
    sink->on_data(batch_of({R"({"id":9007199254740992,"v":1})",
                            R"({"id":9007199254740993,"v":2})",
                            R"({"id":9007199254740993.0,"v":3})",
                            R"({"id":1e19,"v":4})",
                            R"({"id":2e19,"v":5})",
                            R"({"id":0.5,"v":6})",
                            R"({"id":0.1234567,"v":7})",
                            R"({"id":"","v":8})",
                            "{\"id\":\"" + std::string(513, 'x') + "\",\"v\":9}",
                            R"({"id":{"a":1},"v":10})",
                            R"({"id":[1],"v":11})"}));
    sink->flush();
    EXPECT_EQ(counter_value(kDropped) - before, 5u);
    const auto docs = srv.docs();
    ASSERT_EQ(docs.size(), 5u);
    EXPECT_EQ(docs.at("9007199254740992"), R"({"id":9007199254740992,"v":1})");
    // 9007199254740993 written twice, as an integer and as 9007199254740993.0:
    // one document, the later record's.
    EXPECT_EQ(docs.at("9007199254740993"), R"({"id":9007199254740993.0,"v":3})");
    EXPECT_EQ(docs.at("10000000000000000000"), R"({"id":1e19,"v":4})");
    EXPECT_EQ(docs.at("20000000000000000000"), R"({"id":2e19,"v":5})");
    EXPECT_EQ(docs.at("0.500000"), R"({"id":0.5,"v":6})");
}

// ---- the response: a batch is written only when the response says so -------

TEST(EsBulkResult, AcceptsOnlyAResponseThatConfirmsEveryOperation) {
    auto ok = es_bulk_result(resp(200, bulk_reply({201, 200})), 2);
    EXPECT_TRUE(ok.ok);
    EXPECT_TRUE(ok.failed_transient.empty());
    EXPECT_TRUE(ok.failed_permanent.empty());
}

// Each of these answered a POST with a 2xx. None confirms what was written, so
// each resends the whole batch (and, once retries run out, fails the job):
// nothing is counted written and nothing is dead-lettered. A non-object 2xx and
// an "errors":false with an empty or missing items[] used to count as success,
// so the batch was cleared and the checkpoint acknowledged without the server
// having confirmed a single record.
TEST(EsBulkResult, AResponseThatDoesNotConfirmEveryOperationIsNeverSuccess) {
    const std::string two_ok = bulk_reply({201, 201});
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"not JSON", "not json"},
        {"empty", ""},
        {"truncated", two_ok.substr(0, two_ok.size() / 2)},
        {"an array", "[]"},
        {"a string", R"("ok")"},
        {"a number", "42"},
        {"null", "null"},
        {"an empty object", "{}"},
        {"errors:false and no items", R"({"took":1,"errors":false})"},
        {"items and no errors",
         R"({"took":1,"items":[{"index":{"status":201}},{"index":{"status":201}}]})"},
        {"errors not a boolean",
         R"({"errors":"false","items":[{"index":{"status":201}},{"index":{"status":201}}]})"},
        {"errors:false and an empty items", R"({"took":1,"errors":false,"items":[]})"},
        {"too few items", bulk_reply({201})},
        {"too many items", bulk_reply({201, 201, 201})},
        {"items not an array", R"({"errors":false,"items":{"index":{"status":201}}})"},
        {"an item that is not an object",
         R"({"errors":false,"items":[{"index":{"status":201}},7]})"},
        {"an item with two actions",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":{"status":201},"create":{"status":201}}]})"},
        {"an item for another action",
         R"({"errors":false,"items":[{"index":{"status":201}},{"create":{"status":201}}]})"},
        {"an item result that is not an object",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":201}]})"},
        {"an item with no status",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":{}}]})"},
        {"a status that is text",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":{"status":"201"}}]})"},
        {"a status that is not an HTTP status",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":{"status":99}}]})"},
        {"a status past 599",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":{"status":600}}]})"},
        {"a fractional status",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":{"status":201.5}}]})"},
        {"errors:false over a failed item", bulk_reply({201, 429}, false)},
        {"errors:true over every item written", bulk_reply({201, 201}, true)},
        {"a success with an error attached",
         R"({"errors":false,"items":[{"index":{"status":201}},{"index":{"status":201,"error":{"type":"x"}}}]})"},
    };
    for (const auto& [what, body] : cases) {
        for (int status : {200, 201, 202, 204}) {
            const auto r = es_bulk_result(resp(status, body), 2);
            EXPECT_FALSE(r.ok) << what << " under " << status;
            EXPECT_EQ(r.failed_transient, (std::vector<std::size_t>{0, 1}))
                << what << " under " << status;
            EXPECT_TRUE(r.failed_permanent.empty()) << what << " under " << status;
            EXPECT_FALSE(r.detail.empty()) << what << " under " << status;
        }
    }
}

TEST(EsBulkResult, PerItemTransientVsPermanent) {
    // item 0 indexed (201), item 1 throttled (429 transient), item 2 mapping
    // error (400 permanent).
    const std::string body =
        R"({"errors":true,"items":[)"
        R"({"index":{"status":201}},)"
        R"({"index":{"status":429,"error":{"type":"es_rejected_execution_exception"}}},)"
        R"({"index":{"status":400,"error":{"type":"document_parsing_exception"}}}]})";
    auto r = es_bulk_result(resp(200, body), 3);
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.failed_transient, (std::vector<std::size_t>{1}));
    EXPECT_EQ(r.failed_permanent, (std::vector<std::size_t>{2}));
    EXPECT_TRUE(contains(r.detail, "2 of 3")) << r.detail;
}

// An item failure about the index rather than the document is resent, not
// dead-lettered: 403 for a write to an index with a write block, 404 for an
// index that does not exist, and the two 400s both servers return for an index:
// closed, or with a name that is not valid. Those 400s used to dead-letter every
// record written while the index was closed or misnamed.
TEST(EsBulkResult, AnItemRefusedForItsIndexIsResentNotDeadLettered) {
    const std::string body =
        R"({"errors":true,"items":[)"
        R"({"index":{"status":403,"error":{"type":"cluster_block_exception"}}},)"
        R"({"index":{"status":404,"error":{"type":"index_not_found_exception"}}},)"
        R"({"index":{"status":400,"error":{"type":"index_closed_exception"}}},)"
        R"({"index":{"status":400,"error":{"type":"invalid_index_name_exception"}}},)"
        R"({"index":{"status":503,"error":{"type":"unavailable_shards_exception"}}},)"
        R"({"index":{"status":400,"error":{"type":"document_parsing_exception"}}},)"
        R"({"index":{"status":400,"error":{"type":"mapper_parsing_exception"}}},)"
        R"({"index":{"status":409,"error":{"type":"version_conflict_engine_exception"}}}]})";
    auto r = es_bulk_result(resp(200, body), 8);
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.failed_transient, (std::vector<std::size_t>{0, 1, 2, 3, 4}));
    EXPECT_EQ(r.failed_permanent, (std::vector<std::size_t>{5, 6, 7}));
}

// An HTTP error for the whole request concerns no one record, so it is resent
// and then fails the job, never dead-letters. A 400 there used to dead-letter
// the whole batch: both servers answer a request holding one _id over 512 bytes
// with a 400 for the whole request, which under dlq=drop discarded every record
// sent with it.
TEST(EsBulkResult, AnHttpErrorForTheWholeRequestIsNeverDeadLettered) {
    for (int status : {0, 400, 401, 403, 404, 409, 413, 422, 429, 500, 502, 503}) {
        const auto r = es_bulk_result(
            resp(
                status,
                R"({"error":{"type":"action_request_validation_exception","reason":"Validation Failed"},"status":400})"),
            3);
        EXPECT_FALSE(r.ok) << status;
        EXPECT_EQ(r.failed_transient.size(), 3u) << status;
        EXPECT_TRUE(r.failed_permanent.empty()) << status;
        if (status != 0) {
            EXPECT_TRUE(contains(r.detail, "action_request_validation_exception")) << status;
        }
    }
}

// ---- the sink over the model: what the index holds --------------------------

TEST(ElasticsearchSink, ErrorsFalseResponseSucceeds) {
    BulkModel srv;
    auto sink = make_es_bulk_sink(options_for(srv));
    sink->open();
    sink->on_data(batch_of({R"({"a":1})"}));
    EXPECT_NO_THROW(sink->flush());
    EXPECT_EQ(srv.bodies().size(), 1u);
}

TEST(ElasticsearchSink, PartialFailureErrorsTrueIsTreatedAsFailure) {
    BulkModel srv;
    srv.fail_every_item(503);
    auto o = options_for(srv);
    o.max_retries = 1;  // fast
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"a":1})"}));
    try {
        sink->flush();
        FAIL() << "expected throw on errors:true partial failure";
    } catch (const std::runtime_error& e) {
        // 2xx-but-rejected keeps a body snippet and the item's failure.
        EXPECT_TRUE(contains(e.what(), "HTTP 200")) << e.what();
        EXPECT_TRUE(contains(e.what(), "es_rejected_execution_exception")) << e.what();
    }
    EXPECT_TRUE(srv.docs().empty());
}

// A response that does not confirm the batch, answered to every attempt: the
// flush fails once its retries run out, nothing is dead-lettered even under
// dlq=drop, and the records stay pending, so the barrier that asked for the
// flush fails (its checkpoint is not acknowledged) and the next flush sends
// them again.
TEST(ElasticsearchSink, AnUnconfirmedResponseNeverClearsTheBatchOrPassesTheBarrier) {
    BulkModel srv;
    for (int call = 1; call <= 6; ++call) {
        // The server applied the writes and then answered with half a body.
        srv.script(call, replying(200, R"({"took":1,"errors":false,"items":[{"in)", call % 2 == 0));
    }
    auto o = options_for(srv);
    o.document_id = "id";
    o.dlq_policy = DlqPolicy::Drop;
    o.max_retries = 2;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(kDropped);
    sink->on_data(batch_of({R"({"id":"a","v":1})", R"({"id":"b","v":1})"}));
    EXPECT_THROW(sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}}),
                 std::runtime_error);
    EXPECT_EQ(srv.calls(), 3);
    EXPECT_THROW(sink->flush(), std::runtime_error);
    EXPECT_EQ(srv.calls(), 6);
    EXPECT_EQ(counter_value(kDropped) - before, 0u);
    // The model answers properly from call 7: the same two records go out again.
    sink->flush();
    const auto bodies = srv.bodies();
    ASSERT_EQ(bodies.size(), 7u);
    EXPECT_EQ(bodies[6], bodies[0]);
    const auto docs = srv.docs();
    EXPECT_EQ(docs.at("a"), R"({"id":"a","v":1})");
    EXPECT_EQ(docs.at("b"), R"({"id":"b","v":1})");
    sink->flush();  // nothing pending now
    EXPECT_EQ(srv.calls(), 7);
}

// An HTTP error for the whole request, after which the server may or may not
// have applied the writes: the whole active set is resent in order, so with
// document_id the documents end at their latest records and none is doubled.
TEST(ElasticsearchSink, AWholeRequestFailureResendsTheBatchInOrder) {
    BulkModel srv;
    srv.script(1, replying(502, "bad gateway", true));
    srv.script(2, replying(429, "{}", false));
    auto o = options_for(srv);
    o.document_id = "id";
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"id":"k","v":1})", R"({"id":"j","v":1})", R"({"id":"k","v":2})"}));
    sink->flush();
    const auto bodies = srv.bodies();
    ASSERT_EQ(bodies.size(), 3u);
    EXPECT_EQ(bodies[1], bodies[0]);
    EXPECT_EQ(bodies[2], bodies[0]);
    const auto docs = srv.docs();
    EXPECT_EQ(docs.size(), 2u);
    EXPECT_EQ(docs.at("k"), R"({"id":"k","v":2})");
}

// Without document_id a record is a new document each time it is written, so
// only the failed item is resent; resending a written one would index it twice.
TEST(ElasticsearchSink, ResendsOnlyTheFailedItemNotTheWholeBatch) {
    BulkModel srv;
    srv.script(1, failing({{1, {429, "es_rejected_execution_exception"}}}));
    auto sink = make_es_bulk_sink(options_for(srv));
    sink->open();
    sink->on_data(batch_of({R"({"a":1})", R"({"a":2})"}));
    EXPECT_NO_THROW(sink->flush());  // item 1 (429) resent, item 0 not re-indexed
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 2u);
    // First POST carried both records; the resend carried ONLY the failed one.
    EXPECT_TRUE(contains(got[0], R"({"a":1})")) << got[0];
    EXPECT_TRUE(contains(got[0], R"({"a":2})")) << got[0];
    EXPECT_TRUE(contains(got[1], R"({"a":2})")) << got[1];
    EXPECT_FALSE(contains(got[1], R"({"a":1})")) << got[1];  // succeeded doc not re-sent
    EXPECT_EQ(srv.docs().size(), 2u);
}

// The repeated-key failure: two records name one document, the earlier write
// fails transiently, the later one is written. Resent alone, the earlier record
// landed after the later one and left the older value stored, with the flush
// reporting success. Now it goes out with the later record, in order.
TEST(ElasticsearchSink, AResentWriteGoesOutWithTheLaterWritesOfItsDocument) {
    BulkModel srv;
    srv.script(1, failing({{0, {429, "es_rejected_execution_exception"}}}));
    auto o = options_for(srv);
    o.document_id = "id";
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"id":"k","v":1})", R"({"id":"j","v":1})", R"({"id":"k","v":2})"}));
    sink->flush();
    const auto bodies = srv.bodies();
    ASSERT_EQ(bodies.size(), 2u);
    // The resend: k's failed write, then k's later one; j, written and named by
    // no failed record, is not resent.
    const auto first_k = bodies[1].find(R"({"id":"k","v":1})");
    const auto second_k = bodies[1].find(R"({"id":"k","v":2})");
    ASSERT_NE(first_k, std::string::npos) << bodies[1];
    ASSERT_NE(second_k, std::string::npos) << bodies[1];
    EXPECT_LT(first_k, second_k) << bodies[1];
    EXPECT_FALSE(contains(bodies[1], R"("_id":"j")")) << bodies[1];
    EXPECT_EQ(srv.docs().at("k"), R"({"id":"k","v":2})");
    EXPECT_EQ(srv.docs().at("j"), R"({"id":"j","v":1})");
}

// Repeated ids across partial retries and batch boundaries, with transient item
// failures on several calls and a record refused for good: each document ends at
// its latest record that the index accepts.
TEST(ElasticsearchSink, RepeatedIdsEndAtTheirLatestRecordAcrossRetriesAndBatches) {
    BulkModel srv;
    // Batch 1 (k1 v1, k2 v1, k1 v2): k1 v1 fails, then on the resend k1 v2 does.
    srv.script(1, failing({{0, {503, "unavailable_shards_exception"}}}));
    srv.script(2, failing({{1, {429, "es_rejected_execution_exception"}}}));
    // Batch 2 (k2 v2, k1 v3, k2 v3): k2 v3 is refused for good (mapping).
    srv.script(4,
               failing({{0, {429, "es_rejected_execution_exception"}},
                        {2, {400, "document_parsing_exception"}}}));
    auto o = options_for(srv);
    o.document_id = "id";
    o.batch_records = 3;
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"id":"k1","v":1})",
                            R"({"id":"k2","v":1})",
                            R"({"id":"k1","v":2})",
                            R"({"id":"k2","v":2})",
                            R"({"id":"k1","v":3})",
                            R"({"id":"k2","v":3})"}));
    sink->flush();
    const auto docs = srv.docs();
    EXPECT_EQ(docs.at("k1"), R"({"id":"k1","v":3})");
    // k2 v3 never written; k2 v2, its latest accepted record, stays.
    EXPECT_EQ(docs.at("k2"), R"({"id":"k2","v":2})");
}

// Every item failing transiently on every attempt (throttling, an index that
// refuses writes): the flush gives up after max_retries + 1 attempts with its
// records pending, nothing dead-lettered, in about the time its backoff adds
// up to.
TEST(ElasticsearchSink, ThrottlingIsRetriedAndThenFailsBoundedWithTheBatchPending) {
    BulkModel srv;
    srv.fail_every_item(429);
    auto o = options_for(srv);
    o.document_id = "id";
    o.dlq_policy = DlqPolicy::Drop;
    o.max_retries = 3;  // pauses of 200, 400 and 800 ms
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(kDropped);
    sink->on_data(batch_of({R"({"id":"a","v":1})"}));
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_THROW(sink->flush(), std::runtime_error);
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(srv.calls(), 4);
    EXPECT_GE(took, std::chrono::milliseconds{1400});
    EXPECT_LT(took, std::chrono::milliseconds{10000});
    EXPECT_EQ(counter_value(kDropped) - before, 0u);
    // Throttling ends: a few throttled attempts are absorbed by the retries.
    srv.fail_every_item(0);
    srv.script(5, failing({{0, {429, "es_rejected_execution_exception"}}}));
    sink->flush();
    EXPECT_EQ(srv.docs().at("a"), R"({"id":"a","v":1})");
}

TEST(ElasticsearchSink, MixedItemResultsUnderBothPolicies) {
    const auto script_mixed = [](BulkModel& srv) {
        srv.script(1,
                   failing({{1, {429, "es_rejected_execution_exception"}},
                            {2, {400, "document_parsing_exception"}}}));
    };
    const auto recs = batch_of({R"({"id":"a","v":1})",
                                R"({"id":"b","v":1})",
                                R"({"id":"c","n":"x"})",
                                R"({"id":"d","v":1})"});
    {
        BulkModel srv;
        script_mixed(srv);
        auto o = options_for(srv);
        o.document_id = "id";
        o.dlq_policy = DlqPolicy::Drop;
        auto sink = make_es_bulk_sink(o);
        sink->open();
        const auto before = counter_value(kDropped);
        sink->on_data(recs);
        sink->flush();
        EXPECT_EQ(counter_value(kDropped) - before, 1u);
        const auto docs = srv.docs();
        EXPECT_EQ(docs.size(), 3u);
        EXPECT_EQ(docs.count("c"), 0u);
        EXPECT_EQ(docs.at("b"), R"({"id":"b","v":1})");
    }
    {
        BulkModel srv;
        script_mixed(srv);
        auto o = options_for(srv);
        o.document_id = "id";
        auto sink = make_es_bulk_sink(o);
        sink->open();
        sink->on_data(recs);
        try {
            sink->flush();
            FAIL() << "a record refused for good fails the sink under dlq=fail";
        } catch (const std::runtime_error& e) {
            EXPECT_TRUE(contains(e.what(), "permanently rejected")) << e.what();
            EXPECT_TRUE(contains(e.what(), "document_parsing_exception")) << e.what();
        }
    }
}

TEST(ElasticsearchSink, PermanentItemDroppedUnderDropPolicy) {
    BulkModel srv;
    srv.script(1, failing({{0, {400, "document_parsing_exception"}}}));
    auto o = options_for(srv);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(kDropped);
    sink->on_data(batch_of({R"({"a":1})"}));
    EXPECT_NO_THROW(sink->flush());  // poison mapping error dead-lettered, not crash-looped
    EXPECT_EQ(counter_value(kDropped) - before, 1u);
}

TEST(ElasticsearchSink, EmitsConnectorMetricsOnFlush) {
    BulkModel srv;
    auto sink = make_es_bulk_sink(options_for(srv));
    sink->open();
    // Delta-based so the test is robust to the shared global registry.
    const std::string records =
        R"(clink_connector_records_total{connector="elasticsearch_sink",direction="sink"})";
    const auto before = counter_value(records);
    sink->on_data(batch_of({R"({"a":1})", R"({"a":2})", R"({"a":3})"}));
    sink->flush();
    EXPECT_EQ(counter_value(records) - before, 3u);
}

// ---- cancellation ------------------------------------------------------------

namespace {

// A sink attached to a runtime context whose cancel signal the case controls,
// as its runner attaches it.
template <typename SinkPtr>
struct Cancellable {
    explicit Cancellable(SinkPtr s) : sink(std::move(s)) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4242}, "elasticsearch_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink->attach_runtime(ctx.get());
    }
    ~Cancellable() { sink->attach_runtime(nullptr); }
    // Starts flush() on a thread, cancels after `after`, and returns how long
    // flush() then took to return and what it threw.
    std::pair<std::chrono::milliseconds, std::string> cancel_during_flush(
        std::chrono::milliseconds after) {
        std::string error;
        std::chrono::steady_clock::time_point cancelled_at;
        std::thread t([&] {
            try {
                sink->flush();
            } catch (const std::exception& e) {
                error = e.what();
            }
        });
        std::this_thread::sleep_for(after);
        cancelled_at = std::chrono::steady_clock::now();
        cancel->store(true);
        t.join();
        return {std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - cancelled_at),
                error};
    }

    SinkPtr sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

}  // namespace

// A cancelled task (a cancelled job, or one restarting after another task
// failed) stops retrying at the next pause instead of sleeping through the rest
// of its retries, which at the most could keep it for minutes; the flush throws
// with the batch still pending. The same for the plain bulk sink the other HTTP
// sinks build on.
TEST(ElasticsearchSink, CancellationCutsTheRetryPauseShort) {
    BulkModel srv;
    srv.fail_every_item(503);
    auto o = options_for(srv);
    o.max_retries = 10;  // pauses of 0.2, 0.4, 0.8, 1.6, 3.2, 6.4 s and more
    Cancellable c(make_es_bulk_sink(o));
    c.sink->open();
    c.sink->on_data(batch_of({R"({"a":1})"}));
    const auto [took, error] = c.cancel_during_flush(std::chrono::milliseconds{900});
    EXPECT_LT(took, std::chrono::milliseconds{1000});
    EXPECT_TRUE(contains(error, "cancelled while retrying")) << error;
    EXPECT_LE(srv.calls(), 4);

    BulkModel plain;
    for (int call = 1; call <= 20; ++call) {
        plain.script(call, replying(503, "busy", false));
    }
    BatchedHttpBulkSink<std::string>::Options po;
    po.http.base_url = plain.url();
    po.path = "/_plain";
    po.max_retries = 10;
    Cancellable p(std::make_shared<BatchedHttpBulkSink<std::string>>(
        po, [](std::string& out, const std::string& rec) { out += rec; }));
    p.sink->open();
    p.sink->on_data(batch_of({R"({"a":1})"}));
    const auto [took2, error2] = p.cancel_during_flush(std::chrono::milliseconds{900});
    EXPECT_LT(took2, std::chrono::milliseconds{1000});
    EXPECT_TRUE(contains(error2, "cancelled while retrying")) << error2;
}

TEST(ElasticsearchSink, RequiresUrlAndIndex) {
    EsBulkOptions o;
    o.index = "logs";  // no url
    EXPECT_THROW(make_es_bulk_sink(o), std::runtime_error);
    EsBulkOptions o2;
    o2.url = "http://x:1";  // no index
    EXPECT_THROW(make_es_bulk_sink(o2), std::runtime_error);
}
