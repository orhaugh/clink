// Elasticsearch and OpenSearch LIVE tests of the bulk sink: one instantiation
// per product, each SKIPPED unless its server is named:
//
//   Elasticsearch/SearchSinkLive.*   CLINK_ELASTICSEARCH_TEST_ENDPOINT
//   OpenSearch/SearchSinkLive.*      CLINK_OPENSEARCH_TEST_ENDPOINT
//
// scripts/search-live.sh starts the pinned server of each and names it. Every
// case first checks that the server is the product it was named as, and reads
// what the index holds back through its own client: the documents' contents,
// not only how many there are.
//
// Where a case needs a fault the server will not produce on demand, the sink
// writes through a proxy, and the case says so: BulkFaultProxy withholds an
// operation and reports it failed, garbles a reply after the server applied the
// request, or refuses requests with a status; TcpFaultProxy withholds a reply
// after the server applied the request and then resets the connection. What the
// server holds afterwards is still read from the server. The rest (index-level
// refusals, mapping failures, the whole-request 400 for a long _id) is the
// server's own behaviour. Each case prints one "[search-live]" line naming what
// it verified.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/base64.hpp"
#include "clink/http_connector/bulk_sink_builders.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "tests/integration/bulk_fault_proxy.hpp"
#include "tests/integration/docker_command.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::config::JsonValue;
using clink::http_connector::BatchedHttpBulkSink;
using clink::http_connector::DlqPolicy;
using clink::http_connector::EsBulkOptions;
using clink::http_connector::HttpRequest;
using clink::http_connector::HttpResponse;
using clink::http_connector::make_es_bulk_sink;
using clink::itest::BulkFaultProxy;
using clink::itest::TcpFaultProxy;
using namespace std::chrono_literals;

namespace {

struct Product {
    const char* name;           // "elasticsearch" or "opensearch"
    const char* env;            // the variable naming its server
    const char* sink;           // the sink's factory name, which its metrics carry
    const char* throttled_env;  // the variable naming its throttled server
    const char* secured;        // the prefix of the variables naming its secured server
    const char* cluster;        // the prefix of the variables naming its three-node cluster
};

const Product kElasticsearch{"elasticsearch",
                             "CLINK_ELASTICSEARCH_TEST_ENDPOINT",
                             "elasticsearch_sink",
                             "CLINK_ELASTICSEARCH_THROTTLED_ENDPOINT",
                             "CLINK_ELASTICSEARCH_SECURED",
                             "CLINK_ELASTICSEARCH_CLUSTER"};
const Product kOpenSearch{"opensearch",
                          "CLINK_OPENSEARCH_TEST_ENDPOINT",
                          "opensearch_sink",
                          "CLINK_OPENSEARCH_THROTTLED_ENDPOINT",
                          "CLINK_OPENSEARCH_SECURED",
                          "CLINK_OPENSEARCH_CLUSTER"};

Batch<std::string> batch_of(const std::vector<std::string>& recs) {
    Batch<std::string> b;
    for (const auto& r : recs) {
        b.emplace(r);
    }
    return b;
}

// Polls `cond` until it holds or `timeout` passes.
template <typename Cond>
bool await_until(Cond&& cond, std::chrono::milliseconds timeout) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < until) {
        if (cond()) {
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return cond();
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

// A JSON text as this repository prints it, so two texts compare as values.
std::string canon(const std::string& json) {
    return clink::config::parse(json).serialize(0);
}

// A sink attached to a runtime context whose cancel signal the case controls.
struct Cancellable {
    explicit Cancellable(std::shared_ptr<clink::Sink<std::string>> s) : sink(std::move(s)) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4343}, "search_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink->attach_runtime(ctx.get());
    }
    ~Cancellable() { sink->attach_runtime(nullptr); }
    std::shared_ptr<clink::Sink<std::string>> sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

class SearchSinkLive : public ::testing::TestWithParam<Product> {
protected:
    void SetUp() override { set_up_against(GetParam().env); }

    // The server `env` names, checked to be the product named.
    void set_up_against(const char* env) {
        const char* e = std::getenv(env);
        if (e == nullptr || *e == '\0') {
            GTEST_SKIP() << "set " << env << " (scripts/search-live.sh does)";
        }
        endpoint_ = e;
        auto c = client();
        const auto r = c.get("/");
        ASSERT_EQ(r.status, 200) << "the server at " << endpoint_ << " did not answer: " << r.error
                                 << r.body;
        const auto j = clink::config::parse(r.body);
        const auto& version = j.as_object().at("version").as_object();
        const auto dist = version.find("distribution");
        const bool opensearch = dist != version.end() && dist->second.is_string() &&
                                dist->second.as_string() == "opensearch";
        ASSERT_EQ(opensearch, std::string{GetParam().name} == "opensearch")
            << env << " names a server that is not " << GetParam().name << ": " << r.body;
        version_ = version.at("number").as_string();
        index_ = new_index_name();
    }

    void TearDown() override {
        if (endpoint_.empty()) {
            return;
        }
        auto c = client();
        for (const auto& idx : created_) {
            c.del("/" + idx);
        }
        if (auto_create_off_) {
            c.put("/_cluster/settings",
                  R"({"persistent":{"action.auto_create_index":null}})",
                  "application/json");
        }
    }

    std::string new_index_name() {
        static int n = 0;
        auto name = std::string{"clink-it-"} + GetParam().name + "-" +
                    std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(n++);
        created_.push_back(name);
        return name;
    }

    HttpRequest client() const {
        HttpRequest::Options o;
        o.base_url = endpoint_;
        return HttpRequest{o};
    }

    std::uint16_t endpoint_port() const {
        return static_cast<std::uint16_t>(std::stoi(endpoint_.substr(endpoint_.rfind(':') + 1)));
    }

    EsBulkOptions options(const std::string& url, const std::string& index) const {
        EsBulkOptions o;
        o.url = url;
        o.index = index;
        o.document_id = "id";
        o.batch_records = 1000;
        o.max_retries = 3;
        o.name = GetParam().sink;
        return o;
    }

    std::string dropped_metric() const {
        return std::string{R"(clink_connector_dropped_records_total{connector=")"} +
               GetParam().sink + R"(",direction="sink"})";
    }

    // What `index` holds: each document's _id and its source, as values. A
    // read that gets no response is tried again a few times: it is the case's
    // own client, not the sink, so retrying it hides nothing about the sink.
    std::map<std::string, std::string> stored(const std::string& index) {
        HttpResponse r;
        for (int attempt = 0; attempt < 5; ++attempt) {
            if (attempt > 0) {
                std::this_thread::sleep_for(500ms);
            }
            auto c = client();
            c.post("/" + index + "/_refresh", "", "application/json");
            r = c.post("/" + index + "/_search",
                       R"({"query":{"match_all":{}},"size":10000})",
                       "application/json");
            if (r.status != 0) {
                break;
            }
        }
        std::map<std::string, std::string> out;
        if (r.status == 404) {
            return out;  // not created yet
        }
        if (r.status != 200) {
            ADD_FAILURE() << "reading " << index << " failed: " << r.status << " " << r.error << " "
                          << r.body;
            return out;
        }
        const auto j = clink::config::parse(r.body);
        for (const auto& hit : j.as_object().at("hits").as_object().at("hits").as_array()) {
            const auto& h = hit.as_object();
            out[h.at("_id").as_string()] = h.at("_source").serialize(0);
        }
        return out;
    }

    // The documents a list of records leaves, with document_id: each id's
    // latest record, as a value.
    static std::map<std::string, std::string> latest_by_id(const std::vector<std::string>& recs) {
        std::map<std::string, std::string> out;
        for (const auto& r : recs) {
            const auto j = clink::config::parse(r);
            const auto& id = j.as_object().at("id");
            out[id.is_string() ? id.as_string() : id.serialize(0)] = j.serialize(0);
        }
        return out;
    }

    void report(const char* what, const std::string& detail) const {
        std::printf("[search-live] product=%s version=%s case=%s %s\n",
                    GetParam().name,
                    version_.c_str(),
                    what,
                    detail.c_str());
        std::fflush(stdout);
    }

    std::string endpoint_;
    std::string version_;
    std::string index_;
    std::vector<std::string> created_;
    bool auto_create_off_{false};
};

std::vector<std::string> keyed_records(int n, int ids) {
    std::vector<std::string> out;
    for (int i = 0; i < n; ++i) {
        out.push_back(R"({"id":"k)" + std::to_string(i % ids) + R"(","v":)" + std::to_string(i) +
                      R"(,"pad":"0123456789abcdef"})");
    }
    return out;
}

}  // namespace

TEST_P(SearchSinkLive, EveryRecordIsStoredAsSent) {
    std::vector<std::string> recs;
    for (int i = 0; i < 300; ++i) {
        // String and integer ids, nested values and a unicode string.
        recs.push_back(i % 2 == 0 ? R"({"id":"s)" + std::to_string(i) + R"(","n":)" +
                                        std::to_string(i) + R"(,"tags":["a","b"],"name":"café"})"
                                  : R"({"id":)" + std::to_string(i) + R"(,"n":)" +
                                        std::to_string(i) + R"(,"nested":{"x":1.5,"y":null}})");
    }
    auto o = options(endpoint_, index_);
    o.batch_records = 64;  // several flushes by size, then the rest by flush()
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    const auto got = stored(index_);
    std::map<std::string, std::string> want;
    for (std::size_t i = 0; i < recs.size(); ++i) {
        want[(i % 2 == 0 ? "s" : "") + std::to_string(i)] = canon(recs[i]);
    }
    EXPECT_EQ(got, want);
    report("every_record_stored_as_sent",
           std::to_string(got.size()) + " documents read back, each equal to its record");
}

TEST_P(SearchSinkLive, RepeatedIdsEndAtTheirLatestRecordWithinAndAcrossBatches) {
    const auto recs = keyed_records(500, 37);
    auto o = options(endpoint_, index_);
    o.batch_records = 23;  // ids repeat inside batches and across their boundaries
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    const auto got = stored(index_);
    EXPECT_EQ(got, latest_by_id(recs));
    report("repeated_ids_end_at_latest",
           "500 records over 37 ids in batches of 23: each document is its id's latest record");
}

// The repeated-key failure on a real server: the earlier of two writes to one
// document fails transiently (the proxy withholds it from the server and reports
// 429), the later one is written. The sink resends the earlier write with the
// later one, so the document ends at the later value. The control resends the
// failed write alone, as the sink used to, through the same fault: the server
// then ends at the older value.
TEST_P(SearchSinkLive, AWriteFailedForThrottlingIsResentWithTheLaterWritesOfItsDocument) {
    const std::vector<std::string> recs = {
        R"({"id":"k","v":1})", R"({"id":"j","v":1})", R"({"id":"k","v":2})"};
    BulkFaultProxy proxy(endpoint_);
    proxy.withhold(0, 429, "es_rejected_execution_exception");
    auto sink = make_es_bulk_sink(options(proxy.url(), index_));
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    EXPECT_EQ(proxy.faults_applied(), 1U);
    const auto bodies = proxy.bodies();
    ASSERT_EQ(bodies.size(), 2U);
    const auto a = bodies[1].find(R"({"id":"k","v":1})");
    const auto b = bodies[1].find(R"({"id":"k","v":2})");
    EXPECT_TRUE(a != std::string::npos && b != std::string::npos && a < b) << bodies[1];
    const auto got = stored(index_);
    EXPECT_EQ(got.at("k"), canon(R"({"id":"k","v":2})"));
    EXPECT_EQ(got.at("j"), canon(R"({"id":"j","v":1})"));

    // The control: the same fault, the failed write resent alone.
    const auto control_index = new_index_name();
    BulkFaultProxy control_proxy(endpoint_);
    control_proxy.withhold(0, 429, "es_rejected_execution_exception");
    BatchedHttpBulkSink<std::string>::Options po;
    po.http.base_url = control_proxy.url();
    po.path = "/_bulk";
    po.content_type = "application/x-ndjson";
    po.framing = clink::http_connector::BulkFraming::Ndjson;
    po.response_handler = [](const HttpResponse& r, std::size_t count) {
        return clink::http_connector::es_bulk_result(r, count);
    };
    auto unkeyed = std::make_shared<BatchedHttpBulkSink<std::string>>(
        po, [&control_index](std::string& out, const std::string& rec) {
            const auto id = clink::config::parse(rec).as_object().at("id").as_string();
            out += R"({"index":{"_index":")" + control_index + R"(","_id":")" + id + "\"}}\n" + rec;
        });
    unkeyed->open();
    unkeyed->on_data(batch_of(recs));
    unkeyed->flush();
    const auto control = stored(control_index);
    EXPECT_EQ(control.at("k"), canon(R"({"id":"k","v":1})"))
        << "the control did not reproduce the stale overwrite, so the case proves nothing";
    report("throttled_write_resent_with_later_writes",
           "proxy withheld k v1 with 429; the resend carried k v1 then k v2; k stored v2; control "
           "(failed write resent alone) left k at v1");
}

// A reply lost after the server applied the batch: the TCP proxy forwards the
// request whole, the server applies it, the proxy withholds the reply and then
// resets the connection. The sink resends: with document_id the documents end
// as their records, once each; without it the resend indexes every record a
// second time, which is the at-least-once the sink documents.
TEST_P(SearchSinkLive, AReplyLostAfterTheServerAppliedTheBatchIsResent) {
    for (const bool with_id : {true, false}) {
        const auto index = with_id ? index_ : new_index_name();
        std::vector<std::string> recs;
        for (int i = 0; i < 50; ++i) {
            recs.push_back(R"({"id":"r)" + std::to_string(i) + R"(","v":)" + std::to_string(i) +
                           "}");
        }
        TcpFaultProxy proxy(endpoint_port());
        proxy.set_request_marker("POST /_bulk");
        proxy.hold_at_upload(1);
        auto o = options("http://127.0.0.1:" + std::to_string(proxy.port()), index);
        if (!with_id) {
            o.document_id.clear();
        }
        auto sink = make_es_bulk_sink(o);
        sink->open();
        sink->on_data(batch_of(recs));
        std::string error;
        std::thread flusher([&] {
            try {
                sink->flush();
            } catch (const std::exception& e) {
                error = e.what();
            }
        });
        const bool held = await_until(
            [&] { return proxy.uploads_held() == 1 && proxy.held_clients_quiet(300ms); }, 30s);
        // The server applied the batch while its reply was held.
        const bool applied_before_loss =
            held && await_until([&] { return stored(index).size() == 50; }, 30s);
        proxy.drop_held();
        flusher.join();
        ASSERT_TRUE(held) << "the proxy never held the bulk request";
        ASSERT_TRUE(applied_before_loss) << "the server had not applied the batch";
        EXPECT_TRUE(error.empty()) << error;
        const auto got = stored(index);
        if (with_id) {
            EXPECT_EQ(got, latest_by_id(recs));
        } else {
            EXPECT_EQ(got.size(), 100U) << "without document_id the resend indexes again";
        }
    }
    report("reply_lost_after_apply",
           "tcp proxy held the reply after the server applied 50 records, then reset; resent: with "
           "document_id 50 documents equal to their records, without it 100");
}

// A 2xx that confirms nothing (the proxy forwards the request, the server
// applies it, and the reply is cut in half): the flush fails once its retries
// run out, nothing is dead-lettered even under dlq=drop, and the next flush
// sends the same batch again.
TEST_P(SearchSinkLive, AnUnconfirmedReplyNeverClearsTheBatch) {
    const auto recs = keyed_records(20, 20);
    BulkFaultProxy proxy(endpoint_);
    proxy.garble(2);
    auto o = options(proxy.url(), index_);
    o.dlq_policy = DlqPolicy::Drop;
    o.max_retries = 1;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(dropped_metric());
    sink->on_data(batch_of(recs));
    try {
        sink->flush();
        FAIL() << "a flush whose every reply confirms nothing succeeded";
    } catch (const std::exception& e) {
        EXPECT_TRUE(contains(e.what(), "does not confirm the batch")) << e.what();
    }
    EXPECT_EQ(counter_value(dropped_metric()) - before, 0U);
    EXPECT_EQ(proxy.requests(), 2U);
    sink->flush();
    const auto bodies = proxy.bodies();
    ASSERT_EQ(bodies.size(), 3U);
    EXPECT_EQ(bodies[2], bodies[0]) << "the pending batch was not the one sent again";
    EXPECT_EQ(stored(index_), latest_by_id(recs));
    report("unconfirmed_reply_keeps_batch",
           "proxy garbled 2 replies after the server applied them; flush failed, 0 dead-lettered; "
           "the next flush resent the same batch; 20 documents equal to their records");
}

// An index that refuses every write, for reasons about the index rather than any
// record, as the servers report them: a closed index (400 index_closed_exception),
// a write block (403), a missing index with auto-creation off (404) and a name
// that is not valid (400 invalid_index_name_exception). Under dlq=drop none of it
// is dead-lettered: the flush fails, and once the index takes writes again the
// same records are written.
TEST_P(SearchSinkLive, AnIndexThatRefusesWritesFailsTheFlushAndDropsNothing) {
    auto c = client();
    std::string seen;
    const auto refused_then_written = [&](const std::string& index,
                                          const std::function<void()>& heal) {
        const auto recs = keyed_records(10, 10);
        auto o = options(endpoint_, index);
        o.dlq_policy = DlqPolicy::Drop;
        o.max_retries = 1;
        auto sink = make_es_bulk_sink(o);
        sink->open();
        const auto before = counter_value(dropped_metric());
        sink->on_data(batch_of(recs));
        try {
            sink->flush();
            ADD_FAILURE() << "writes to " << index << " were accepted";
        } catch (const std::exception& e) {
            seen += std::string{" "} + e.what();
        }
        EXPECT_EQ(counter_value(dropped_metric()) - before, 0U) << index;
        if (heal) {
            heal();
            sink->flush();
            EXPECT_EQ(stored(index), latest_by_id(recs)) << index;
        }
    };

    ASSERT_EQ(c.put("/" + index_, "", "application/json").status, 200);
    ASSERT_EQ(c.post("/" + index_ + "/_close", "", "application/json").status, 200);
    refused_then_written(index_, [&] {
        ASSERT_EQ(c.post("/" + index_ + "/_open", "", "application/json").status, 200);
        await_until(
            [&] {
                return c.get("/_cluster/health/" + index_ + "?wait_for_status=yellow&timeout=5s")
                           .status == 200;
            },
            30s);
    });
    EXPECT_TRUE(contains(seen, "index_closed_exception")) << seen;

    const auto blocked = new_index_name();
    ASSERT_EQ(c.put("/" + blocked, "", "application/json").status, 200);
    ASSERT_EQ(
        c.put("/" + blocked + "/_settings", R"({"index.blocks.write":true})", "application/json")
            .status,
        200);
    refused_then_written(blocked, [&] {
        ASSERT_EQ(c.put("/" + blocked + "/_settings",
                        R"({"index.blocks.write":false})",
                        "application/json")
                      .status,
                  200);
    });
    EXPECT_TRUE(contains(seen, "cluster_block_exception")) << seen;

    const auto missing = new_index_name();
    ASSERT_EQ(c.put("/_cluster/settings",
                    R"({"persistent":{"action.auto_create_index":"false"}})",
                    "application/json")
                  .status,
              200);
    auto_create_off_ = true;
    refused_then_written(
        missing, [&] { ASSERT_EQ(c.put("/" + missing, "", "application/json").status, 200); });
    EXPECT_TRUE(contains(seen, "index_not_found_exception")) << seen;
    ASSERT_EQ(c.put("/_cluster/settings",
                    R"({"persistent":{"action.auto_create_index":null}})",
                    "application/json")
                  .status,
              200);
    auto_create_off_ = false;

    refused_then_written("Not-A-Valid-Index-Name", {});
    EXPECT_TRUE(contains(seen, "invalid_index_name_exception")) << seen;
    report("index_refusing_writes",
           "closed (400 index_closed_exception), write block (403), missing (404), invalid name "
           "(400 invalid_index_name_exception): each flush failed, 0 dead-lettered; once the index "
           "took writes the same records were stored");
}

// A record the index refuses as a document (a value its mapping cannot hold) is
// dead-lettered under dlq=drop while the rest are stored, and fails the sink
// under the default policy. Elasticsearch names it document_parsing_exception,
// OpenSearch mapper_parsing_exception; both answer 400.
TEST_P(SearchSinkLive, ARecordTheIndexRefusesIsDeadLetteredAndTheRestStored) {
    auto c = client();
    const auto created = c.put("/" + index_,
                               R"({"mappings":{"properties":{"n":{"type":"integer"}}}})",
                               "application/json");
    ASSERT_EQ(created.status, 200) << created.body;
    const std::vector<std::string> recs = {
        R"({"id":"good","n":7})", R"({"id":"bad","n":"not-an-int"})", R"({"id":"good2","n":8})"};
    auto o = options(endpoint_, index_);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(dropped_metric());
    sink->on_data(batch_of(recs));
    EXPECT_NO_THROW(sink->flush());
    EXPECT_EQ(counter_value(dropped_metric()) - before, 1U);
    const auto got = stored(index_);
    EXPECT_EQ(
        got,
        (std::map<std::string, std::string>{{"good", canon(recs[0])}, {"good2", canon(recs[2])}}));

    auto strict = make_es_bulk_sink(options(endpoint_, index_));
    strict->open();
    strict->on_data(batch_of({recs[1]}));
    try {
        strict->flush();
        FAIL() << "a refused record did not fail the sink under dlq=fail";
    } catch (const std::exception& e) {
        EXPECT_TRUE(contains(e.what(), "permanently rejected")) << e.what();
        EXPECT_TRUE(contains(e.what(), "parsing_exception")) << e.what();
    }
    report("record_refused_by_mapping",
           "1 of 3 dead-lettered under drop, the other 2 stored as sent; fails under fail");
}

// Records the server would refuse for the whole request (an _id over 512 bytes)
// or that would break the request (a record spanning lines), and a record that is
// not a JSON object, are refused before they are sent: the rest of the batch is
// stored. The control shows the server's own answer to a long _id: a 400 for the
// whole request, which used to fail every record sent with it.
TEST_P(SearchSinkLive, RecordsTheServerWouldRefuseForTheWholeRequestAreRefusedAlone) {
    auto c = client();
    const auto control =
        c.post("/_bulk",
               R"({"index":{"_index":")" + index_ + R"(","_id":")" + std::string(513, 'x') +
                   "\"}}\n" + R"({"v":1})" + "\n" + R"({"index":{"_index":")" + index_ +
                   R"(","_id":"fine"}})" + "\n" + R"({"v":2})" + "\n",
               "application/x-ndjson");
    EXPECT_EQ(control.status, 400) << "the server took a long _id: " << control.body;
    const std::vector<std::string> recs = {R"({"id":"good1","v":1})",
                                           R"({"id":")" + std::string(513, 'x') + R"(","v":2})",
                                           "{\"id\":\"multi\",\n\"v\":3}",
                                           "42",
                                           R"({"id":"good2","v":5})"};
    auto o = options(endpoint_, index_);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto before = counter_value(dropped_metric());
    sink->on_data(batch_of(recs));
    sink->flush();
    EXPECT_EQ(counter_value(dropped_metric()) - before, 3U);
    EXPECT_EQ(
        stored(index_),
        (std::map<std::string, std::string>{{"good1", canon(recs[0])}, {"good2", canon(recs[4])}}));
    report("whole_request_refusals_refused_alone",
           "control: a 513-byte _id got HTTP " + std::to_string(control.status) +
               " for the whole request; through the sink the long id, the multi-line record and a "
               "non-object were refused alone and the other 2 stored");
}

TEST_P(SearchSinkLive, DocumentIdsAreStoredUnderTheIdsTheContractGives) {
    const std::vector<std::string> recs = {R"({"id":9007199254740992,"v":1})",
                                           R"({"id":9007199254740993,"v":2})",
                                           R"({"id":9007199254740993.0,"v":3})",
                                           R"({"id":18446744073709551615,"v":4})",
                                           R"({"id":1e19,"v":5})",
                                           R"({"id":0.5,"v":6})",
                                           R"({"id":"0.500000","v":7})",
                                           R"({"id":true,"v":8})"};
    // The ids' values differ in type, so the index maps no field: a dynamic
    // mapping would type `id` from the first record and refuse the others as
    // documents, which is not what this case is about.
    auto c = client();
    const auto created =
        c.put("/" + index_, R"({"mappings":{"dynamic":false}})", "application/json");
    ASSERT_EQ(created.status, 200) << created.body;
    auto sink = make_es_bulk_sink(options(endpoint_, index_));
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    const auto got = stored(index_);
    const std::map<std::string, std::string> want = {
        {"9007199254740992", canon(recs[0])},
        {"9007199254740993", canon(recs[2])},
        {"18446744073709551615", canon(recs[3])},
        {"10000000000000000000", canon(recs[4])},
        {"0.500000", canon(recs[6])},
        {"true", canon(recs[7])},
    };
    EXPECT_EQ(got, want);
    report("document_id_contract",
           "8 records, 6 documents under the contract's ids; 9007199254740993 and "
           "9007199254740993.0 one document, 0.5 and \"0.500000\" one document");
}

// Whole-request throttling (the proxy answers 429 without forwarding): retried
// with backoff and written once it ends; past max_retries the flush fails in
// bounded time, nothing dead-lettered, and the batch is still pending.
TEST_P(SearchSinkLive, ThrottlingIsRetriedThenFailsBoundedWithTheBatchPending) {
    const std::string throttled =
        R"({"error":{"type":"es_rejected_execution_exception","reason":"rejected"},"status":429})";
    const auto recs = keyed_records(30, 30);
    BulkFaultProxy proxy(endpoint_);
    proxy.refuse(2, 429, throttled);
    auto o = options(proxy.url(), index_);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    EXPECT_EQ(proxy.requests(), 3U);
    EXPECT_EQ(stored(index_), latest_by_id(recs));

    const auto index2 = new_index_name();
    BulkFaultProxy proxy2(endpoint_);
    proxy2.refuse(100, 429, throttled);
    auto o2 = options(proxy2.url(), index2);
    o2.dlq_policy = DlqPolicy::Drop;
    o2.max_retries = 2;  // pauses of 200 and 400 ms
    auto sink2 = make_es_bulk_sink(o2);
    sink2->open();
    const auto before = counter_value(dropped_metric());
    sink2->on_data(batch_of(recs));
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_THROW(sink2->flush(), std::runtime_error);
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, 600ms);
    EXPECT_LT(took, 10s);
    EXPECT_EQ(proxy2.requests(), 3U);
    EXPECT_EQ(counter_value(dropped_metric()) - before, 0U);
    proxy2.clear();
    sink2->flush();
    EXPECT_EQ(stored(index2), latest_by_id(recs));
    report("throttling",
           "proxy answered 2 requests 429: written on the third; answered every request 429: "
           "failed after 3 attempts in " +
               std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(took).count()) +
               " ms, 0 dead-lettered, the batch written once throttling ended");
}

// A flush retrying against an endpoint that keeps failing stops at the next pause
// once its task is cancelled, and its batch stays pending.
TEST_P(SearchSinkLive, CancellationStopsARetryingFlush) {
    const auto recs = keyed_records(10, 10);
    BulkFaultProxy proxy(endpoint_);
    proxy.refuse(100, 503, R"({"error":{"type":"unavailable","reason":"down"},"status":503})");
    auto o = options(proxy.url(), index_);
    o.max_retries = 10;
    Cancellable c(make_es_bulk_sink(o));
    c.sink->open();
    c.sink->on_data(batch_of(recs));
    std::string error;
    std::thread flusher([&] {
        try {
            c.sink->flush();
        } catch (const std::exception& e) {
            error = e.what();
        }
    });
    std::this_thread::sleep_for(900ms);
    const auto t0 = std::chrono::steady_clock::now();
    c.cancel->store(true);
    flusher.join();
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(took, 1s);
    EXPECT_TRUE(contains(error, "cancelled while retrying")) << error;
    c.cancel->store(false);
    proxy.clear();
    c.sink->flush();
    EXPECT_EQ(stored(index_), latest_by_id(recs));
    report("cancellation",
           "cancelled 900 ms into a retrying flush: returned in " +
               std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(took).count()) +
               " ms; the pending batch written after");
}

// A request the server never answers (the TCP proxy swallows it) is interrupted
// once the task is cancelled, instead of holding the task for the 30 s read
// timeout; the batch stays pending and is written once the server answers.
TEST_P(SearchSinkLive, CancellationInterruptsARequestInFlight) {
    const auto recs = keyed_records(10, 10);
    TcpFaultProxy proxy(endpoint_port());
    proxy.set_blackhole(true);
    Cancellable c(
        make_es_bulk_sink(options("http://127.0.0.1:" + std::to_string(proxy.port()), index_)));
    c.sink->open();
    c.sink->on_data(batch_of(recs));
    std::string error;
    std::thread flusher([&] {
        try {
            c.sink->flush();
        } catch (const std::exception& e) {
            error = e.what();
        }
    });
    std::this_thread::sleep_for(500ms);
    const auto t0 = std::chrono::steady_clock::now();
    c.cancel->store(true);
    flusher.join();
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(took, 1s);
    EXPECT_TRUE(contains(error, "cancelled while a POST")) << error;
    proxy.set_blackhole(false);
    c.cancel->store(false);
    c.sink->flush();
    EXPECT_EQ(stored(index_), latest_by_id(recs));
    report("cancellation_in_flight",
           "tcp proxy swallowed the request; cancelled after 500 ms: returned in " +
               std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(took).count()) +
               " ms rather than at the 30 s read timeout; the pending batch written after");
}

// The server's own throttling, against a server of the product started with a
// write pool of one thread and a write queue of one (CLINK_<PRODUCT>_THROTTLED_
// ENDPOINT; scripts/search-live.sh starts it). A bulk request fans out one write
// per shard, and the server rejects those beyond the two it can hold with 429,
// item by item (es_rejected_execution_exception from Elasticsearch,
// rejected_execution_exception from OpenSearch). Nothing is injected: the sink
// writes through a BulkFaultProxy with no fault armed, which records the server's
// replies so a case can count the 429s it returned.
class SearchSinkThrottledLive : public SearchSinkLive {
protected:
    void SetUp() override { set_up_against(GetParam().throttled_env); }

    // An index whose writes fan out over eight shards.
    void create_sharded(const std::string& index) {
        auto c = client();
        const auto r = c.put("/" + index,
                             R"({"settings":{"number_of_shards":8,"number_of_replicas":0}})",
                             "application/json");
        ASSERT_EQ(r.status, 200) << r.body;
    }
};

TEST_P(SearchSinkThrottledLive, TheServersThrottlingIsRetriedUntilEveryRecordIsStored) {
    create_sharded(index_);
    const auto recs = keyed_records(2000, 400);
    BulkFaultProxy proxy(endpoint_);
    auto o = options(proxy.url(), index_);
    o.max_retries = 12;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    const auto throttled = proxy.upstream_items_with_status(429);
    EXPECT_GT(throttled, 0U) << "the server never throttled a write, so the case proves nothing";
    EXPECT_EQ(proxy.faults_applied(), 0U);
    EXPECT_EQ(stored(index_), latest_by_id(recs));
    report("server_throttling_retried",
           "the server answered " + std::to_string(throttled) + " item(s) 429 over " +
               std::to_string(proxy.requests()) +
               " requests; 2000 records over 400 ids stored, each id at its latest record");
}

TEST_P(SearchSinkThrottledLive, ThrottlingPastTheRetriesFailsTheFlushAndDropsNothing) {
    create_sharded(index_);
    BulkFaultProxy proxy(endpoint_);
    auto o = options(proxy.url(), index_);
    o.max_retries = 0;
    o.dlq_policy = DlqPolicy::Drop;
    const auto before = counter_value(dropped_metric());
    // Each round sends a fresh batch once, until the server throttles one.
    std::vector<std::string> all;
    std::vector<std::string> last_round;
    std::shared_ptr<clink::Sink<std::string>> pending;
    for (int round = 0; round < 10 && !pending; ++round) {
        auto recs = keyed_records(400, 400);
        for (auto& r : recs) {
            r.insert(r.size() - 1, R"(,"round":)" + std::to_string(round));
        }
        auto sink = make_es_bulk_sink(o);
        sink->open();
        sink->on_data(batch_of(recs));
        try {
            sink->flush();
        } catch (const std::exception& e) {
            // Elasticsearch names it es_rejected_execution_exception, OpenSearch
            // rejected_execution_exception.
            EXPECT_TRUE(contains(e.what(), "rejected_execution_exception")) << e.what();
            pending = sink;
        }
        all.insert(all.end(), recs.begin(), recs.end());
        last_round = recs;
    }
    ASSERT_TRUE(pending) << "the server never throttled a flush";
    EXPECT_EQ(counter_value(dropped_metric()) - before, 0U);
    // The failed flush kept its whole batch: the next flush sends it again as it
    // was. With max_retries=0 every flush resends the whole batch, which this
    // server throttles again, so a sink that retries writes it instead.
    const auto sent = proxy.requests();
    try {
        pending->flush();
    } catch (const std::exception&) {
    }
    const auto bodies = proxy.bodies();
    ASSERT_GT(bodies.size(), sent);
    EXPECT_EQ(bodies[sent], bodies[sent - 1]) << "the pending batch was not sent again whole";
    auto retrying = options(proxy.url(), index_);
    retrying.max_retries = 12;
    auto writer = make_es_bulk_sink(retrying);
    writer->open();
    writer->on_data(batch_of(last_round));
    writer->flush();
    EXPECT_EQ(stored(index_), latest_by_id(all));
    report("server_throttling_exhausted",
           "with max_retries=0 a throttled flush failed, 0 dead-lettered, and the next flush "
           "sent the same batch again; " +
               std::to_string(proxy.upstream_items_with_status(429)) +
               " item(s) answered 429 by the server");
}

// The sink against a server with its own security on (CLINK_<PRODUCT>_SECURED_
// ENDPOINT, _CA, _USER, _PASSWORD; scripts/search-live.sh starts it with a password
// generated for the run and passed only through the environment): HTTPS with a
// certificate from the server's own CA, and every request authenticated.
// Elasticsearch configures both itself; OpenSearch's security plugin installs its
// demo certificates, which name 127.0.0.1. Each case makes its own users (and,
// on Elasticsearch, an API key) through the administrator's credentials, with
// passwords generated in the case, and removes them. No credential is printed:
// the cases assert that the sink's errors repeat none of them.
class SearchSinkSecuredLive : public ::testing::TestWithParam<Product> {
protected:
    void SetUp() override {
        const std::string prefix = GetParam().secured;
        endpoint_ = env_or(prefix + "_ENDPOINT");
        ca_ = env_or(prefix + "_CA");
        admin_user_ = env_or(prefix + "_USER");
        admin_password_ = env_or(prefix + "_PASSWORD");
        if (endpoint_.empty() || ca_.empty() || admin_user_.empty() || admin_password_.empty()) {
            GTEST_SKIP() << "set " << prefix << "_ENDPOINT, _CA, _USER and _PASSWORD "
                         << "(scripts/search-live.sh does)";
        }
        ASSERT_TRUE(HttpRequest::tls_supported()) << "this build has no TLS support";
        auto a = admin();
        const auto r = a.get("/");
        ASSERT_EQ(r.status, 200) << "the secured server did not answer its administrator: "
                                 << r.status << " " << r.error;
        const auto j = clink::config::parse(r.body);
        const auto& version = j.as_object().at("version").as_object();
        const auto dist = version.find("distribution");
        const bool opensearch = dist != version.end() && dist->second.is_string() &&
                                dist->second.as_string() == "opensearch";
        ASSERT_EQ(opensearch, is_opensearch())
            << prefix << " names a server that is not " << GetParam().name;
        version_ = version.at("number").as_string();
        static int n = 0;
        index_ = std::string{"clink-sec-"} + GetParam().name + "-" +
                 std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(n++);
    }

    void TearDown() override {
        if (endpoint_.empty()) {
            return;
        }
        auto a = admin();
        a.del("/" + index_);
        for (const auto& path : cleanup_) {
            a.del(path);
        }
    }

    static std::string env_or(const std::string& name) {
        const char* v = std::getenv(name.c_str());
        return v == nullptr ? std::string{} : std::string{v};
    }

    bool is_opensearch() const { return std::string{GetParam().name} == "opensearch"; }

    static std::string basic(const std::string& user, const std::string& password) {
        return "Basic " + clink::base64_encode(user + ":" + password);
    }

    // A password the server's policy takes: mixed case, a digit, a symbol.
    static std::string new_password() {
        static std::mt19937_64 rng{std::random_device{}()};
        const std::string alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";
        std::string pw = "Aa7!";
        for (int i = 0; i < 24; ++i) {
            pw += alphabet[rng() % alphabet.size()];
        }
        return pw;
    }

    HttpRequest admin() const {
        HttpRequest::Options o;
        o.base_url = endpoint_;
        o.ca_cert_path = ca_;
        o.headers["Authorization"] = basic(admin_user_, admin_password_);
        return HttpRequest{o};
    }

    // A user who may write (or with `write` false, only read) this case's indices,
    // and with `api_keys` also make API keys for itself (Elasticsearch).
    void create_user(const std::string& name,
                     const std::string& password,
                     bool write,
                     bool api_keys = false) {
        auto a = admin();
        const std::string role = name + "-role";
        HttpResponse r;
        if (is_opensearch()) {
            const std::string actions = write ? R"(["crud","create_index"])" : R"(["read"])";
            const std::string cluster =
                write ? R"(["cluster_composite_ops"])" : R"(["cluster_composite_ops_ro"])";
            r = a.put(
                "/_plugins/_security/api/roles/" + role,
                R"({"cluster_permissions":)" + cluster +
                    R"(,"index_permissions":[{"index_patterns":["clink-sec-*"],"allowed_actions":)" +
                    actions + "}]}",
                "application/json");
            ASSERT_TRUE(r.status == 200 || r.status == 201) << r.status << " " << r.body;
            cleanup_.push_back("/_plugins/_security/api/roles/" + role);
            r = a.put("/_plugins/_security/api/internalusers/" + name,
                      R"({"password":")" + password + R"("})",
                      "application/json");
            ASSERT_TRUE(r.status == 200 || r.status == 201) << r.status;
            cleanup_.push_back("/_plugins/_security/api/internalusers/" + name);
            r = a.put("/_plugins/_security/api/rolesmapping/" + role,
                      R"({"users":[")" + name + R"("]})",
                      "application/json");
            ASSERT_TRUE(r.status == 200 || r.status == 201) << r.status << " " << r.body;
            cleanup_.push_back("/_plugins/_security/api/rolesmapping/" + role);
        } else {
            const std::string privileges =
                write ? R"(["create_index","auto_configure","write"])" : R"(["read"])";
            const std::string cluster = api_keys ? R"(["manage_own_api_key"])" : "[]";
            r = a.put("/_security/role/" + role,
                      R"({"cluster":)" + cluster +
                          R"(,"indices":[{"names":["clink-sec-*"],"privileges":)" + privileges +
                          "}]}",
                      "application/json");
            ASSERT_EQ(r.status, 200) << r.body;
            cleanup_.push_back("/_security/role/" + role);
            r = a.put("/_security/user/" + name,
                      R"({"password":")" + password + R"(","roles":[")" + role + R"("]})",
                      "application/json");
            ASSERT_EQ(r.status, 200) << r.status;
            cleanup_.push_back("/_security/user/" + name);
        }
    }

    EsBulkOptions options(const std::string& authorization) const {
        EsBulkOptions o;
        o.url = endpoint_;
        o.index = index_;
        o.document_id = "id";
        o.batch_records = 1000;
        o.max_retries = 1;
        o.dlq_policy = DlqPolicy::Drop;
        o.tls_ca_file = ca_;
        o.headers = "Authorization: " + authorization;
        o.name = GetParam().sink;
        return o;
    }

    std::string dropped_metric() const {
        return std::string{R"(clink_connector_dropped_records_total{connector=")"} +
               GetParam().sink + R"(",direction="sink"})";
    }

    std::map<std::string, std::string> stored() {
        auto a = admin();
        a.post("/" + index_ + "/_refresh", "", "application/json");
        const auto r = a.post("/" + index_ + "/_search",
                              R"({"query":{"match_all":{}},"size":10000})",
                              "application/json");
        std::map<std::string, std::string> out;
        if (r.status != 200) {
            return out;
        }
        const auto j = clink::config::parse(r.body);
        for (const auto& hit : j.as_object().at("hits").as_object().at("hits").as_array()) {
            const auto& h = hit.as_object();
            out[h.at("_id").as_string()] = h.at("_source").serialize(0);
        }
        return out;
    }

    // The sink's error for `recs` written with `o`: it must fail, dead-letter
    // nothing, and repeat none of `secrets`.
    std::string refused(const EsBulkOptions& o,
                        const std::vector<std::string>& recs,
                        const std::vector<std::string>& secrets) {
        const auto before = counter_value(dropped_metric());
        auto sink = make_es_bulk_sink(o);
        sink->open();
        sink->on_data(batch_of(recs));
        std::string error;
        try {
            sink->flush();
            ADD_FAILURE() << "the write was accepted";
        } catch (const std::exception& e) {
            error = e.what();
        }
        EXPECT_EQ(counter_value(dropped_metric()) - before, 0U);
        for (const auto& s : secrets) {
            EXPECT_EQ(error.find(s), std::string::npos) << "the sink's error repeats a credential";
        }
        return error;
    }

    void report(const char* what, const std::string& detail) const {
        std::printf("[search-live] product=%s version=%s case=%s %s\n",
                    GetParam().name,
                    version_.c_str(),
                    what,
                    detail.c_str());
        std::fflush(stdout);
    }

    std::string endpoint_;
    std::string ca_;
    std::string admin_user_;
    std::string admin_password_;
    std::string version_;
    std::string index_;
    std::vector<std::string> cleanup_;
};

TEST_P(SearchSinkSecuredLive, AWriterOverVerifiedTlsStoresEveryRecord) {
    const auto pw = new_password();
    create_user("clink-writer", pw, true);
    const auto recs = keyed_records(300, 60);
    auto sink = make_es_bulk_sink(options(basic("clink-writer", pw)));
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    std::map<std::string, std::string> want;
    for (const auto& r : recs) {
        const auto j = clink::config::parse(r);
        want[j.as_object().at("id").as_string()] = j.serialize(0);
    }
    EXPECT_EQ(stored(), want);
    report("secured_writer",
           "https verified against the server's CA, basic auth as a user who "
           "may only write clink-sec-*: 300 records, 60 documents at their latest");
}

// Elasticsearch: an API key the writer makes for itself. OpenSearch, whose
// security plugin has no API keys here: a second writer.
TEST_P(SearchSinkSecuredLive, AnApiKeyOrASecondUserWritesTheSame) {
    const auto pw = new_password();
    create_user("clink-writer", pw, true, /*api_keys=*/true);
    std::string authorization;
    if (is_opensearch()) {
        const auto pw2 = new_password();
        create_user("clink-writer2", pw2, true);
        authorization = basic("clink-writer2", pw2);
    } else {
        HttpRequest::Options o;
        o.base_url = endpoint_;
        o.ca_cert_path = ca_;
        o.headers["Authorization"] = basic("clink-writer", pw);
        HttpRequest as_writer(o);
        const auto r =
            as_writer.post("/_security/api_key", R"({"name":"clink-it-key"})", "application/json");
        ASSERT_EQ(r.status, 200) << r.status;
        const auto j = clink::config::parse(r.body);
        const auto& k = j.as_object();
        // The key lives as long as the secured server, which the gate removes.
        authorization = "ApiKey " + k.at("encoded").as_string();
    }
    const auto recs = keyed_records(20, 20);
    auto sink = make_es_bulk_sink(options(authorization));
    sink->open();
    sink->on_data(batch_of(recs));
    sink->flush();
    EXPECT_EQ(stored().size(), 20U);
    report(is_opensearch() ? "secured_second_user" : "secured_api_key",
           is_opensearch() ? "a second writer, over verified https: 20 documents"
                           : "an API key the writer made, over verified https: 20 documents");
}

TEST_P(SearchSinkSecuredLive, AWrongPasswordIsRetriedThenFailsWithoutRepeatingIt) {
    const auto pw = new_password();
    create_user("clink-writer", pw, true);
    const auto wrong = new_password();
    const auto header = basic("clink-writer", wrong);
    const auto error = refused(options(header), keyed_records(5, 5), {wrong, pw, header});
    EXPECT_TRUE(contains(error, "HTTP 401")) << error;
    EXPECT_TRUE(stored().empty());
    report("secured_wrong_password",
           "HTTP 401 for the whole request: retried, then the flush failed; 0 dead-lettered; the "
           "error names neither password nor the header");
}

TEST_P(SearchSinkSecuredLive, AUserWithoutWriteRightsFailsWithoutDeadLettering) {
    const auto pw = new_password();
    create_user("clink-reader", pw, false);
    const auto error = refused(options(basic("clink-reader", pw)), keyed_records(5, 5), {pw});
    EXPECT_TRUE(contains(error, "403")) << error;
    EXPECT_TRUE(stored().empty());
    report("secured_reader",
           "a user who may only read: 403, retried, then the flush failed; 0 dead-lettered");
}

TEST_P(SearchSinkSecuredLive, AnUntrustedCertificateFailsTheFlush) {
    const auto pw = new_password();
    create_user("clink-writer", pw, true);
    auto untrusted = options(basic("clink-writer", pw));
    untrusted.tls_ca_file.clear();  // the system's trust store, which lacks this CA
    const auto error = refused(untrusted, keyed_records(5, 5), {pw});
    EXPECT_TRUE(stored().empty());
    // verify_tls=false writes, without checking whose server it is.
    auto unverified = options(basic("clink-writer", pw));
    unverified.tls_ca_file.clear();
    unverified.verify_tls = false;
    auto sink = make_es_bulk_sink(unverified);
    sink->open();
    sink->on_data(batch_of(keyed_records(5, 5)));
    sink->flush();
    EXPECT_EQ(stored().size(), 5U);
    report("secured_untrusted_certificate",
           "without the server's CA the TLS handshake failed: retried, then the flush failed, 0 "
           "dead-lettered (" +
               error.substr(0, std::min<std::size_t>(error.size(), 160)) +
               "); verify_tls=false wrote without checking");
}

// The sink against a three-node cluster of the product (CLINK_<PRODUCT>_CLUSTER_
// ENDPOINT, node 1, and _NODES, the three containers in order; scripts/search-
// live.sh starts them from docker/search-cluster.yml). The sink writes to node 1
// only, as it takes one URL. Each case kills a node with `docker kill` while the
// sink writes, which is the server's own failure, not a proxy's: the sink writes
// through a BulkFaultProxy with no fault armed only so the case can count the
// item failures the cluster itself returned.
class SearchSinkClusterLive : public SearchSinkLive {
protected:
    void SetUp() override {
        const std::string prefix = GetParam().cluster;
        const char* nodes = std::getenv((prefix + "_NODES").c_str());
        if (nodes == nullptr || *nodes == '\0') {
            GTEST_SKIP() << "set " << prefix
                         << "_ENDPOINT and _NODES (scripts/search-live.sh does)";
        }
        std::string list = nodes;
        for (std::size_t pos = 0; pos <= list.size();) {
            const auto comma = list.find(',', pos);
            nodes_.push_back(
                list.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos));
            if (comma == std::string::npos) {
                break;
            }
            pos = comma + 1;
        }
        ASSERT_EQ(nodes_.size(), 3U) << prefix << "_NODES names " << nodes_.size() << " nodes";
        // Every node up, whatever an earlier case left.
        for (const auto& n : nodes_) {
            clink::test::run_docker_command("docker start " + n);
        }
        set_up_against((prefix + "_ENDPOINT").c_str());
        if (::testing::Test::IsSkipped() || ::testing::Test::HasFatalFailure()) {
            return;
        }
        ASSERT_TRUE(cluster_green(120s)) << "the cluster never had three nodes and every shard";
    }

    bool cluster_green(std::chrono::seconds timeout) {
        return await_until(
            [&] {
                auto c = client();
                const auto r =
                    c.get("/_cluster/health?wait_for_status=green&wait_for_nodes=3&timeout=1s");
                return r.status == 200;
            },
            timeout);
    }

    void kill_node(std::size_t i) {
        const auto r = clink::test::run_docker_command("docker kill " + nodes_[i]);
        ASSERT_EQ(r.status, 0) << r.out;
    }
    void start_node(std::size_t i) {
        const auto r = clink::test::run_docker_command("docker start " + nodes_[i]);
        ASSERT_EQ(r.status, 0) << r.out;
    }

    // An index of three shards, each with a replica on another node.
    void create_replicated(const std::string& index) {
        auto c = client();
        const auto r = c.put("/" + index,
                             R"({"settings":{"number_of_shards":3,"number_of_replicas":1}})",
                             "application/json");
        ASSERT_EQ(r.status, 200) << r.body;
        ASSERT_TRUE(cluster_green(120s)) << "the replicas of " << index << " were never assigned";
    }

    std::vector<std::string> nodes_;
};

TEST_P(SearchSinkClusterLive, ANodeKilledWhileTheSinkWritesLosesNothing) {
    create_replicated(index_);
    if (HasFatalFailure()) {
        return;
    }
    BulkFaultProxy proxy(endpoint_);
    auto o = options(proxy.url(), index_);
    o.max_retries = 8;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    std::vector<std::string> all;
    std::vector<std::vector<std::string>> batches;
    for (int b = 0; b < 40; ++b) {
        std::vector<std::string> batch;
        for (int i = 0; i < 100; ++i) {
            const int n = b * 100 + i;
            batch.push_back(R"({"id":"k)" + std::to_string(n % 300) + R"(","v":)" +
                            std::to_string(n) + "}");
        }
        all.insert(all.end(), batch.begin(), batch.end());
        batches.push_back(std::move(batch));
    }
    std::atomic<int> done{0};
    std::atomic<int> failed_flushes{0};
    std::atomic<bool> stop{false};
    // Joined on every way out of the case, so a failed assertion cannot leave it
    // running.
    std::jthread writer([&] {
        for (const auto& batch : batches) {
            if (stop.load()) {
                return;
            }
            sink->on_data(batch_of(batch));
            for (int attempt = 0; attempt < 40 && !stop.load(); ++attempt) {
                try {
                    sink->flush();
                    break;
                } catch (const std::exception&) {
                    failed_flushes.fetch_add(1);
                    std::this_thread::sleep_for(500ms);
                }
            }
            done.fetch_add(1);
            std::this_thread::sleep_for(100ms);
        }
    });
    struct StopWriter {
        std::atomic<bool>& stop;
        std::jthread& writer;
        ~StopWriter() {
            stop.store(true);
            if (writer.joinable()) {
                writer.join();
            }
        }
    } stop_writer{stop, writer};
    ASSERT_TRUE(await_until([&] { return done.load() >= 8; }, 60s));
    const int at_kill = done.load();
    kill_node(2);
    ASSERT_TRUE(await_until([&] { return done.load() >= at_kill + 12; }, 180s))
        << "the sink stopped writing after a node was killed";
    start_node(2);
    writer.join();
    EXPECT_LT(at_kill, 40) << "the kill landed after every write";
    ASSERT_TRUE(cluster_green(180s)) << "the cluster did not recover its third node";
    EXPECT_EQ(stored(index_), latest_by_id(all));
    report("cluster_node_killed",
           "3 nodes, 3 shards with a replica each; node 3 killed after " + std::to_string(at_kill) +
               " of 40 flushes and started again: the cluster answered " +
               std::to_string(proxy.upstream_failed_items()) + " item(s) with a failure, " +
               std::to_string(failed_flushes.load()) +
               " flush(es) failed and were flushed again; 4000 records over 300 ids stored, each "
               "id at its latest");
}

TEST_P(SearchSinkClusterLive, TheNodeTheSinkWritesToKilledFailsTheFlushUntilItReturns) {
    create_replicated(index_);
    if (HasFatalFailure()) {
        return;
    }
    auto o = options(endpoint_, index_);
    o.max_retries = 1;
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto first = keyed_records(50, 50);
    sink->on_data(batch_of(first));
    sink->flush();
    const auto before = counter_value(dropped_metric());
    kill_node(0);
    std::vector<std::string> second;
    for (int i = 0; i < 50; ++i) {
        second.push_back(R"({"id":"k)" + std::to_string(i) + R"(","v":)" +
                         std::to_string(1000 + i) + "}");
    }
    sink->on_data(batch_of(second));
    std::string error;
    try {
        sink->flush();
    } catch (const std::exception& e) {
        error = e.what();
    }
    EXPECT_FALSE(error.empty()) << "a flush to a killed node succeeded";
    EXPECT_EQ(counter_value(dropped_metric()) - before, 0U);
    start_node(0);
    ASSERT_TRUE(cluster_green(180s)) << "node 1 did not rejoin";
    sink->flush();
    std::vector<std::string> all = first;
    all.insert(all.end(), second.begin(), second.end());
    EXPECT_EQ(stored(index_), latest_by_id(all));
    report("cluster_sink_node_killed",
           "the node the sink writes to killed: the flush failed (" +
               error.substr(0, std::min<std::size_t>(error.size(), 120)) +
               "), 0 dead-lettered; once it rejoined the pending batch was stored");
}

INSTANTIATE_TEST_SUITE_P(Elasticsearch,
                         SearchSinkClusterLive,
                         ::testing::Values(kElasticsearch),
                         [](const auto&) { return std::string{"es"}; });
INSTANTIATE_TEST_SUITE_P(OpenSearch,
                         SearchSinkClusterLive,
                         ::testing::Values(kOpenSearch),
                         [](const auto&) { return std::string{"os"}; });

INSTANTIATE_TEST_SUITE_P(Elasticsearch,
                         SearchSinkSecuredLive,
                         ::testing::Values(kElasticsearch),
                         [](const auto&) { return std::string{"es"}; });
INSTANTIATE_TEST_SUITE_P(OpenSearch,
                         SearchSinkSecuredLive,
                         ::testing::Values(kOpenSearch),
                         [](const auto&) { return std::string{"os"}; });

INSTANTIATE_TEST_SUITE_P(Elasticsearch,
                         SearchSinkThrottledLive,
                         ::testing::Values(kElasticsearch),
                         [](const auto&) { return std::string{"es"}; });
INSTANTIATE_TEST_SUITE_P(OpenSearch,
                         SearchSinkThrottledLive,
                         ::testing::Values(kOpenSearch),
                         [](const auto&) { return std::string{"os"}; });

INSTANTIATE_TEST_SUITE_P(Elasticsearch,
                         SearchSinkLive,
                         ::testing::Values(kElasticsearch),
                         [](const auto&) { return std::string{"es"}; });
INSTANTIATE_TEST_SUITE_P(OpenSearch,
                         SearchSinkLive,
                         ::testing::Values(kOpenSearch),
                         [](const auto&) { return std::string{"os"}; });
