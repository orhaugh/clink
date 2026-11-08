// The Splunk HEC sink against a Splunk server someone supplies, read back with
// a search of the test's own through Splunk's REST API. Nothing here starts a
// server: the cases skip unless every variable below is set, and
// scripts/splunk-live.sh refuses to run without them, so their absence is
// never reported as a pass.
//
//   CLINK_SPLUNK_HEC_TEST_ENDPOINT      HEC base URL, e.g. https://splunk:8088
//   CLINK_SPLUNK_HEC_TEST_TOKEN         a token without indexer acknowledgement
//   CLINK_SPLUNK_HEC_TEST_ACK_TOKEN     a token with indexer acknowledgement
//   CLINK_SPLUNK_TEST_INDEX             an index both tokens may write to
//   CLINK_SPLUNK_TEST_SEARCH_ENDPOINT   management API base, e.g. https://splunk:8089
//   CLINK_SPLUNK_TEST_SEARCH_USER / _PASSWORD   a user that may search the index
//   CLINK_SPLUNK_TEST_VERIFY_TLS        "false" for a self-signed certificate
//
// No token or password is printed. Each case tags its events with a run id of
// its own and searches for exactly that id.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/base64.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/splunk_hec_sink.hpp"

using clink::Batch;
using clink::http_connector::DlqPolicy;
using clink::http_connector::make_splunk_hec_sink;
using clink::http_connector::SplunkHecAck;
using clink::http_connector::SplunkHecOptions;

namespace {

using namespace std::chrono_literals;

std::string env_or(const char* name, const std::string& fallback = {}) {
    const char* v = std::getenv(name);
    return v == nullptr || *v == '\0' ? fallback : std::string{v};
}

const char* const kRequired[] = {"CLINK_SPLUNK_HEC_TEST_ENDPOINT",
                                 "CLINK_SPLUNK_HEC_TEST_TOKEN",
                                 "CLINK_SPLUNK_HEC_TEST_ACK_TOKEN",
                                 "CLINK_SPLUNK_TEST_INDEX",
                                 "CLINK_SPLUNK_TEST_SEARCH_ENDPOINT",
                                 "CLINK_SPLUNK_TEST_SEARCH_USER",
                                 "CLINK_SPLUNK_TEST_SEARCH_PASSWORD"};

std::string missing() {
    std::string out;
    for (const char* v : kRequired) {
        if (env_or(v).empty()) {
            out += (out.empty() ? "" : ", ") + std::string{v};
        }
    }
    return out;
}

bool verify_tls() {
    return env_or("CLINK_SPLUNK_TEST_VERIFY_TLS", "true") != "false";
}

std::string run_id() {
    std::random_device rd;
    return "clink-live-" + std::to_string(rd()) + std::to_string(rd());
}

std::string form_encode(const std::string& s) {
    static const char* kHex = "0123456789ABCDEF";
    std::string out;
    for (const char c : s) {
        const auto u = static_cast<unsigned char>(c);
        if ((u >= 'A' && u <= 'Z') || (u >= 'a' && u <= 'z') || (u >= '0' && u <= '9') ||
            u == '-' || u == '_' || u == '.' || u == '~') {
            out.push_back(c);
        } else {
            out.push_back('%');
            out.push_back(kHex[u >> 4]);
            out.push_back(kHex[u & 0x0F]);
        }
    }
    return out;
}

// The _raw of every event the index holds for `run`, through the search
// export endpoint.
std::vector<std::string> export_raw(const std::string& run) {
    clink::http_connector::HttpRequest::Options o;
    o.base_url = env_or("CLINK_SPLUNK_TEST_SEARCH_ENDPOINT");
    o.verify_tls = verify_tls();
    o.rw_timeout_ms = 60000;
    o.headers["Authorization"] = "Basic " + clink::http_connector::base64_encode(
                                                env_or("CLINK_SPLUNK_TEST_SEARCH_USER") + ":" +
                                                env_or("CLINK_SPLUNK_TEST_SEARCH_PASSWORD"));
    clink::http_connector::HttpRequest http(o);
    const std::string search = "search index=" + env_or("CLINK_SPLUNK_TEST_INDEX") +
                               " clink_run=\"" + run + "\" | fields _raw";
    const std::string body =
        "search=" + form_encode(search) + "&output_mode=json&earliest_time=-2h&latest_time=%2B1h";
    const auto res =
        http.post("/services/search/jobs/export", body, "application/x-www-form-urlencoded");
    std::vector<std::string> raws;
    if (res.status != 200) {
        ADD_FAILURE() << "the search failed: HTTP " << res.status;
        return raws;
    }
    std::size_t pos = 0;
    while (pos < res.body.size()) {
        auto eol = res.body.find('\n', pos);
        if (eol == std::string::npos) {
            eol = res.body.size();
        }
        const std::string line = res.body.substr(pos, eol - pos);
        pos = eol + 1;
        try {
            const auto v = clink::config::parse(line);
            if (v.contains("result") && v.at("result").contains("_raw")) {
                raws.push_back(v.at("result").at("_raw").as_string());
            }
        } catch (const std::exception&) {
            // A line that is not a result (an empty line, a preview message).
        }
    }
    return raws;
}

// As export_raw, once `want` events are searchable or 120 s have passed, and
// then once more 5 s later, for a stray duplicate to show.
std::vector<std::string> search_raw(const std::string& run, std::size_t want) {
    const auto until = std::chrono::steady_clock::now() + 120s;
    auto raws = export_raw(run);
    while (raws.size() < want && std::chrono::steady_clock::now() < until) {
        std::this_thread::sleep_for(2s);
        raws = export_raw(run);
    }
    std::this_thread::sleep_for(5s);
    return export_raw(run);
}

SplunkHecOptions options(const char* token_var) {
    SplunkHecOptions o;
    o.url = env_or("CLINK_SPLUNK_HEC_TEST_ENDPOINT");
    o.token = env_or(token_var);
    o.index = env_or("CLINK_SPLUNK_TEST_INDEX");
    o.sourcetype = "_json";
    o.verify_tls = verify_tls();
    o.batch_records = 20;
    return o;
}

// Record n of `run`: a string with escapes and Unicode, an integer past 2^53
// and a decimal written with a trailing zero.
std::string record(const std::string& run, int n) {
    return R"({"clink_run":")" + run + R"(","n":)" + std::to_string(n) +
           R"(,"big":9007199254740993,"d":1.50,"s":"line\nbreak é \"q\" \\"})";
}

Batch<std::string> batch(const std::vector<std::string>& recs) {
    Batch<std::string> b;
    for (const auto& r : recs) {
        b.emplace(r);
    }
    return b;
}

template <typename F>
std::string error_of(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

class SplunkHecLive : public ::testing::Test {
protected:
    void SetUp() override {
        if (const auto m = missing(); !m.empty()) {
            GTEST_SKIP() << "set " << m << " (scripts/splunk-live.sh lists them)";
        }
    }

    // Every record of `run` searchable exactly once, its event equal to the
    // record. Whether _raw keeps the numbers' text as sent is Splunk's to
    // decide, so it is printed, not asserted.
    static void expect_indexed(const std::string& run, const std::vector<std::string>& recs) {
        const auto raws = search_raw(run, recs.size());
        std::size_t as_sent = 0;
        std::set<std::int64_t> seen;
        for (const auto& raw : raws) {
            if (raw.find("9007199254740993") != std::string::npos &&
                raw.find("1.50") != std::string::npos) {
                ++as_sent;
            }
            clink::config::JsonValue v;
            try {
                v = clink::config::parse(raw);
            } catch (const std::exception&) {
                ADD_FAILURE() << "an event's _raw is not JSON (" << raw.size() << " bytes)";
                continue;
            }
            const auto n = v.at("n").as_int();
            seen.insert(n);
            EXPECT_EQ(v, clink::config::parse(record(run, static_cast<int>(n)))) << raw;
            EXPECT_EQ(v.at("big").as_int(), 9007199254740993) << "the integer past 2^53, exactly";
        }
        std::printf("[splunk-live] run=%s sent=%zu found=%zu numbers_as_sent=%zu\n",
                    run.c_str(),
                    recs.size(),
                    raws.size(),
                    as_sent);
        EXPECT_EQ(raws.size(), recs.size()) << "each record once";
        EXPECT_EQ(seen.size(), recs.size());
    }
};

}  // namespace

TEST_F(SplunkHecLive, EveryAcceptedEventIsSearchableWithItsPayload) {
    const auto run = run_id();
    std::vector<std::string> recs;
    for (int n = 0; n < 50; ++n) {
        recs.push_back(record(run, n));
    }
    auto sink = make_splunk_hec_sink(options("CLINK_SPLUNK_HEC_TEST_TOKEN"));
    sink->open();
    sink->on_data(batch(recs));
    sink->flush();
    expect_indexed(run, recs);
}

TEST_F(SplunkHecLive, RefusedRecordsNeverReachTheIndex) {
    const auto run = run_id();
    auto o = options("CLINK_SPLUNK_HEC_TEST_TOKEN");
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    const std::vector<std::string> good{record(run, 0), record(run, 1)};
    sink->on_data(batch({good[0],
                         R"({"clink_run":")" + run + R"(","n":9},"index":"main")",
                         R"({"clink_run":")" + run + R"(","n":8,"n":7})",
                         good[1]}));
    sink->flush();
    expect_indexed(run, good);
}

TEST_F(SplunkHecLive, IndexerAcknowledgementConfirmsEveryRequestBeforeTheBarrier) {
    const auto run = run_id();
    std::vector<std::string> recs;
    for (int n = 0; n < 50; ++n) {
        recs.push_back(record(run, n));
    }
    auto o = options("CLINK_SPLUNK_HEC_TEST_ACK_TOKEN");
    o.ack = SplunkHecAck::Indexer;
    o.ack_poll_interval = 500ms;
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    sink->on_data(batch(recs));
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    expect_indexed(run, recs);
}

TEST_F(SplunkHecLive, EachMisconfigurationIsRefusedWithItsCause) {
    const auto run = run_id();
    const auto attempt = [&](SplunkHecOptions o) {
        o.max_retries = 0;
        auto sink = make_splunk_hec_sink(o);
        sink->open();
        sink->on_data(batch({record(run, 0)}));
        return error_of([&] { sink->flush(); });
    };
    auto bad_token = options("CLINK_SPLUNK_HEC_TEST_TOKEN");
    bad_token.token = "00000000-0000-0000-0000-000000000000";
    const auto token = attempt(bad_token);
    EXPECT_NE(token.find("refused the token"), std::string::npos) << token;

    auto bad_index = options("CLINK_SPLUNK_HEC_TEST_TOKEN");
    bad_index.index = "clink_no_such_index_" + std::to_string(std::random_device{}());
    const auto index = attempt(bad_index);
    EXPECT_NE(index.find("allowed indexes"), std::string::npos) << index;

    // Each token used in the other mode.
    const auto channel = attempt(options("CLINK_SPLUNK_HEC_TEST_ACK_TOKEN"));
    EXPECT_NE(channel.find("set ack='indexer'"), std::string::npos) << channel;
    auto no_ack = options("CLINK_SPLUNK_HEC_TEST_TOKEN");
    no_ack.ack = SplunkHecAck::Indexer;
    const auto ack = attempt(no_ack);
    EXPECT_NE(ack.find("does not use indexer acknowledgement"), std::string::npos) << ack;
    std::printf("[splunk-live] refusals: token, index, channel, acknowledgement\n");
    // The refused requests wrote nothing for this run, except perhaps the
    // last, which HEC may have accepted before the sink found no ackId.
    EXPECT_LE(search_raw(run, 1).size(), 1U);
}
