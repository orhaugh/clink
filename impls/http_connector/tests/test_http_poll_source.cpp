// The HTTP polling source. Its page reader on its own: the response shapes it
// accepts and refuses, numbers kept as served, and the cursor contract. Then
// the source against a scripted local endpoint whose every response the test
// sets: paging, bounded completion, 204 and 304, conditional requests across
// cursor moves, restores and reopens, exact and encoded cursors, an endpoint
// that ignores the cursor, the resource bounds, retries and throttling,
// cancellation, what errors name, the factory's option parsing, and the
// capability record and the delivery analyser's verdicts.
//
// What a local server establishes is how the source treats each response; it
// says nothing about any particular third-party API, whose own contract
// decides whether cursor mode is safe against it.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <filesystem>
#include <functional>
#include <httplib.h>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/delivery_guarantee.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/http_poll_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace {

using namespace std::chrono_literals;
using clink::CheckpointId;
using clink::Emitter;
using clink::OperatorId;
using clink::StreamElement;
using clink::http_connector::compare_integer_cursors;
using clink::http_connector::HttpPollCursorType;
using clink::http_connector::HttpPollOptions;
using clink::http_connector::HttpPollPage;
using clink::http_connector::HttpPollPageRules;
using clink::http_connector::HttpPollSource;
using clink::http_connector::read_http_poll_page;
using clink::http_connector::url_encode;
using Clock = std::chrono::steady_clock;

const OperatorId kOp{0x5011};

// ---- the scripted endpoint ------------------------------------------------------------

// Every GET is answered by the handler the test sets, and recorded first.
class Endpoint {
public:
    using Handler = std::function<void(const httplib::Request&, httplib::Response&)>;
    struct Seen {
        std::string path;
        std::string target;         // as sent: the path and the raw query
        std::string since;          // the cursor parameter, decoded; "<none>" when absent
        std::string if_none_match;  // empty when absent
        Clock::time_point at;
    };

    Endpoint() {
        svr_.Get(R"(/.*)", [this](const httplib::Request& req, httplib::Response& res) {
            Handler h;
            {
                const std::lock_guard lk(mu_);
                seen_.push_back({req.path,
                                 req.target,
                                 req.has_param("since") ? req.get_param_value("since") : "<none>",
                                 req.get_header_value("If-None-Match"),
                                 Clock::now()});
                h = handler_;
            }
            if (h) {
                h(req, res);
            } else {
                res.status = 500;
            }
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~Endpoint() {
        release();
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    Endpoint(const Endpoint&) = delete;
    Endpoint& operator=(const Endpoint&) = delete;

    void on(Handler h) {
        const std::lock_guard lk(mu_);
        handler_ = std::move(h);
    }
    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    std::vector<Seen> seen() {
        const std::lock_guard lk(mu_);
        return seen_;
    }
    std::size_t count() {
        const std::lock_guard lk(mu_);
        return seen_.size();
    }
    // A handler that must stall waits here, until release() or `cap`.
    void hold(std::chrono::milliseconds cap) {
        std::unique_lock lk(gate_mu_);
        gate_cv_.wait_for(lk, cap, [&] { return released_; });
    }
    void release() {
        {
            const std::lock_guard lk(gate_mu_);
            released_ = true;
        }
        gate_cv_.notify_all();
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    Handler handler_;
    std::vector<Seen> seen_;
    std::mutex gate_mu_;
    std::condition_variable gate_cv_;
    bool released_{false};
};

// The retained dataset: record n is {"seq":n,"v":"r<n>"}. GET ?since=<seq>
// answers {"items":[...]}, at most `page` records after <seq>, ascending.
std::string record(long n) {
    return "{\"seq\":" + std::to_string(n) + ",\"v\":\"r" + std::to_string(n) + "\"}";
}
Endpoint::Handler feed(long total, long page) {
    return [total, page](const httplib::Request& req, httplib::Response& res) {
        const long since = req.has_param("since") ? std::stol(req.get_param_value("since")) : 0;
        std::string body = "{\"items\":[";
        for (long n = since + 1, k = 0; n <= total && k < page; ++n, ++k) {
            body += (k == 0 ? "" : ",") + record(n);
        }
        res.set_content(body + "]}", "application/json");
    };
}
std::vector<std::string> records(long from, long to) {
    std::vector<std::string> out;
    for (long n = from; n <= to; ++n) {
        out.push_back(record(n));
    }
    return out;
}

HttpPollOptions cursor_opts(const Endpoint& ep) {
    HttpPollOptions o;
    o.url = ep.url();
    o.path = "/feed";
    o.records_field = "items";
    o.cursor_param = "since";
    o.cursor_field = "seq";
    o.cursor_type = HttpPollCursorType::Integer;
    o.interval = 0ms;
    o.retry_base_backoff = 1ms;
    o.max_backoff = 1ms;
    return o;
}
HttpPollOptions resource_opts(const Endpoint& ep) {
    HttpPollOptions o;
    o.url = ep.url();
    o.path = "/resource";
    o.interval = 0ms;
    o.retry_base_backoff = 1ms;
    o.max_backoff = 1ms;
    return o;
}

Emitter<std::string> capturing(std::vector<std::string>& got) {
    return Emitter<std::string>{[&got](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                got.push_back(r.value());
            }
        }
        return true;
    }};
}

// Calls produce() until it returns false (true is returned then), `enough`
// holds or `limit` passes. A throw propagates.
bool pump(HttpPollSource& src,
          std::vector<std::string>& got,
          const std::function<bool()>& enough,
          std::chrono::milliseconds limit = 5000ms) {
    auto em = capturing(got);
    const auto deadline = Clock::now() + limit;
    while (Clock::now() < deadline) {
        if (!src.produce(em)) {
            return true;
        }
        if (enough && enough()) {
            return false;
        }
    }
    return false;
}

// What pump() threw, or empty when it did not within `limit`.
std::string pump_error(HttpPollSource& src,
                       std::vector<std::string>& got,
                       std::chrono::milliseconds limit = 5000ms) {
    try {
        pump(src, got, nullptr, limit);
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

std::string thrown(const std::function<void()>& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

bool has(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
}

HttpPollPage read(std::string_view body, const HttpPollPageRules& rules, std::string cursor = {}) {
    return read_http_poll_page(body, rules, cursor);
}
HttpPollPageRules integer_rules(std::string records_field = "items") {
    HttpPollPageRules r;
    r.records_field = std::move(records_field);
    r.cursor_field = "seq";
    r.cursor_type = HttpPollCursorType::Integer;
    return r;
}

std::filesystem::path scratch_dir(const std::string& tag) {
    auto p = std::filesystem::temp_directory_path() /
             ("clink-http-poll-" + tag + "-" + std::to_string(::getpid()));
    std::filesystem::remove_all(p);
    std::filesystem::create_directories(p);
    return p;
}

}  // namespace

// ---- the page reader ------------------------------------------------------------------

TEST(HttpPollPage, ReadsAnArrayOrTheConfiguredEnvelope) {
    const auto bare = read(" [ {\"a\": 1},\n  {\"b\" : [2, 3]} ] \n", HttpPollPageRules{});
    ASSERT_TRUE(bare.valid) << bare.why;
    EXPECT_EQ(bare.records, (std::vector<std::string>{R"({"a":1})", R"({"b":[2,3]})"}));
    EXPECT_TRUE(bare.last_cursor.empty());

    HttpPollPageRules env;
    env.records_field = "items";
    const auto wrapped = read(R"({"meta":{"items":[9]},"items":[{"id":1}],"next":null})", env);
    ASSERT_TRUE(wrapped.valid) << wrapped.why;
    EXPECT_EQ(wrapped.records, (std::vector<std::string>{R"({"id":1})"}));

    // A key written with escapes is the same key.
    const auto escaped = read(R"({"it\u0065ms":[1,"two"]})", env);
    ASSERT_TRUE(escaped.valid) << escaped.why;
    EXPECT_EQ(escaped.records, (std::vector<std::string>{"1", "\"two\""}));

    const auto empty = read(R"({"items":[]})", env);
    ASSERT_TRUE(empty.valid) << empty.why;
    EXPECT_TRUE(empty.records.empty());
}

TEST(HttpPollPage, KeepsEveryTokenAsServed) {
    // Integers past what a double holds exactly, decimals past a double's
    // precision, exponents and string escapes are emitted byte for byte; only
    // the whitespace between tokens goes.
    const std::string body =
        "[\n  {\n    \"id\": 18446744073709551615,\n    \"p\": 3.141592653589793238462643383279,\n"
        "    \"e\": 1E+300, \"z\": -0.0,\n    \"s\": \"caf\\u00e9 \\/ \\\"q\\\" a  b\",\n"
        "    \"u\": \"\xC3\xA9\"\n  }\n]";
    const auto page = read(body, HttpPollPageRules{});
    ASSERT_TRUE(page.valid) << page.why;
    ASSERT_EQ(page.records.size(), 1U);
    EXPECT_EQ(
        page.records[0],
        "{\"id\":18446744073709551615,\"p\":3.141592653589793238462643383279,"
        "\"e\":1E+300,\"z\":-0.0,\"s\":\"caf\\u00e9 \\/ \\\"q\\\" a  b\",\"u\":\"\xC3\xA9\"}");
}

TEST(HttpPollPage, ARecordClinksJsonReaderCannotDecodeIsRefused) {
    // Valid JSON that clink's JSON reader, which SQL decodes rows with, holds
    // no value for: emitting it would hand the next operator a record it cannot
    // read.
    for (const char* body : {R"([{"id":18446744073709551616}])",
                             R"([{"id":-9223372036854775809}])",
                             R"([1, 2, 1e400])"}) {
        const auto page = read(body, HttpPollPageRules{});
        EXPECT_FALSE(page.valid) << body;
        EXPECT_TRUE(has(page.why, "holds a value clink's JSON reader cannot decode")) << page.why;
        EXPECT_TRUE(has(page.why, "as strings")) << page.why;
        EXPECT_TRUE(page.records.empty());
    }
}

TEST(HttpPollPage, AMissingNullOrMistypedRecordsFieldIsRefused) {
    HttpPollPageRules env;
    env.records_field = "items";
    for (const auto& [body, why] : std::vector<std::pair<std::string, std::string>>{
             {R"({"data":[]})", "the response has no 'items'"},
             {R"({})", "the response has no 'items'"},
             {R"({"items":null})", "the response's 'items' is null, not an array"},
             {R"({"items":{}})", "the response's 'items' is an object, not an array"},
             {R"({"items":"[]"})", "the response's 'items' is a string, not an array"},
             {R"({"items":0})", "the response's 'items' is a number, not an array"},
             {R"({"items":[],"items":[1]})", "the response has 'items' more than once"},
             {R"([{"id":1}])", "the response is an array, not an object holding 'items'"},
             {R"("items")", "the response is a string, not an object holding 'items'"},
         }) {
        const auto page = read(body, env);
        EXPECT_FALSE(page.valid) << body;
        EXPECT_TRUE(has(page.why, why)) << body << " -> " << page.why;
        EXPECT_TRUE(page.records.empty());
    }
    const auto object = read(R"({"items":[]})", HttpPollPageRules{});
    EXPECT_FALSE(object.valid);
    EXPECT_TRUE(has(object.why, "is an object, not an array of records")) << object.why;
}

TEST(HttpPollPage, MalformedJsonIsRefused) {
    std::string deep(300, '[');
    deep += std::string(300, ']');
    for (const std::string& body : std::vector<std::string>{
             std::string{""},
             std::string{"   \n"},
             std::string{R"([{"a":1})"},
             std::string{R"([{"a":1},])"},
             std::string{R"([{'a':1}])"},
             std::string{R"([{"a":01}])"},
             std::string{R"([{"a":1.}])"},
             std::string{R"([{"a":-}])"},
             std::string{R"([{"a":1}] x)"},
             std::string{R"([{"a":1}][])"},
             std::string{R"([NaN])"},
             std::string{R"([tru])"},
             std::string{R"({"items":[1,2,]})"},
             std::string{R"([{"a" 1}])"},
             std::string{R"([{a:1}])"},
             std::string{"[\"a\x01"
                         "b\"]"},
             std::string{R"(["\ud800"])"},
             std::string{R"(["\udc00x"])"},
             std::string{R"(["\x"])"},
             std::string{R"(["\u12"])"},
             std::string{"[\"\xFF\"]"},
             std::string{"[\"\xC0\xAF\"]"},
             std::string{"[\"\xE2\x82\"]"},
             std::string{"[\"\xED\xA0\x80\"]"},
             std::string{"<html>Bad gateway</html>"},
             deep,
         }) {
        const auto page = read(body, HttpPollPageRules{});
        EXPECT_FALSE(page.valid) << "accepted: " << body.substr(0, 40);
        EXPECT_TRUE(page.records.empty());
        EXPECT_TRUE(has(page.why, "not valid JSON") || has(page.why, "has no body"))
            << body.substr(0, 40) << " -> " << page.why;
    }
}

TEST(HttpPollPage, BoundsTheRecordsOfOnePage) {
    HttpPollPageRules r;
    r.max_records = 3;
    EXPECT_TRUE(read("[1,2,3]", r).valid);
    const auto over = read("[1,2,3,4]", r);
    EXPECT_FALSE(over.valid);
    EXPECT_TRUE(has(over.why, "more than 3 records (max_response_records)")) << over.why;
}

TEST(HttpPollPage, EveryRecordCarriesOneUsableCursor) {
    for (const auto& [body, why] : std::vector<std::pair<std::string, std::string>>{
             {R"({"items":[{"seq":1},{"v":2}]})", "record 2 has no 'seq'"},
             {R"({"items":[{"seq":null}]})", "record 1's 'seq' is null, which cannot be a cursor"},
             {R"({"items":[{"seq":true}]})", "record 1's 'seq' is a boolean"},
             {R"({"items":[{"seq":{"n":1}}]})", "record 1's 'seq' is an object"},
             {R"({"items":[{"seq":[1]}]})", "record 1's 'seq' is an array"},
             {R"({"items":[{"seq":1.5}]})", "record 1's 'seq' 1.5 is not an integer"},
             {R"({"items":[{"seq":1e3}]})", "record 1's 'seq' 1e3 is not an integer"},
             {R"({"items":[{"seq":"abc"}]})", "record 1's 'seq' \"abc\" is not an integer"},
             {R"({"items":[{"seq":"007"}]})", "record 1's 'seq' \"007\" is not an integer"},
             {R"({"items":[{"seq":1,"seq":2}]})", "record 1 has 'seq' more than once"},
             {R"({"items":[7]})", "record 1 is a number, not an object holding 'seq'"},
         }) {
        const auto page = read(body, integer_rules());
        EXPECT_FALSE(page.valid) << body;
        EXPECT_TRUE(has(page.why, why)) << body << " -> " << page.why;
        EXPECT_TRUE(page.records.empty());
        EXPECT_TRUE(page.last_cursor.empty());
    }
    HttpPollPageRules opaque = integer_rules();
    opaque.cursor_type = HttpPollCursorType::Opaque;
    const auto empty = read(R"({"items":[{"seq":""}]})", opaque);
    EXPECT_FALSE(empty.valid);
    EXPECT_TRUE(has(empty.why, "record 1's 'seq' is an empty string")) << empty.why;
    const auto big = read("{\"items\":[{\"seq\":\"" + std::string(1025, 'x') + "\"}]}", opaque);
    EXPECT_FALSE(big.valid);
    EXPECT_TRUE(has(big.why, "longer than 1024 bytes")) << big.why;
}

TEST(HttpPollPage, IntegerCursorsRiseUniquelyPastTheRequestCursor) {
    const auto ok = read(R"({"items":[{"seq":6},{"seq":9},{"seq":"10"}]})", integer_rules(), "5");
    ASSERT_TRUE(ok.valid) << ok.why;
    EXPECT_EQ(ok.last_cursor, "10");

    const auto down = read(R"({"items":[{"seq":1},{"seq":3},{"seq":2}]})", integer_rules());
    EXPECT_FALSE(down.valid);
    EXPECT_TRUE(has(down.why, "record 3's 'seq' 2 comes after record 2's 3: the values must rise"))
        << down.why;

    // A run of equal values: the cursor only moves past a value, so a page
    // that ends inside the run would skip the rest of it.
    const auto tie = read(R"({"items":[{"seq":4},{"seq":4}]})", integer_rules());
    EXPECT_FALSE(tie.valid);
    EXPECT_TRUE(has(tie.why, "records 1 and 2 share the 'seq' 4")) << tie.why;
    EXPECT_TRUE(has(tie.why, "each value must be unique")) << tie.why;

    for (const char* at : {R"({"items":[{"seq":5}]})", R"({"items":[{"seq":3},{"seq":8}]})"}) {
        const auto behind = read(at, integer_rules(), "5");
        EXPECT_FALSE(behind.valid) << at;
        EXPECT_TRUE(has(behind.why, "is not after the cursor 5 the request carried")) << behind.why;
    }
}

TEST(HttpPollPage, IntegerCursorsAreExactAtAnySize) {
    // Past what a double holds, as numbers up to 64 bits and as strings beyond.
    const auto page =
        read(R"({"items":[{"seq":9223372036854775808},{"seq":18446744073709551615},)"
             R"({"seq":"18446744073709551616"},{"seq":"99999999999999999999999999999999999999"}]})",
             integer_rules(),
             "9223372036854775807");
    ASSERT_TRUE(page.valid) << page.why;
    EXPECT_EQ(page.last_cursor, "99999999999999999999999999999999999999");
    EXPECT_EQ(page.records[1], R"({"seq":18446744073709551615})");
    const auto wide = read(R"({"items":[{"seq":18446744073709551616}]})", integer_rules());
    EXPECT_FALSE(wide.valid);
    EXPECT_TRUE(has(wide.why, "holds a value clink's JSON reader cannot decode")) << wide.why;

    const auto negative = read(R"({"items":[{"seq":-3},{"seq":-0},{"seq":2}]})", integer_rules());
    ASSERT_TRUE(negative.valid) << negative.why;
    EXPECT_EQ(negative.last_cursor, "2");

    EXPECT_LT(compare_integer_cursors("9", "10"), 0);
    EXPECT_GT(compare_integer_cursors("-9", "-10"), 0);
    EXPECT_LT(compare_integer_cursors("-1", "0"), 0);
    EXPECT_EQ(compare_integer_cursors("18446744073709551616", "18446744073709551616"), 0);
    EXPECT_GT(compare_integer_cursors("18446744073709551616", "18446744073709551615"), 0);

    const auto too_long =
        read("{\"items\":[{\"seq\":1" + std::string(100, '0') + "}]}", integer_rules());
    EXPECT_FALSE(too_long.valid);
    EXPECT_TRUE(has(too_long.why, "is not an integer of at most 100 digits")) << too_long.why;
}

TEST(HttpPollPage, OpaqueCursorsAreComparedOnlyForEquality) {
    HttpPollPageRules r = integer_rules();
    r.cursor_field = "t";
    r.cursor_type = HttpPollCursorType::Opaque;
    // No order is assumed between tokens, so a "falling" pair passes.
    const auto page = read(R"({"items":[{"t":"b9"},{"t":"a1"},{"t":1.50}]})", r, "z");
    ASSERT_TRUE(page.valid) << page.why;
    EXPECT_EQ(page.last_cursor, "1.50");  // a number's text, as written

    const auto repeated = read(R"({"items":[{"t":"x"},{"t":"y"},{"t":"x"}]})", r);
    EXPECT_FALSE(repeated.valid);
    EXPECT_TRUE(has(repeated.why, "records 1 and 3 share the 't' x")) << repeated.why;

    const auto at_cursor = read(R"({"items":[{"t":"y"},{"t":"c"}]})", r, "c");
    EXPECT_FALSE(at_cursor.valid);
    EXPECT_TRUE(has(at_cursor.why, "record 2's 't' is the cursor the request carried (c)"))
        << at_cursor.why;

    // Timestamps shared by several records: the tie an exclusive cursor skips
    // past when a page ends inside it.
    r.cursor_field = "ts";
    const auto tie =
        read(R"({"items":[{"ts":"2026-11-07T10:00:00Z"},{"ts":"2026-11-07T10:00:00Z"}]})", r);
    EXPECT_FALSE(tie.valid);
    EXPECT_TRUE(has(tie.why, "share the 'ts' 2026-11-07T10:00:00Z")) << tie.why;
}

// ---- the options ----------------------------------------------------------------------

TEST(HttpPollConfig, CursorModeTakesBothOptionsAndResourceModeNeither) {
    const auto refused = [](const std::function<void(HttpPollOptions&)>& set) {
        HttpPollOptions o;
        o.url = "http://127.0.0.1:1";
        set(o);
        return thrown([&] { HttpPollSource s{o}; });
    };
    EXPECT_EQ(refused([](auto&) {}), "");
    EXPECT_EQ(refused([](auto& o) {
                  o.cursor_param = "since";
                  o.cursor_field = "seq";
                  o.cursor_type = HttpPollCursorType::Integer;
                  o.initial_cursor = "-5";
              }),
              "");
    EXPECT_TRUE(has(refused([](auto& o) { o.cursor_param = "since"; }),
                    "cursor mode takes both 'cursor_param'"));
    EXPECT_TRUE(has(refused([](auto& o) { o.cursor_field = "seq"; }), "has only 'cursor_field'"));
    EXPECT_TRUE(has(refused([](auto& o) { o.initial_cursor = "5"; }),
                    "'initial_cursor' needs cursor mode"));
    EXPECT_TRUE(has(refused([](auto& o) { o.cursor_type = HttpPollCursorType::Integer; }),
                    "'cursor_type' needs cursor mode"));
    for (const char* bad : {"007", "+5", "5.0", "1e3", "x", "-"}) {
        EXPECT_TRUE(has(refused([bad](auto& o) {
                            o.cursor_param = "since";
                            o.cursor_field = "seq";
                            o.cursor_type = HttpPollCursorType::Integer;
                            o.initial_cursor = bad;
                        }),
                        "'initial_cursor' must be an integer"))
            << bad;
    }
}

TEST(HttpPollConfig, TheUrlIsRefusedWithoutEchoingIt) {
    const auto refused = [](const std::string& url) {
        HttpPollOptions o;
        o.url = url;
        return thrown([&] { HttpPollSource s{o}; });
    };
    const auto userinfo = refused("http://reader:s3cr3t-pw@127.0.0.1:8080");
    EXPECT_TRUE(has(userinfo, "must not carry a user or password")) << userinfo;
    EXPECT_FALSE(has(userinfo, "s3cr3t-pw")) << userinfo;
    EXPECT_FALSE(has(userinfo, "reader")) << userinfo;
    EXPECT_TRUE(has(refused("http://127.0.0.1:8080/api"), "put the path"));
    EXPECT_TRUE(has(refused("http://127.0.0.1:8080?token=x"), "put the path"));
    EXPECT_TRUE(has(refused("ftp://127.0.0.1"), "must start with http:// or https://"));
    EXPECT_TRUE(has(refused("http://127.0.0.1:0"), "a port that is not 1 to 65535"));
    EXPECT_TRUE(has(refused("http://127.0.0.1:70000"), "a port that is not 1 to 65535"));
    EXPECT_TRUE(has(refused("http://"), "names no host"));
    EXPECT_TRUE(has(refused(""), "'url' is required"));
    EXPECT_EQ(refused("http://[::1]:8080"), "");
    EXPECT_EQ(refused("http://localhost"), "");
}

TEST(HttpPollConfig, ValuesOutOfRangeAreRefused) {
    const auto refused = [](const std::function<void(HttpPollOptions&)>& set) {
        HttpPollOptions o;
        o.url = "http://127.0.0.1:1";
        set(o);
        return thrown([&] { HttpPollSource s{o}; });
    };
    EXPECT_TRUE(has(refused([](auto& o) { o.max_retries = 21; }), "'max_retries' must be 0 to 20"));
    EXPECT_TRUE(has(refused([](auto& o) { o.max_retries = -1; }), "'max_retries' must be 0 to 20"));
    EXPECT_TRUE(has(refused([](auto& o) { o.jitter_frac = 1.5; }), "'jitter_frac' must be 0 to 1"));
    EXPECT_TRUE(has(refused([](auto& o) { o.interval = -1ms; }), "'poll_interval_ms'"));
    EXPECT_TRUE(has(refused([](auto& o) { o.max_response_bytes = 10; }), "'max_response_bytes'"));
    EXPECT_TRUE(
        has(refused([](auto& o) { o.max_response_records = 0; }), "'max_response_records'"));
    EXPECT_TRUE(has(refused([](auto& o) { o.request_timeout = 0ms; }), "'request_timeout_ms'"));
    EXPECT_TRUE(has(refused([](auto& o) { o.max_backoff = 100ms; }),
                    "'max_backoff_ms' must be 200 to 600000"));
    EXPECT_TRUE(has(refused([](auto& o) { o.path = "events"; }), "'path' must start with '/'"));
    EXPECT_TRUE(has(refused([](auto& o) { o.path = "/a b"; }), "'path' must not hold spaces"));
    EXPECT_TRUE(has(refused([](auto& o) { o.path = "/a#b"; }), "a fragment"));
    EXPECT_TRUE(has(refused([](auto& o) { o.path = "/a\r\nX: y"; }), "control characters"));
}

TEST(HttpPollConfig, TheFactoryParsesEveryOptionStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "http_poll_source", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"url", "http://127.0.0.1:1"},
                  {"path", "/feed"},
                  {"cursor_param", "since"},
                  {"cursor_field", "seq"},
                  {"cursor_type", "integer"},
                  {"initial_cursor", "12"},
                  {"records_field", "items"},
                  {"bounded", "true"},
                  {"poll_interval_ms", "50"},
                  {"jitter_frac", "0.25"},
                  {"max_retries", "3"},
                  {"retry_base_backoff_ms", "100"},
                  {"max_backoff_ms", "2000"},
                  {"retry_timeout_ms", "30000"},
                  {"connect_timeout_ms", "2000"},
                  {"request_timeout_ms", "10000"},
                  {"max_response_bytes", "1048576"},
                  {"max_response_records", "5000"},
                  {"verify_tls", "false"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    for (const auto& [key, value, rule] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"bounded", "yes", "'bounded' must be 'true' or 'false'"},
             {"bounded", "True", "'bounded' must be 'true' or 'false'"},
             {"verify_tls", "0", "'verify_tls' must be 'true' or 'false'"},
             {"max_retries", "abc", "'max_retries' must be an integer"},
             {"max_retries", "21", "'max_retries' must be 0 to 20"},
             {"poll_interval_ms", "10s", "'poll_interval_ms' must be an integer"},
             {"poll_interval_ms", "-1", "'poll_interval_ms' must be 0 to 3600000"},
             {"jitter_frac", "x", "'jitter_frac' must be a number from 0 to 1"},
             {"jitter_frac", "2", "'jitter_frac' must be a number from 0 to 1"},
             {"cursor_type", "timestamp", "'cursor_type' must be 'opaque' or 'integer'"},
             {"cursor_field", "", "'cursor_field' is empty"},
             {"initial_cursor", "007", "'initial_cursor' must be an integer"},
             {"max_response_bytes", "12", "'max_response_bytes' must be 64 to"},
             {"request_timeout_ms", "0", "'request_timeout_ms' must be 1 to 600000"},
         }) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_TRUE(has(thrown([&] { (void)f->build(c); }), rule)) << key << "=" << value;
    }
    auto resource = ctx;
    resource.params.erase("cursor_param");
    resource.params.erase("cursor_field");
    resource.params.erase("initial_cursor");
    EXPECT_TRUE(has(thrown([&] { (void)f->build(resource); }), "'cursor_type' needs cursor mode"));
    resource.params.erase("cursor_type");
    EXPECT_NO_THROW((void)f->build(resource));
}

// ---- the source against the endpoint --------------------------------------------------

TEST(HttpPollSourceCursor, PagesThroughARetainedFeed) {
    Endpoint ep;
    ep.on(feed(25, 10));
    HttpPollSource src{cursor_opts(ep)};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return got.size() >= 25 && ep.count() >= 4; }));
    EXPECT_EQ(got, records(1, 25));
    EXPECT_EQ(src.cursor(), "25");
    const auto seen = ep.seen();
    ASSERT_GE(seen.size(), 4U);
    EXPECT_EQ(seen[0].since, "<none>");
    EXPECT_EQ(seen[1].since, "10");
    EXPECT_EQ(seen[2].since, "20");
    EXPECT_EQ(seen[3].since, "25");
}

TEST(HttpPollSourceCursor, ABoundedRunEndsAtTheFirstValidEmptyPage) {
    Endpoint ep;
    ep.on(feed(25, 10));
    auto o = cursor_opts(ep);
    o.bounded = true;
    HttpPollSource src{o};
    EXPECT_TRUE(src.is_bounded());
    std::vector<std::string> got;
    EXPECT_TRUE(pump(src, got, nullptr));
    EXPECT_EQ(got, records(1, 25));
    EXPECT_EQ(ep.count(), 4U);  // three pages and the empty one that ends it
    std::vector<std::string> more;
    auto em = capturing(more);
    EXPECT_FALSE(src.produce(em));
    EXPECT_EQ(ep.count(), 4U);
}

TEST(HttpPollSourceCursor, AMalformedPageNeverEndsABoundedRun) {
    // Each of these used to read as "no records" and end a bounded job early.
    for (const auto& body : {std::string{R"({"data":[]})"},
                             std::string{R"({"items":null})"},
                             std::string{R"({"items":"none"})"},
                             std::string{"{}"},
                             std::string{"[]"},
                             std::string{"not json"},
                             std::string{R"({"items":[{"seq":11},{"seq":12})"}}) {
        Endpoint ep;
        ep.on([&body](const httplib::Request& req, httplib::Response& res) {
            if (!req.has_param("since")) {
                res.set_content(R"({"items":[{"seq":10}]})", "application/json");
            } else {
                res.set_content(body, "application/json");
            }
        });
        auto o = cursor_opts(ep);
        o.bounded = true;
        HttpPollSource src{o};
        std::vector<std::string> got;
        const auto error = pump_error(src, got);
        EXPECT_FALSE(error.empty()) << body;
        EXPECT_TRUE(has(error, "nothing of it was emitted and the cursor did not move")) << error;
        EXPECT_EQ(got, (std::vector<std::string>{R"({"seq":10})"})) << body;
        EXPECT_EQ(src.cursor(), "10") << body;
    }
}

TEST(HttpPollSourceCursor, NoContentIsAnEmptyPage) {
    Endpoint ep;
    std::atomic<bool> data{false};
    ep.on([&](const httplib::Request&, httplib::Response& res) {
        if (data.load()) {
            res.set_content(R"({"items":[{"seq":1}]})", "application/json");
        } else {
            res.status = 204;
        }
    });
    {
        HttpPollSource src{cursor_opts(ep)};
        std::vector<std::string> got;
        EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 3; }));
        EXPECT_TRUE(got.empty());
        EXPECT_EQ(src.cursor(), "");
        data = true;
        EXPECT_FALSE(pump(src, got, [&] { return !got.empty(); }));
        EXPECT_EQ(src.cursor(), "1");
    }
    data = false;
    auto o = cursor_opts(ep);
    o.bounded = true;
    HttpPollSource bounded{o};
    std::vector<std::string> got;
    const auto before = ep.count();
    EXPECT_TRUE(pump(bounded, got, nullptr));
    EXPECT_TRUE(got.empty());
    EXPECT_EQ(ep.count(), before + 1);
}

TEST(HttpPollSourceCursor, TheCursorIsSentExactlyAndEncoded) {
    Endpoint ep;
    ep.on([](const httplib::Request& req, httplib::Response& res) {
        const auto since = req.get_param_value("since");
        if (since == "9007199254740993") {
            res.set_content(R"({"items":[{"seq":18446744073709551615}]})", "application/json");
        } else if (since == "18446744073709551615") {
            res.set_content(R"({"items":[{"seq":"123456789012345678901234567890"}]})",
                            "application/json");
        } else {
            res.set_content(R"({"items":[]})", "application/json");
        }
    });
    auto o = cursor_opts(ep);
    o.initial_cursor = "9007199254740993";  // 2^53 + 1, which a double rounds
    HttpPollSource src{o};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 3; }));
    EXPECT_EQ(got,
              (std::vector<std::string>{R"({"seq":18446744073709551615})",
                                        R"({"seq":"123456789012345678901234567890"})"}));
    auto seen = ep.seen();
    EXPECT_EQ(seen[0].since, "9007199254740993");
    EXPECT_EQ(seen[1].since, "18446744073709551615");
    EXPECT_EQ(seen[2].since, "123456789012345678901234567890");

    // An opaque cursor with reserved characters arrives exactly as it left.
    Endpoint opaque;
    const std::string first = "2026-11-07T10:00:00+01:00 a&b=c/d?";
    const std::string next = "tok en+/=%7E";
    opaque.on([&](const httplib::Request& req, httplib::Response& res) {
        const auto since = req.get_param_value("since");
        if (since == first) {
            res.set_content(R"({"items":[{"t":"tok en+/=%7E"}]})", "application/json");
        } else {
            res.set_content(R"({"items":[]})", "application/json");
        }
    });
    HttpPollOptions oo;
    oo.url = opaque.url();
    oo.path = "/feed?kind=a%20b";
    oo.records_field = "items";
    oo.cursor_param = "since";
    oo.cursor_field = "t";
    oo.initial_cursor = first;
    oo.interval = 0ms;
    HttpPollSource tokens{oo};
    std::vector<std::string> got2;
    EXPECT_FALSE(pump(tokens, got2, [&] { return opaque.count() >= 2; }));
    seen = opaque.seen();
    ASSERT_GE(seen.size(), 2U);
    EXPECT_EQ(seen[0].since, first);
    EXPECT_EQ(seen[0].target, "/feed?kind=a%20b&since=" + url_encode(first));
    EXPECT_EQ(seen[1].since, next);
    EXPECT_EQ(tokens.cursor(), next);
}

TEST(HttpPollSourceCursor, AnEndpointThatIgnoresTheCursorFails) {
    Endpoint ep;
    ep.on([](const httplib::Request&, httplib::Response& res) {
        res.set_content(R"({"items":[{"seq":1},{"seq":2}]})", "application/json");
    });
    HttpPollSource src{cursor_opts(ep)};
    std::vector<std::string> got;
    const auto error = pump_error(src, got);
    EXPECT_TRUE(has(error, "record 1's 'seq' 1 is not after the cursor 2 the request carried"))
        << error;
    EXPECT_EQ(got, (std::vector<std::string>{R"({"seq":1})", R"({"seq":2})"}));
    EXPECT_EQ(src.cursor(), "2");
    EXPECT_EQ(ep.count(), 2U);

    // With opaque tokens the source can still tell its own cursor coming back.
    auto o = cursor_opts(ep);
    o.cursor_type = HttpPollCursorType::Opaque;
    HttpPollSource opaque{o};
    std::vector<std::string> got2;
    const auto error2 = pump_error(opaque, got2);
    EXPECT_TRUE(has(error2, "record 2's 'seq' is the cursor the request carried (2)")) << error2;
    EXPECT_EQ(got2.size(), 2U);
}

TEST(HttpPollSourceCursor, ConditionalRequestsApplyOnlyToTheSameRequest) {
    // A server whose validator is collection-wide and answers 304 to ANY
    // If-None-Match, as a broken one does, while its records move on.
    Endpoint ep;
    std::atomic<long> total{0};
    ep.on([&](const httplib::Request& req, httplib::Response& res) {
        if (!req.get_header_value("If-None-Match").empty()) {
            res.status = 304;
            return;
        }
        res.set_header("ETag", "\"all-" + std::to_string(total.load()) + "\"");
        feed(total.load(), 10)(req, res);
    });
    HttpPollSource src{cursor_opts(ep)};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 3; }));  // empty, then 304s
    total = 3;
    // The request after an empty page repeats it, with the validator, and the
    // server says "not modified": the source keeps polling the same request.
    EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 5; }));
    EXPECT_TRUE(got.empty());
    auto seen = ep.seen();
    EXPECT_TRUE(seen[0].if_none_match.empty());
    EXPECT_EQ(seen[1].if_none_match, "\"all-0\"");
    // Once the position moves, no request carries a validator for another one.
    HttpPollSource fresh{cursor_opts(ep)};
    std::vector<std::string> got2;
    const auto first = ep.count();
    total = 13;
    EXPECT_FALSE(pump(fresh, got2, [&] { return got2.size() >= 13; }));
    EXPECT_EQ(got2, records(1, 13));
    seen = ep.seen();
    for (std::size_t k = first; k < seen.size(); ++k) {
        if (seen[k].since != "<none>" && seen[k].since != "13") {
            EXPECT_TRUE(seen[k].if_none_match.empty()) << seen[k].target;
        }
    }
}

TEST(HttpPollSourceCursor, ARestoreDropsTheConditionalStateAndReplays) {
    const auto dir = scratch_dir("restore-etag");
    // The same broken validator: any If-None-Match gets 304.
    Endpoint ep;
    std::atomic<long> total{5};
    ep.on([&](const httplib::Request& req, httplib::Response& res) {
        if (!req.get_header_value("If-None-Match").empty()) {
            res.status = 304;
            return;
        }
        res.set_header("ETag", "\"v\"");
        feed(total.load(), 10)(req, res);
    });
    auto o = cursor_opts(ep);
    o.initial_cursor = "5";
    HttpPollSource src{o};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 1; }));  // since=5: empty, ETag kept

    clink::StateBackendSpec spec;
    spec.uri = "file://" + (dir / "a").string();
    auto built = clink::StateBackendFactory::default_instance().build(spec);
    src.snapshot_offset(*built.backend, kOp, CheckpointId{1});
    (void)built.backend->snapshot(CheckpointId{1});

    total = 8;  // 6, 7 and 8 arrive; the stale validator hides them from `src`
    EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 3; }));
    EXPECT_TRUE(got.empty());

    // Restored into the same instance: the position comes back without the
    // validator, so the records the checkpoint does not cover are read.
    EXPECT_TRUE(src.restore_offset(*built.backend, kOp));
    src.open();
    EXPECT_FALSE(pump(src, got, [&] { return got.size() >= 3; }));
    EXPECT_EQ(got, records(6, 8));

    // Restored into a fresh source from the checkpoint the runtime wrote.
    clink::StateBackendSpec again;
    again.uri = "file://" + (dir / "b").string();
    again.restore_uri = spec.uri;
    again.restore_checkpoint_id = 1;
    auto restored = clink::StateBackendFactory::default_instance().build(again);
    ASSERT_TRUE(restored.restore_from.has_value());
    const auto [first_kg, last_kg] = clink::key_group_range_for_subtask(0, 1);
    restored.backend->restore(*restored.restore_from, clink::KeyGroupRange{first_kg, last_kg});
    HttpPollSource fresh{o};
    EXPECT_TRUE(fresh.restore_offset(*restored.backend, kOp));
    EXPECT_EQ(fresh.cursor(), "5");
    fresh.open();
    std::vector<std::string> got2;
    const auto before = ep.count();
    EXPECT_FALSE(pump(fresh, got2, [&] { return got2.size() >= 3; }));
    EXPECT_EQ(got2, records(6, 8));
    EXPECT_TRUE(ep.seen()[before].if_none_match.empty());
    std::filesystem::remove_all(dir);
}

TEST(HttpPollSourceCursor, RestoreAndReopenResumeAtTheCheckpointedCursor) {
    const auto dir = scratch_dir("restore");
    Endpoint ep;
    ep.on(feed(25, 10));
    clink::StateBackendSpec spec;
    spec.uri = "file://" + (dir / "a").string();
    auto built = clink::StateBackendFactory::default_instance().build(spec);

    HttpPollSource first{cursor_opts(ep)};
    std::vector<std::string> before_checkpoint;
    EXPECT_FALSE(pump(first, before_checkpoint, [&] { return before_checkpoint.size() >= 10; }));
    first.snapshot_offset(*built.backend, kOp, CheckpointId{3});
    (void)built.backend->snapshot(CheckpointId{3});
    // Emitted after the checkpoint, then lost with the process.
    std::vector<std::string> lost;
    EXPECT_FALSE(pump(first, lost, [&] { return !lost.empty(); }));
    EXPECT_EQ(lost, records(11, 20));

    clink::StateBackendSpec again;
    again.uri = "file://" + (dir / "b").string();
    again.restore_uri = spec.uri;
    again.restore_checkpoint_id = 3;
    auto restored = clink::StateBackendFactory::default_instance().build(again);
    ASSERT_TRUE(restored.restore_from.has_value());
    const auto [first_kg, last_kg] = clink::key_group_range_for_subtask(0, 1);
    restored.backend->restore(*restored.restore_from, clink::KeyGroupRange{first_kg, last_kg});
    HttpPollSource second{cursor_opts(ep)};
    ASSERT_TRUE(second.restore_offset(*restored.backend, kOp));
    second.open();
    std::vector<std::string> after;
    const auto at = ep.count();
    EXPECT_FALSE(pump(second, after, [&] { return after.size() >= 15; }));
    EXPECT_EQ(ep.seen()[at].since, "10");
    auto all = before_checkpoint;
    all.insert(all.end(), after.begin(), after.end());
    EXPECT_EQ(all, records(1, 25));  // every record once, in order

    // A reopen keeps the cursor and polls again from it.
    second.close();
    second.open();
    std::vector<std::string> none;
    const auto reopened = ep.count();
    EXPECT_FALSE(pump(second, none, [&] { return ep.count() > reopened; }));
    EXPECT_EQ(ep.seen()[reopened].since, "25");
    EXPECT_TRUE(none.empty());
    std::filesystem::remove_all(dir);
}

TEST(HttpPollSourceCursor, ARestoredCursorTheConfigurationCannotReadIsRefused) {
    clink::InMemoryStateBackend backend;
    const auto store = [&](const std::string& v) {
        backend.put_operator_state(
            kOp, "__poll_cursor__", clink::StateBackend::ValueView{v.data(), v.size()});
    };
    HttpPollOptions o;
    o.url = "http://127.0.0.1:1";
    o.cursor_param = "since";
    o.cursor_field = "seq";
    o.cursor_type = HttpPollCursorType::Integer;
    // What the earlier source wrote, under the same key.
    store("20");
    HttpPollSource earlier{o};
    EXPECT_TRUE(earlier.restore_offset(backend, kOp));
    EXPECT_EQ(earlier.cursor(), "20");
    store("1.8446744073709552e+19");  // an earlier source's rounded cursor
    HttpPollSource rounded{o};
    EXPECT_TRUE(has(thrown([&] { (void)rounded.restore_offset(backend, kOp); }),
                    "is not an integer, which cursor_type 'integer' needs"));
    HttpPollOptions resource;
    resource.url = o.url;
    store("20");
    HttpPollSource r{resource};
    EXPECT_TRUE(has(thrown([&] { (void)r.restore_offset(backend, kOp); }),
                    "the checkpoint holds a cursor, but this source is in resource mode"));
    store("");
    HttpPollSource r2{resource};
    EXPECT_TRUE(r2.restore_offset(backend, kOp));
}

TEST(HttpPollSourceResource, EveryResponseIsEmittedAndAnUnchangedOneIsNot) {
    Endpoint ep;
    std::atomic<int> version{1};
    ep.on([&](const httplib::Request& req, httplib::Response& res) {
        const std::string etag = "\"v" + std::to_string(version.load()) + "\"";
        if (req.get_header_value("If-None-Match") == etag) {
            res.status = 304;
            return;
        }
        res.set_header("ETag", etag);
        res.set_content("[{\"v\":" + std::to_string(version.load()) + "},{\"k\":0}]",
                        "application/json");
    });
    HttpPollSource src{resource_opts(ep)};
    EXPECT_FALSE(src.cursor_mode());
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 3; }));
    EXPECT_EQ(got, (std::vector<std::string>{R"({"v":1})", R"({"k":0})"}));
    version = 2;
    EXPECT_FALSE(pump(src, got, [&] { return got.size() >= 4; }));
    EXPECT_EQ(got[2], R"({"v":2})");
    const auto seen = ep.seen();
    EXPECT_TRUE(seen[0].if_none_match.empty());
    EXPECT_EQ(seen[1].if_none_match, "\"v1\"");
    EXPECT_EQ(src.cursor(), "");
}

TEST(HttpPollSourceResource, ABoundedRunEndsAfterOneResponse) {
    Endpoint ep;
    ep.on([](const httplib::Request&, httplib::Response& res) {
        res.set_header("ETag", "\"same\"");
        res.set_content(R"([{"id":"a"},{"id":"b"},{"id":"c"}])", "application/json");
    });
    auto o = resource_opts(ep);
    o.bounded = true;
    HttpPollSource src{o};
    std::vector<std::string> got;
    EXPECT_TRUE(pump(src, got, nullptr));
    EXPECT_EQ(got.size(), 3U);
    EXPECT_EQ(ep.count(), 1U);  // it used to poll again, emitting the same records each time

    ep.on([](const httplib::Request&, httplib::Response& res) { res.status = 204; });
    HttpPollSource empty{o};
    std::vector<std::string> none;
    EXPECT_TRUE(pump(empty, none, nullptr));
    EXPECT_TRUE(none.empty());
}

TEST(HttpPollSourceResource, A304ToARequestThatDidNotAskFails) {
    Endpoint ep;
    ep.on([](const httplib::Request&, httplib::Response& res) { res.status = 304; });
    HttpPollSource src{resource_opts(ep)};
    std::vector<std::string> got;
    EXPECT_TRUE(has(pump_error(src, got), "HTTP 304 to a request that carried no If-None-Match"));

    // A bounded run never asks, so it never takes "not modified" for an answer.
    Endpoint bounded_ep;
    bounded_ep.on(feed(3, 10));
    auto o = cursor_opts(bounded_ep);
    o.bounded = true;
    HttpPollSource bounded{o};
    std::vector<std::string> got2;
    EXPECT_TRUE(pump(bounded, got2, nullptr));
    for (const auto& s : bounded_ep.seen()) {
        EXPECT_TRUE(s.if_none_match.empty());
    }
}

TEST(HttpPollSourceResponses, UnexpectedStatusesFail) {
    for (const auto& [status, why] : std::vector<std::pair<int, std::string>>{
             {201, "HTTP 201, which is not 200, 204 or 304"},
             {202, "HTTP 202, which is not 200, 204 or 304"},
             {206, "HTTP 206, which is not 200, 204 or 304"},
             {301, "HTTP 301, a redirect the source does not follow"},
             {307, "HTTP 307, a redirect the source does not follow"},
             {400, "HTTP 400"},
             {401, "HTTP 401: the endpoint refused the credentials; check 'headers'"},
             {403, "HTTP 403: the credentials may not read this resource"},
             {404, "HTTP 404: check 'url' and 'path'"},
             {501, "HTTP 501"},
         }) {
        Endpoint ep;
        const int s = status;
        ep.on([s](const httplib::Request&, httplib::Response& res) {
            res.status = s;
            if (s == 301 || s == 307) {
                res.set_header("Location", "/elsewhere");
            }
            res.set_content("[]", "application/json");
        });
        auto o = resource_opts(ep);
        o.max_retries = 5;
        HttpPollSource src{o};
        std::vector<std::string> got;
        const auto error = pump_error(src, got);
        EXPECT_TRUE(has(error, why)) << status << " -> " << error;
        EXPECT_EQ(ep.count(), 1U) << status << " is not retried";
        EXPECT_TRUE(got.empty());
    }
}

TEST(HttpPollSourceBounds, AnOversizedResponseFails) {
    Endpoint ep;
    ep.on([](const httplib::Request&, httplib::Response& res) {
        res.set_content("[\"" + std::string(4096, 'x') + "\"]", "application/json");
    });
    auto o = resource_opts(ep);
    o.max_response_bytes = 1024;
    HttpPollSource src{o};
    std::vector<std::string> got;
    const auto error = pump_error(src, got);
    EXPECT_TRUE(has(error, "the response passed max_response_bytes (1024)")) << error;
    EXPECT_EQ(ep.count(), 1U);
    EXPECT_TRUE(got.empty());

    auto many = resource_opts(ep);
    many.max_response_records = 2;
    ep.on([](const httplib::Request&, httplib::Response& res) {
        res.set_content("[1,2,3]", "application/json");
    });
    HttpPollSource counted{many};
    std::vector<std::string> got2;
    EXPECT_TRUE(has(pump_error(counted, got2), "more than 2 records (max_response_records)"));
    EXPECT_TRUE(got2.empty());
}

TEST(HttpPollSourceBounds, ACompressedResponseCountsAsItDecompresses) {
    // 4 KiB of JSON that gzip makes far smaller than the 1 KiB bound.
    const std::string body = "[\"" + std::string(4096, 'y') + "\"]";
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
    std::string gz;
    httplib::detail::gzip_compressor gzip;
    ASSERT_TRUE(gzip.compress(body.data(), body.size(), true, [&gz](const char* d, std::size_t n) {
        gz.append(d, n);
        return true;
    }));
    ASSERT_LT(gz.size(), 1024U);
#else
    const std::string gz = body;  // without zlib the client cannot read gzip at all
#endif
    Endpoint ep;
    ep.on([&gz](const httplib::Request&, httplib::Response& res) {
        res.set_header("Content-Encoding", "gzip");
        res.set_content(gz, "application/json");
    });
    auto o = resource_opts(ep);
    o.max_response_bytes = 1024;
    HttpPollSource src{o};
    std::vector<std::string> got;
    const auto error = pump_error(src, got);
    EXPECT_FALSE(error.empty());
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
    EXPECT_TRUE(has(error, "the response passed max_response_bytes (1024)")) << error;
#endif
    EXPECT_TRUE(got.empty());
}

TEST(HttpPollSourceBounds, ASlowResponseIsBoundedAsAWhole) {
    // Headers at once, then the body a few bytes every 100 ms for 3 s: each
    // read is quick, the whole is not.
    Endpoint ep;
    ep.on([&ep](const httplib::Request&, httplib::Response& res) {
        res.set_chunked_content_provider(
            "application/json", [&ep, sent = 0](std::size_t, httplib::DataSink& sink) mutable {
                if (sent == 0) {
                    sink.write("[", 1);
                }
                if (sent++ < 30) {
                    ep.hold(100ms);
                    sink.write("1,", 2);
                    return true;
                }
                sink.write("1]", 2);
                sink.done();
                return true;
            });
    });
    auto o = resource_opts(ep);
    o.request_timeout = 300ms;
    o.max_retries = 1;
    HttpPollSource src{o};
    std::vector<std::string> got;
    const auto start = Clock::now();
    const auto error = pump_error(src, got, 10000ms);
    const auto took = Clock::now() - start;
    EXPECT_TRUE(has(error, "no complete response within request_timeout_ms (300 ms)")) << error;
    EXPECT_TRUE(has(error, "gave up after 2 attempts (max_retries 1)")) << error;
    EXPECT_LT(took, 2500ms);
    EXPECT_TRUE(got.empty());

    // A server that holds back even its headers.
    ep.on([&ep](const httplib::Request&, httplib::Response& res) {
        ep.hold(3000ms);
        res.set_content("[]", "application/json");
    });
    auto o2 = resource_opts(ep);
    o2.request_timeout = 300ms;
    o2.max_retries = 0;
    HttpPollSource held{o2};
    const auto start2 = Clock::now();
    EXPECT_TRUE(has(pump_error(held, got, 10000ms), "no complete response within"));
    EXPECT_LT(Clock::now() - start2, 1500ms);
}

TEST(HttpPollSourceRetries, TransientFailuresAreRetriedThenExhausted) {
    Endpoint ep;
    std::atomic<int> failing{1};
    ep.on([&](const httplib::Request& req, httplib::Response& res) {
        if (failing.load() > 0) {
            failing.fetch_sub(1);
            res.status = 503;
            return;
        }
        feed(2, 10)(req, res);
    });
    HttpPollSource src{cursor_opts(ep)};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return got.size() >= 2; }));
    EXPECT_EQ(got, records(1, 2));  // once each, after one retry
    EXPECT_EQ(ep.seen()[1].since, "<none>");

    for (const int status : {408, 425, 429, 500, 502, 503, 504}) {
        Endpoint down;
        down.on([status](const httplib::Request&, httplib::Response& res) { res.status = status; });
        auto o = resource_opts(down);
        o.max_retries = 2;
        HttpPollSource failing_src{o};
        std::vector<std::string> none;
        const auto error = pump_error(failing_src, none);
        EXPECT_TRUE(has(
            error, "HTTP " + std::to_string(status) + "; gave up after 3 attempts (max_retries 2)"))
            << error;
        EXPECT_EQ(down.count(), 3U);
    }

    // No server at all: the connection is refused, which is worth retrying.
    std::string closed_url;
    {
        Endpoint gone;
        closed_url = gone.url();
    }
    HttpPollOptions o;
    o.url = closed_url;
    o.interval = 0ms;
    o.retry_base_backoff = 1ms;
    o.max_backoff = 1ms;
    o.max_retries = 1;
    HttpPollSource refused{o};
    std::vector<std::string> none;
    EXPECT_TRUE(has(pump_error(refused, none), "no response ("));
}

TEST(HttpPollSourceRetries, RetryTimeoutBoundsAPoll) {
    Endpoint ep;
    ep.on([](const httplib::Request&, httplib::Response& res) { res.status = 500; });
    auto o = resource_opts(ep);
    o.max_retries = 20;
    o.retry_base_backoff = 100ms;
    o.max_backoff = 100ms;
    o.retry_timeout = 350ms;
    HttpPollSource src{o};
    std::vector<std::string> got;
    const auto start = Clock::now();
    const auto error = pump_error(src, got);
    EXPECT_TRUE(has(error, "the next attempt would end past retry_timeout_ms (350 ms)")) << error;
    EXPECT_LT(Clock::now() - start, 1500ms);
    EXPECT_LE(ep.count(), 5U);
}

TEST(HttpPollSourceRetries, ThrottlingHonoursRetryAfterWithinBounds) {
    Endpoint ep;
    std::atomic<int> throttled{1};
    ep.on([&](const httplib::Request& req, httplib::Response& res) {
        if (throttled.load() > 0) {
            throttled.fetch_sub(1);
            res.status = 429;
            res.set_header("Retry-After", "1");
            return;
        }
        feed(1, 10)(req, res);
    });
    auto o = cursor_opts(ep);
    o.retry_timeout = 10000ms;
    HttpPollSource src{o};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return !got.empty(); }));
    auto seen = ep.seen();
    ASSERT_GE(seen.size(), 2U);
    EXPECT_GE(seen[1].at - seen[0].at, 900ms);  // waited as asked, not 1 ms

    // An HTTP date works the same way.
    Endpoint dated;
    std::atomic<int> once{1};
    dated.on([&](const httplib::Request& req, httplib::Response& res) {
        if (once.fetch_sub(1) > 0) {
            const auto when = std::chrono::system_clock::to_time_t(
                std::chrono::system_clock::now() + std::chrono::seconds{2});
            char buf[64];
            std::tm tm{};
            gmtime_r(&when, &tm);
            std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
            res.status = 503;
            res.set_header("Retry-After", buf);
            return;
        }
        feed(1, 10)(req, res);
    });
    auto od = cursor_opts(dated);
    od.retry_timeout = 10000ms;
    HttpPollSource date_src{od};
    std::vector<std::string> got2;
    EXPECT_FALSE(pump(date_src, got2, [&] { return !got2.empty(); }));
    seen = dated.seen();
    ASSERT_GE(seen.size(), 2U);
    EXPECT_GE(seen[1].at - seen[0].at, 900ms);

    // A wait past the poll's retry budget fails at once rather than waiting.
    Endpoint slow;
    slow.on([](const httplib::Request&, httplib::Response& res) {
        res.status = 429;
        res.set_header("Retry-After", "120");
    });
    auto os = resource_opts(slow);
    os.retry_timeout = 2000ms;
    HttpPollSource budget{os};
    const auto start = Clock::now();
    const auto error = pump_error(budget, got);
    EXPECT_TRUE(
        has(error, "the endpoint asked to wait 120000 ms, which would end past retry_timeout_ms"))
        << error;
    EXPECT_LT(Clock::now() - start, 1000ms);
    EXPECT_EQ(slow.count(), 1U);
}

TEST(HttpPollSourceCancel, ACancelEndsAWaitAtOnce) {
    Endpoint ep;
    ep.on(feed(3, 10));
    auto o = cursor_opts(ep);
    o.interval = 60000ms;
    HttpPollSource src{o};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return got.size() >= 3; }));
    std::atomic<bool> stopped{false};
    Clock::time_point stopped_at{};
    std::thread runner([&] {
        auto em = capturing(got);
        while (src.produce(em)) {
        }
        stopped_at = Clock::now();
        stopped = true;
    });
    std::this_thread::sleep_for(300ms);
    EXPECT_FALSE(stopped.load());  // waiting out the interval, a slice at a time
    const auto cancelled_at = Clock::now();
    src.cancel();
    runner.join();
    EXPECT_LT(stopped_at - cancelled_at, 250ms);
    EXPECT_EQ(ep.count(), 1U);

    // And the pause before a retry.
    Endpoint down;
    down.on([](const httplib::Request&, httplib::Response& res) { res.status = 503; });
    auto od = resource_opts(down);
    od.retry_base_backoff = 30000ms;
    od.max_backoff = 30000ms;
    od.retry_timeout = 600000ms;
    HttpPollSource backing_off{od};
    std::thread runner2([&] {
        std::vector<std::string> none;
        auto em = capturing(none);
        while (backing_off.produce(em)) {
        }
    });
    std::this_thread::sleep_for(300ms);
    const auto t0 = Clock::now();
    backing_off.cancel();
    runner2.join();
    EXPECT_LT(Clock::now() - t0, 250ms);
    EXPECT_EQ(down.count(), 1U);
}

TEST(HttpPollSourceCancel, ACancelEndsARequestInFlight) {
    Endpoint ep;
    ep.on([&ep](const httplib::Request&, httplib::Response& res) {
        ep.hold(10000ms);
        res.set_content(R"({"items":[{"seq":1}]})", "application/json");
    });
    auto o = cursor_opts(ep);
    o.request_timeout = 60000ms;
    HttpPollSource src{o};
    std::vector<std::string> got;
    std::atomic<bool> returned{false};
    bool result = true;
    std::thread runner([&] {
        auto em = capturing(got);
        result = src.produce(em);
        returned = true;
    });
    while (ep.count() == 0) {
        std::this_thread::sleep_for(5ms);
    }
    std::this_thread::sleep_for(100ms);
    EXPECT_FALSE(returned.load());
    const auto t0 = Clock::now();
    src.cancel();
    runner.join();
    EXPECT_LT(Clock::now() - t0, 500ms);
    EXPECT_FALSE(result);
    EXPECT_TRUE(got.empty());
    EXPECT_EQ(src.cursor(), "");
    ep.release();
}

TEST(HttpPollSourceErrors, NoCredentialOrQueryReachesAnError) {
    const std::vector<std::string> secrets = {
        "tok-SECRET-1", "key-SECRET-2", "qs-SECRET-3", "access_token"};
    const auto clean = [&](const std::string& error) {
        for (const auto& s : secrets) {
            if (has(error, s)) {
                return false;
            }
        }
        return has(error, "GET /feed: ");
    };
    const auto source_for = [](const Endpoint& ep) {
        auto o = cursor_opts(ep);
        o.path = "/feed?access_token=qs-SECRET-3&x=1";
        o.headers = "Authorization: Bearer tok-SECRET-1; X-Api-Key: key-SECRET-2";
        o.max_retries = 1;
        return o;
    };
    for (const auto& handler : std::vector<Endpoint::Handler>{
             [](const httplib::Request&, httplib::Response& res) { res.status = 401; },
             [](const httplib::Request&, httplib::Response& res) { res.status = 503; },
             [](const httplib::Request& req, httplib::Response& res) {
                 // A body that echoes what it was sent, as a debugging endpoint might.
                 res.set_content(req.get_header_value("Authorization") + req.target, "text/plain");
             },
             [](const httplib::Request&, httplib::Response& res) {
                 res.set_content(R"({"items":[{"v":1}]})", "application/json");
             },
         }) {
        Endpoint ep;
        ep.on(handler);
        HttpPollSource src{source_for(ep)};
        std::vector<std::string> got;
        const auto error = pump_error(src, got);
        EXPECT_FALSE(error.empty());
        EXPECT_TRUE(clean(error)) << error;
    }
    // The headers did reach the endpoint.
    Endpoint ep;
    std::string auth;
    ep.on([&](const httplib::Request& req, httplib::Response& res) {
        auth = req.get_header_value("Authorization");
        res.set_content(R"({"items":[]})", "application/json");
    });
    HttpPollSource src{source_for(ep)};
    std::vector<std::string> got;
    EXPECT_FALSE(pump(src, got, [&] { return ep.count() >= 1; }));
    EXPECT_EQ(auth, "Bearer tok-SECRET-1");
}

// ---- the capability record and the delivery analyser ----------------------------------

namespace {
clink::cluster::OperatorSpec poll_op(std::string type,
                                     std::string id,
                                     std::vector<std::string> inputs,
                                     std::map<std::string, std::string> params) {
    clink::cluster::OperatorSpec s;
    s.type = std::move(type);
    s.id = std::move(id);
    s.inputs = std::move(inputs);
    s.params = std::move(params);
    return s;
}
}  // namespace

TEST(HttpPollCapability, ReplayRequiresCursorMode) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("http_poll");
    ASSERT_NE(caps, nullptr);
    EXPECT_TRUE(caps->is_source);
    EXPECT_FALSE(caps->is_sink);
    EXPECT_TRUE(caps->replayable);
    EXPECT_EQ(caps->replay_requires_options,
              (std::vector<std::string>{"cursor_field", "cursor_param"}));
    EXPECT_TRUE(caps->self_check().empty());
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("http_poll_source"), "http_poll");
    const auto* http = clink::connectors::CapabilityRegistry::instance().find("http");
    ASSERT_NE(http, nullptr);
    EXPECT_FALSE(http->is_source);
    EXPECT_TRUE(http->self_check().empty());

    // A declaration that lists replay options without replaying is refused.
    clink::connectors::ConnectorCapabilities wrong{.name = "w", .version = "1", .is_source = true};
    wrong.replay_requires_options = {"x"};
    EXPECT_FALSE(wrong.self_check().empty());
}

TEST(HttpPollCapability, AStrongerGuaranteeNeedsCursorMode) {
    clink::cluster::CheckpointConfig ckpt;
    ckpt.checkpoint_dir = "/tmp/ckpt";
    ckpt.interval_ms = 1000;
    ckpt.state_backend_uri = "file:///tmp/state";
    const std::map<std::string, std::string> cursor{
        {"url", "http://h:1"}, {"cursor_param", "since"}, {"cursor_field", "seq"}};
    clink::cluster::JobGraphSpec g;
    g.ops.push_back(poll_op("http_poll_source", "src", {}, cursor));
    g.ops.push_back(poll_op("file_2pc_sink_string",
                            "snk",
                            {"src"},
                            {{"dir", "/tmp/out"}, {"delivery_guarantee", "exactly_once"}}));
    clink::connectors::GuaranteeReport report;
    EXPECT_EQ(clink::cluster::check_delivery_guarantee(g, ckpt, &report), "")
        << report.render_text();
    EXPECT_EQ(report.level, clink::connectors::EndToEndGuarantee::EndToEndExactlyOnce)
        << report.render_text();

    // Resource mode keeps no position: at-most-once, and every stronger
    // request is refused, naming what would make the source replay.
    g.ops[0].params = {{"url", "http://h:1"}};
    for (const char* want : {"at_least_once", "exactly_once"}) {
        g.ops[1].params["delivery_guarantee"] = want;
        const auto refused = clink::cluster::check_delivery_guarantee(g, ckpt, &report);
        EXPECT_TRUE(has(refused, "cannot replay from a checkpointed position")) << refused;
        EXPECT_TRUE(has(refused, "only with option 'cursor_field'")) << refused;
        EXPECT_EQ(report.level, clink::connectors::EndToEndGuarantee::AtMostOnceSource);
    }
    g.ops[0].params = {{"url", "http://h:1"}, {"cursor_field", "seq"}};
    EXPECT_TRUE(has(clink::cluster::check_delivery_guarantee(g, ckpt, &report),
                    "only with option 'cursor_param'"));
    g.ops[1].params["delivery_guarantee"] = "at_most_once";
    EXPECT_EQ(clink::cluster::check_delivery_guarantee(g, ckpt, &report), "");
}
