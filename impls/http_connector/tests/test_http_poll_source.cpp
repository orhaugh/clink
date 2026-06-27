// HTTP polling source: GETs a JSON endpoint, emits the array elements, threads
// the cursor (last record's cursor_field) into the next request as a query param,
// and supports a nested records_field. Driven against a local httplib stub.

#include <atomic>
#include <chrono>
#include <httplib.h>
#include <mutex>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/http_poll_source.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Emitter;
using clink::Source;
using clink::StreamElement;
using clink::http_connector::HttpPollOptions;
using clink::http_connector::make_http_poll_source;

namespace {

// Stub that serves /items as a JSON array, switching on the `since` query param
// so two polls return different pages; /wrapped returns {"data":[...]}.
class PollStub {
public:
    PollStub() {
        svr_.Get("/items", [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                since_seen_.push_back(req.has_param("since") ? req.get_param_value("since")
                                                             : std::string{"<none>"});
            }
            const std::string since = req.has_param("since") ? req.get_param_value("since") : "";
            if (since.empty()) {
                res.set_content(R"([{"id":1,"v":"a"},{"id":2,"v":"b"}])", "application/json");
            } else if (since == "2") {
                res.set_content(R"([{"id":3,"v":"c"}])", "application/json");
            } else {
                res.set_content("[]", "application/json");
            }
        });
        svr_.Get("/wrapped", [](const httplib::Request&, httplib::Response& res) {
            res.set_content(R"({"data":[{"id":1},{"id":2}],"page":1})", "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~PollStub() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    std::vector<std::string> since_seen() {
        std::lock_guard<std::mutex> lk(mu_);
        return since_seen_;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> since_seen_;
};

struct Captured {
    std::vector<std::string> values;
};

Emitter<std::string> capturing(Captured& sink) {
    return Emitter<std::string>{[&sink](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.values.push_back(r.value());
            }
        }
        return true;
    }};
}

bool contains(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
}

HttpPollOptions base_opts(const std::string& url) {
    HttpPollOptions o;
    o.url = url;
    o.interval = std::chrono::milliseconds{0};  // no sleeping in tests
    return o;
}

}  // namespace

TEST(HttpPollSource, EmitsArrayElementsAndAdvancesCursor) {
    PollStub srv;
    auto o = base_opts(srv.url());
    o.path = "/items";
    o.cursor_param = "since";
    o.cursor_field = "id";
    auto src = make_http_poll_source(std::move(o));
    Captured cap;
    auto em = capturing(cap);
    src->produce(em);  // no cursor -> ids 1,2; cursor -> "2"
    src->produce(em);  // since=2 -> id 3
    ASSERT_EQ(cap.values.size(), 3u);
    EXPECT_TRUE(contains(cap.values[0], "\"id\":1")) << cap.values[0];
    EXPECT_TRUE(contains(cap.values[2], "\"id\":3")) << cap.values[2];
    // Second request carried the cursor from the last record of the first page.
    auto seen = srv.since_seen();
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], "<none>");
    EXPECT_EQ(seen[1], "2");
}

TEST(HttpPollSource, RecordsFieldUnwrapsNestedArray) {
    PollStub srv;
    auto o = base_opts(srv.url());
    o.path = "/wrapped";
    o.records_field = "data";
    o.cursor_field = "id";
    auto src = make_http_poll_source(std::move(o));
    Captured cap;
    auto em = capturing(cap);
    src->produce(em);
    ASSERT_EQ(cap.values.size(), 2u);
    EXPECT_TRUE(contains(cap.values[0], "\"id\":1")) << cap.values[0];
}

TEST(HttpPollSource, RequiresUrl) {
    HttpPollOptions o;  // no url
    EXPECT_THROW(make_http_poll_source(std::move(o)), std::runtime_error);
}

TEST(HttpPollSource, UrlEncodesCursorValue) {
    using clink::http_connector::url_encode;
    EXPECT_EQ(url_encode("2026-06-27T00:00:00Z"), "2026-06-27T00%3A00%3A00Z");
    EXPECT_EQ(url_encode("a b&c"), "a%20b%26c");
    EXPECT_EQ(url_encode("plain-1.0_x~"), "plain-1.0_x~");
}
