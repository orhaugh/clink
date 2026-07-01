// The HTTP model-inference provider against a local httplib stub: it POSTs the
// feature columns as JSON, and maps the JSON response into the model's OUTPUT columns.

#include <chrono>
#include <httplib.h>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/http_connector/http_model_provider.hpp"
#include "clink/sql/row.hpp"

namespace {

using namespace std::chrono_literals;

// POST /infer echoes the request's "text" feature back as {"label": <text>, "conf": 0.9}.
// POST /boom returns 500 (to exercise the error path).
class InferStub {
public:
    InferStub() {
        svr_.Post("/infer", [](const httplib::Request& req, httplib::Response& res) {
            auto parsed = clink::config::parse(req.body);
            std::string text;
            if (parsed.is_object() && parsed.as_object().find("text") != parsed.as_object().end()) {
                text = parsed.at("text").as_string();
            }
            clink::config::JsonObject o;
            o["label"] = clink::config::JsonValue{text};
            o["conf"] = clink::config::JsonValue{0.9};
            res.set_content(clink::config::JsonValue{std::move(o)}.serialize(0),
                            "application/json");
            res.status = 200;
        });
        svr_.Post("/boom",
                  [](const httplib::Request&, httplib::Response& res) { res.status = 500; });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~InferStub() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
};

}  // namespace

TEST(HttpModelProvider, PredictRoundTripsFeaturesAndOutputs) {
    InferStub stub;
    std::map<std::string, std::string> opts;
    opts["endpoint"] = stub.url() + "/infer";
    opts["output_columns"] = "label,conf";
    auto provider = clink::http_connector::make_http_model_provider(opts);

    clink::sql::Row features;
    features.values["text"] = clink::config::JsonValue{std::string("hello")};
    const clink::sql::Row out = provider->predict(features);

    ASSERT_TRUE(out.values.find("label") != out.values.end());
    EXPECT_EQ(out.values.at("label").as_string(), "hello");  // echoed by the stub
    ASSERT_TRUE(out.values.find("conf") != out.values.end());
    EXPECT_NEAR(out.values.at("conf").as_number(), 0.9, 1e-9);
}

TEST(HttpModelProvider, PredictThrowsOnErrorStatus) {
    InferStub stub;
    std::map<std::string, std::string> opts;
    opts["endpoint"] = stub.url() + "/boom";
    opts["output_columns"] = "label";
    auto provider = clink::http_connector::make_http_model_provider(opts);

    clink::sql::Row features;
    features.values["text"] = clink::config::JsonValue{std::string("x")};
    EXPECT_THROW((void)provider->predict(features), std::runtime_error);
}

TEST(HttpModelProvider, MissingEndpointRejected) {
    const std::map<std::string, std::string> opts;  // no endpoint
    EXPECT_THROW((void)clink::http_connector::make_http_model_provider(opts), std::runtime_error);
}
