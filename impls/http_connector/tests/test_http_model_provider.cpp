// The HTTP model-inference provider against a local httplib stub: it POSTs the
// feature columns as JSON, and maps the JSON response into the model's OUTPUT columns.

#include <atomic>
#include <chrono>
#include <httplib.h>
#include <memory>
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
        // POST /flaky returns 503 on the first call and 200 (echo) after, so a provider
        // with retries recovers rather than surfacing the transient failure.
        svr_.Post("/flaky",
                  [calls = flaky_calls_](const httplib::Request& req, httplib::Response& res) {
                      if (calls->fetch_add(1) == 0) {
                          res.status = 503;
                          return;
                      }
                      auto parsed = clink::config::parse(req.body);
                      std::string text;
                      if (parsed.is_object() &&
                          parsed.as_object().find("text") != parsed.as_object().end()) {
                          text = parsed.at("text").as_string();
                      }
                      clink::config::JsonObject o;
                      o["label"] = clink::config::JsonValue{text};
                      res.set_content(clink::config::JsonValue{std::move(o)}.serialize(0),
                                      "application/json");
                      res.status = 200;
                  });
        // POST /infer_batch takes a JSON array of feature objects and returns a JSON
        // array of predictions in the same order (one per input row).
        svr_.Post("/infer_batch", [](const httplib::Request& req, httplib::Response& res) {
            auto parsed = clink::config::parse(req.body);
            clink::config::JsonArray out;
            if (parsed.is_array()) {
                for (const auto& el : parsed.as_array()) {
                    std::string text;
                    if (el.is_object() && el.as_object().find("text") != el.as_object().end()) {
                        text = el.at("text").as_string();
                    }
                    clink::config::JsonObject o;
                    o["label"] = clink::config::JsonValue{text};
                    o["conf"] = clink::config::JsonValue{0.9};
                    out.push_back(clink::config::JsonValue{std::move(o)});
                }
            }
            res.set_content(clink::config::JsonValue{std::move(out)}.serialize(0),
                            "application/json");
            res.status = 200;
        });
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

    [[nodiscard]] int flaky_call_count() const { return flaky_calls_->load(); }

private:
    std::shared_ptr<std::atomic<int>> flaky_calls_ = std::make_shared<std::atomic<int>>(0);
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

TEST(HttpModelProvider, PredictBatchRoundTripsArray) {
    InferStub stub;
    std::map<std::string, std::string> opts;
    opts["endpoint"] = stub.url() + "/infer_batch";
    opts["output_columns"] = "label,conf";
    opts["max_batch_size"] = "8";
    auto provider = clink::http_connector::make_http_model_provider(opts);
    EXPECT_EQ(provider->max_batch_size(), 8u);  // routes ML_PREDICT to the batching op

    std::vector<clink::sql::Row> batch;
    for (const char* t : {"a", "b", "c"}) {
        clink::sql::Row f;
        f.values["text"] = clink::config::JsonValue{std::string(t)};
        batch.push_back(std::move(f));
    }
    const std::vector<clink::sql::Row> out = provider->predict_batch(batch);
    ASSERT_EQ(out.size(), 3u);  // one prediction per input row, in order
    EXPECT_EQ(out[0].values.at("label").as_string(), "a");
    EXPECT_EQ(out[1].values.at("label").as_string(), "b");
    EXPECT_EQ(out[2].values.at("label").as_string(), "c");
    EXPECT_NEAR(out[2].values.at("conf").as_number(), 0.9, 1e-9);
}

TEST(HttpModelProvider, RetriesTransientFailure) {
    InferStub stub;
    std::map<std::string, std::string> opts;
    opts["endpoint"] = stub.url() + "/flaky";
    opts["output_columns"] = "label";
    opts["max_retries"] = "2";
    opts["retry_backoff_ms"] = "1";  // keep the test fast
    auto provider = clink::http_connector::make_http_model_provider(opts);

    clink::sql::Row features;
    features.values["text"] = clink::config::JsonValue{std::string("hi")};
    // First attempt is 503, the retry hits 200 - predict succeeds instead of throwing.
    const clink::sql::Row out = provider->predict(features);
    EXPECT_EQ(out.values.at("label").as_string(), "hi");
    EXPECT_GE(stub.flaky_call_count(), 2);  // at least one retry happened
}

TEST(HttpModelProvider, MissingEndpointRejected) {
    const std::map<std::string, std::string> opts;  // no endpoint
    EXPECT_THROW((void)clink::http_connector::make_http_model_provider(opts), std::runtime_error);
}
