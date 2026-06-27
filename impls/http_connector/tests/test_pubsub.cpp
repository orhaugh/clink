// Offline tests for the Google Cloud Pub/Sub connector: the pure REST helpers
// (base64, path/body builders, pull-response parsing, id validation) plus a full
// sink -> source -> ack round-trip against an in-process httplib stub that
// faithfully emulates the :publish / :pull / :acknowledge endpoints. Runs in CI
// with no external dependency.

#include <atomic>
#include <chrono>
#include <deque>
#include <httplib.h>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/base64.hpp"
#include "clink/http_connector/pubsub.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using clink::Batch;
using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StateBackend;
using clink::StreamElement;
using namespace clink::http_connector;

namespace {

// --- pure helpers ---

TEST(PubSubBase64, RoundTripsAllRemainders) {
    for (const std::string& s : {std::string{},
                                 std::string{"f"},
                                 std::string{"fo"},
                                 std::string{"foo"},
                                 std::string{"foob"},
                                 std::string{"fooba"},
                                 std::string{"foobar"}}) {
        const std::string enc = base64_encode(s);
        auto dec = base64_decode(enc);
        ASSERT_TRUE(dec.has_value()) << s;
        EXPECT_EQ(*dec, s) << "enc=" << enc;
    }
    // Known vector + embedded binary (NUL).
    EXPECT_EQ(base64_encode("foobar"), "Zm9vYmFy");
    const std::string bin{'\x00', '\x01', '\xff', '\x10', '\x00'};
    EXPECT_EQ(*base64_decode(base64_encode(bin)), bin);
}

TEST(PubSubBase64, DecodeToleratesWhitespaceAndRejectsGarbage) {
    EXPECT_EQ(*base64_decode("Zm9v\nYmFy"), "foobar");
    EXPECT_FALSE(base64_decode("not base64!*").has_value());
}

TEST(PubSubPaths, BuildVerbSuffixedResourcePaths) {
    EXPECT_EQ(pubsub_publish_path("p", "t"), "/v1/projects/p/topics/t:publish");
    EXPECT_EQ(pubsub_pull_path("p", "s"), "/v1/projects/p/subscriptions/s:pull");
    EXPECT_EQ(pubsub_ack_path("p", "s"), "/v1/projects/p/subscriptions/s:acknowledge");
}

TEST(PubSubIds, RejectInjectionAndEmpty) {
    EXPECT_THROW(pubsub_check_id("topic", ""), std::runtime_error);
    EXPECT_THROW(pubsub_check_id("topic", "a/b"), std::runtime_error);
    EXPECT_THROW(pubsub_check_id("topic", "a:b"), std::runtime_error);
    EXPECT_THROW(pubsub_check_id("topic", "a b"), std::runtime_error);
    // Allowlist also rejects non-injecting-but-illegal chars up front.
    EXPECT_THROW(pubsub_check_id("topic", "a\"b"), std::runtime_error);
    EXPECT_THROW(pubsub_check_id("topic", "a{b}"), std::runtime_error);
    EXPECT_NO_THROW(pubsub_check_id("topic", "good-name_1.2~plus+ok"));
}

TEST(PubSubBodies, MessageFragmentAndPullAndAck) {
    EXPECT_EQ(pubsub_message_fragment("foobar"), R"({"data":"Zm9vYmFy"})");
    EXPECT_EQ(pubsub_pull_body(50, true), R"({"maxMessages":50,"returnImmediately":true})");
    EXPECT_EQ(pubsub_pull_body(1, false), R"({"maxMessages":1,"returnImmediately":false})");
    EXPECT_EQ(pubsub_ack_body({"a", "b"}), R"({"ackIds":["a","b"]})");
    EXPECT_EQ(pubsub_ack_body({}), R"({"ackIds":[]})");
}

TEST(PubSubParse, PullResponseDecodesAndSkipsUnackable) {
    const std::string body = R"({"receivedMessages":[
        {"ackId":"ack-1","message":{"data":"Zm9vYmFy","messageId":"m1"}},
        {"message":{"data":"eA=="}},
        {"ackId":"ack-3","message":{"data":"%%%bad"}}
    ]})";
    auto msgs = pubsub_parse_pull_response(body);
    ASSERT_EQ(msgs.size(), 2u);  // the no-ackId message is dropped
    EXPECT_EQ(msgs[0].ack_id, "ack-1");
    EXPECT_EQ(msgs[0].payload, "foobar");
    EXPECT_EQ(msgs[0].message_id, "m1");
    EXPECT_TRUE(msgs[0].data_ok);
    EXPECT_EQ(msgs[1].ack_id, "ack-3");
    EXPECT_FALSE(msgs[1].data_ok);  // undecodable data -> flagged, not emitted
}

TEST(PubSubParse, EmptyAndAbsentReceivedMessages) {
    EXPECT_TRUE(pubsub_parse_pull_response("{}").empty());
    EXPECT_TRUE(pubsub_parse_pull_response(R"({"receivedMessages":[]})").empty());
    EXPECT_THROW(pubsub_parse_pull_response("not json"), std::runtime_error);
}

TEST(PubSubOptionValidation, SinkAndSourceRejectMissingIds) {
    PubSubSinkOptions so;
    so.topic = "t";  // project empty
    EXPECT_THROW(make_pubsub_publish_sink(so), std::runtime_error);
    PubSubSourceOptions ro;
    ro.project = "p";  // subscription empty
    EXPECT_THROW(PubSubPullSource{ro}, std::runtime_error);

    PubSubSourceOptions ok;
    ok.project = "p";
    ok.subscription = "s";
    PubSubPullSource src{ok};
    EXPECT_FALSE(src.is_bounded());
    EXPECT_EQ(src.name(), "pubsub_source");
}

// --- in-process emulator stub: store-and-forward over the real wire ---

class PubSubStub {
public:
    PubSubStub(const std::string& project, const std::string& topic, const std::string& sub) {
        svr_.Post(pubsub_publish_path(project, topic),
                  [this](const httplib::Request& req, httplib::Response& res) {
                      std::lock_guard<std::mutex> lk(mu_);
                      std::string ids = R"({"messageIds":[)";
                      bool first = true;
                      auto root = clink::config::parse(req.body);
                      for (const auto& m : root.as_object().at("messages").as_array()) {
                          const std::string b64 = m.as_object().at("data").as_string();
                          Stored s;
                          s.payload = *base64_decode(b64);
                          s.message_id = "m" + std::to_string(seq_);
                          s.ack_id = "ack" + std::to_string(seq_);
                          ++seq_;
                          if (!first) {
                              ids.push_back(',');
                          }
                          first = false;
                          ids += "\"" + s.message_id + "\"";
                          queue_.push_back(std::move(s));
                      }
                      ids += "]}";
                      res.set_content(ids, "application/json");
                  });
        svr_.Post(pubsub_pull_path(project, sub),
                  [this](const httplib::Request& req, httplib::Response& res) {
                      std::lock_guard<std::mutex> lk(mu_);
                      int max_messages = 100;
                      auto root = clink::config::parse(req.body);
                      if (auto it = root.as_object().find("maxMessages");
                          it != root.as_object().end() && it->second.is_number()) {
                          max_messages = static_cast<int>(it->second.as_number());
                      }
                      std::string body = R"({"receivedMessages":[)";
                      int n = 0;
                      bool first = true;
                      for (auto& s : queue_) {
                          if (s.acked || s.outstanding || n >= max_messages) {
                              continue;
                          }
                          s.outstanding = true;
                          ++n;
                          if (!first) {
                              body.push_back(',');
                          }
                          first = false;
                          body += R"({"ackId":")" + s.ack_id + R"(","message":{"data":")" +
                                  base64_encode(s.payload) + R"(","messageId":")" + s.message_id +
                                  "\"}}";
                      }
                      body += "]}";
                      res.set_content(body, "application/json");
                  });
        svr_.Post(pubsub_ack_path(project, sub),
                  [this](const httplib::Request& req, httplib::Response& res) {
                      std::lock_guard<std::mutex> lk(mu_);
                      auto root = clink::config::parse(req.body);
                      for (const auto& a : root.as_object().at("ackIds").as_array()) {
                          const std::string id = a.as_string();
                          for (auto& s : queue_) {
                              if (s.ack_id == id) {
                                  s.acked = true;
                              }
                          }
                      }
                      res.status = 200;
                  });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~PubSubStub() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    std::size_t unacked() {
        std::lock_guard<std::mutex> lk(mu_);
        std::size_t n = 0;
        for (const auto& s : queue_) {
            if (!s.acked) {
                ++n;
            }
        }
        return n;
    }

private:
    struct Stored {
        std::string payload;
        std::string message_id;
        std::string ack_id;
        bool outstanding{false};
        bool acked{false};
    };
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::deque<Stored> queue_;
    int seq_{0};
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

TEST(PubSubRoundTrip, PublishThenPullThenAck) {
    const std::string project = "test-project";
    const std::string topic = "t";
    const std::string sub = "s";
    PubSubStub stub(project, topic, sub);

    // Publish three records through the sink (PubSubMessages framing + base64).
    PubSubSinkOptions so;
    so.http.base_url = stub.url();
    so.project = project;
    so.topic = topic;
    so.batch_records = 100;
    auto sink = make_pubsub_publish_sink(so);
    sink->open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"id":1})"});
    b.emplace(std::string{R"({"id":2})"});
    b.emplace(std::string{R"({"id":3})"});
    sink->on_data(b);
    sink->flush();
    sink->close();
    ASSERT_EQ(stub.unacked(), 3u);

    // Pull them back, then acknowledge on a checkpoint.
    PubSubSourceOptions ro;
    ro.http.base_url = stub.url();
    ro.project = project;
    ro.subscription = sub;
    ro.poll_interval = std::chrono::milliseconds{0};
    PubSubPullSource src{ro};
    src.open();
    Captured cap;
    auto em = capturing(cap);
    src.produce(em);
    ASSERT_EQ(cap.values.size(), 3u);
    EXPECT_EQ(cap.values[0], R"({"id":1})");
    EXPECT_EQ(cap.values[2], R"({"id":3})");
    EXPECT_EQ(src.unacked_count(), 3u);

    InMemoryStateBackend backend;
    const OperatorId op{1};
    src.snapshot_offset(backend, op, CheckpointId{1});
    EXPECT_EQ(src.unacked_count(), 0u);
    EXPECT_EQ(src.acked_total(), 3u);
    EXPECT_EQ(stub.unacked(), 0u) << "server side acknowledged";
    EXPECT_TRUE(src.restore_offset(backend, op));

    // A second pull after ack returns nothing (the messages are gone).
    Captured cap2;
    auto em2 = capturing(cap2);
    src.produce(em2);
    EXPECT_TRUE(cap2.values.empty());
    src.close();
}

}  // namespace
