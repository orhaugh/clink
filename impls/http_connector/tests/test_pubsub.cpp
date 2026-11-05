// Offline tests for the Google Cloud Pub/Sub connector: the pure REST helpers
// (base64, path/body builders, pull-response parsing, id validation), a full
// sink -> source -> ack round-trip, and the source's acknowledgement contract
// (acknowledged only once the covering checkpoint completes; leases held and
// bounded; redelivery under a new ackId; failures, aborts, crashes, the
// outstanding bound, close and undecodable data) against an in-process httplib
// stub of the :publish / :pull / :modifyAckDeadline / :acknowledge endpoints.
// Runs in CI with no external dependency.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <httplib.h>
#include <mutex>
#include <set>
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
//
// What the source relies on, as the service behaves: each pull delivers a
// message under a NEW ackId and leases it for the subscription's deadline;
// modifyAckDeadline moves the lease of a message whose ackId is its current one
// (0 hands it back at once); acknowledge acks a message only through its
// current, unexpired ackId, so an ack with a superseded or expired one does
// nothing; and a message whose lease ran out is delivered again. One requested
// second lasts ms_per_second milliseconds, so leases can be tested quickly.

struct StubLeases {
    int sub_deadline_ms{10000};  // how long a pull leases a message
    int ms_per_second{1000};     // how long one requested second lasts
};

class PubSubStub {
public:
    PubSubStub(const std::string& project,
               const std::string& topic,
               const std::string& sub,
               StubLeases cfg = {})
        : cfg_(cfg) {
        svr_.Post(pubsub_publish_path(project, topic),
                  [this](const httplib::Request& req, httplib::Response& res) {
                      std::lock_guard<std::mutex> lk(mu_);
                      std::string ids = R"({"messageIds":[)";
                      bool first = true;
                      auto root = clink::config::parse(req.body);
                      for (const auto& m : root.as_object().at("messages").as_array()) {
                          Stored st;
                          st.data = m.as_object().at("data").as_string();
                          st.message_id = "m" + std::to_string(queue_.size());
                          if (!first) {
                              ids.push_back(',');
                          }
                          first = false;
                          ids += "\"" + st.message_id + "\"";
                          queue_.push_back(std::move(st));
                      }
                      ids += "]}";
                      res.set_content(ids, "application/json");
                  });
        svr_.Post(pubsub_pull_path(project, sub),
                  [this](const httplib::Request& req, httplib::Response& res) {
                      std::lock_guard<std::mutex> lk(mu_);
                      ++pulls_;
                      int max_messages = 100;
                      auto root = clink::config::parse(req.body);
                      if (auto it = root.as_object().find("maxMessages");
                          it != root.as_object().end() && it->second.is_number()) {
                          max_messages = static_cast<int>(it->second.as_number());
                      }
                      const auto now = Clock::now();
                      std::string body = R"({"receivedMessages":[)";
                      int n = 0;
                      for (auto& st : queue_) {
                          if (st.acked || st.lease_until > now || n >= max_messages) {
                              continue;
                          }
                          ++st.deliveries;
                          st.ack_id = st.message_id + "-d" + std::to_string(st.deliveries);
                          st.lease_until = now + std::chrono::milliseconds{cfg_.sub_deadline_ms};
                          body += (n++ > 0 ? "," : "");
                          body += R"({"ackId":")" + st.ack_id + R"(","message":{"data":")" +
                                  st.data + R"(","messageId":")" + st.message_id + "\"}}";
                      }
                      body += "]}";
                      res.set_content(body, "application/json");
                  });
        svr_.Post(pubsub_modack_path(project, sub),
                  [this](const httplib::Request& req, httplib::Response& res) {
                      std::lock_guard<std::mutex> lk(mu_);
                      ++modacks_;
                      const auto now = Clock::now();
                      auto root = clink::config::parse(req.body);
                      const auto seconds =
                          static_cast<int>(root.as_object().at("ackDeadlineSeconds").as_number());
                      for (const auto& a : root.as_object().at("ackIds").as_array()) {
                          if (Stored* st = current(a.as_string(), now)) {
                              st->lease_until =
                                  now + std::chrono::milliseconds{seconds * cfg_.ms_per_second};
                              if (seconds == 0) {
                                  ++nacked_;
                              }
                          }
                      }
                      res.status = 200;
                  });
        svr_.Post(pubsub_ack_path(project, sub),
                  [this](const httplib::Request& req, httplib::Response& res) {
                      std::lock_guard<std::mutex> lk(mu_);
                      ++ack_requests_;
                      if (fail_acks_ > 0) {
                          --fail_acks_;
                          res.status = fail_status_;
                          return;
                      }
                      const auto now = Clock::now();
                      auto root = clink::config::parse(req.body);
                      for (const auto& a : root.as_object().at("ackIds").as_array()) {
                          if (Stored* st = current(a.as_string(), now)) {
                              st->acked = true;
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

    // Publish without going through the sink: `data` is the message's data
    // field as sent, base64 or not.
    void publish_raw(const std::string& data) {
        std::lock_guard<std::mutex> lk(mu_);
        Stored st;
        st.data = data;
        st.message_id = "m" + std::to_string(queue_.size());
        queue_.push_back(std::move(st));
    }
    void publish(const std::vector<std::string>& payloads) {
        for (const auto& p : payloads) {
            publish_raw(base64_encode(p));
        }
    }
    void fail_acks(int n, int status) {
        std::lock_guard<std::mutex> lk(mu_);
        fail_acks_ = n;
        fail_status_ = status;
    }
    std::size_t unacked() {
        std::lock_guard<std::mutex> lk(mu_);
        return static_cast<std::size_t>(std::count_if(
            queue_.begin(), queue_.end(), [](const Stored& st) { return !st.acked; }));
    }
    int deliveries(std::size_t i) {
        std::lock_guard<std::mutex> lk(mu_);
        return queue_.at(i).deliveries;
    }
    int pulls() {
        std::lock_guard<std::mutex> lk(mu_);
        return pulls_;
    }
    int modacks() {
        std::lock_guard<std::mutex> lk(mu_);
        return modacks_;
    }
    int nacked() {
        std::lock_guard<std::mutex> lk(mu_);
        return nacked_;
    }
    int ack_requests() {
        std::lock_guard<std::mutex> lk(mu_);
        return ack_requests_;
    }

private:
    using Clock = std::chrono::steady_clock;
    struct Stored {
        std::string data;  // the data field as published
        std::string message_id;
        std::string ack_id;  // the current delivery's
        Clock::time_point lease_until{};
        int deliveries{0};
        bool acked{false};
    };
    // The message whose current, unexpired ackId this is.
    Stored* current(const std::string& ack_id, Clock::time_point now) {
        for (auto& st : queue_) {
            if (!st.acked && st.ack_id == ack_id && st.lease_until > now) {
                return &st;
            }
        }
        return nullptr;
    }

    StubLeases cfg_;
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::deque<Stored> queue_;
    int pulls_{0};
    int modacks_{0};
    int nacked_{0};
    int ack_requests_{0};
    int fail_acks_{0};
    int fail_status_{503};
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

TEST(PubSubRoundTrip, PublishThenPullThenAckOnceTheCheckpointCompletes) {
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

    // The barrier seals them; nothing is acknowledged until the checkpoint
    // completes, and then on the source's next turn.
    InMemoryStateBackend backend;
    const OperatorId op{1};
    src.snapshot_offset(backend, op, CheckpointId{1});
    EXPECT_EQ(stub.unacked(), 3u) << "acknowledged at the barrier";
    src.notify_checkpoint_complete(CheckpointId{1});
    EXPECT_EQ(stub.unacked(), 3u) << "acknowledged off the source thread";
    Captured cap2;
    auto em2 = capturing(cap2);
    src.produce(em2);
    EXPECT_EQ(src.unacked_count(), 0u);
    EXPECT_EQ(src.acked_total(), 3u);
    EXPECT_EQ(stub.unacked(), 0u) << "server side acknowledged";
    EXPECT_TRUE(src.restore_offset(backend, op));
    EXPECT_TRUE(cap2.values.empty()) << "the messages are gone";
    src.close();
}

// --- the source's acknowledgement contract ---------------------------------

class PubSubSourceAck : public ::testing::Test {
protected:
    static constexpr const char* kProject = "test-project";
    static constexpr const char* kTopic = "t";
    static constexpr const char* kSub = "s";

    // Leases in tenths of a second: a requested second lasts 100 ms, and a
    // pull leases a message for 150 ms until the source takes its own lease.
    // The source sends whole seconds, rounding its lease up, so a test that
    // changes ack_deadline scales the stub to match.
    static StubLeases fast_leases() { return StubLeases{150, 100}; }

    PubSubSourceOptions options(const PubSubStub& stub) {
        PubSubSourceOptions o;
        o.http.base_url = stub.url();
        o.project = kProject;
        o.subscription = kSub;
        o.poll_interval = std::chrono::milliseconds{0};
        o.max_retries = 2;
        o.retry_base_backoff = std::chrono::milliseconds{1};
        o.ack_deadline = std::chrono::milliseconds{100};  // a 1 s lease: 100 ms here
        return o;
    }

    static std::vector<std::string> payloads(const std::string& prefix, int n) {
        std::vector<std::string> v;
        for (int i = 0; i < n; ++i) {
            v.push_back(prefix + std::to_string(i));
        }
        return v;
    }

    // Produce until `want` records have arrived, or the bound runs out.
    static void drain(PubSubPullSource& src, Captured& cap, std::size_t want, int turns = 50) {
        for (int i = 0; i < turns && cap.values.size() < want; ++i) {
            auto em = capturing(cap);
            src.produce(em);
        }
    }
    // Run the source's turns for a while: leases are extended, acks posted.
    static void turns_for(PubSubPullSource& src, Captured& cap, std::chrono::milliseconds d) {
        const auto until = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < until) {
            auto em = capturing(cap);
            src.produce(em);
            std::this_thread::sleep_for(std::chrono::milliseconds{5});
        }
    }
    static std::set<std::string> as_set(const std::vector<std::string>& v) {
        return {v.begin(), v.end()};
    }

    InMemoryStateBackend backend;
    const OperatorId op{1};
};

TEST_F(PubSubSourceAck, AnAbortedCheckpointsMessagesAreNotAcknowledgedAndComeBack) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    stub.publish(payloads("a", 3));
    PubSubPullSource src{options(stub)};
    src.open();
    Captured cap;
    drain(src, cap, 3);
    ASSERT_EQ(cap.values.size(), 3u);
    src.snapshot_offset(backend, op, CheckpointId{1});
    src.notify_checkpoint_aborted(CheckpointId{1});
    Captured later;
    turns_for(src, later, std::chrono::milliseconds{50});
    EXPECT_EQ(stub.unacked(), 3u) << "an aborted checkpoint acknowledged its messages";
    EXPECT_EQ(src.leased_count(), 0u) << "the source still holds the aborted messages' leases";
    // With no lease held the server delivers them again.
    PubSubPullSource again{options(stub)};
    again.open();
    Captured back;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (back.values.size() < 3 && std::chrono::steady_clock::now() < until) {
        drain(again, back, 3, 1);
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    EXPECT_EQ(as_set(back.values), as_set(payloads("a", 3)));
}

// The leases of an aborted checkpoint's messages are no longer extended, but
// the source still hands them back as it closes, so the restart receives them
// at once rather than when a long lease runs out.
TEST_F(PubSubSourceAck, AnAbortedCheckpointsMessagesAreHandedBackAtClose) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    stub.publish(payloads("r", 3));
    auto o = options(stub);
    o.ack_deadline = std::chrono::seconds{60};  // 6 s here: longer than the test waits
    PubSubPullSource src{o};
    src.open();
    Captured cap;
    drain(src, cap, 3);
    src.snapshot_offset(backend, op, CheckpointId{1});
    src.notify_checkpoint_aborted(CheckpointId{1});
    auto em = capturing(cap);
    src.produce(em);
    EXPECT_EQ(src.leased_count(), 0u);
    src.close();
    EXPECT_EQ(stub.nacked(), 3) << "close did not hand back the aborted checkpoint's messages";
    PubSubPullSource next{options(stub)};
    next.open();
    Captured back;
    drain(next, back, 3, 3);
    EXPECT_EQ(as_set(back.values), as_set(payloads("r", 3)));
}

TEST_F(PubSubSourceAck, ACrashAfterTheSnapshotLeavesTheMessagesForRedelivery) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    stub.publish(payloads("b", 4));
    {
        PubSubPullSource src{options(stub)};
        src.open();
        Captured cap;
        drain(src, cap, 4);
        ASSERT_EQ(cap.values.size(), 4u);
        src.snapshot_offset(backend, op, CheckpointId{1});
        // The process dies here: no completion, no close.
    }
    EXPECT_EQ(stub.unacked(), 4u);
    PubSubPullSource restarted{options(stub)};
    restarted.open();
    Captured back;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (back.values.size() < 4 && std::chrono::steady_clock::now() < until) {
        drain(restarted, back, 4, 1);
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    EXPECT_EQ(as_set(back.values), as_set(payloads("b", 4))) << "a message was lost";
}

TEST_F(PubSubSourceAck, AFailedAcknowledgementAfterCompletionFailsTheSubtaskAndLosesNothing) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    stub.publish(payloads("c", 3));
    {
        PubSubPullSource src{options(stub)};
        src.open();
        Captured cap;
        drain(src, cap, 3);
        src.snapshot_offset(backend, op, CheckpointId{1});
        src.notify_checkpoint_complete(CheckpointId{1});
        stub.fail_acks(100, 503);
        auto em = capturing(cap);
        EXPECT_THROW(src.produce(em), std::runtime_error);
        EXPECT_EQ(stub.ack_requests(), 3) << "the acknowledge was not retried (max_retries 2)";
    }
    stub.fail_acks(0, 503);
    EXPECT_EQ(stub.unacked(), 3u);
    PubSubPullSource restarted{options(stub)};
    restarted.open();
    Captured back;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (back.values.size() < 3 && std::chrono::steady_clock::now() < until) {
        drain(restarted, back, 3, 1);
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    EXPECT_EQ(as_set(back.values), as_set(payloads("c", 3))) << "a message was lost";
}

TEST_F(PubSubSourceAck, TransientAcknowledgementFailuresAreRetried) {
    PubSubStub stub(kProject, kTopic, kSub);
    stub.publish(payloads("t", 2));
    PubSubPullSource src{options(stub)};
    src.open();
    Captured cap;
    drain(src, cap, 2);
    src.snapshot_offset(backend, op, CheckpointId{1});
    src.notify_checkpoint_complete(CheckpointId{1});
    stub.fail_acks(2, 503);
    auto em = capturing(cap);
    EXPECT_NO_THROW(src.produce(em));
    EXPECT_EQ(stub.unacked(), 0u);
}

TEST_F(PubSubSourceAck, ACrashAfterCompletionBeforeTheAcknowledgementDeliversAgain) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    stub.publish(payloads("d", 3));
    {
        PubSubPullSource src{options(stub)};
        src.open();
        Captured cap;
        drain(src, cap, 3);
        src.snapshot_offset(backend, op, CheckpointId{1});
        src.notify_checkpoint_complete(CheckpointId{1});
        // The process dies before the source's next turn posts the acknowledge.
    }
    EXPECT_EQ(stub.unacked(), 3u);
    PubSubPullSource restarted{options(stub)};
    restarted.open();
    Captured back;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (back.values.size() < 3 && std::chrono::steady_clock::now() < until) {
        drain(restarted, back, 3, 1);
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    EXPECT_EQ(as_set(back.values), as_set(payloads("d", 3))) << "duplicates, not a loss";
}

TEST_F(PubSubSourceAck, EachCompletionAcknowledgesOnlyTheIntervalsItCovers) {
    PubSubStub stub(kProject, kTopic, kSub);
    PubSubPullSource src{options(stub)};
    src.open();
    Captured cap;
    stub.publish(payloads("x", 2));  // m0, m1
    drain(src, cap, 2);
    src.snapshot_offset(backend, op, CheckpointId{1});
    stub.publish(payloads("y", 2));  // m2, m3
    drain(src, cap, 4);
    src.snapshot_offset(backend, op, CheckpointId{2});
    stub.publish(payloads("z", 1));  // m4, in the open interval
    drain(src, cap, 5);
    ASSERT_EQ(cap.values.size(), 5u);

    src.notify_checkpoint_complete(CheckpointId{1});
    auto em = capturing(cap);
    src.produce(em);
    EXPECT_EQ(stub.unacked(), 3u) << "checkpoint 1's completion reached past its interval";

    src.notify_checkpoint_complete(CheckpointId{2});
    src.notify_checkpoint_complete(CheckpointId{1});  // late
    src.notify_checkpoint_complete(CheckpointId{2});  // repeated
    src.notify_checkpoint_aborted(CheckpointId{1});   // late and contradictory
    src.notify_checkpoint_complete(CheckpointId{9});  // above anything sealed
    src.produce(em);
    EXPECT_EQ(stub.unacked(), 1u) << "only the open interval's message stays unacknowledged";
    EXPECT_EQ(src.acked_total(), 4u);

    src.snapshot_offset(backend, op, CheckpointId{10});
    src.notify_checkpoint_complete(CheckpointId{10});
    src.produce(em);
    EXPECT_EQ(stub.unacked(), 0u);
    EXPECT_EQ(src.acked_total(), 5u) << "a message was acknowledged twice";
    src.close();
}

TEST_F(PubSubSourceAck, LeasesHoldMessagesWhileACheckpointIsDelayed) {
    // A requested second lasts 300 ms here, and the source asks for one (its
    // 300 ms lease, rounded up), extending every 150 ms: wide enough that a slow
    // turn on a loaded test host does not let a lease run out.
    PubSubStub stub(kProject, kTopic, kSub, StubLeases{150, 300});
    stub.publish(payloads("l", 3));
    auto o = options(stub);
    o.ack_deadline = std::chrono::milliseconds{300};
    PubSubPullSource src{o};
    src.open();
    Captured cap;
    drain(src, cap, 3);
    src.snapshot_offset(backend, op, CheckpointId{1});
    // Three lease periods, and six times the subscription's own deadline, pass
    // before the checkpoint completes; another subscriber pulls throughout and
    // gets nothing, because the source keeps extending its leases.
    PubSubPullSource rival{options(stub)};
    rival.open();
    Captured stolen;
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds{900};
    while (std::chrono::steady_clock::now() < until) {
        Captured none;
        turns_for(src, none, std::chrono::milliseconds{20});
        drain(rival, stolen, 1, 1);
    }
    EXPECT_TRUE(stolen.values.empty()) << "a lease ran out while its checkpoint was delayed";
    EXPECT_GE(stub.modacks(), 6) << "the source did not extend its leases";
    src.notify_checkpoint_complete(CheckpointId{1});
    Captured none;
    turns_for(src, none, std::chrono::milliseconds{20});
    EXPECT_EQ(stub.unacked(), 0u);
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(stub.deliveries(i), 1) << "message " << i << " was delivered again";
    }
}

TEST_F(PubSubSourceAck, ALeasePastMaxLeaseIsDeliveredAgainAndBothDeliveriesAreAcknowledged) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    stub.publish(payloads("e", 1));
    auto o = options(stub);
    o.max_lease = std::chrono::milliseconds{250};
    PubSubPullSource src{o};
    src.open();
    Captured cap;
    drain(src, cap, 1);
    src.snapshot_offset(backend, op, CheckpointId{1});
    // The checkpoint is still out when max_lease passes: the source stops
    // extending, the lease runs out, and its next pull receives the message
    // again under a new ackId.
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while (cap.values.size() < 2 && std::chrono::steady_clock::now() < until) {
        turns_for(src, cap, std::chrono::milliseconds{20});
    }
    ASSERT_EQ(cap.values.size(), 2u) << "the message was not delivered again";
    EXPECT_EQ(stub.deliveries(0), 2);
    src.snapshot_offset(backend, op, CheckpointId{2});
    src.notify_checkpoint_complete(CheckpointId{2});
    Captured none;
    turns_for(src, none, std::chrono::milliseconds{20});
    EXPECT_EQ(stub.unacked(), 0u) << "the second delivery's ackId was not acknowledged";
    EXPECT_EQ(src.acked_total(), 2u);
}

TEST_F(PubSubSourceAck, AtTheBoundPullsPauseUntilACheckpointCompletes) {
    PubSubStub stub(kProject, kTopic, kSub);
    stub.publish(payloads("p", 5));
    auto o = options(stub);
    o.max_outstanding = 2;
    PubSubPullSource src{o};
    src.open();
    Captured cap;
    auto em = capturing(cap);
    src.produce(em);
    EXPECT_EQ(cap.values.size(), 2u) << "a pull asked for more than the bound leaves room for";
    const int pulls = stub.pulls();
    src.produce(em);
    src.produce(em);
    EXPECT_EQ(cap.values.size(), 2u);
    EXPECT_EQ(stub.pulls(), pulls) << "the source pulled at its bound";
    src.snapshot_offset(backend, op, CheckpointId{1});
    src.notify_checkpoint_complete(CheckpointId{1});
    src.produce(em);
    EXPECT_EQ(cap.values.size(), 4u);
    EXPECT_EQ(stub.unacked(), 3u);
}

TEST_F(PubSubSourceAck, CloseAcknowledgesWhatCompletedAndHandsTheRestBack) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    auto o = options(stub);
    o.ack_deadline = std::chrono::seconds{60};  // a lease that would outlast the test
    PubSubPullSource src{o};
    src.open();
    Captured cap;
    stub.publish(payloads("f", 2));
    drain(src, cap, 2);
    src.snapshot_offset(backend, op, CheckpointId{1});
    src.notify_checkpoint_complete(CheckpointId{1});
    stub.publish(payloads("g", 2));
    drain(src, cap, 4);
    src.snapshot_offset(backend, op, CheckpointId{2});  // never completes
    stub.publish(payloads("h", 1));
    drain(src, cap, 5);  // after the last barrier
    src.cancel();
    src.close();
    EXPECT_EQ(stub.unacked(), 3u) << "close acknowledged what no completed checkpoint covers";
    EXPECT_EQ(stub.nacked(), 3);
    // Handed back, the three are delivered at once, not when the leases end.
    PubSubPullSource next{options(stub)};
    next.open();
    Captured back;
    drain(next, back, 3, 3);
    EXPECT_EQ(as_set(back.values), (std::set<std::string>{"g0", "g1", "h0"}));
}

TEST_F(PubSubSourceAck, UndecodableDataStopsTheSubtaskByDefault) {
    PubSubStub stub(kProject, kTopic, kSub, fast_leases());
    stub.publish(payloads("ok", 1));
    stub.publish_raw("@@not-base64@@");
    stub.publish(payloads("ok-after", 1));
    PubSubPullSource src{options(stub)};
    src.open();
    Captured cap;
    auto em = capturing(cap);
    try {
        src.produce(em);
        ADD_FAILURE() << "an undecodable message did not stop the source";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("on_decode_error"), std::string::npos) << e.what();
    }
    EXPECT_TRUE(cap.values.empty()) << "part of a failed pull was emitted";
    EXPECT_EQ(src.unacked_count(), 0u) << "part of a failed pull is held";
    EXPECT_EQ(stub.unacked(), 3u);
    EXPECT_EQ(stub.nacked(), 3) << "the pull was not handed back";
}

TEST_F(PubSubSourceAck, UndecodableDataIsAcknowledgedAndDroppedWhenAskedTo) {
    PubSubStub stub(kProject, kTopic, kSub);
    stub.publish(payloads("ok", 1));
    stub.publish_raw("@@not-base64@@");
    auto o = options(stub);
    o.on_decode_error = PubSubSourceOptions::OnDecodeError::Drop;
    PubSubPullSource src{o};
    src.open();
    Captured cap;
    auto em = capturing(cap);
    src.produce(em);
    EXPECT_EQ(cap.values, (std::vector<std::string>{"ok0"}));
    EXPECT_EQ(stub.unacked(), 2u) << "the dropped message was acknowledged before its checkpoint";
    src.snapshot_offset(backend, op, CheckpointId{1});
    src.notify_checkpoint_complete(CheckpointId{1});
    src.produce(em);
    EXPECT_EQ(stub.unacked(), 0u);
    src.close();
}

}  // namespace
