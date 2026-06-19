// End-to-end Beam-style pipeline integration test.
//
// Builds the entire pipeline below from a single JSON config file,
// drives it through an in-process mock Kafka cluster, and asserts the
// final state of two output topics:
//
//   incoming-events
//          ↓
//     kafka_source
//          ↓
//     parse  (map)            "id=42|score=88"  →  KafkaMessage with key+headers
//          ↓
//     enrich (map)            adds a "tier" header derived from score
//          ↓
//     classify (split, 2 branches)
//        ├── 0 (valid)        score >= 60                 → enriched-events
//        └── 1 (rejects)      score < 60 OR malformed     → rejected-events
//
// The point of the test is not the pipeline itself but the proof that
// you can describe it entirely in JSON and have the runtime build the
// equivalent C++ Dag. The transform functions are registered by name
// in the loader; the JSON references them by name.

#include <gtest/gtest.h>

#if defined(CLINK_HAS_KAFKA) && defined(CLINK_HAS_KAFKA_MOCK)

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

#include "clink/config/json.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/kafka/pipeline_loader.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;
using namespace clink::config;
using namespace clink::kafka;
using namespace std::chrono_literals;

namespace {

// Identical to the helper in test_kafka.cpp but kept local because we
// don't want a header just for this. ~30 lines.
class MockCluster {
public:
    MockCluster() {
        char err[512] = {};
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "log_level", "0", err, sizeof(err));
        host_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, err, sizeof(err));
        if (host_ == nullptr) {
            throw std::runtime_error(std::string{"MockCluster host: "} + err);
        }
        cluster_ = rd_kafka_mock_cluster_new(host_, /*broker_count*/ 1);
        if (cluster_ == nullptr) {
            rd_kafka_destroy(host_);
            throw std::runtime_error("MockCluster: failed to start");
        }
        bootstrap_ = rd_kafka_mock_cluster_bootstraps(cluster_);
    }
    ~MockCluster() {
        if (cluster_ != nullptr) {
            rd_kafka_mock_cluster_destroy(cluster_);
        }
        if (host_ != nullptr) {
            rd_kafka_destroy(host_);
        }
    }
    MockCluster(const MockCluster&) = delete;
    MockCluster& operator=(const MockCluster&) = delete;
    MockCluster(MockCluster&&) = delete;
    MockCluster& operator=(MockCluster&&) = delete;

    std::string brokers() const { return bootstrap_; }

    void create_topic(const std::string& topic, int partitions = 1) {
        rd_kafka_mock_topic_create(cluster_, topic.c_str(), partitions, /*replication*/ 1);
    }

private:
    rd_kafka_t* host_{nullptr};
    rd_kafka_mock_cluster_t* cluster_{nullptr};
    std::string bootstrap_;
};

// Bare-bones producer for seeding records into the input topic.
void produce_one(const std::string& brokers, const std::string& topic, const std::string& payload) {
    char err[512]{};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), err, sizeof(err));
    rd_kafka_conf_set(conf, "log_level", "0", err, sizeof(err));
    rd_kafka_t* p = rd_kafka_new(RD_KAFKA_PRODUCER, conf, err, sizeof(err));
    ASSERT_NE(p, nullptr) << err;
    rd_kafka_resp_err_t rc =
        rd_kafka_producev(p,
                          RD_KAFKA_V_TOPIC(topic.c_str()),
                          RD_KAFKA_V_VALUE(const_cast<char*>(payload.data()), payload.size()),
                          RD_KAFKA_V_END);
    ASSERT_EQ(rc, RD_KAFKA_RESP_ERR_NO_ERROR);
    rd_kafka_flush(p, 5000);
    rd_kafka_destroy(p);
}

// Drains a topic via a one-shot KafkaSource, returning all messages
// observed within the deadline.
std::vector<KafkaMessage> drain_topic(const std::string& brokers,
                                      const std::string& topic,
                                      const std::string& group_id,
                                      std::size_t expected,
                                      std::chrono::milliseconds deadline) {
    KafkaSource::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.group_id = group_id;
    opts.auto_offset_reset = "earliest";

    KafkaSource src(std::move(opts));
    src.open();

    BoundedChannel<StreamElement<KafkaMessage>> ch(64);
    Emitter<KafkaMessage> em(&ch);

    std::vector<KafkaMessage> out;
    const auto end = std::chrono::steady_clock::now() + deadline;
    while (out.size() < expected && std::chrono::steady_clock::now() < end) {
        src.produce(em);
        while (auto el = ch.try_pop()) {
            if (!el->is_data()) {
                continue;
            }
            for (auto& r : el->as_data()) {
                out.push_back(std::move(r.value()));
                if (out.size() == expected) {
                    src.cancel();
                    src.close();
                    return out;
                }
            }
        }
    }
    src.cancel();
    src.close();
    return out;
}

// "id=42|score=88" → returns KafkaMessage with the score in headers.
// Returns a payload of "INVALID" if the input doesn't parse.
KafkaMessage parse_event(const KafkaMessage& in) {
    KafkaMessage out;
    const auto& payload = in.payload;

    auto pipe = payload.find('|');
    if (pipe == std::string::npos) {
        out.payload = "INVALID";
        return out;
    }
    const std::string id_part = payload.substr(0, pipe);
    const std::string score_part = payload.substr(pipe + 1);
    if (!id_part.starts_with("id=") || !score_part.starts_with("score=")) {
        out.payload = "INVALID";
        return out;
    }

    out.payload = payload;
    out.key = id_part.substr(3);
    out.headers.push_back(KafkaHeader{.key = "score", .value = score_part.substr(6)});
    return out;
}

// Adds a "tier" header derived from the parsed score. Bronze < 60,
// Silver 60..79, Gold >= 80. Pass-through for messages we couldn't parse.
KafkaMessage enrich_with_tier(const KafkaMessage& in) {
    if (in.payload == "INVALID") {
        return in;
    }
    KafkaMessage out = in;
    int score = 0;
    for (const auto& h : in.headers) {
        if (h.key == "score") {
            try {
                score = std::stoi(h.value);
            } catch (...) {
                score = 0;
            }
            break;
        }
    }
    std::string tier = "Bronze";
    if (score >= 80)
        tier = "Gold";
    else if (score >= 60)
        tier = "Silver";
    out.headers.push_back(KafkaHeader{.key = "tier", .value = std::move(tier)});
    return out;
}

// 0 = valid (score >= 60 AND parsed), 1 = rejects.
int classify(const KafkaMessage& m) {
    if (m.payload == "INVALID") {
        return 1;
    }
    for (const auto& h : m.headers) {
        if (h.key == "tier" && (h.value == "Silver" || h.value == "Gold")) {
            return 0;
        }
    }
    return 1;  // Bronze, or no tier header
}

}  // namespace

TEST(PipelineConfig, JsonConfiguredKafkaPipelineWithSideOutputs) {
    MockCluster mock;
    mock.create_topic("incoming-events");
    mock.create_topic("enriched-events");
    mock.create_topic("rejected-events");

    // Seed 6 records: 3 valid (score >= 60), 1 low-score (rejected),
    // 2 unparseable (rejected).
    produce_one(mock.brokers(), "incoming-events", "id=alice|score=88");
    produce_one(mock.brokers(), "incoming-events", "id=bob|score=65");
    produce_one(mock.brokers(), "incoming-events", "id=carol|score=99");
    produce_one(mock.brokers(), "incoming-events", "id=dave|score=42");
    produce_one(mock.brokers(), "incoming-events", "no-pipe-here");
    produce_one(mock.brokers(), "incoming-events", "totally|broken");

    // Build the JSON config. Brokers come from the mock cluster.
    std::ostringstream cfg;
    cfg << R"({
  "pipeline": {
    "stages": [
      {
        "name": "in",
        "type": "kafka_source",
        "params": {
          "brokers": ")"
        << mock.brokers() << R"(",
          "topic": "incoming-events",
          "group_id": "demo-pipeline",
          "auto_offset_reset": "earliest"
        }
      },
      {
        "name": "parse",
        "type": "map",
        "input": "in",
        "params": {"fn": "parse_event"}
      },
      {
        "name": "enrich",
        "type": "map",
        "input": "parse",
        "params": {"fn": "enrich_with_tier"}
      },
      {
        "name": "classify",
        "type": "split",
        "input": "enrich",
        "params": {"fn": "classify_valid_or_reject", "branches": 2}
      },
      {
        "name": "valid_out",
        "type": "kafka_sink",
        "input": "classify.0",
        "params": {
          "brokers": ")"
        << mock.brokers() << R"(",
          "topic": "enriched-events"
        }
      },
      {
        "name": "reject_out",
        "type": "kafka_sink",
        "input": "classify.1",
        "params": {
          "brokers": ")"
        << mock.brokers() << R"(",
          "topic": "rejected-events"
        }
      }
    ]
  }
})";

    KafkaPipelineLoader loader;
    loader.register_map_fn("parse_event", parse_event);
    loader.register_map_fn("enrich_with_tier", enrich_with_tier);
    loader.register_selector_fn("classify_valid_or_reject", classify);

    Dag dag;
    loader.load(parse(cfg.str()), dag);

    // Run the pipeline in a thread; cancel after a deadline since
    // KafkaSource is unbounded.
    LocalExecutor exec(std::move(dag));
    std::thread runner([&exec] { exec.run(); });

    // Wait for the expected counts on each output topic, then cancel
    // the executor so the test can exit cleanly.
    auto valid = drain_topic(mock.brokers(), "enriched-events", "valid-collector", 3, 8s);
    auto rejected = drain_topic(mock.brokers(), "rejected-events", "reject-collector", 3, 8s);

    exec.cancel();
    if (runner.joinable()) {
        runner.join();
    }

    // Three valid records, in some order.
    ASSERT_EQ(valid.size(), 3u);
    std::vector<std::string> valid_payloads;
    valid_payloads.reserve(valid.size());
    for (const auto& m : valid) {
        valid_payloads.push_back(m.payload);
        // The enrichment chain should have added a tier header.
        bool saw_tier = false;
        for (const auto& h : m.headers) {
            if (h.key == "tier") {
                saw_tier = true;
                EXPECT_TRUE(h.value == "Silver" || h.value == "Gold");
            }
        }
        EXPECT_TRUE(saw_tier);
        EXPECT_TRUE(m.key.has_value());
    }
    std::sort(valid_payloads.begin(), valid_payloads.end());
    EXPECT_EQ(
        valid_payloads,
        (std::vector<std::string>{"id=alice|score=88", "id=bob|score=65", "id=carol|score=99"}));

    // Three rejects: 1 low-score and 2 invalid.
    ASSERT_EQ(rejected.size(), 3u);
    int invalid_count = 0;
    int low_score_count = 0;
    for (const auto& m : rejected) {
        if (m.payload == "INVALID") {
            ++invalid_count;
        } else {
            ++low_score_count;
            EXPECT_EQ(m.payload, "id=dave|score=42");
        }
    }
    EXPECT_EQ(invalid_count, 2);
    EXPECT_EQ(low_score_count, 1);
}

#else  // !CLINK_HAS_KAFKA || !CLINK_HAS_KAFKA_MOCK

TEST(PipelineConfig, KafkaSupportIsRequiredForThisTest) {
    GTEST_SKIP() << "Built without librdkafka or rdkafka_mock.h.";
}

#endif
