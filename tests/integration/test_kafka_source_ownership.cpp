// Deterministic partition ownership at the broker level: parallel Kafka
// source subtasks consume exactly the partitions the ownership rule gives
// them, from their own restored offsets, regardless of consumer-group state
// or start order. QUAL-01 run C failed because ownership was delegated to
// consumer-group rebalancing: the first subtask to join briefly owned every
// partition, consumed the backlog, and the real owners then seeked their
// partitions back to stale restored offsets, double-absorbing the span into
// window state. With ownership deterministic there is no join, no
// rebalance, and no order sensitivity: a subtask started alone must NOT
// touch partitions it does not own, however much unread data they hold.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "tests/integration/docker_kafka.hpp"

namespace {

using namespace std::chrono_literals;

std::string le8(std::int64_t v) {
    std::string s(8, '\0');
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        s[static_cast<std::size_t>(i)] = static_cast<char>((u >> (i * 8)) & 0xFF);
    }
    return s;
}

void put_offset_row(clink::InMemoryStateBackend& backend,
                    clink::OperatorId op_id,
                    std::int32_t partition,
                    std::int64_t offset) {
    const std::string key = "__kafka_off__:" + std::to_string(partition);
    const std::string value = le8(offset);
    backend.put_operator_state(op_id,
                               clink::StateBackend::KeyView{key.data(), key.size()},
                               clink::StateBackend::ValueView{value.data(), value.size()});
}

// Drain whatever the source will give within a quiet period; returns
// (partition, payload) pairs in consumption order.
std::vector<std::pair<std::int32_t, std::string>> drain_quiet(clink::KafkaSource& source,
                                                              std::chrono::milliseconds quiet,
                                                              std::chrono::milliseconds budget) {
    std::vector<std::pair<std::int32_t, std::string>> got;
    clink::Emitter<clink::KafkaMessage> out(clink::Emitter<clink::KafkaMessage>::Forward(
        [&](clink::StreamElement<clink::KafkaMessage> element) {
            if (element.is_data()) {
                for (const auto& record : element.as_data()) {
                    got.emplace_back(record.value().partition, record.value().payload);
                }
            }
            return true;
        }));
    const auto deadline = std::chrono::steady_clock::now() + budget;
    auto last_growth = std::chrono::steady_clock::now();
    std::size_t previous = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        source.produce(out);
        if (got.size() != previous) {
            previous = got.size();
            last_growth = std::chrono::steady_clock::now();
        } else if (std::chrono::steady_clock::now() - last_growth >= quiet) {
            break;
        }
    }
    return got;
}

class KafkaSourceOwnershipTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (clink::test::DockerKafka::docker_available()) {
            kafka_ = std::make_unique<clink::test::DockerKafka>();
        }
    }
    static void TearDownTestSuite() { kafka_.reset(); }

    void SetUp() override {
        if (kafka_ == nullptr) {
            GTEST_SKIP() << "Docker not available; skipping Kafka source ownership gate";
        }
        topic_ = "clink_source_ownership_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        kafka_->create_topic(topic_, 2);
    }

    static std::unique_ptr<clink::test::DockerKafka> kafka_;
    std::string topic_;
};

std::unique_ptr<clink::test::DockerKafka> KafkaSourceOwnershipTest::kafka_;

TEST_F(KafkaSourceOwnershipTest, SubtasksConsumeOnlyOwnedPartitionsFromOwnRestoredOffsets) {
    // Ten records in each partition; both subtasks restore the UNION map
    // {0: 5, 1: 5}, exactly what a restore hands them.
    {
        clink::KafkaSink::Options opts;
        opts.brokers = kafka_->brokers();
        opts.topic = topic_;
        opts.metric_prefix.clear();
        clink::KafkaSink sink(std::move(opts));
        sink.open();
        clink::Batch<clink::KafkaMessage> batch;
        for (std::int32_t partition = 0; partition < 2; ++partition) {
            for (int i = 0; i < 10; ++i) {
                clink::KafkaMessage message{"p" + std::to_string(partition) + "-" +
                                            std::to_string(i)};
                message.partition = partition;
                batch.emplace(std::move(message));
            }
        }
        sink.on_data(batch);
        sink.flush();
        sink.close();
    }

    const clink::OperatorId op_id{7};
    auto make_source = [&](std::uint32_t subtask) {
        clink::KafkaSource::Options opts;
        opts.brokers = kafka_->brokers();
        opts.topic = topic_;
        opts.group_id = "ownership-shared-group";  // shared on purpose
        opts.auto_offset_reset = "earliest";
        opts.commit_mode = clink::KafkaSource::CommitMode::Manual;
        opts.max_batch_size = 4;
        opts.subtask_index = subtask;
        opts.source_parallelism = 2;
        return std::make_unique<clink::KafkaSource>(std::move(opts));
    };

    // Subtask 1 starts ALONE, with partition 0 holding unread data and a
    // restored offset for it in the handed union. It must consume partition
    // 1 from offset 5 and nothing else.
    auto second = make_source(1);
    {
        clink::InMemoryStateBackend backend;
        put_offset_row(backend, op_id, 0, 5);
        put_offset_row(backend, op_id, 1, 5);
        ASSERT_TRUE(second->restore_offset(backend, op_id));
    }
    second->open();
    const auto got_second = drain_quiet(*second, 1'500ms, 20'000ms);
    second->close();
    ASSERT_EQ(got_second.size(), 5u)
        << "subtask 1 must consume exactly its own partition's unread records";
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(got_second[static_cast<std::size_t>(i)].first, 1);
        EXPECT_EQ(got_second[static_cast<std::size_t>(i)].second, "p1-" + std::to_string(5 + i));
    }

    // Subtask 0, started later into the same group, picks up its own
    // partition from its own restored offset - untouched by subtask 1.
    auto first = make_source(0);
    {
        clink::InMemoryStateBackend backend;
        put_offset_row(backend, op_id, 0, 5);
        put_offset_row(backend, op_id, 1, 5);
        ASSERT_TRUE(first->restore_offset(backend, op_id));
    }
    first->open();
    const auto got_first = drain_quiet(*first, 1'500ms, 20'000ms);
    first->close();
    ASSERT_EQ(got_first.size(), 5u)
        << "subtask 0's partition must be intact at its restored offset";
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(got_first[static_cast<std::size_t>(i)].first, 0);
        EXPECT_EQ(got_first[static_cast<std::size_t>(i)].second, "p0-" + std::to_string(5 + i));
    }
}

}  // namespace
