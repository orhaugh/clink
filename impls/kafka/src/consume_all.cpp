#include "clink/kafka/consume_all.hpp"

#include <chrono>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

namespace clink::kafka {

std::vector<std::string> consume_all_committed(
    const std::string& brokers,
    const std::string& topic,
    std::chrono::milliseconds timeout,
    const std::vector<std::pair<std::string, std::string>>& extra_conf) {
    return consume_all(brokers, topic, "read_committed", timeout, extra_conf);
}

std::vector<std::string> consume_all(
    const std::string& brokers,
    const std::string& topic,
    const std::string& isolation,
    std::chrono::milliseconds timeout,
    const std::vector<std::pair<std::string, std::string>>& extra_conf) {
    std::string err;
    auto cfg = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto set_or_throw = [&](const std::string& k, const std::string& v) {
        if (cfg->set(k, v, err) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("consume_all_committed: config '" + k + "': " + err);
        }
    };
    set_or_throw("bootstrap.servers", brokers);
    set_or_throw("group.id", "clink-consume-all");  // required by the API, never committed to
    set_or_throw("enable.auto.commit", "false");
    set_or_throw("auto.offset.reset", "earliest");
    set_or_throw("isolation.level", isolation);
    // EOF per partition is the termination signal: EOF sits at the last
    // stable offset, so an open transaction holds EOF back and the drain
    // times out - which is the leak signal the callers assert on.
    set_or_throw("enable.partition.eof", "true");
    for (const auto& [k, v] : extra_conf) {
        set_or_throw(k, v);
    }

    std::unique_ptr<RdKafka::KafkaConsumer> consumer(
        RdKafka::KafkaConsumer::create(cfg.get(), err));
    if (consumer == nullptr) {
        throw std::runtime_error("consume_all_committed: create consumer failed: " + err);
    }

    // Assign every partition from the beginning (assign, not subscribe: no
    // group protocol, no rebalance latency, deterministic coverage).
    RdKafka::Metadata* md_raw = nullptr;
    {
        std::unique_ptr<RdKafka::Topic> t(
            RdKafka::Topic::create(consumer.get(), topic, nullptr, err));
        const auto rc =
            consumer->metadata(false, t.get(), &md_raw, static_cast<int>(timeout.count()));
        if (rc != RdKafka::ERR_NO_ERROR) {
            consumer->close();
            throw std::runtime_error("consume_all_committed: metadata failed: " +
                                     RdKafka::err2str(rc));
        }
    }
    std::unique_ptr<RdKafka::Metadata> md(md_raw);
    std::vector<RdKafka::TopicPartition*> assignment;
    for (const auto* tmd : *md->topics()) {
        if (tmd->topic() != topic) {
            continue;
        }
        for (const auto* pmd : *tmd->partitions()) {
            assignment.push_back(RdKafka::TopicPartition::create(
                topic, pmd->id(), RdKafka::Topic::OFFSET_BEGINNING));
        }
    }
    if (assignment.empty()) {
        consumer->close();
        throw std::runtime_error("consume_all_committed: topic '" + topic + "' has no partitions");
    }
    const auto rc = consumer->assign(assignment);
    if (rc != RdKafka::ERR_NO_ERROR) {
        RdKafka::TopicPartition::destroy(assignment);
        consumer->close();
        throw std::runtime_error("consume_all_committed: assign failed: " + RdKafka::err2str(rc));
    }

    std::vector<std::string> out;
    std::set<int> pending_eof;
    for (const auto* p : assignment) {
        pending_eof.insert(p->partition());
    }
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pending_eof.empty()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            RdKafka::TopicPartition::destroy(assignment);
            consumer->close();
            throw std::runtime_error(
                "consume_all_committed: timed out before every partition reached EOF - " +
                std::to_string(pending_eof.size()) +
                " partition(s) still pending. An open transaction pinning the last stable "
                "offset looks exactly like this.");
        }
        std::unique_ptr<RdKafka::Message> m(consumer->consume(200));
        switch (m->err()) {
            case RdKafka::ERR_NO_ERROR:
                out.emplace_back(static_cast<const char*>(m->payload()), m->len());
                break;
            case RdKafka::ERR__PARTITION_EOF:
                pending_eof.erase(m->partition());
                break;
            case RdKafka::ERR__TIMED_OUT:
                break;
            default:
                RdKafka::TopicPartition::destroy(assignment);
                consumer->close();
                throw std::runtime_error("consume_all_committed: consume error: " + m->errstr());
        }
    }
    RdKafka::TopicPartition::destroy(assignment);
    consumer->close();
    return out;
}

}  // namespace clink::kafka
