#pragma once

// MQTT source: subscribes to a topic (filter) and emits each received message as
// one std::string. By default the message payload is emitted verbatim (the clean
// round-trip with mqtt_sink); with include_topic=true each message is emitted as
// a JSON object {"topic":"...","payload":"..."} so a wildcard subscription can
// carry the per-message topic (the routing key for most MQTT deployments, e.g.
// sensors/+/temperature). include_topic assumes UTF-8 text payloads; for binary
// payloads use the verbatim mode.
//
// DELIVERY: MQTT has no replayable offset/cursor (unlike Kafka or the Redis
// stream PEL). Durability is the BROKER SESSION: the source always runs with a
// stable per-subtask client_id and a PERSISTENT session (clean_session is forced
// off by the factory), so the broker retains the subscription and queues QoS 1/2
// messages for this client while it is disconnected, redelivering un-acked ones on
// reconnect - which gives at-least-once across a reconnect. libmosquitto auto-acks
// a QoS 1/2 message when
// the on_message callback returns, so a process crash AFTER the callback but
// BEFORE the next checkpoint loses those in-flight messages - i.e. delivery is
// at-least-once for clean reconnects and best-effort across a hard crash. A
// downstream idempotent/keyed-dedup consumer is the robust path. This source
// therefore does not implement a checkpoint offset (there is none to persist);
// it relies on the broker session, so snapshot_offset/restore_offset are the
// no-op base defaults.
//
// PARALLELISM: MQTT delivers every matching message to every subscriber, so a
// naive parallel subscription would duplicate. Two modes:
//   - default (shared_group empty): only subtask 0 connects and subscribes; the
//     other subtasks are dormant (idle, never emit). Correct at any parallelism,
//     but only one subtask does the work. Best run at parallelism 1.
//   - shared_group set: each subtask subscribes to "$share/<group>/<topic>" (a
//     shared subscription); the broker load-balances matching messages across the
//     subtasks. Requires a broker that supports shared subscriptions (mosquitto
//     does, for MQTT 3.1.1 clients too; it is an MQTT v5 spec feature).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mqtt/mqtt_client.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mqtt {

struct MqttSourceOptions {
    ConnectOptions conn;
    std::string topic;         // topic filter to subscribe to (required)
    int qos{1};                // subscription QoS (0/1/2)
    std::string shared_group;  // non-empty -> "$share/<group>/<topic>" shared subscription
    bool include_topic{
        false};  // true -> emit {"topic":...,"payload":...} JSON, else payload verbatim
    std::chrono::milliseconds block{500};  // mosquitto_loop wait (bounds cancel latency)
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"mqtt_source"};
};

class MqttSource : public Source<std::string> {
public:
    explicit MqttSource(MqttSourceOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0 && opts_.shared_group.empty()) {
        if (opts_.topic.empty()) {
            throw std::runtime_error(opts_.name + ": 'topic' is required");
        }
        if (opts_.qos < 0 || opts_.qos > 2) {
            throw std::runtime_error(opts_.name + ": 'qos' must be 0, 1 or 2");
        }
        if (opts_.block.count() <= 0) {
            opts_.block = std::chrono::milliseconds{500};
        }
    }

    void open() override {
        if (dormant_) {
            return;
        }
        conn_ = std::make_unique<MqttConnection>(opts_.conn);
        // The synchronous loop dispatches on_message inline on the produce()
        // thread, so this buffer needs no lock.
        conn_->set_message_cb([this](const char* topic, const void* payload, std::size_t len) {
            received_.emplace_back(topic == nullptr ? std::string{} : std::string(topic),
                                   std::string(static_cast<const char*>(payload), len));
        });
        // subscribe() awaits the SUBACK, so the subscription is registered on the
        // broker before the first produce() returns (any messages that arrive
        // during the wait are buffered into received_ and drained below, not lost).
        conn_->subscribe(subscribe_topic_(), opts_.qos);
    }

    bool produce(Emitter<std::string>& out) override {
        if (dormant_) {
            if (this->cancelled()) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{200});
            return !this->cancelled();
        }
        if (this->cancelled() || conn_ == nullptr) {
            return false;
        }

        int rc = conn_->loop(static_cast<int>(opts_.block.count()), /*max_packets=*/1);
        // A user callback that threw during this loop() (e.g. bad_alloc growing the
        // receive buffer) was caught at the C boundary; surface it as a produce()
        // error so the job replays from the last checkpoint.
        if (std::string cb_err; conn_->consume_callback_error(cb_err)) {
            clink::metrics::connector::error_inc("mqtt", "source");
            throw std::runtime_error(opts_.name + ": message callback failed: " + cb_err);
        }
        if (rc != MOSQ_ERR_SUCCESS) {
            // Connection lost / no connection: try a single reconnect. The source
            // requires a persistent broker session (clean_session=false, forced by
            // the factory), so the broker retains the subscription across the
            // reconnect and redelivers un-acked messages. If reconnect also fails,
            // surface it so the job restarts from the last checkpoint.
            clink::metrics::connector::error_inc("mqtt", "source");
            if (conn_->reconnect() != MOSQ_ERR_SUCCESS) {
                throw std::runtime_error(opts_.name + ": connection lost and reconnect failed: " +
                                         mosquitto_strerror(rc));
            }
            return !this->cancelled();
        }

        if (!received_.empty()) {
            Batch<std::string> batch;
            std::uint64_t bytes = 0;
            for (auto& [topic, payload] : received_) {
                std::string rec =
                    opts_.include_topic ? envelope_(topic, payload) : std::move(payload);
                bytes += rec.size();
                batch.emplace(std::move(rec));
            }
            received_.clear();  // drained; clear AFTER emit so nothing is dropped
            const auto n = batch.size();
            clink::metrics::connector::records_in_inc("mqtt", n);
            clink::metrics::connector::bytes_in_inc("mqtt", bytes);
            out.emit_data(std::move(batch));
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    void close() override { conn_.reset(); }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    // Test/observability accessor: the effective topic this subtask subscribes to.
    [[nodiscard]] std::string subscribe_topic() const { return subscribe_topic_(); }
    [[nodiscard]] bool dormant() const noexcept { return dormant_; }

private:
    std::string subscribe_topic_() const {
        if (opts_.shared_group.empty()) {
            return opts_.topic;
        }
        return "$share/" + opts_.shared_group + "/" + opts_.topic;
    }

    static std::string envelope_(const std::string& topic, const std::string& payload) {
        clink::config::JsonObject obj;
        obj["topic"] = clink::config::JsonValue{topic};
        obj["payload"] = clink::config::JsonValue{payload};
        return clink::config::JsonValue{std::move(obj)}.serialize(0);
    }

    MqttSourceOptions opts_;
    bool dormant_{false};
    std::unique_ptr<MqttConnection> conn_;
    std::vector<std::pair<std::string, std::string>>
        received_;  // (topic, payload) filled by the cb
};

}  // namespace clink::mqtt
