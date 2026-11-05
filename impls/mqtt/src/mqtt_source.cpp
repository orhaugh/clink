#include "clink/mqtt/mqtt_source.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/checkpoint_ack_ledger.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mqtt/mqtt_subscriber.hpp"
#include "clink/runtime/log_buffer.hpp"

namespace clink::mqtt {

namespace {

constexpr const char* kLabel = "mqtt";

// One acknowledgement the source owes the broker.
struct PendingAck {
    std::uint64_t connection{0};  // the connection the message arrived on
    std::uint64_t seq{0};         // arrival order, across connections
    std::uint16_t packet_id{0};
    std::uint8_t qos{0};
};

std::string envelope(const std::string& topic, const std::string& payload) {
    clink::config::JsonObject obj;
    obj["topic"] = clink::config::JsonValue{topic};
    obj["payload"] = clink::config::JsonValue{payload};
    return clink::config::JsonValue{std::move(obj)}.serialize(0);
}

}  // namespace

struct MqttSource::Impl {
    MqttSourceOptions opts;
    bool dormant{false};
    std::unique_ptr<MqttSubscriber> sub;
    // snapshot_offset seals, notify_checkpoint_* move, produce() and close() take,
    // so every broker call stays on the thread that owns the connection.
    clink::connectors::CheckpointAckLedger<PendingAck> ledger;
    std::uint64_t connection{0};  // connections opened (produce thread)
    std::uint64_t seq{0};         // produce thread
    std::atomic<std::uint64_t> acknowledged{0};
    std::atomic<std::uint64_t> reconnects{0};
    std::vector<Delivery> got;

    explicit Impl(MqttSourceOptions o)
        : opts(std::move(o)), dormant(opts.subtask_idx != 0 && opts.shared_group.empty()) {
        if (opts.topic.empty()) {
            throw std::runtime_error(opts.name + ": 'topic' is required");
        }
        if (opts.qos < 0 || opts.qos > 2) {
            throw std::runtime_error(opts.name + ": 'qos' must be 0, 1 or 2");
        }
        if (opts.protocol_version != protocol::kMqtt5 &&
            opts.protocol_version != protocol::kMqtt311) {
            throw std::runtime_error(opts.name + ": protocol_version must be 5 or 3.1.1");
        }
        if (opts.receive_maximum == 0) {
            throw std::runtime_error(opts.name + ": receive_maximum must be at least 1");
        }
        if (opts.session_expiry_s == 0) {
            throw std::runtime_error(opts.name +
                                     ": session_expiry_s must be at least 1: a session that "
                                     "ends with its connection delivers nothing again");
        }
        if (opts.block.count() <= 0) {
            opts.block = std::chrono::milliseconds{500};
        }
    }

    std::string subscribe_topic() const {
        if (opts.shared_group.empty()) {
            return opts.topic;
        }
        return "$share/" + opts.shared_group + "/" + opts.topic;
    }

    SubscriberOptions subscriber_options() const {
        SubscriberOptions s;
        s.conn = opts.conn;
        s.protocol_version = opts.protocol_version;
        s.receive_maximum = opts.receive_maximum;
        s.session_expiry_s = opts.session_expiry_s;
        s.name = opts.name;
        return s;
    }

    // Connect, then subscribe when the broker holds no session for this client
    // (or always, on the first connection). Throws when the broker cannot be
    // reached or refuses.
    void connect(bool first) {
        ++connection;
        const bool resumed = sub->connect();
        if (first || !resumed) {
            if (!first) {
                clink::log::warn("mqtt.source",
                                 opts.name + ": the broker no longer holds the session of '" +
                                     opts.conn.client_id +
                                     "'; what it held unacknowledged is gone");
            }
            sub->subscribe(subscribe_topic(), opts.qos);
        }
    }

    // A new connection, after which the broker delivers again everything the
    // session holds unacknowledged. Throws when it cannot connect, so the job
    // restarts from its last checkpoint.
    void reconnect(const std::string& why) {
        clink::metrics::connector::error_inc(kLabel, "source");
        clink::log::warn("mqtt.source",
                         opts.name + ": reconnecting (" + why +
                             "); the broker delivers again what the session holds "
                             "unacknowledged");
        sub->disconnect();
        ++reconnects;
        connect(/*first=*/false);
    }

    // Acknowledge, on the produce thread, what completed checkpoints cover.
    // Returns why a reconnect is owed, if one is: an aborted checkpoint, or a
    // connection lost while acknowledging.
    std::optional<std::string> drain_acks() {
        auto handover = ledger.take();
        if (sub == nullptr) {
            return std::nullopt;
        }
        // The first message of an aborted checkpoint that this connection
        // delivered. MQTT requires acknowledgements in arrival order, so nothing
        // after it may be acknowledged; only a new connection delivers it again.
        std::optional<std::uint64_t> first_released;
        for (const auto& r : handover.released) {
            if (r.connection == connection &&
                (!first_released.has_value() || r.seq < *first_released)) {
                first_released = r.seq;
            }
        }
        auto& ready = handover.ready;
        std::sort(ready.begin(), ready.end(), [](const PendingAck& a, const PendingAck& b) {
            return a.seq < b.seq;
        });
        for (const auto& a : ready) {
            if (a.connection != connection) {
                continue;  // its connection is gone, and the broker delivered it again
            }
            if (first_released.has_value() && a.seq > *first_released) {
                break;
            }
            if (!sub->acknowledge(a.packet_id, a.qos)) {
                return "connection lost while acknowledging: " + sub->lost_reason();
            }
            acknowledged.fetch_add(1, std::memory_order_relaxed);
        }
        if (first_released.has_value()) {
            return std::string{"a checkpoint aborted"};
        }
        return std::nullopt;
    }
};

MqttSource::MqttSource(MqttSourceOptions opts) : impl_(std::make_unique<Impl>(std::move(opts))) {}

MqttSource::~MqttSource() = default;

void MqttSource::open() {
    if (impl_->dormant) {
        return;
    }
    impl_->sub = std::make_unique<MqttSubscriber>(impl_->subscriber_options());
    impl_->connect(/*first=*/true);
}

bool MqttSource::produce(Emitter<std::string>& out) {
    auto& im = *impl_;
    if (im.dormant) {
        if (this->cancelled()) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
        return !this->cancelled();
    }
    if (this->cancelled() || im.sub == nullptr) {
        return false;
    }

    if (const auto why = im.drain_acks()) {
        im.reconnect(*why);
    }

    im.got.clear();
    const bool alive = im.sub->poll(im.opts.block, im.got);
    if (!im.got.empty()) {
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        for (auto& d : im.got) {
            if (d.qos > 0) {
                im.ledger.delivered(PendingAck{im.connection, im.seq++, d.packet_id, d.qos});
            }
            std::string rec =
                im.opts.include_topic ? envelope(d.topic, d.payload) : std::move(d.payload);
            bytes += rec.size();
            batch.emplace(std::move(rec));
        }
        const auto n = batch.size();
        clink::metrics::connector::records_in_inc(kLabel, n);
        clink::metrics::connector::bytes_in_inc(kLabel, bytes);
        out.emit_data(std::move(batch));
    }
    if (!alive) {
        im.reconnect("connection lost: " + im.sub->lost_reason());
    }
    return !this->cancelled();
}

void MqttSource::snapshot_offset(StateBackend& /*backend*/,
                                 OperatorId /*op_id*/,
                                 CheckpointId ckpt_id) {
    // Seal what arrived since the last barrier as this checkpoint's. Nothing is
    // persisted: the broker's session holds what recovery needs.
    impl_->ledger.seal(ckpt_id.value());
}

bool MqttSource::restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) {
    return false;  // nothing to restore: the session delivers again what it holds
}

void MqttSource::notify_checkpoint_complete(CheckpointId ckpt_id) {
    // Commit-dispatch thread: no broker call here. The next produce() acknowledges.
    impl_->ledger.complete(ckpt_id.value());
}

void MqttSource::notify_checkpoint_aborted(CheckpointId ckpt_id) {
    // The next produce() reconnects, which gets the broker to deliver these again.
    impl_->ledger.abort(ckpt_id.value());
}

void MqttSource::close() {
    auto& im = *impl_;
    if (im.sub == nullptr) {
        return;
    }
    // Acknowledge what completed checkpoints cover, and nothing else: the
    // session keeps every other message for the next connection, whether this is
    // a cancel, a failure or the end of a stop. Best effort: what fails here is
    // delivered again.
    try {
        if (const auto why = im.drain_acks(); why && !im.sub->connected()) {
            clink::log::warn("mqtt.source",
                             im.opts.name + ": acknowledging at close: " + *why +
                                 "; the messages a completed checkpoint covers are delivered "
                                 "again");
        }
    } catch (const std::exception& e) {
        clink::log::warn("mqtt.source", im.opts.name + ": acknowledging at close: " + e.what());
    }
    im.sub->disconnect();
    im.sub.reset();
}

std::string MqttSource::name() const {
    return impl_->opts.name;
}

std::string MqttSource::subscribe_topic() const {
    return impl_->subscribe_topic();
}

bool MqttSource::dormant() const noexcept {
    return impl_->dormant;
}

std::size_t MqttSource::unacknowledged() const {
    return impl_->ledger.outstanding();
}

std::uint64_t MqttSource::acknowledged_total() const noexcept {
    return impl_->acknowledged.load(std::memory_order_relaxed);
}

std::uint64_t MqttSource::reconnects() const noexcept {
    return impl_->reconnects.load(std::memory_order_relaxed);
}

}  // namespace clink::mqtt
