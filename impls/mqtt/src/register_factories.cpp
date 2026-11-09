// MQTT connector factory registration (mqtt_source, mqtt_sink).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/capability.hpp"
#include "clink/mqtt/install.hpp"
#include "clink/mqtt/mqtt_client.hpp"
#include "clink/mqtt/mqtt_protocol.hpp"
#include "clink/mqtt/mqtt_sink.hpp"
#include "clink/mqtt/mqtt_source.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::mqtt {

namespace {

// Shared connection options (broker address / auth / keepalive / TLS) off a
// BuildContext. client_id and clean_session are NOT set here - the source and
// sink pick their own (the source defaults to a persistent session, the sink to
// a clean one), each appending the subtask index so parallel subtasks hold
// distinct client ids (an MQTT broker disconnects duplicate ids).
// A boolean option: 'true' or 'false', nothing else. A typo would otherwise
// read as the default with no word said.
bool bool_option(const clink::plugin::BuildContext& ctx,
                 const std::string& factory,
                 const std::string& key,
                 bool fallback) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    if (it->second == "true") {
        return true;
    }
    if (it->second == "false") {
        return false;
    }
    throw std::runtime_error(factory + ": '" + key + "' must be 'true' or 'false' (got '" +
                             it->second + "')");
}

std::int64_t int_option(const clink::plugin::BuildContext& ctx,
                        const std::string& factory,
                        const std::string& key,
                        std::int64_t fallback,
                        std::int64_t min,
                        std::int64_t max = std::numeric_limits<std::int64_t>::max()) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    std::size_t used = 0;
    std::int64_t v = 0;
    try {
        v = std::stoll(it->second, &used);
    } catch (const std::exception&) {
        used = 0;
    }
    if (used == 0 || used != it->second.size() || v < min || v > max) {
        throw std::runtime_error(
            factory + ": '" + key + "' must be an integer " +
            (max == std::numeric_limits<std::int64_t>::max()
                 ? "of at least " + std::to_string(min)
                 : "from " + std::to_string(min) + " to " + std::to_string(max)) +
            " (got '" + it->second + "')");
    }
    return v;
}

ConnectOptions conn_options_from(const clink::plugin::BuildContext& ctx, const std::string& who) {
    ConnectOptions o;
    o.host = ctx.param_or("host", "localhost");
    o.port = static_cast<std::uint16_t>(int_option(ctx, who, "port", 1883, 1, 65535));
    o.keepalive = static_cast<int>(int_option(ctx, who, "keepalive", 60, 5, 65535));
    o.username = ctx.param_or("username", "");
    o.password = ctx.param_or("password", "");
    // TLS (libmosquitto built-in OpenSSL). tls='true' encrypts; tls_ca / tls_capath
    // give the CA to verify the server; tls_cert/tls_key are an optional client
    // cert for mutual TLS; tls_verify='false' skips server-cert + hostname checks.
    // Read strictly: tls='TRUE' once read as false and connected in the clear.
    o.tls = bool_option(ctx, who, "tls", false);
    o.tls_ca = ctx.param_or("tls_ca", "");
    o.tls_capath = ctx.param_or("tls_capath", "");
    o.tls_cert = ctx.param_or("tls_cert", "");
    o.tls_key = ctx.param_or("tls_key", "");
    o.tls_verify = bool_option(ctx, who, "tls_verify", true);
    // A certificate given without tls='true' would be ignored and the connection
    // made in the clear.
    if (!o.tls) {
        for (const char* key : {"tls_ca", "tls_capath", "tls_cert", "tls_key"}) {
            if (ctx.params.count(key) != 0) {
                throw std::runtime_error(who + ": '" + key +
                                         "' is set but 'tls' is not 'true', so the connection "
                                         "would not use TLS");
            }
        }
    }
    return o;
}

std::string client_id_for(const clink::plugin::BuildContext& ctx, const char* fallback) {
    std::string base = ctx.param_or("client_id", fallback);
    return base + "-" + std::to_string(ctx.subtask_idx);
}

// A client id no other connection holds. A broker disconnects a client when
// another connects with its id, so two sinks sharing one, as every sink left
// on a fixed default would (two jobs writing to one broker), fail each other's
// flushes. 23 letters and digits, the form every broker must accept.
std::string own_client_id() {
    static constexpr char kChars[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::uniform_int_distribution<std::size_t> pick(0, sizeof(kChars) - 2);
    std::string id = "clinksink";
    while (id.size() < 23) {
        id += kChars[pick(rd)];
    }
    return id;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    // The sink and the source carry different guarantees, so each has its own
    // record (as kafka and kafka_2pc do): the analyser reads the record whose
    // name is the longest prefix of the op type.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mqtt",
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"libmosquitto"},
        .runtime_dependencies = {"mqtt broker >= 3.1.1 (MQTT 5 by default)"},
        .formats = {"text", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        // The sink waits at every barrier for the broker to acknowledge what it
        // published (PUBACK / PUBCOMP).
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "password"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"ack_timeout_ms"},
        .available_in_sql = false,  // no connector='mqtt' binding in the SQL planner
        .limitations = {"qos=0 is fire-and-forget (at-most-once)",
                        "a flush whose acknowledgements do not all come leaves the "
                        "unacknowledged messages unknown, and the restart publishes them again",
                        "under MQTT 3.1.1 a broker cannot say it refused a message, and one that "
                        "drops a message acknowledges it all the same (Mosquitto does, for a "
                        "publish its ACL denies)",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mqtt_source",
        .version = "1",
        .is_source = true,
        .is_sink = false,
        // The source speaks MQTT through the impl's own client; the impl builds
        // only where libmosquitto, which the sink uses, is found.
        .build_dependencies = {"in-tree MQTT 5 / 3.1.1 client", "libmosquitto (the impl's sink)"},
        .runtime_dependencies = {"mqtt broker >= 3.1.1 (MQTT 5 by default)"},
        .formats = {"text", "bytes", "json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // Recovery is the persistent session's redelivery of what was not
        // acknowledged, and a message is acknowledged once a completed
        // checkpoint covers it. There is no position to replay from, and a
        // checkpoint acknowledged late is delivered again.
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::Acknowledgement,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "password"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {},
        .available_in_sql = false,  // no connector='mqtt' binding in the SQL planner
        .limitations = {"qos=0 is fire-and-forget (at-most-once)",
                        "messages a completed checkpoint covers whose acknowledgement had not "
                        "gone out are delivered again after a crash (duplicates)",
                        "an aborted checkpoint or a lost connection reconnects, and the broker "
                        "delivers again everything the session holds unacknowledged, including "
                        "messages a later checkpoint covers (duplicates)",
                        "a restore from a checkpoint older than one the source acknowledged "
                        "(commit-confirmed restore, a savepoint) does not see that "
                        "checkpoint's messages again",
                        "one checkpoint interval reads at most the session's in-flight window: "
                        "receive_maximum under MQTT 5, the broker's own setting under MQTT "
                        "3.1.1 (mosquitto's is 20 unless configured)",
                        "while acknowledgements wait, the broker queues what it cannot send, "
                        "and a broker that bounds that queue drops what overflows it",
                        "a session the broker no longer holds (expired, or a broker that does "
                        "not persist sessions restarted) takes what it held unacknowledged "
                        "with it",
                        "only subtask 0 subscribes unless shared_group is set",
                        "with shared_group set, the source cannot be rescaled to fewer "
                        "subtasks"},
        // Without shared_group only subtask 0 subscribes, and the subtasks a
        // scale-down removes hold no session.
        .scale_down_refusal =
            "each subtask of a shared subscription holds a persistent session, and the session "
            "of a removed subtask keeps the messages it holds unacknowledged, which no remaining "
            "subtask receives; the broker may keep them until the session expires, which by "
            "default is never",
        .scale_down_refusal_param = "shared_group",
    });

    using clink::plugin::BuildContext;

    // mqtt_sink: PUBLISH each record to a topic. At-least-once (qos >= 1). Params:
    //   host (localhost), port (1883), username, password, keepalive (60)
    //   tls / tls_ca / tls_capath / tls_cert / tls_key / tls_verify
    //   client_id                     - subtask index appended; unset, each sink
    //                                   takes a random id of its own
    //   clean_session (default true)  - false needs client_id
    //   topic (required)              - publish topic
    //   qos (default 1)               - 0 fire-and-forget | 1 at-least-once | 2 once
    //   retain (default false)        - set the MQTT retain flag
    //   batch_records (default 1000)  - flush threshold
    //   max_bytes (default 0 = 16 MiB) - byte-based flush threshold
    //   max_record_bytes (default 16 MiB) - a longer record fails the sink
    //   linger_ms (default 0)         - flush a partial batch this old (0 = off)
    //   ack_timeout_ms (default 30000)- bound the qos>0 ack wait per flush
    //   protocol_version (default 5)  - 5 (MQTT 5) or 3.1.1
    reg.register_sink<std::string>(
        "mqtt_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const std::string who = "mqtt_sink";
            MqttSinkOptions o;
            o.conn = conn_options_from(ctx, who);
            o.conn.clean_session = bool_option(ctx, who, "clean_session", true);
            if (const auto id = ctx.params.find("client_id"); id == ctx.params.end()) {
                if (!o.conn.clean_session) {
                    throw std::runtime_error(
                        who +
                        ": clean_session='false' keeps a session on the broker under the "
                        "client id, so it needs a 'client_id' of the job's own");
                }
                o.conn.client_id = own_client_id();
            } else if (id->second.empty()) {
                throw std::runtime_error(
                    who + ": 'client_id' is empty; leave it out for an id of the sink's own");
            } else {
                o.conn.client_id = client_id_for(ctx, "clink-mqtt-sink");
            }
            o.topic = ctx.param_or("topic");
            o.qos = static_cast<int>(int_option(ctx, who, "qos", 1, 0, 2));
            o.retain = bool_option(ctx, who, "retain", false);
            o.batch_records =
                static_cast<std::size_t>(int_option(ctx, who, "batch_records", 1000, 1, 1000000));
            o.max_bytes =
                static_cast<std::size_t>(int_option(ctx, who, "max_bytes", 0, 0, 1LL << 30));
            o.max_record_bytes = static_cast<std::size_t>(
                int_option(ctx, who, "max_record_bytes", 16LL << 20, 1, 268435455));
            o.max_age = std::chrono::milliseconds{int_option(ctx, who, "linger_ms", 0, 0, 3600000)};
            o.ack_timeout = std::chrono::milliseconds{
                int_option(ctx, who, "ack_timeout_ms", 30000, 1, 3600000)};
            const auto version = ctx.param_or("protocol_version", "5");
            if (version == "5") {
                o.protocol_version = protocol::kMqtt5;
            } else if (version == "3.1.1") {
                o.protocol_version = protocol::kMqtt311;
            } else {
                throw std::runtime_error(
                    who + ": 'protocol_version' must be '5' or '3.1.1' (got '" + version + "')");
            }
            o.name = "mqtt_sink";
            return std::make_shared<MqttSink>(std::move(o));
        });

    // mqtt_source: SUBSCRIBE to a topic filter and emit each message. At least
    // once: a message is acknowledged once a completed checkpoint covers it, and
    // the persistent session delivers again what was not. Params:
    //   host/port/username/password/keepalive + tls*
    //   client_id (default "clink-mqtt-source"; subtask index appended)
    //   topic (required)              - topic filter to subscribe to
    //   qos (default 1)               - subscription QoS
    //   shared_group (default "")     - non-empty -> "$share/<group>/<topic>" so
    //                                   parallel subtasks share the load (else only
    //                                   subtask 0 is active, others dormant)
    //   include_topic (default false) - true -> emit {"topic":..,"payload":..} JSON
    //   block_ms (default 500)        - longest wait for a message (bounds cancel latency)
    //   protocol_version (default 5)  - 5 (MQTT 5) or 3.1.1
    //   receive_maximum (10000)       - MQTT 5: 1..65535 messages the broker may send
    //                                   unacknowledged, so the most read per checkpoint
    //   session_expiry_s (never)      - MQTT 5: how long the broker keeps the session
    //                                   after its connection ends (at least 1)
    reg.register_source<std::string>(
        "mqtt_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            MqttSourceOptions o;
            o.conn = conn_options_from(ctx, "mqtt_source");
            o.conn.client_id = client_id_for(ctx, "clink-mqtt-source");
            // The session always persists (the source does not read clean_session):
            // recovery is the broker delivering again what was not acknowledged.
            o.topic = ctx.param_or("topic");
            o.qos = static_cast<int>(ctx.param_int64_or("qos", 1));
            o.shared_group = ctx.param_or("shared_group", "");
            o.include_topic = ctx.param_or("include_topic", "") == "true";
            o.block = std::chrono::milliseconds{ctx.param_int64_or("block_ms", 500)};
            const auto version = ctx.param_or("protocol_version", "5");
            if (version == "5") {
                o.protocol_version = protocol::kMqtt5;
            } else if (version == "3.1.1") {
                o.protocol_version = protocol::kMqtt311;
            } else {
                throw std::runtime_error("mqtt_source: 'protocol_version' must be '5' or '3.1.1'");
            }
            const auto receive_maximum = ctx.param_int64_or("receive_maximum", 10000);
            if (receive_maximum < 1 || receive_maximum > 65535) {
                throw std::runtime_error(
                    "mqtt_source: 'receive_maximum' must be between 1 and 65535 (the messages "
                    "the broker may send unacknowledged, so the most read per checkpoint)");
            }
            o.receive_maximum = static_cast<std::uint16_t>(receive_maximum);
            const auto expiry =
                ctx.param_int64_or("session_expiry_s", protocol::kSessionNeverExpires);
            if (expiry < 1 || expiry > protocol::kSessionNeverExpires) {
                throw std::runtime_error(
                    "mqtt_source: 'session_expiry_s' must be between 1 and 4294967295 (never): "
                    "a session that ends with its connection delivers nothing again");
            }
            o.session_expiry_s = static_cast<std::uint32_t>(expiry);
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = "mqtt_source";
            return std::make_shared<MqttSource>(std::move(o));
        });
}

}  // namespace clink::mqtt
