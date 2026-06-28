// MQTT connector factory registration (mqtt_source, mqtt_sink).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "clink/mqtt/install.hpp"
#include "clink/mqtt/mqtt_client.hpp"
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
ConnectOptions conn_options_from(const clink::plugin::BuildContext& ctx) {
    ConnectOptions o;
    o.host = ctx.param_or("host", "localhost");
    o.port = static_cast<std::uint16_t>(ctx.param_int64_or("port", 1883));
    o.keepalive = static_cast<int>(ctx.param_int64_or("keepalive", 60));
    o.username = ctx.param_or("username", "");
    o.password = ctx.param_or("password", "");
    // TLS (libmosquitto built-in OpenSSL). tls='true' encrypts; tls_ca / tls_capath
    // give the CA to verify the server; tls_cert/tls_key are an optional client
    // cert for mutual TLS; tls_verify='false' skips server-cert + hostname checks.
    o.tls = ctx.param_or("tls", "") == "true";
    o.tls_ca = ctx.param_or("tls_ca", "");
    o.tls_capath = ctx.param_or("tls_capath", "");
    o.tls_cert = ctx.param_or("tls_cert", "");
    o.tls_key = ctx.param_or("tls_key", "");
    o.tls_verify = ctx.param_or("tls_verify", "true") != "false";
    return o;
}

std::string client_id_for(const clink::plugin::BuildContext& ctx, const char* fallback) {
    std::string base = ctx.param_or("client_id", fallback);
    return base + "-" + std::to_string(ctx.subtask_idx);
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // mqtt_sink: PUBLISH each record to a topic. At-least-once (qos >= 1). Params:
    //   host (localhost), port (1883), username, password, keepalive (60)
    //   tls / tls_ca / tls_capath / tls_cert / tls_key / tls_verify
    //   client_id (default "clink-mqtt-sink"; subtask index appended)
    //   topic (required)              - publish topic
    //   qos (default 1)               - 0 fire-and-forget | 1 at-least-once | 2 once
    //   retain (default false)        - set the MQTT retain flag
    //   batch_records (default 1000)  - flush threshold
    //   max_bytes (default 0)         - byte-based flush threshold (0 = off)
    //   linger_ms (default 0)         - flush a partial batch this old (0 = off)
    //   ack_timeout_ms (default 30000)- bound the qos>0 ack wait per flush
    reg.register_sink<std::string>(
        "mqtt_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            MqttSinkOptions o;
            o.conn = conn_options_from(ctx);
            o.conn.client_id = client_id_for(ctx, "clink-mqtt-sink");
            o.conn.clean_session = ctx.param_or("clean_session", "true") != "false";
            o.topic = ctx.param_or("topic");
            o.qos = static_cast<int>(ctx.param_int64_or("qos", 1));
            o.retain = ctx.param_or("retain", "") == "true";
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.max_bytes = static_cast<std::size_t>(ctx.param_int64_or("max_bytes", 0));
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.ack_timeout = std::chrono::milliseconds{ctx.param_int64_or("ack_timeout_ms", 30000)};
            o.name = "mqtt_sink";
            return std::make_shared<MqttSink>(std::move(o));
        });

    // mqtt_source: SUBSCRIBE to a topic filter and emit each message. Delivery is
    // broker-session at-least-once (persistent session); no replayable offset.
    // Params:
    //   host/port/username/password/keepalive + tls*
    //   client_id (default "clink-mqtt-source"; subtask index appended)
    //   topic (required)              - topic filter to subscribe to
    //   qos (default 1)               - subscription QoS
    //   shared_group (default "")     - non-empty -> "$share/<group>/<topic>" so
    //                                   parallel subtasks share the load (else only
    //                                   subtask 0 is active, others dormant)
    //   include_topic (default false) - true -> emit {"topic":..,"payload":..} JSON
    //   block_ms (default 500)        - mosquitto_loop wait (bounds cancel latency)
    reg.register_source<std::string>(
        "mqtt_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            MqttSourceOptions o;
            o.conn = conn_options_from(ctx);
            o.conn.client_id = client_id_for(ctx, "clink-mqtt-source");
            // The source's recovery model REQUIRES a persistent broker session: on a
            // reconnect the broker must still hold the subscription and redeliver
            // un-acked messages. A clean session would silently deliver nothing
            // after a reconnect, so it is forced off (not a user-tunable param).
            o.conn.clean_session = false;
            o.topic = ctx.param_or("topic");
            o.qos = static_cast<int>(ctx.param_int64_or("qos", 1));
            o.shared_group = ctx.param_or("shared_group", "");
            o.include_topic = ctx.param_or("include_topic", "") == "true";
            o.block = std::chrono::milliseconds{ctx.param_int64_or("block_ms", 500)};
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = "mqtt_source";
            return std::make_shared<MqttSource>(std::move(o));
        });
}

}  // namespace clink::mqtt
