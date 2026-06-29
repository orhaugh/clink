// Apache Pulsar connector factory registration (pulsar_source_string, pulsar_sink_string).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/pulsar/connection_params.hpp"
#include "clink/pulsar/install.hpp"
#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/pulsar/pulsar_source.hpp"

namespace clink::pulsar {

namespace {

PulsarConnParams parse_conn(const clink::plugin::BuildContext& ctx) {
    PulsarConnParams c;
    c.service_url = ctx.param_or("service_url", "pulsar://localhost:6650");
    c.token = ctx.param_or("token", "");
    c.operation_timeout_s = static_cast<int>(ctx.param_int64_or("operation_timeout_s", 30));
    return c;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // pulsar_source_string: subscribe to a topic, emit each body as a string. At-least-once
    // (Shared subscription, individual ack at the checkpoint barrier; cursor durable server-side).
    // Params: service_url (pulsar://localhost:6650), token, topic (required), subscription (clink),
    //   receiver_queue_size (1000), receive_timeout_ms (1000), batch_size (256).
    reg.register_source<std::string>(
        "pulsar_source_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            PulsarSource::Options o;
            o.conn = parse_conn(ctx);
            o.topic = ctx.param_or("topic");
            o.subscription = ctx.param_or("subscription", "clink");
            o.receiver_queue_size =
                static_cast<int>(ctx.param_int64_or("receiver_queue_size", 1000));
            o.receive_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("receive_timeout_ms", 1000)};
            o.max_batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            o.name = "pulsar_source";
            if (o.topic.empty()) {
                throw std::runtime_error("pulsar_source: 'topic' is required");
            }
            return std::make_shared<PulsarSource>(std::move(o));
        });

    // pulsar_sink_string: async-publish to a topic, confirmed on each barrier. At-least-once
    // (producer acks awaited at flush; no producer dedup wired). Params: service_url, token,
    //   topic (required), batching (true), send_timeout_ms (30000).
    reg.register_sink<std::string>(
        "pulsar_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            PulsarSink::Options o;
            o.conn = parse_conn(ctx);
            o.topic = ctx.param_or("topic");
            o.batching = ctx.param_or("batching", "true") != "false";
            o.send_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("send_timeout_ms", 30000)};
            o.name = "pulsar_sink";
            if (o.topic.empty()) {
                throw std::runtime_error("pulsar_sink: 'topic' is required");
            }
            return std::make_shared<PulsarSink>(std::move(o));
        });
}

}  // namespace clink::pulsar
