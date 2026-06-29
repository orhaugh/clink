// NATS JetStream connector factory registration (nats_source_string, nats_sink_string).

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/nats/connection_params.hpp"
#include "clink/nats/install.hpp"
#include "clink/nats/nats_sink.hpp"
#include "clink/nats/nats_source.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::nats {

namespace {

NatsConnParams parse_conn(const clink::plugin::BuildContext& ctx) {
    NatsConnParams c;
    c.url = ctx.param_or("url", "nats://localhost:4222");
    c.user = ctx.param_or("user", "");
    c.password = ctx.param_or("password", "");
    c.token = ctx.param_or("token", "");
    c.name = ctx.param_or("client_name", "clink");
    return c;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // nats_source_string: JetStream durable pull consumer; emit each message body as a string.
    // At-least-once (explicit ack at the checkpoint barrier; JetStream redelivers unacked).
    // Params: url (nats://localhost:4222), user/password/token, subject (required), stream,
    //   durable (clink), batch (256), fetch_timeout_ms (1000), ack_wait_s (60), max_ack_pending.
    reg.register_source<std::string>(
        "nats_source_string", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            NatsSource::Options o;
            o.conn = parse_conn(ctx);
            o.subject = ctx.param_or("subject");
            o.stream = ctx.param_or("stream", "");
            o.durable = ctx.param_or("durable", "clink");
            o.batch = static_cast<int>(ctx.param_int64_or("batch", 256));
            o.fetch_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("fetch_timeout_ms", 1000)};
            o.ack_wait = std::chrono::seconds{ctx.param_int64_or("ack_wait_s", 60)};
            o.max_ack_pending = static_cast<int>(ctx.param_int64_or("max_ack_pending", 2048));
            o.name = "nats_source";
            if (o.subject.empty()) {
                throw std::runtime_error("nats_source: 'subject' is required");
            }
            return std::make_shared<NatsSource>(std::move(o));
        });

    // nats_sink_string: JetStream async publish to a subject, confirmed on each barrier.
    // At-least-once (publisher acks awaited at flush; no producer dedup key set).
    // Params: url/user/password/token, subject (required), max_pending (4096),
    //   publish_timeout_ms (30000).
    reg.register_sink<std::string>(
        "nats_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            NatsSink::Options o;
            o.conn = parse_conn(ctx);
            o.subject = ctx.param_or("subject");
            o.max_pending = static_cast<int>(ctx.param_int64_or("max_pending", 4096));
            o.publish_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("publish_timeout_ms", 30000)};
            o.name = "nats_sink";
            if (o.subject.empty()) {
                throw std::runtime_error("nats_sink: 'subject' is required");
            }
            return std::make_shared<NatsSink>(std::move(o));
        });
}

}  // namespace clink::nats
