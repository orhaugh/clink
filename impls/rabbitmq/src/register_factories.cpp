// RabbitMQ / AMQP connector factory registration (rabbitmq_source_string, rabbitmq_sink_string).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/rabbitmq/connection_params.hpp"
#include "clink/rabbitmq/install.hpp"
#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/rabbitmq/rabbitmq_source.hpp"

namespace clink::rabbitmq {

namespace {

RabbitMqConnParams parse_conn(const clink::plugin::BuildContext& ctx) {
    RabbitMqConnParams c;
    c.host = ctx.param_or("host", "localhost");
    c.port = static_cast<int>(ctx.param_int64_or("port", 5672));
    c.vhost = ctx.param_or("vhost", "/");
    c.user = ctx.param_or("user", "guest");
    c.password = ctx.param_or("password", "guest");
    c.heartbeat_s = static_cast<int>(ctx.param_int64_or("heartbeat_s", 60));
    return c;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // rabbitmq_source_string: basic.consume from a queue, emit each body as a string.
    // At-least-once (manual ack at the checkpoint barrier; broker redelivers unacked on restart).
    // Params: host (localhost), port (5672), vhost (/), user/password (guest), queue (required),
    //   consumer_tag, prefetch (256), poll_timeout_ms (100), batch_size (256).
    reg.register_source<std::string>(
        "rabbitmq_source_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            RabbitMqSource::Options o;
            o.conn = parse_conn(ctx);
            o.queue = ctx.param_or("queue");
            o.consumer_tag = ctx.param_or("consumer_tag", "");
            o.declare_queue = ctx.param_or("declare_queue", "true") != "false";
            o.prefetch = static_cast<std::uint16_t>(ctx.param_int64_or("prefetch", 256));
            o.poll_timeout = std::chrono::milliseconds{ctx.param_int64_or("poll_timeout_ms", 100)};
            o.max_batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            o.name = "rabbitmq_source";
            if (o.queue.empty()) {
                throw std::runtime_error("rabbitmq_source: 'queue' is required");
            }
            return std::make_shared<RabbitMqSource>(std::move(o));
        });

    // rabbitmq_sink_string: basic.publish each string to an exchange with a routing key.
    // At-least-once (persistent + publisher confirms; flush waits for broker acks on each barrier).
    // Params: host/port/vhost/user/password (as above), exchange (default ""), routing_key
    //   (required), persistent (true), content_type (application/json), confirm_timeout_ms (30000).
    reg.register_sink<std::string>(
        "rabbitmq_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            RabbitMqSink::Options o;
            o.conn = parse_conn(ctx);
            o.exchange = ctx.param_or("exchange", "");
            o.routing_key = ctx.param_or("routing_key");
            o.persistent = ctx.param_or("persistent", "true") != "false";
            o.content_type = ctx.param_or("content_type", "application/json");
            o.confirm_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("confirm_timeout_ms", 30000)};
            o.name = "rabbitmq_sink";
            if (o.routing_key.empty()) {
                throw std::runtime_error("rabbitmq_sink: 'routing_key' is required");
            }
            return std::make_shared<RabbitMqSink>(std::move(o));
        });
}

}  // namespace clink::rabbitmq
