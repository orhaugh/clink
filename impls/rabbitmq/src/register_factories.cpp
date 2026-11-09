// RabbitMQ / AMQP connector factory registration (rabbitmq_source_string, rabbitmq_sink_string).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

#include "clink/connectors/capability.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/rabbitmq/connection_params.hpp"
#include "clink/rabbitmq/install.hpp"
#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/rabbitmq/rabbitmq_source.hpp"

namespace clink::rabbitmq {

namespace {

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

// The connection options the source and the sink share, read strictly: a
// mistyped port or timeout once read as the default.
RabbitMqConnParams parse_conn(const clink::plugin::BuildContext& ctx, const std::string& who) {
    RabbitMqConnParams c;
    c.host = ctx.param_or("host", "localhost");
    c.port = static_cast<int>(int_option(ctx, who, "port", 5672, 1, 65535));
    c.vhost = ctx.param_or("vhost", "/");
    c.user = ctx.param_or("user", "guest");
    c.password = ctx.param_or("password", "guest");
    c.heartbeat_s = static_cast<int>(int_option(ctx, who, "heartbeat_s", 60, 0, 65535));
    c.connect_timeout =
        std::chrono::milliseconds{int_option(ctx, who, "connect_timeout_ms", 10000, 1, 300000)};
    return c;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "rabbitmq",
        .version = "2",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"librabbitmq"},
        .runtime_dependencies = {"rabbitmq >= 3.8"},
        .formats = {"text", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // Recovery is broker redelivery of unacked messages - the ack is
        // deferred to checkpoint completion, so nothing acked can be lost,
        // but there is no client-side offset to replay from, and a
        // checkpoint acknowledged late is delivered again.
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::Acknowledgement,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "password"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"connect_timeout_ms", "confirm_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"publish has no producer dedup: a replayed tail is re-published",
                        "a confirm means the broker took responsibility for a message: on disk "
                        "for a persistent message on a durable queue, accepted by a majority for "
                        "a quorum queue; a transient message or a queue that is not durable can "
                        "lose a confirmed message when the broker restarts",
                        "plain TCP only: no TLS",
                        "redelivery order after a crash is queue order, not original consume order",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });

    using clink::plugin::BuildContext;

    // rabbitmq_source_string: basic.consume from a queue, emit each body as a string.
    // At-least-once (manual ack once the checkpoint completes; the broker redelivers what was not
    // acknowledged).
    // Params: host (localhost), port (5672), vhost (/), user/password (guest), queue (required),
    //   consumer_tag, prefetch (10000; 1..65535: the most held unacknowledged, so the most read per
    //   checkpoint), poll_timeout_ms (100; 1..600000), batch_size (256; at least 1),
    //   declare_queue (true | false), each read strictly.
    reg.register_source<std::string>(
        "rabbitmq_source_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            RabbitMqSource::Options o;
            o.conn = parse_conn(ctx, "rabbitmq_source_string");
            o.queue = ctx.param_or("queue");
            o.consumer_tag = ctx.param_or("consumer_tag", "");
            const std::string who = "rabbitmq_source_string";
            o.declare_queue = bool_option(ctx, who, "declare_queue", true);
            // basic.qos prefetch_count, where 0 would mean unlimited.
            o.prefetch =
                static_cast<std::uint16_t>(int_option(ctx, who, "prefetch", 10000, 1, 65535));
            o.poll_timeout =
                std::chrono::milliseconds{int_option(ctx, who, "poll_timeout_ms", 100, 1, 600000)};
            o.max_batch_size = static_cast<std::size_t>(int_option(ctx, who, "batch_size", 256, 1));
            o.name = "rabbitmq_source";
            if (o.queue.empty()) {
                throw std::runtime_error("rabbitmq_source: 'queue' is required");
            }
            return std::make_shared<RabbitMqSource>(std::move(o));
        });

    // rabbitmq_sink_string: basic.publish each string to an exchange with a routing key.
    // At-least-once (persistent + publisher confirms; flush waits for broker acks on each barrier).
    // Params: host/port/vhost/user/password/heartbeat_s/connect_timeout_ms (as above), exchange
    //   (default ""), routing_key (required), persistent (true), mandatory (true: an unroutable
    //   message fails the flush), content_type (application/json), confirm_timeout_ms (30000),
    //   max_unconfirmed (10000), max_record_bytes (16 MiB).
    reg.register_sink<std::string>(
        "rabbitmq_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const std::string who = "rabbitmq_sink_string";
            RabbitMqSink::Options o;
            o.conn = parse_conn(ctx, who);
            o.exchange = ctx.param_or("exchange", "");
            o.routing_key = ctx.param_or("routing_key");
            o.persistent = bool_option(ctx, who, "persistent", true);
            o.mandatory = bool_option(ctx, who, "mandatory", true);
            o.content_type = ctx.param_or("content_type", "application/json");
            o.confirm_timeout = std::chrono::milliseconds{
                int_option(ctx, who, "confirm_timeout_ms", 30000, 1, 3600000)};
            o.max_unconfirmed = static_cast<std::size_t>(
                int_option(ctx, who, "max_unconfirmed", 10000, 1, 1000000));
            o.max_record_bytes = static_cast<std::size_t>(
                int_option(ctx, who, "max_record_bytes", 16LL << 20, 1, 512LL << 20));
            o.name = "rabbitmq_sink";
            if (o.routing_key.empty()) {
                throw std::runtime_error("rabbitmq_sink: 'routing_key' is required");
            }
            return std::make_shared<RabbitMqSink>(std::move(o));
        });
}

}  // namespace clink::rabbitmq
