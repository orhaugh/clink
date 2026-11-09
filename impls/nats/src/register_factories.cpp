// NATS JetStream connector factory registration (nats_source_string, nats_sink_string).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

#include "clink/connectors/capability.hpp"
#include "clink/nats/connection_params.hpp"
#include "clink/nats/install.hpp"
#include "clink/nats/nats_sink.hpp"
#include "clink/nats/nats_source.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::nats {

namespace {

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

// The connection options the source and the sink share; the timeouts are read
// strictly, so a mistyped one is refused rather than read as the default.
NatsConnParams parse_conn(const clink::plugin::BuildContext& ctx, const std::string& who) {
    NatsConnParams c;
    c.url = ctx.param_or("url", "nats://localhost:4222");
    c.user = ctx.param_or("user", "");
    c.password = ctx.param_or("password", "");
    c.token = ctx.param_or("token", "");
    c.name = ctx.param_or("client_name", "clink");
    c.connect_timeout =
        std::chrono::milliseconds{int_option(ctx, who, "connect_timeout_ms", 2000, 1, 300000)};
    c.write_timeout =
        std::chrono::milliseconds{int_option(ctx, who, "write_timeout_ms", 5000, 1, 3600000)};
    return c;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "nats",
        .version = "2",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"nats.c"},
        .runtime_dependencies = {"nats server with jetstream"},
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
        .auth_methods = {"none", "token", "password"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"connect_timeout_ms", "write_timeout_ms", "publish_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"publish has no producer dedup: a replayed tail is re-published",
                        "a PubAck means the stream stored the message as it is configured; a "
                        "single-replica stream can lose acknowledged messages if its host fails "
                        "before they reach disk",
                        "TLS is negotiated when the server requires it and verified against the "
                        "system's trusted CAs; there is no option for a private CA, a client "
                        "certificate or skipping verification",
                        "a consumer whose durable is deleted loses its redelivery state",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });

    using clink::plugin::BuildContext;

    // nats_source_string: JetStream durable pull consumer; emit each message body as a string.
    // At-least-once (explicit ack at the checkpoint barrier; JetStream redelivers unacked).
    // Params: url (nats://localhost:4222), user/password/token, subject (required), stream,
    //   durable (clink), batch (256; 1..1000000), fetch_timeout_ms (1000; 1..600000), ack_wait_s
    //   (60; 1 s to a year), max_lease_s (3600; ack_wait_s to a year), max_ack_pending (2048, the
    //   durable's bound across every subtask), each read strictly.
    reg.register_source<std::string>(
        "nats_source_string", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            NatsSource::Options o;
            o.conn = parse_conn(ctx, "nats_source_string");
            o.subject = ctx.param_or("subject");
            o.stream = ctx.param_or("stream", "");
            o.durable = ctx.param_or("durable", "clink");
            const std::string who = "nats_source_string";
            o.batch = static_cast<int>(int_option(ctx, who, "batch", 256, 1, 1000000));
            o.fetch_timeout = std::chrono::milliseconds{
                int_option(ctx, who, "fetch_timeout_ms", 1000, 1, 600000)};
            // A year at most, which nats.c holds in nanoseconds with room to spare.
            const auto ack_wait_s = int_option(ctx, who, "ack_wait_s", 60, 1, 31536000);
            o.ack_wait = std::chrono::seconds{ack_wait_s};
            const auto max_lease_s = int_option(ctx, who, "max_lease_s", 3600, 1, 31536000);
            if (max_lease_s < ack_wait_s) {
                throw std::runtime_error(
                    "nats_source_string: 'max_lease_s' must be at least 'ack_wait_s'");
            }
            o.max_lease = std::chrono::seconds{max_lease_s};
            o.max_ack_pending = static_cast<int>(int_option(
                ctx, who, "max_ack_pending", 2048, 1, std::numeric_limits<std::int32_t>::max()));
            o.name = "nats_source";
            if (o.subject.empty()) {
                throw std::runtime_error("nats_source: 'subject' is required");
            }
            return std::make_shared<NatsSource>(std::move(o));
        });

    // nats_sink_string: JetStream async publish to a subject, confirmed on each barrier.
    // At-least-once (publisher acks awaited at flush; no producer dedup key set).
    // Params: url/user/password/token/client_name/connect_timeout_ms/write_timeout_ms, subject
    //   (required), max_pending (4096), max_pending_bytes (64 MiB), publish_timeout_ms (30000).
    reg.register_sink<std::string>(
        "nats_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const std::string who = "nats_sink_string";
            NatsSink::Options o;
            o.conn = parse_conn(ctx, who);
            o.subject = ctx.param_or("subject");
            o.max_pending = static_cast<int>(int_option(ctx, who, "max_pending", 4096, 1, 1000000));
            o.max_pending_bytes = static_cast<std::size_t>(
                int_option(ctx, who, "max_pending_bytes", 64LL << 20, 1, 4LL << 30));
            o.publish_timeout = std::chrono::milliseconds{
                int_option(ctx, who, "publish_timeout_ms", 30000, 1, 3600000)};
            o.name = "nats_sink";
            if (o.subject.empty()) {
                throw std::runtime_error("nats_sink: 'subject' is required");
            }
            return std::make_shared<NatsSink>(std::move(o));
        });
}

}  // namespace clink::nats
