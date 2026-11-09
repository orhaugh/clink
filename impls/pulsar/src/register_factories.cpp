// Apache Pulsar connector factory registration (pulsar_source_string, pulsar_sink_string).

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
#include "clink/pulsar/connection_params.hpp"
#include "clink/pulsar/install.hpp"
#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/pulsar/pulsar_source.hpp"

namespace clink::pulsar {

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

// The connection options the source and the sink share, read strictly.
PulsarConnParams parse_conn(const clink::plugin::BuildContext& ctx, const std::string& who) {
    PulsarConnParams c;
    c.service_url = ctx.param_or("service_url", "pulsar://localhost:6650");
    c.token = ctx.param_or("token", "");
    c.operation_timeout_s =
        static_cast<int>(int_option(ctx, who, "operation_timeout_s", 30, 1, 3600));
    c.tls_ca = ctx.param_or("tls_ca", "");
    c.tls_verify_hostname = bool_option(ctx, who, "tls_verify_hostname", true);
    return c;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "pulsar",
        .version = "2",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"pulsar-client-cpp"},
        .runtime_dependencies = {"pulsar >= 2.8"},
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
        .auth_methods = {"none", "token"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"operation_timeout_s", "send_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"publish has no producer dedup: a replayed tail is re-published",
                        "close() waits at most operation_timeout_s for the broker while the "
                        "client fails what is still unanswered",
                        "a Shared subscription does not preserve per-key order across consumers",
                        "a Shared subscription (the default) is replayed by broker redelivery, "
                        "which can hand a restarted consumer its unacknowledged messages after "
                        "newer ones; subscription_type='exclusive' replays in topic order from "
                        "the checkpointed position, at parallelism 1 on a non-partitioned topic",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });

    using clink::plugin::BuildContext;

    // pulsar_source_string: subscribe to a topic, emit each body as a string. At-least-once
    // (individual ack at checkpoint completion; see PulsarSource for the two ways a restart is
    // replayed). Params: service_url (pulsar://localhost:6650), token, topic (required),
    //   subscription (clink), subscription_type (shared | exclusive), subscribe_timeout_ms
    //   (60000), receiver_queue_size (1000), receive_timeout_ms (1000), batch_size (256).
    reg.register_source<std::string>(
        "pulsar_source_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            PulsarSource::Options o;
            o.conn = parse_conn(ctx, "pulsar_source_string");
            o.topic = ctx.param_or("topic");
            o.subscription = ctx.param_or("subscription", "clink");
            o.receiver_queue_size =
                static_cast<int>(ctx.param_int64_or("receiver_queue_size", 1000));
            o.receive_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("receive_timeout_ms", 1000)};
            o.max_batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            o.name = "pulsar_source";
            o.subscription_type = ctx.param_or("subscription_type", "shared");
            o.subscribe_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("subscribe_timeout_ms", 60000)};
            if (o.topic.empty()) {
                throw std::runtime_error("pulsar_source: 'topic' is required");
            }
            // An exclusive subscription has one consumer: a second subtask could never subscribe,
            // and a restore's seek moves the cursor for every consumer on it.
            if (o.subscription_type == "exclusive" && ctx.parallelism > 1) {
                throw std::runtime_error(
                    "pulsar_source: subscription_type 'exclusive' has a single consumer, so the "
                    "source must run at parallelism 1, not " +
                    std::to_string(ctx.parallelism));
            }
            return std::make_shared<PulsarSource>(std::move(o));
        });

    // pulsar_sink_string: async-publish to a topic, confirmed on each barrier. At-least-once
    // (producer acks awaited at flush; no producer dedup wired). Params: service_url, token,
    //   operation_timeout_s, tls_ca, tls_verify_hostname, topic (required), batching (true),
    //   send_timeout_ms (30000), max_pending (1000), max_pending_bytes (64 MiB).
    reg.register_sink<std::string>(
        "pulsar_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const std::string who = "pulsar_sink_string";
            PulsarSink::Options o;
            o.conn = parse_conn(ctx, who);
            o.topic = ctx.param_or("topic");
            o.batching = bool_option(ctx, who, "batching", true);
            o.send_timeout = std::chrono::milliseconds{
                int_option(ctx, who, "send_timeout_ms", 30000, 1, 3600000)};
            o.max_pending = static_cast<int>(int_option(ctx, who, "max_pending", 1000, 1, 500000));
            o.max_pending_bytes = static_cast<std::size_t>(
                int_option(ctx, who, "max_pending_bytes", 64LL << 20, 1, 4LL << 30));
            o.name = "pulsar_sink";
            if (o.topic.empty()) {
                throw std::runtime_error("pulsar_sink: 'topic' is required");
            }
            return std::make_shared<PulsarSink>(std::move(o));
        });
}

}  // namespace clink::pulsar
