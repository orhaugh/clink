// WebSocket connector factory registration (websocket_source_string).

#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "clink/connectors/capability.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/websocket/install.hpp"
#include "clink/websocket/websocket_source.hpp"

namespace clink::websocket {

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "websocket",
        .version = "2",
        .is_source = true,
        .is_sink = false,
        .build_dependencies = {"in-tree RFC 6455 client"},
        .runtime_dependencies = {"websocket endpoint"},
        .formats = {"text"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // A live push stream with no position: messages sent while the
        // client is down or reconnecting are gone.
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = false,
        .delivery = clink::connectors::DeliveryGuarantee::AtMostOnce,
        .transactional = false,
        // The client sends no handshake headers of the caller's: credentials go
        // in the URL or in the subscription message.
        .auth_methods = {"none"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"open_timeout_ms", "reconnect_backoff_max_ms"},
        .available_in_sql = true,
        .limitations = {"no recovery relationship to already-processed messages: the feed is live",
                        "messages sent while the client is down or reconnecting are lost",
                        "no handshake headers of the caller's: credentials go in the URL or the "
                        "subscription message",
                        "binary messages are skipped, and text is passed on without a UTF-8 "
                        "check"},
    });

    using clink::plugin::BuildContext;

    // websocket_source_string: one text message = one record. At-most-once
    // across restarts (see websocket_source.hpp for the full delivery
    // statement). Params:
    //   url (required)                  - ws://host[:port]/path or wss://...
    //   subscribe                       - text frame sent after every (re)connect
    //   read_block_ms (default 500)     - produce() read window
    //   ping_interval_ms (default 30000)- client keepalive pings; 0 disables
    //   open_timeout_ms (default 10000) - connect + handshake deadline
    //   max_messages (default 0)        - stop after N messages (0 = unbounded)
    //   reconnect (default true)        - re-dial dropped connections
    //   reconnect_backoff_max_ms (default 30000)
    //   tls_verify (default true)       - wss:// certificate + hostname checks
    reg.register_source<std::string>(
        "websocket_source_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            const std::string who = "websocket_source";
            const auto integer =
                [&ctx, &who](
                    const char* key, std::int64_t fallback, std::int64_t min, std::int64_t max) {
                    if (!ctx.params.contains(key)) {
                        return fallback;
                    }
                    const std::string v = ctx.param_or(key);
                    std::int64_t out = 0;
                    const auto* end = v.data() + v.size();
                    const auto [ptr, ec] = std::from_chars(v.data(), end, out);
                    if (v.empty() || ec != std::errc{} || ptr != end || out < min || out > max) {
                        throw std::runtime_error(who + ": '" + key + "' must be an integer from " +
                                                 std::to_string(min) + " to " +
                                                 std::to_string(max) + "; it is '" + v + "'");
                    }
                    return out;
                };
            const auto boolean = [&ctx, &who](const char* key, bool fallback) {
                if (!ctx.params.contains(key)) {
                    return fallback;
                }
                const std::string v = ctx.param_or(key);
                if (v != "true" && v != "false") {
                    throw std::runtime_error(who + ": '" + key +
                                             "' must be 'true' or 'false'; it is '" + v + "'");
                }
                return v == "true";
            };
            WebSocketSourceOptions o;
            o.url = ctx.param_or("url");
            o.subscribe = ctx.param_or("subscribe", "");
            o.block = std::chrono::milliseconds{integer("read_block_ms", 500, 1, 60'000)};
            o.ping_interval =
                std::chrono::milliseconds{integer("ping_interval_ms", 30'000, 0, 3'600'000)};
            o.open_timeout =
                std::chrono::milliseconds{integer("open_timeout_ms", 10'000, 1, 600'000)};
            o.max_messages = static_cast<std::uint64_t>(
                integer("max_messages", 0, 0, std::numeric_limits<std::int64_t>::max()));
            o.reconnect = boolean("reconnect", true);
            o.reconnect_backoff_max = std::chrono::milliseconds{
                integer("reconnect_backoff_max_ms", 30'000, 1, 3'600'000)};
            o.tls_verify = boolean("tls_verify", true);
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism == 0 ? 1 : ctx.parallelism;
            o.name = "websocket_source";
            return std::make_shared<WebSocketSource>(std::move(o));
        });
}

}  // namespace clink::websocket
