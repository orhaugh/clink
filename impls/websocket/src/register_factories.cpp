// WebSocket connector factory registration (websocket_source_string).

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "clink/connectors/capability.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/websocket/install.hpp"
#include "clink/websocket/websocket_source.hpp"

namespace clink::websocket {

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "websocket",
        .version = "1",
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
        .auth_methods = {"none", "headers"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"reconnect_backoff_max_ms"},
        .available_in_sql = true,
        .limitations = {"no recovery relationship to already-processed messages: the feed is live"},
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
            WebSocketSourceOptions o;
            o.url = ctx.param_or("url");
            o.subscribe = ctx.param_or("subscribe", "");
            o.block = std::chrono::milliseconds{ctx.param_int64_or("read_block_ms", 500)};
            o.ping_interval =
                std::chrono::milliseconds{ctx.param_int64_or("ping_interval_ms", 30'000)};
            o.open_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("open_timeout_ms", 10'000)};
            o.max_messages = static_cast<std::uint64_t>(ctx.param_int64_or("max_messages", 0));
            o.reconnect = ctx.param_or("reconnect", "true") != "false";
            o.reconnect_backoff_max =
                std::chrono::milliseconds{ctx.param_int64_or("reconnect_backoff_max_ms", 30'000)};
            o.tls_verify = ctx.param_or("tls_verify", "true") != "false";
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism == 0 ? 1 : ctx.parallelism;
            o.name = "websocket_source";
            return std::make_shared<WebSocketSource>(std::move(o));
        });
}

}  // namespace clink::websocket
