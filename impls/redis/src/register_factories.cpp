// Redis Streams connector factory registration (redis_source, redis_sink).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/redis/install.hpp"
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_sink.hpp"
#include "clink/redis/redis_source.hpp"

namespace clink::redis {

namespace {

// Shared connection options (host/port/password/username/db) off a BuildContext.
ConnectOptions conn_options_from(const clink::plugin::BuildContext& ctx) {
    ConnectOptions o;
    o.host = ctx.param_or("host", "localhost");
    o.port = static_cast<std::uint16_t>(ctx.param_int64_or("port", 6379));
    o.username = ctx.param_or("username", "");
    o.password = ctx.param_or("password", "");
    o.db = static_cast<int>(ctx.param_int64_or("db", 0));
    return o;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // redis_sink: XADD each record onto a stream. At-least-once. Params:
    //   host (localhost), port (6379), username, password, db (0)
    //   stream (required)         - target stream key
    //   field (default "v")       - field name holding each record's payload
    //   maxlen (default 0)        - XADD MAXLEN cap (0 = unbounded)
    //   approx_maxlen (default 1) - MAXLEN ~ (approx) vs = (exact)
    //   batch_records (default 1000) - pipeline flush threshold
    //   max_bytes (default 0)     - byte-based flush threshold (0 = off)
    //   linger_ms (default 0)     - flush a partial batch this old (0 = off)
    reg.register_sink<std::string>(
        "redis_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            RedisSinkOptions o;
            o.conn = conn_options_from(ctx);
            o.stream = ctx.param_or("stream");
            o.field = ctx.param_or("field", "v");
            o.maxlen = static_cast<std::size_t>(ctx.param_int64_or("maxlen", 0));
            o.approx_maxlen = ctx.param_int64_or("approx_maxlen", 1) != 0;
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.max_bytes = static_cast<std::size_t>(ctx.param_int64_or("max_bytes", 0));
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "redis_sink";
            return std::make_shared<RedisSink>(std::move(o));
        });

    // redis_source: XREADGROUP from a stream via a consumer group; each subtask is
    // one group consumer. At-least-once (PEL replay + XACK-on-checkpoint). Params:
    //   host/port/username/password/db
    //   stream (required), group (required)
    //   consumer_prefix (default "clink") - consumer = "<prefix>-<subtask_idx>"
    //   field (default "v")       - single-field round-trip: emit this field verbatim
    //   count (default 500)       - XREADGROUP COUNT
    //   block_ms (default 500)    - XREADGROUP BLOCK (bounds cancel latency)
    //   start_id (default "$")    - group create position ("$" new-only | "0" history)
    reg.register_source<std::string>(
        "redis_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            RedisSourceOptions o;
            o.conn = conn_options_from(ctx);
            o.stream = ctx.param_or("stream");
            o.group = ctx.param_or("group");
            o.consumer_prefix = ctx.param_or("consumer_prefix", "clink");
            o.field = ctx.param_or("field", "v");
            o.count = static_cast<int>(ctx.param_int64_or("count", 500));
            o.block = std::chrono::milliseconds{ctx.param_int64_or("block_ms", 500)};
            o.start_id = ctx.param_or("start_id", "$");
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = "redis_source";
            return std::make_shared<RedisSource>(std::move(o));
        });
}

}  // namespace clink::redis
