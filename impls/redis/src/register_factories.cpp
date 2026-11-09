// Redis Streams connector factory registration (redis_source, redis_sink).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/capability.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/redis/install.hpp"
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_sink.hpp"
#include "clink/redis/redis_source.hpp"
#include "clink/redis/redis_upsert_sink.hpp"

namespace clink::redis {

namespace {

// Whether this build can open a TLS connection (libhiredis_ssl was found).
#ifdef CLINK_REDIS_TLS
constexpr bool kTls = true;
#else
constexpr bool kTls = false;
#endif

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

// Shared connection options (host/port/password/username/db/TLS) off a
// BuildContext, read strictly: tls='TRUE' once read as false and connected in
// the clear.
ConnectOptions conn_options_from(const clink::plugin::BuildContext& ctx, const std::string& who) {
    ConnectOptions o;
    o.host = ctx.param_or("host", "localhost");
    o.port = static_cast<std::uint16_t>(int_option(ctx, who, "port", 6379, 1, 65535));
    o.username = ctx.param_or("username", "");
    o.password = ctx.param_or("password", "");
    o.db = static_cast<int>(int_option(ctx, who, "db", 0, 0, std::numeric_limits<int>::max()));
    // TLS (requires a build with libhiredis_ssl). tls='true' encrypts the
    // connection; tls_ca/cert/key are optional PEM paths; tls_sni is the name the
    // server's certificate must carry (default host); tls_verify='false' skips
    // server-cert verification; tls_verify_hostname='false' keeps the chain check
    // but accepts a certificate that names another host.
    o.tls = bool_option(ctx, who, "tls", false);
    o.tls_ca = ctx.param_or("tls_ca", "");
    o.tls_cert = ctx.param_or("tls_cert", "");
    o.tls_key = ctx.param_or("tls_key", "");
    o.tls_sni = ctx.param_or("tls_sni", "");
    o.tls_verify = bool_option(ctx, who, "tls_verify", true);
    o.tls_verify_hostname = bool_option(ctx, who, "tls_verify_hostname", true);
    // A certificate or name given without tls='true' would be ignored and the
    // connection made in the clear.
    if (!o.tls) {
        for (const char* key : {"tls_ca", "tls_cert", "tls_key", "tls_sni"}) {
            if (ctx.params.count(key) != 0) {
                throw std::runtime_error(who + ": '" + key +
                                         "' is set but 'tls' is not 'true', so the connection "
                                         "would not use TLS");
            }
        }
    }
    return o;
}

// The bounds and command timeout both sinks read.
template <typename Options>
void read_sink_bounds(const clink::plugin::BuildContext& ctx, const std::string& who, Options& o) {
    o.batch_records =
        static_cast<std::size_t>(int_option(ctx, who, "batch_records", 1000, 1, 1000000));
    o.max_bytes = static_cast<std::size_t>(int_option(ctx, who, "max_bytes", 0, 0, 1LL << 30));
    o.max_record_bytes = static_cast<std::size_t>(
        int_option(ctx, who, "max_record_bytes", 16LL << 20, 1, 512LL << 20));
    o.conn.command_timeout =
        std::chrono::milliseconds{int_option(ctx, who, "command_timeout_ms", 30000, 1, 3600000)};
}

// Split a comma-separated option value, trimming spaces, dropping empties.
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < s.size()) {
        std::size_t j = s.find(',', i);
        if (j == std::string::npos) {
            j = s.size();
        }
        std::size_t b = i;
        std::size_t e = j;
        while (b < e && s[b] == ' ') {
            ++b;
        }
        while (e > b && s[e - 1] == ' ') {
            --e;
        }
        if (e > b) {
            out.push_back(s.substr(b, e - b));
        }
        i = j + 1;
    }
    return out;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "redis",
        .version = "2",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"hiredis"},
        .runtime_dependencies = {"redis >= 5 (streams)"},
        .formats = {"text", "json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // The source is a consumer-group reader with no client-side offset:
        // XACK waits for the checkpoint that covers an entry to complete, and
        // the pending-entries list delivers again whatever was not acknowledged.
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::Acknowledgement,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "password"},
        .tls = kTls,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"command_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"XADD has no dedup: a replayed tail is re-appended to the stream",
                        "MAXLEN trims the oldest entries whether or not a consumer group has "
                        "read them",
                        "a sink write is acknowledged once the server applied it in memory; "
                        "the sink waits for neither persistence nor replication",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it",
                        "source entries a completed checkpoint covers but whose XACK had not "
                        "landed are delivered again after a crash (duplicates)",
                        "a restore from a checkpoint older than one the source acknowledged "
                        "(commit-confirmed restore, a savepoint) does not see that "
                        "checkpoint's entries again",
                        "the source holds at most max_pending unacknowledged entries per "
                        "subtask; a job that completes no checkpoint stops reading there",
                        "the source cannot be rescaled to fewer subtasks"},
        .scale_down_refusal =
            "a Redis Streams source subtask is a consumer of its group, and a removed "
            "subtask's consumer keeps its pending entries, which no remaining subtask reads "
            "again; the source cannot claim them safely, since Redis cannot tell a consumer "
            "that has gone from one that is still reading",
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "redis_upsert",
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"hiredis"},
        .runtime_dependencies = {"redis >= 5"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent,
        .transactional = false,
        .idempotency_key_option = "key_columns",
        .auth_methods = {"none", "password"},
        .tls = kTls,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"command_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"changelog contract: deletes are DEL by key",
                        "not transactional: a reader can see a flush partly applied, and a key "
                        "can show an earlier value while a restored job catches up",
                        "a write is acknowledged once the server applied it in memory; the "
                        "sink waits for neither persistence nor replication",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });

    using clink::plugin::BuildContext;

    // redis_sink: XADD each record onto a stream. At-least-once. Params:
    //   host (localhost), port (6379), username, password, db (0), tls*
    //   stream (required)         - target stream key
    //   field (default "v")       - field name holding each record's payload
    //   maxlen (default 0)        - XADD MAXLEN cap (0 = unbounded)
    //   approx_maxlen (default 1) - 1 or true: MAXLEN ~ (approx); 0 or false: = (exact)
    //   batch_records (default 1000) - pipeline flush threshold
    //   max_bytes (default 0 = 16 MiB) - byte-based flush threshold
    //   max_record_bytes (default 16 MiB) - a longer record fails the sink
    //   command_timeout_ms (default 30000) - the longest a command waits
    //   linger_ms (default 0)     - flush a partial batch this old (0 = off)
    reg.register_sink<std::string>(
        "redis_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const std::string who = "redis_sink";
            RedisSinkOptions o;
            o.conn = conn_options_from(ctx, who);
            o.stream = ctx.param_or("stream");
            o.field = ctx.param_or("field", "v");
            o.maxlen = static_cast<std::size_t>(int_option(ctx, who, "maxlen", 0, 0));
            // Documented as 1 or 0; true and false are taken too, and anything
            // else is refused (a 'false' once read as approximate).
            const auto approx = ctx.params.find("approx_maxlen");
            if (approx != ctx.params.end()) {
                if (approx->second == "1" || approx->second == "true") {
                    o.approx_maxlen = true;
                } else if (approx->second == "0" || approx->second == "false") {
                    o.approx_maxlen = false;
                } else {
                    throw std::runtime_error(who +
                                             ": 'approx_maxlen' must be 1, 0, true or false "
                                             "(got '" +
                                             approx->second + "')");
                }
            }
            read_sink_bounds(ctx, who, o);
            o.max_age = std::chrono::milliseconds{int_option(ctx, who, "linger_ms", 0, 0, 3600000)};
            o.name = who;
            return std::make_shared<RedisSink>(std::move(o));
        });

    // redis_upsert_sink: changelog-aware key-value sink, mode='upsert'. Maintains
    // a keyspace by PRIMARY KEY - SET <key> <json> for insert/update_after, DEL
    // <key> for delete/update_before, where <key> = key_prefix + the PK tuple.
    // Effectively-once on the keyspace for a stable PK. Lets a retracting SQL
    // query maintain a Redis key-value view (the Streams sink can only append).
    // params: host/port/... (as redis_sink), key_columns (the PRIMARY KEY,
    // threaded from the SQL path), key_prefix (optional), batch_records,
    // max_bytes, max_record_bytes, command_timeout_ms (as redis_sink).
    reg.register_sink<std::string>(
        "redis_upsert_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            const std::string who = "redis_upsert_sink";
            RedisUpsertSinkOptions o;
            o.conn = conn_options_from(ctx, who);
            o.key_columns = split_csv(ctx.param_or("key_columns", ""));
            o.key_prefix = ctx.param_or("key_prefix", "");
            read_sink_bounds(ctx, who, o);
            o.name = who;
            return std::make_shared<RedisUpsertSink>(std::move(o));
        });

    // redis_source: XREADGROUP from a stream via a consumer group; each subtask is
    // one group consumer. At-least-once (PEL replay + XACK at checkpoint
    // completion). Params:
    //   host/port/username/password/db
    //   stream (required), group (required)
    //   consumer_prefix (default "clink") - consumer = "<prefix>-<subtask_idx>"
    //   field (default "v")       - single-field round-trip: emit this field verbatim
    //   count (default 500)       - XREADGROUP COUNT
    //   block_ms (default 500)    - XREADGROUP BLOCK (bounds cancel latency)
    //   start_id (default "$")    - group create position ("$" new-only | "0" history)
    //   max_pending (default 100000) - entries held unacknowledged before reading pauses
    reg.register_source<std::string>(
        "redis_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            RedisSourceOptions o;
            o.conn = conn_options_from(ctx, "redis_source");
            o.stream = ctx.param_or("stream");
            o.group = ctx.param_or("group");
            o.consumer_prefix = ctx.param_or("consumer_prefix", "clink");
            o.field = ctx.param_or("field", "v");
            o.count = static_cast<int>(ctx.param_int64_or("count", 500));
            o.block = std::chrono::milliseconds{ctx.param_int64_or("block_ms", 500)};
            o.start_id = ctx.param_or("start_id", "$");
            const auto max_pending = ctx.param_int64_or("max_pending", 100000);
            if (max_pending < 1) {
                throw std::runtime_error("redis_source: 'max_pending' must be at least 1");
            }
            o.max_pending = static_cast<std::size_t>(max_pending);
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = "redis_source";
            return std::make_shared<RedisSource>(std::move(o));
        });
}

}  // namespace clink::redis
