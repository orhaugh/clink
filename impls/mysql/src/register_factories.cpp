// MySQL connector factory registration (mysql_source, mysql_sink).

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "clink/connectors/capability.hpp"
#include "clink/mysql/install.hpp"
#include "clink/mysql/mysql_cdc_source.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_json_upsert_sink.hpp"
#include "clink/mysql/mysql_sink.hpp"
#include "clink/mysql/mysql_source.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::mysql {

namespace {

// Split a comma-separated option value, trimming surrounding spaces, dropping
// empties. "a, b ,c" -> {"a","b","c"}; "" -> {}.
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

ConnectOptions conn_options_from(const clink::plugin::BuildContext& ctx) {
    ConnectOptions o;
    o.host = ctx.param_or("host", "localhost");
    o.port = static_cast<std::uint16_t>(ctx.param_int64_or("port", 3306));
    o.user = ctx.param_or("user", "");
    o.password = ctx.param_or("password", "");
    o.database = ctx.param_or("database", "");
    // TLS: ssl='true' encrypts the connection; ssl_ca/cert/key are optional PEM
    // paths; ssl_verify='false' skips server-cert verification (dev/self-signed).
    o.ssl = ctx.param_or("ssl", "") == "true";
    o.ssl_ca = ctx.param_or("ssl_ca", "");
    o.ssl_cert = ctx.param_or("ssl_cert", "");
    o.ssl_key = ctx.param_or("ssl_key", "");
    o.ssl_verify = ctx.param_or("ssl_verify", "true") != "false";
    return o;
}

// A whole-number option: absent, the fallback; present, it must parse in full
// and lie in [lo, hi], or the factory refuses it rather than using the
// fallback in its place.
std::int64_t int_option(const clink::plugin::BuildContext& ctx,
                        const char* who,
                        const std::string& key,
                        std::int64_t fallback,
                        std::int64_t lo,
                        std::int64_t hi) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    const std::string& text = it->second;
    std::int64_t v = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), v);
    if (text.empty() || ec != std::errc{} || end != text.data() + text.size() || v < lo || v > hi) {
        throw std::runtime_error(std::string{who} + ": '" + key + "' must be a whole number from " +
                                 std::to_string(lo) + " to " + std::to_string(hi) + "; it is '" +
                                 text + "'");
    }
    return v;
}

// A 'true' / 'false' option: absent, the fallback; anything else is refused.
bool bool_option(const clink::plugin::BuildContext& ctx,
                 const char* who,
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
    throw std::runtime_error(std::string{who} + ": '" + key +
                             "' must be 'true' or 'false'; it is '" + it->second + "'");
}

// The buffer, statement and retry bounds both sinks read.
template <typename Options>
void read_sink_bounds(const clink::plugin::BuildContext& ctx, const char* who, Options& o) {
    o.batch_records =
        static_cast<std::size_t>(int_option(ctx, who, "batch_records", 1000, 1, 1000000));
    o.max_bytes = static_cast<std::size_t>(int_option(ctx, who, "max_bytes", 0, 0, 1LL << 30));
    const auto statement = int_option(ctx, who, "max_statement_bytes", 4LL << 20, 1024, 1LL << 30);
    o.max_statement_bytes = static_cast<std::size_t>(statement);
    o.max_record_bytes = static_cast<std::size_t>(int_option(
        ctx, who, "max_record_bytes", std::min<std::int64_t>(1LL << 20, statement), 64, statement));
    o.lock_wait_timeout =
        std::chrono::seconds{int_option(ctx, who, "lock_wait_timeout_s", 30, 1, 3600)};
    o.max_retries = static_cast<int>(int_option(ctx, who, "max_retries", 3, 0, 10));
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mysql",
        .version = "2",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"libmariadb (mariadb-connector-c)"},
        .runtime_dependencies = {"MySQL 8.0 or 8.4 (tested); on_duplicate='update' needs 8.0.19 "
                                 "or later"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Either,
        // The polling source checkpoints its cursor and resumes from it. The
        // binlog change stream has its own record, mysql_cdc.
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::Lsn,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"password"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"lock_wait_timeout_s"},
        .available_in_sql = true,
        .limitations =
            {"the sink is at-least-once: a restored job writes again what its last checkpoint "
             "did not cover, which append duplicates and on_duplicate='update' rewrites by key; "
             "no two-phase commit",
             "on_duplicate='update' needs exactly one PRIMARY KEY or UNIQUE index on the table, "
             "on whole values, all of whose columns the sink writes",
             "a failed write fails the sink until it is opened again; a COMMIT whose reply is "
             "lost leaves its flush's outcome unknown",
             "a flush is one transaction only on a transactional engine; on another, a failed "
             "flush can leave its first statements applied",
             "tested against MySQL servers only, not MariaDB",
             "query source re-reads from the start on restart",
             "the sink must be the only sink on its chain: Dag::add_sink refuses another sink "
             "beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mysql_cdc",
        .version = "1",
        .is_source = true,
        .is_sink = false,
        .build_dependencies = {"libmariadb (mariadb-connector-c, for its replication API)"},
        .runtime_dependencies =
            {"MySQL 8.0 or 8.4 with binlog_format=ROW and binlog_row_image=FULL",
             "REPLICATION SLAVE and REPLICATION CLIENT; RELOAD for the locked "
             "initial snapshot"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // The checkpoint holds the binlog coordinate of the last transaction
        // boundary; a restore streams again from there.
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::Lsn,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"password"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"heartbeat_ms", "snapshot_lock_wait_s"},
        .available_in_sql = true,
        .limitations =
            {"a restore streams again the transactions after the checkpoint's binlog coordinate, "
             "so a sink sees them twice",
             "a checkpoint waits for an open transaction to end, and for the initial snapshot to "
             "finish; a long transaction or snapshot delays every checkpoint by as long",
             "a failure during the initial snapshot restarts it from the beginning",
             "binlog coordinates belong to one server: after a failover to another server the "
             "source refuses its checkpoint; there is no GTID positioning",
             "with binlog_row_metadata=MINIMAL a change to a captured table's columns stops the "
             "source; with FULL each event is decoded with the definition it was written with",
             "TRUNCATE, DROP TABLE or RENAME TABLE of a captured table, statement-logged changes "
             "to one, XA transactions, binlog_transaction_compression and partial JSON updates "
             "stop the source",
             "on_decode_error='drop' skips an event the source cannot decode, losing its "
             "changes; the default 'fail' stops the source",
             "snapshot_lock='false' makes the snapshot-to-stream boundary at-least-once: a "
             "change made while the snapshot starts can appear twice",
             "MariaDB servers are not supported by the change stream",
             "only subtask 0 reads the binlog; the other subtasks stay idle"},
        .replay_lost_with_options = {"on_decode_error=drop"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mysql_upsert",
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"libmariadb (mariadb-connector-c)"},
        .runtime_dependencies = {"MySQL 8.0.19 or later (tested 8.0 and 8.4), a table on a "
                                 "transactional engine (InnoDB)"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent,
        .transactional = false,
        .idempotency_key_option = "key_columns",
        .auth_methods = {"password"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"lock_wait_timeout_s"},
        .available_in_sql = true,
        .limitations =
            {"requires a PRIMARY KEY or UNIQUE index on exactly key_columns, on whole values, "
             "and no other unique index",
             "update_columns, when set, must name every non-key column",
             "with more than one subtask every key column must tell values apart as the stream "
             "does (an integer, DECIMAL, VARBINARY, VARCHAR with a binary NO PAD collation, or "
             "a DATETIME or TIMESTAMP of millisecond precision or finer written from epoch "
             "milliseconds) and each record must write its key as its column compares; at "
             "parallelism 1 two keys the table treats as one row share it, the later change "
             "winning",
             "converges only with one writer per key: a replay rewrites rows, and can "
             "overwrite a change another writer made to them since",
             "not two-phase commit; a COMMIT whose reply is lost leaves its flush's outcome "
             "unknown, and the restored job applies it again",
             "the sink must be the only sink on its chain: Dag::add_sink refuses another sink "
             "beside it"},
    });

    using clink::plugin::BuildContext;

    // mysql_sink: rows of JSON-object records into a table, at-least-once. Every
    // option is read strictly: a value the sink cannot use is refused rather
    // than replaced by its default. Params:
    //   host/port/user/password/database
    //   table (required)          - target table
    //   columns                   - comma-separated projection / column order;
    //       on the SQL path it defaults to the table's declared schema, so it is
    //       only required when constructing the sink outside SQL.
    //   on_duplicate ("" [default, plain INSERT] | "update")  - "update" appends
    //       ON DUPLICATE KEY UPDATE, by the table's one PRIMARY KEY or UNIQUE
    //       index. (This is NOT clink mode='upsert', the changelog contract the
    //       planner routes to mysql_upsert_sink.)
    //   update_columns            - its SET list (empty = all columns)
    //   batch_records (default 1000; 1 to 1000000) - flush at this many rows
    //   max_bytes (default 16 MiB; 0 = the default) - flush at this much SQL
    //   linger_ms (default 0 = off) - flush rows buffered this long
    //   max_record_bytes (default 1 MiB), max_statement_bytes (default 4 MiB)
    //   lock_wait_timeout_s (default 30; 1 to 3600), max_retries (default 3; 0 to 10)
    reg.register_sink<std::string>(
        "mysql_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            constexpr const char* who = "mysql_sink";
            MysqlSinkOptions o;
            o.conn = conn_options_from(ctx);
            o.table = ctx.param_or("table");
            o.columns = split_csv(ctx.param_or("columns", ""));
            if (o.columns.empty()) {
                // SQL Row path: fall back to the declared table schema so the user
                // need not repeat columns= matching the DDL (avoids silent drift).
                o.columns = columns_from_schema(ctx.param_or("schema_columns", ""));
            }
            o.timestamp_columns =
                sqljson::parse_timestamp_columns(ctx.param_or("timestamp_columns", ""));
            const auto on_duplicate = ctx.param_or("on_duplicate", "");
            if (!on_duplicate.empty() && on_duplicate != "update") {
                throw std::runtime_error(std::string{who} +
                                         ": 'on_duplicate' must be empty or 'update'; it is '" +
                                         on_duplicate + "'");
            }
            o.upsert = on_duplicate == "update";
            o.update_columns = split_csv(ctx.param_or("update_columns", ""));
            read_sink_bounds(ctx, who, o);
            o.max_age = std::chrono::milliseconds{int_option(ctx, who, "linger_ms", 0, 0, 3600000)};
            o.name = who;
            return std::make_shared<MysqlSink>(std::move(o));
        });

    // mysql_upsert_sink: changelog-aware, mode='upsert'. Maintains a table by its
    // key - insert/update_after upsert (INSERT ON DUPLICATE KEY UPDATE),
    // delete/update_before DELETE by key, applied in the order each key last
    // changed. Effectively-once on the table for a stable key and one writer
    // per key. Params: table (required), columns (or schema_columns),
    // key_columns (the PRIMARY KEY, threaded from the SQL path), update_columns
    // (must name every non-key column), and the bounds mysql_sink reads except
    // linger_ms (batch_records counts buffered keys).
    reg.register_sink<std::string>(
        "mysql_upsert_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            constexpr const char* who = "mysql_upsert_sink";
            MysqlJsonUpsertSinkOptions o;
            o.conn = conn_options_from(ctx);
            o.table = ctx.param_or("table");
            o.columns = split_csv(ctx.param_or("columns", ""));
            if (o.columns.empty()) {
                o.columns = columns_from_schema(ctx.param_or("schema_columns", ""));
            }
            o.timestamp_columns =
                sqljson::parse_timestamp_columns(ctx.param_or("timestamp_columns", ""));
            o.key_columns = split_csv(ctx.param_or("key_columns", ""));
            o.update_columns = split_csv(ctx.param_or("update_columns", ""));
            read_sink_bounds(ctx, who, o);
            o.parallelism = ctx.parallelism;
            o.name = who;
            return std::make_shared<MysqlJsonUpsertSink>(std::move(o));
        });

    // mysql_source: incremental cursor SELECT, each row a JSON-object string.
    // At-least-once (cursor checkpoint). Params:
    //   host/port/user/password/database
    //   table (required), cursor_column (required; ascending, NOT NULL, unique
    //       unless id_column is set)
    //   id_column                 - optional UNIQUE tie-breaker (keyset pagination)
    //       so a non-unique cursor_column does not drop rows at a page boundary
    //   initial_cursor (cold-start), batch_size (default 1000)
    //   poll_ms (default 1000), jitter_frac (default 0)
    reg.register_source<std::string>(
        "mysql_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            MysqlPollOptions o;
            o.conn = conn_options_from(ctx);
            o.table = ctx.param_or("table");
            o.cursor_column = ctx.param_or("cursor_column");
            o.id_column = ctx.param_or("id_column", "");
            o.initial_cursor = ctx.param_or("initial_cursor", "");
            o.batch_size = static_cast<int>(ctx.param_int64_or("batch_size", 1000));
            o.interval = std::chrono::milliseconds{ctx.param_int64_or("poll_ms", 1000)};
            o.jitter_frac = std::stod(ctx.param_or("jitter_frac", "0"));
            // bounded='true' (or mode='snapshot') = one-shot: read the table once
            // then finish, instead of tailing it forever.
            o.bounded =
                ctx.param_or("bounded", "") == "true" || ctx.param_or("mode", "") == "snapshot";
            o.name = "mysql_source";
            return clink::read_on_first_subtask<std::string>(ctx.subtask_idx,
                                                             make_mysql_poll_source(o));
        });

    // mysql_cdc_source: binlog CDC. Streams row-level changes from the master's
    // binary log (mariadb_rpl) as flat JSON objects with __op/__table/__lsn/__xid.
    // At-least-once: the checkpoint holds the binlog coordinate of the last
    // transaction boundary, and the source holds barriers inside a transaction
    // and during the snapshot. The master must run binlog_format=ROW +
    // binlog_row_image=FULL and the user must hold REPLICATION SLAVE +
    // REPLICATION CLIENT. Every option below is read strictly: a value the
    // source cannot use is refused rather than replaced by its default. Params:
    //   host/port/user/password/database
    //   server_id (REQUIRED, 1 to 4294967295, unique among the master's replicas)
    //   tables                  - comma-separated "db.table" allowlist (empty = all)
    //   start_file / start_pos  - cold-start coordinate ("" => current master head)
    //   initial_snapshot ('true'|'false', default false) - read the existing rows
    //       of `tables` as of a binlog point, then stream from it. `tables` must
    //       then be non-empty, fully-qualified 'db.table' InnoDB tables.
    //   snapshot_lock ('true'|'false', default true) - FLUSH TABLES WITH READ LOCK
    //       while the snapshot point is read (needs RELOAD); 'false' makes the
    //       snapshot-to-stream boundary at-least-once instead of exact.
    //   snapshot_lock_wait_s (default 10; 1 to 3600) - how long that lock may wait
    //   heartbeat_ms (default 1000; 1 to 3600000) - bounds cancel/checkpoint
    //       latency on an idle server
    //   on_decode_error ('fail' [default] | 'drop') - an event the source cannot
    //       decode stops it, or with 'drop' goes to the dead-letter path and is
    //       skipped, losing its changes
    reg.register_source<std::string>(
        "mysql_cdc_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            constexpr const char* who = "mysql_cdc_source";
            MysqlCdcOptions o;
            o.conn = conn_options_from(ctx);
            o.server_id =
                static_cast<std::uint32_t>(int_option(ctx, who, "server_id", 0, 0, 4294967295LL));
            o.tables = split_csv(ctx.param_or("tables", ""));
            o.start_file = ctx.param_or("start_file", "");
            o.start_pos = static_cast<std::uint64_t>(
                int_option(ctx, who, "start_pos", 4, 4, std::numeric_limits<std::int64_t>::max()));
            o.enable_initial_snapshot = bool_option(ctx, who, "initial_snapshot", false);
            o.snapshot_lock = bool_option(ctx, who, "snapshot_lock", true);
            o.snapshot_lock_wait_s = int_option(ctx, who, "snapshot_lock_wait_s", 10, 1, 3600);
            o.heartbeat_ms = int_option(ctx, who, "heartbeat_ms", 1000, 1, 3600000);
            const auto on_decode_error = ctx.param_or("on_decode_error", "fail");
            if (on_decode_error != "fail" && on_decode_error != "drop") {
                throw std::runtime_error(std::string{who} +
                                         ": 'on_decode_error' must be 'fail' or 'drop'; it is '" +
                                         on_decode_error + "'");
            }
            o.decode_error_policy = on_decode_error == "drop" ? MysqlDecodeErrorPolicy::Drop
                                                              : MysqlDecodeErrorPolicy::Fail;
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = who;
            return make_mysql_cdc_source(o);
        });
}

}  // namespace clink::mysql
