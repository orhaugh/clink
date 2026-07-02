// MySQL connector factory registration (mysql_source, mysql_sink).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "clink/mysql/install.hpp"
#include "clink/mysql/mysql_cdc_source.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_json_upsert_sink.hpp"
#include "clink/mysql/mysql_sink.hpp"
#include "clink/mysql/mysql_source.hpp"
#include "clink/operators/sink_operator.hpp"
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

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // mysql_sink: batched multi-row INSERT of JSON-object rows into a table.
    // At-least-once (effectively-once with mode='upsert' + a PK/UNIQUE key).
    // Params:
    //   host/port/user/password/database
    //   table (required)          - target table
    //   columns                   - comma-separated projection / column order;
    //       on the SQL path it defaults to the table's declared schema, so it is
    //       only required when constructing the sink outside SQL.
    //   on_duplicate ("" [default, plain INSERT] | "update")  - "update" appends
    //       ON DUPLICATE KEY UPDATE = idempotent insert-or-update by PK on replay.
    //       (This is NOT clink mode='upsert', which is a changelog contract with
    //       delete tombstones this sink does not implement - that is rejected by
    //       the planner.)
    //   update_columns            - comma-separated ON DUPLICATE SET list (empty = all)
    //   batch_records (default 1000), max_bytes (0=off), linger_ms (0=off)
    reg.register_sink<std::string>(
        "mysql_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            MysqlSinkOptions o;
            o.conn = conn_options_from(ctx);
            o.table = ctx.param_or("table");
            o.columns = split_csv(ctx.param_or("columns", ""));
            if (o.columns.empty()) {
                // SQL Row path: fall back to the declared table schema so the user
                // need not repeat columns= matching the DDL (avoids silent drift).
                o.columns = columns_from_schema(ctx.param_or("schema_columns", ""));
            }
            o.upsert = ctx.param_or("on_duplicate", "") == "update";
            o.update_columns = split_csv(ctx.param_or("update_columns", ""));
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.max_bytes = static_cast<std::size_t>(ctx.param_int64_or("max_bytes", 0));
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "mysql_sink";
            return std::make_shared<MysqlSink>(std::move(o));
        });

    // mysql_upsert_sink: changelog-aware, mode='upsert'. Maintains a table by
    // PRIMARY KEY - insert/update_after upsert (INSERT ON DUPLICATE KEY UPDATE),
    // delete/update_before DELETE by key. Effectively-once on the sink table for a
    // stable PK. Lets a retracting SQL query maintain a MySQL table. params:
    //   table (required), columns (or schema_columns), key_columns (the PRIMARY
    //   KEY, threaded from the SQL path), update_columns, batch_records.
    reg.register_sink<std::string>(
        "mysql_upsert_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            MysqlJsonUpsertSinkOptions o;
            o.conn = conn_options_from(ctx);
            o.table = ctx.param_or("table");
            o.columns = split_csv(ctx.param_or("columns", ""));
            if (o.columns.empty()) {
                o.columns = columns_from_schema(ctx.param_or("schema_columns", ""));
            }
            o.key_columns = split_csv(ctx.param_or("key_columns", ""));
            o.update_columns = split_csv(ctx.param_or("update_columns", ""));
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.name = "mysql_upsert_sink";
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
            return make_mysql_poll_source(o);
        });

    // mysql_cdc_source: binlog CDC. Streams row-level changes from the master's
    // binary log (mariadb_rpl) as flat JSON objects with __op/__table/__lsn/__xid.
    // At-least-once ((file,position) checkpoint). The master must run
    // binlog_format=ROW + binlog_row_image=FULL and the user must hold
    // REPLICATION SLAVE + REPLICATION CLIENT. Params:
    //   host/port/user/password/database
    //   server_id (REQUIRED, non-zero, unique vs the master)
    //   tables                  - comma-separated "db.table" allowlist (empty = all)
    //   start_file / start_pos  - cold-start coordinate ("" => current master head)
    //   initial_snapshot ('true') - bootstrap existing rows of `tables` before
    //       streaming; captures a consistent binlog point and reads each table as-of
    //       it, then streams from there (gap-free). `tables` must then be non-empty
    //       and fully-qualified 'db.table' (so snapshot scope == stream scope).
    //   snapshot_lock ('false')   - skip FLUSH TABLES WITH READ LOCK during the
    //       snapshot (no RELOAD privilege needed; snapshot/stream boundary becomes
    //       at-least-once instead of exact). Default locked.
    //   heartbeat_ms (default 1000) - bounds cancel/checkpoint latency on idle
    reg.register_source<std::string>(
        "mysql_cdc_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            MysqlCdcOptions o;
            o.conn = conn_options_from(ctx);
            o.server_id = static_cast<std::uint32_t>(ctx.param_int64_or("server_id", 0));
            o.tables = split_csv(ctx.param_or("tables", ""));
            o.start_file = ctx.param_or("start_file", "");
            o.start_pos = static_cast<std::uint64_t>(ctx.param_int64_or("start_pos", 4));
            o.enable_initial_snapshot = ctx.param_or("initial_snapshot", "") == "true";
            o.snapshot_lock = ctx.param_or("snapshot_lock", "true") != "false";
            o.heartbeat_ms = ctx.param_int64_or("heartbeat_ms", 1000);
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = "mysql_cdc_source";
            return make_mysql_cdc_source(o);
        });
}

}  // namespace clink::mysql
