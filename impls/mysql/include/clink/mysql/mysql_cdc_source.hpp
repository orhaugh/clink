#pragma once

// MySQL binlog CDC source. Streams row-level changes (insert/update/delete) from
// a MySQL server's binary log via the mariadb-connector-c replication API
// (mariadb_rpl_*), emitting each change as one flat JSON object string with
// __op/__table/__lsn/__xid metadata - byte-identical to the Postgres CDC source
// (both go through clink::cdc::cdc_event_to_json_row), so the SQL Row path binds
// the data columns by name.
//
// REQUIRES binlog_format=ROW and binlog_row_image=FULL on the server (checked at
// open, and per event, since a session can override both), and REPLICATION
// SLAVE + REPLICATION CLIENT for the connecting user. With
// binlog_row_metadata=FULL every TABLE_MAP names its columns, and each event is
// decoded with the definition it was written with; with MINIMAL (MySQL's
// default) the names come from information_schema, and a change to a captured
// table's columns stops the source (mysql_cdc_processor.hpp).
//
// DELIVERY: at-least-once. The checkpoint holds the binlog coordinate of the
// last transaction boundary, the server's UUID and, for information_schema
// decoded tables, the definitions they were decoded with. The source holds
// checkpoint barriers inside a transaction (Source::barrier_ready), so a
// checkpoint covers whole transactions and a restore replays only what came
// after it. A restored checkpoint is read strictly: a malformed row, a binlog
// file no longer on the server, a position past its end or another server's
// coordinate fail the source; it never resumes from the head in their place.
// An event the source cannot decode follows decode_error_policy: Fail (the
// default) stops the source before emitting anything of it; Drop reports it to
// the dead-letter path and skips it, losing its changes.
//
// INITIAL SNAPSHOT (enable_initial_snapshot): on a first start (no restored
// checkpoint) the source reads each `tables` table as of a binlog coordinate P,
// emitting every row as an Insert, then streams from P. With snapshot_lock (the
// default) it takes FLUSH TABLES WITH READ LOCK just long enough to read P and
// open a consistent-snapshot transaction (needs RELOAD; the wait is bounded by
// snapshot_lock_wait_s), so P is exact; without it P precedes the snapshot's
// view, and changes in between are emitted again after the snapshot already
// shows them. The source holds checkpoint barriers until the snapshot ends, so
// no checkpoint covers part of it: a failure during the snapshot restores the
// state from before it, and the restarted source snapshots afresh. Every
// snapshot table must be InnoDB (refused otherwise) and named "db.table".
//
// PARALLELISM: a binlog stream is single-reader. Only subtask 0 streams; other
// subtasks are dormant (emit nothing) so a parallel job does not open N
// conflicting dumps with the same server-id.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/mysql/mysql_cdc_processor.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mysql {

struct MysqlCdcOptions {
    ConnectOptions conn;
    std::uint32_t server_id{0};           // replica server-id, REQUIRED, unique vs the master
    std::string start_file;               // cold-start binlog file ("" => current master position)
    std::uint64_t start_pos{4};           // cold-start position (4 = just past the magic header)
    std::vector<std::string> tables;      // allowlist of "db.table" (empty = all tables)
    bool enable_initial_snapshot{false};  // bootstrap existing rows before streaming (see header)
    bool snapshot_lock{true};  // FTWRL for an exact, overlap-free snapshot point (needs RELOAD)
    // How long FLUSH TABLES WITH READ LOCK may wait for the lock (the session's
    // lock_wait_timeout), in seconds: the wait blocks the server's writers too.
    std::int64_t snapshot_lock_wait_s{10};
    std::int64_t heartbeat_ms{1000};  // master heartbeat period; bounds cancel/checkpoint latency
    MysqlDecodeErrorPolicy decode_error_policy{MysqlDecodeErrorPolicy::Fail};
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"mysql_cdc_source"};
};

// Build a MySQL binlog CDC source emitting flat-JSON change rows on the string
// channel. Throws on invalid options.
std::shared_ptr<Source<std::string>> make_mysql_cdc_source(const MysqlCdcOptions& opts);

}  // namespace clink::mysql
