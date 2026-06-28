#pragma once

// MySQL binlog CDC source. Streams row-level changes (insert/update/delete) from
// a MySQL/MariaDB master's binary log via the mariadb-connector-c replication
// API (mariadb_rpl_*), emitting each change as one flat JSON object string with
// __op/__table/__lsn/__xid metadata - byte-identical to the Postgres CDC source
// (both go through clink::cdc::cdc_event_to_json_row), so the SQL Row path binds
// the data columns by name.
//
// REQUIRES the master to run binlog_format=ROW + binlog_row_image=FULL and the
// connecting user to hold REPLICATION SLAVE + REPLICATION CLIENT (and SELECT on
// information_schema, which every user has). Column names + signedness are
// resolved from information_schema (MySQL's default binlog metadata omits them).
//
// DELIVERY: at-least-once for the decodable change stream; the checkpoint cursor
// is (binlog_file, position). An undecodable row event (schema drift / unseen
// table-map) is dropped + counted (at-most-once for those), never emitted
// half-populated. See the source .cpp header comment for the full caveats.
//
// INITIAL SNAPSHOT (enable_initial_snapshot): on a FIRST start (no restored
// checkpoint) the source first bootstraps existing rows, then streams. It captures
// a consistent binlog coordinate P and reads each `tables` table as-of P, emitting
// every row as an Insert change, then streams the binlog from P - so existing data
// and ongoing changes form one gap-free stream. With snapshot_lock=true (default)
// it briefly takes FLUSH TABLES WITH READ LOCK (needs the RELOAD/FLUSH_TABLES
// privilege) so P is exact (no overlap); with snapshot_lock=false no global lock is
// taken and the snapshot/stream boundary is at-least-once (a few rows replay). The
// snapshot is NOT checkpointed mid-way: a crash during it re-snapshots from scratch
// on restart (at-least-once); once streaming begins, (file,pos) checkpoints resume
// normally and a restart with a restored checkpoint SKIPS the snapshot. `tables`
// must be fully-qualified "db.table" when the snapshot is enabled (a bare name
// streams same-named tables across every DB but the snapshot scans only one, so it
// would partially capture). Snapshot consistency relies on InnoDB MVCC; a
// non-transactional engine (MyISAM) has no consistent-snapshot view.
//
// PARALLELISM: a binlog stream is single-reader. Only subtask 0 streams; other
// subtasks are dormant (emit nothing) so a parallel job does not open N
// conflicting dumps with the same server-id.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

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
    std::int64_t heartbeat_ms{1000};  // master heartbeat period; bounds cancel/checkpoint latency
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"mysql_cdc_source"};
};

// Build a MySQL binlog CDC source emitting flat-JSON change rows on the string
// channel. Throws on invalid options (server_id == 0).
std::shared_ptr<Source<std::string>> make_mysql_cdc_source(const MysqlCdcOptions& opts);

}  // namespace clink::mysql
