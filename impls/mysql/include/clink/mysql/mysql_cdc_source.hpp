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
    std::uint32_t server_id{0};       // replica server-id, REQUIRED, unique vs the master
    std::string start_file;           // cold-start binlog file ("" => current master position)
    std::uint64_t start_pos{4};       // cold-start position (4 = just past the magic header)
    std::vector<std::string> tables;  // allowlist of "db.table" (empty = all tables)
    std::int64_t heartbeat_ms{1000};  // master heartbeat period; bounds cancel/checkpoint latency
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"mysql_cdc_source"};
};

// Build a MySQL binlog CDC source emitting flat-JSON change rows on the string
// channel. Throws on invalid options (server_id == 0).
std::shared_ptr<Source<std::string>> make_mysql_cdc_source(const MysqlCdcOptions& opts);

}  // namespace clink::mysql
