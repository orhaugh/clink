#pragma once

// Pure rules of the MySQL binlog CDC source, testable without a server: the
// checkpoint row's format and its strict reading, the classes of binlog
// events, the classes of QUERY-event statements (transaction control, DDL and
// the tables it names, statement-logged DML), the transaction-boundary
// tracker, the readers of a TABLE_MAP's optional metadata, and the check of a
// binlog column type against information_schema's DATA_TYPE.

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace clink::mysql {

// ---- the checkpoint row ---------------------------------------------------------------

// What a checkpoint holds for the stream: the binlog coordinate of the last
// transaction boundary, the server that coordinate belongs to, and the
// definition of each captured table in force at that coordinate, which a
// TABLE_MAP written with binlog_row_metadata=MINIMAL is decoded with.
struct MysqlCdcCheckpoint {
    std::string file;
    std::uint64_t pos{4};
    // @@server_uuid of the server the coordinate belongs to; empty in a
    // checkpoint written before the source recorded it.
    std::string server_uuid;
    // "db.table" -> schema_fingerprint() of its definition at the coordinate,
    // or kUnknownDefinition followed by why the source does not know it there.
    std::map<std::string, std::string> schemas;
    // True when the source was still in its initial snapshot. The source never
    // passes a barrier then, so only a defect writes it; a restore refuses it.
    bool snapshot_unfinished{false};
    // 0: the earlier format (coordinate only); 1: this one.
    int version{1};
};

// The most definitions a checkpoint carries, and the longest one. The source
// refuses to start with more captured tables than that, and records a longer
// definition as not known.
inline constexpr std::size_t kMaxCheckpointDefinitions = 65536;
inline constexpr std::size_t kMaxDefinitionBytes = 4 * 1024 * 1024;

// Marks a schemas entry whose definition the source does not know. A
// fingerprint never starts with it: it starts with a length.
inline constexpr char kUnknownDefinition = '!';

// A checkpoint row that is not one this source wrote: wrong length, unknown
// version, a field out of range. Never treated as an absent checkpoint.
class MysqlCdcCheckpointError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

[[nodiscard]] std::string encode_mysql_cdc_checkpoint(const MysqlCdcCheckpoint& cp);

// Reads a checkpoint row strictly, in this format or the earlier one, and
// throws MysqlCdcCheckpointError, naming what is wrong, for anything else.
[[nodiscard]] MysqlCdcCheckpoint decode_mysql_cdc_checkpoint(std::string_view bytes);

// A binlog file name as MySQL writes one: a basename of A-Z, a-z, 0-9, '_',
// '.' and '-', then '.' and at least six digits, at most 255 characters.
[[nodiscard]] bool valid_binlog_file_name(std::string_view name);

// A server UUID as @@server_uuid shows one: 36 characters, hex digits and
// hyphens at 8-4-4-4-12.
[[nodiscard]] bool valid_server_uuid(std::string_view uuid);

// ---- binlog events ----------------------------------------------------------------------

enum class BinlogEventClass {
    Informational,     // no change and no boundary of its own
    TransactionStart,  // a GTID event opens an event group
    Query,             // BEGIN, COMMIT, DDL and the rest: see classify_query
    Xid,               // an InnoDB commit closes the group
    TableMap,
    Rows,         // write, update or delete rows the source decodes
    Rotate,       // the stream moves to the next binlog file
    Heartbeat,    // the master is idle: everything sent so far is whole groups
    Stop,         // the master stopped
    Unsupported,  // carries changes the source cannot decode
    Fatal,        // the master reports lost events
};

// The class of a binlog event type, and for Unsupported and Fatal why.
[[nodiscard]] BinlogEventClass classify_binlog_event(int type, std::string* why = nullptr);

// Whether a rows event type is a version 1 UPDATE, whose update-columns
// bitmap the client library consumes (a version 2 one leaves it at the start
// of the row data).
[[nodiscard]] bool is_v1_update_rows_event(int type);

// ---- QUERY-event statements ---------------------------------------------------------------

struct QueryStatement {
    enum class Kind {
        Begin,          // BEGIN, START TRANSACTION
        Commit,         // COMMIT, XA COMMIT ... ONE PHASE
        Rollback,       // ROLLBACK (not ROLLBACK TO)
        XaStart,        // XA START/BEGIN: a transaction that may be rolled back after prepare
        InTransaction,  // SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO, XA END/COMMIT/ROLLBACK...
        Ddl,            // a statement that changes tables
        Dml,            // a statement-logged INSERT, UPDATE, DELETE, REPLACE or LOAD
        Other,          // anything that changes no captured row (SET, GRANT, FLUSH...)
    };
    enum class Effect {
        Alter,   // ALTER TABLE, CREATE/DROP INDEX, a RENAME TABLE swap
        Rename,  // RENAME TABLE, ALTER TABLE ... RENAME: the table leaves its name
        Drop,    // DROP TABLE
        // Rows change without row events: TRUNCATE, or an ALTER TABLE that
        // drops, truncates, exchanges, discards or imports a partition or a
        // tablespace.
        Truncate,
        Create,  // CREATE TABLE, or a name a RENAME moves a table into
        DropDatabase,
        Write,  // statement-logged DML
    };
    struct Target {
        Effect effect{Effect::Alter};
        std::string db;     // the statement's database when it names none
        std::string table;  // empty for DropDatabase
        // For Alter: whether the statement may change the table's columns.
        // False only for changes the parser knows leave them alone (indexes,
        // constraints, table options, partitioning, column defaults).
        bool columns{true};
    };
    Kind kind{Kind::Other};
    std::vector<Target> targets;
    // False when the statement changes tables the parser could not name (a
    // multi-table UPDATE or DELETE, an unrecognised form of DDL): the source
    // treats it as touching every captured table.
    bool targets_known{true};
};

// Classifies a QUERY event's statement. `db` is the event's current database,
// which an unqualified table name belongs to.
[[nodiscard]] QueryStatement classify_query(std::string_view statement, std::string_view db);

// ---- transaction boundaries ---------------------------------------------------------------

// Tracks whether the stream is inside an event group. A coordinate is a safe
// resume point only between groups: resuming inside one would orphan its rows
// from their TABLE_MAP, or replay half a transaction. MySQL opens a DML
// transaction with a GTID event and BEGIN and closes it with XID or COMMIT; a
// DDL statement is its own group after a GTID event; MariaDB's GTID event
// implies BEGIN unless it is standalone.
class TransactionTracker {
public:
    void on_gtid(bool implies_begin) {
        open_ = true;
        begun_ = implies_begin;
    }
    void on_begin() {
        open_ = true;
        begun_ = true;
    }
    // XID, COMMIT or ROLLBACK: the group ends here.
    void on_end() {
        open_ = false;
        begun_ = false;
    }
    // Rows: inside a group, opened if no GTID or BEGIN was seen.
    void on_rows() { open_ = true; }
    // A statement that is not transaction control: the end of its own group
    // unless a BEGIN opened one. Returns true when it ended a group.
    bool on_statement() {
        if (!begun_) {
            open_ = false;
            return true;
        }
        return false;
    }
    [[nodiscard]] bool open() const noexcept { return open_; }

private:
    bool open_{false};
    bool begun_{false};
};

// ---- TABLE_MAP optional metadata (binlog_row_metadata=FULL) --------------------------------

// The COLUMN_NAME field: one length-encoded string per column. Empty when the
// field is malformed or names another number of columns.
[[nodiscard]] std::vector<std::string> parse_column_names(const std::uint8_t* data,
                                                          std::size_t size,
                                                          std::uint32_t columns);

// The ENUM_STR_VALUE and SET_STR_VALUE fields: for each ENUM (or SET)
// column, a count and that many length-encoded strings. nullopt when
// malformed.
[[nodiscard]] std::optional<std::vector<std::vector<std::string>>> parse_str_value_lists(
    const std::uint8_t* data, std::size_t size);

// ---- schema checks ---------------------------------------------------------------------

// Whether a TABLE_MAP column type (with the real type MySQL keeps in the first
// metadata byte of a STRING column) is the information_schema DATA_TYPE
// `data_type`: the check that catches a column changed, added, dropped or
// moved between the event and the schema it is decoded with.
[[nodiscard]] bool binlog_type_matches(std::uint8_t binlog_type,
                                       std::uint8_t metadata0,
                                       std::string_view data_type);

// One column of a table, as information_schema gives it.
struct InformationSchemaColumn {
    std::string name;
    std::string data_type;    // DATA_TYPE, e.g. "int"
    std::string column_type;  // COLUMN_TYPE, e.g. "int unsigned", "enum('a','b')"
};

// One resolved column, as a fingerprint records it.
struct ResolvedColumnSpec {
    std::string name;
    std::string data_type;
    bool is_unsigned{false};
    std::vector<std::string> labels;  // ENUM/SET members
};

// A stable text naming every column's name, type, signedness and members, in
// order, unambiguous whatever the names hold.
[[nodiscard]] std::string schema_fingerprint(const std::vector<ResolvedColumnSpec>& columns);

// The columns a fingerprint records; nullopt unless `text` is exactly a
// fingerprint of at least one column.
[[nodiscard]] std::optional<std::vector<ResolvedColumnSpec>> parse_schema_fingerprint(
    std::string_view text);

// A column as the source decodes with it: DATA_TYPE in lower case, the
// signedness and the ENUM or SET members read off COLUMN_TYPE.
[[nodiscard]] ResolvedColumnSpec resolve_column(const InformationSchemaColumn& c);

}  // namespace clink::mysql
