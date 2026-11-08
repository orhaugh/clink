#pragma once

// The MySQL CDC source's event processing, apart from the client library. It
// takes binlog events as BinlogEvent values, keeps the stream's state (the
// table maps, the definition each captured table is in force with, whether a
// transaction is open, the coordinates), turns row events into change
// records, and applies the decode-error policy. The source feeds it from
// mariadb_rpl; the tests feed it events they build.
//
// Definitions. A TABLE_MAP written with binlog_row_metadata=FULL names its
// columns and lists its ENUM and SET members, so its rows are decoded with
// the definition they were written with, whatever happened to the table later.
// One written with MINIMAL names nothing, so its rows are decoded with the
// table's definition in force at the coordinate: the source reads it at the
// coordinate the stream starts from (or takes it from the restored
// checkpoint) and hands it to start(); every checkpoint carries it on. A DDL
// statement that may change a captured table's columns ends what the
// processor knows of its definition, and a MINIMAL TABLE_MAP for it after that
// fails the source under either policy: information_schema describes the
// table as it is now, not as it was when the event was written, and the
// processor does not guess. A FULL TABLE_MAP needs no definition.
//
// Faults. Each event the processor cannot handle is one of: an unsupported
// event type, a corrupt row payload, a row event without its TABLE_MAP, a
// TABLE_MAP that disagrees with the definition in force, a partial row image,
// or a change not logged as rows (a statement-logged change, a TRUNCATE, a
// table dropped or renamed away, a row change inside an XA transaction).
// Under MysqlDecodeErrorPolicy::Fail it throws before emitting anything of the
// event; under Drop it reports the event and goes on, emitting none of its
// rows. Rows of a table outside the allowlist are skipped and counted, not
// faults. An incident event and a MINIMAL TABLE_MAP of a table whose
// definition is not known fail under either policy.

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/mysql/mysql_cdc_rules.hpp"
#include "clink/mysql/mysql_row_decode.hpp"

namespace clink::mysql {

enum class MysqlDecodeErrorPolicy { Fail, Drop };

// One binlog event, as the processor needs it. Byte fields are copies, valid
// for the call.
struct BinlogEvent {
    int type{0};
    std::uint64_t next_pos{0};
    // ROTATE
    std::string rotate_file;
    std::uint64_t rotate_pos{0};
    // GTID: MariaDB's flags say whether the group is a single statement.
    bool mariadb_gtid{false};
    bool gtid_standalone{false};
    std::int64_t gtid_seq{0};
    // QUERY
    std::string query_db;
    std::string query;
    // TABLE_MAP
    std::uint64_t table_id{0};
    std::string db;
    std::string table;
    std::vector<std::uint8_t> column_types;
    std::vector<std::uint8_t> metadata;
    std::vector<std::string> column_names;  // empty unless binlog_row_metadata=FULL
    std::optional<std::vector<std::vector<std::string>>> enum_values;
    std::optional<std::vector<std::vector<std::string>>> set_values;
    // The SIGNEDNESS bitmap, one bit per numeric column, most significant bit
    // first; empty when the TABLE_MAP has none.
    std::vector<std::uint8_t> signedness;
    // ROWS
    std::uint32_t column_count{0};
    std::vector<std::uint8_t> present_columns;         // the columns-present bitmap
    std::vector<std::uint8_t> update_present_columns;  // a version 1 UPDATE's, parsed apart
    const std::uint8_t* row_data{nullptr};             // valid for the call
    std::size_t row_data_size{0};
};

enum class DecodeFault {
    UnsupportedEvent,
    CorruptPayload,
    MissingTableMap,
    SchemaMismatch,
    PartialRowImage,
    UnsupportedStatement,
};

[[nodiscard]] const char* decode_fault_name(DecodeFault f);

// An event the processor could not handle under the fail policy, or one no
// policy may drop. The message names the fault, the table and the position,
// never row contents.
class MysqlCdcDecodeError : public std::runtime_error {
public:
    MysqlCdcDecodeError(const std::string& what, std::optional<DecodeFault> fault)
        : std::runtime_error(what), fault_(fault) {}
    [[nodiscard]] std::optional<DecodeFault> fault() const noexcept { return fault_; }

private:
    std::optional<DecodeFault> fault_;
};

// An event dropped under the drop policy, for the dead-letter path.
struct DroppedEvent {
    DecodeFault fault{DecodeFault::CorruptPayload};
    std::string why;
    std::string table;     // "db.table", when known
    std::string payload;   // the row image in hex, or the statement
    std::string location;  // file:pos
};

struct ProcessorOptions {
    std::vector<std::string> tables;  // "db.table" or bare names; empty captures every table
    MysqlDecodeErrorPolicy policy{MysqlDecodeErrorPolicy::Fail};
};

class BinlogEventProcessor {
public:
    explicit BinlogEventProcessor(ProcessorOptions opts);

    // Where the stream starts. `definitions` maps "db.table" to the table's
    // definition in force there, as the checkpoint records it (a
    // schema_fingerprint, or kUnknownDefinition and why it is not known);
    // `unrecorded` says why a captured table it does not list has no known
    // definition.
    void start(std::string file,
               std::uint64_t pos,
               const std::map<std::string, std::string>& definitions,
               std::string unrecorded);

    // Processes one event, appending its change records (flat JSON) to `out`.
    void process(const BinlogEvent& ev, std::vector<std::string>& out);

    // True between event groups: a coordinate here is a safe resume point.
    [[nodiscard]] bool between_transactions() const noexcept { return !tracker_.open(); }
    [[nodiscard]] const std::string& file() const noexcept { return file_; }
    [[nodiscard]] std::uint64_t pos() const noexcept { return pos_; }
    // The last coordinate between groups.
    [[nodiscard]] const std::string& committed_file() const noexcept { return committed_file_; }
    [[nodiscard]] std::uint64_t committed_pos() const noexcept { return committed_pos_; }
    // The definitions in force, as start() takes them and a checkpoint keeps
    // them. A DDL statement is a group of its own, so between groups they are
    // those of the committed coordinate.
    [[nodiscard]] std::map<std::string, std::string> definitions() const;

    // Events dropped since the last call (drop policy only).
    std::vector<DroppedEvent> take_dropped();
    // Row events of tables outside the allowlist, skipped.
    [[nodiscard]] std::uint64_t filtered_events() const noexcept { return filtered_; }

private:
    struct TableState {
        std::vector<ColumnMeta> metas;
        CdcTableSchema schema;
        bool filtered{false};
        std::string unusable;  // why its rows cannot be decoded, when they cannot
    };
    // A captured table's definition in force, or why it is not known.
    struct Definition {
        std::vector<ResolvedColumnSpec> columns;  // empty when not known
        std::string unknown;
    };

    [[nodiscard]] bool captured(const std::string& db, const std::string& table) const;
    [[nodiscard]] std::string location() const;
    // Handles a fault under the policy: throws under Fail, records it under Drop.
    void fault(DecodeFault f,
               const std::string& why,
               const std::string& table,
               std::string payload);
    [[noreturn]] void fatal(const std::string& why) const;
    // Why the definition of `key` is not known, or nullptr when it is.
    [[nodiscard]] const std::string* unknown_definition(const std::string& key) const;
    void forget(const std::string& key, const std::string& why);

    void on_table_map(const BinlogEvent& ev);
    void on_rows(const BinlogEvent& ev, std::vector<std::string>& out);
    void on_query(const BinlogEvent& ev);
    void on_ddl(const QueryStatement& q, const BinlogEvent& ev);
    void mark_boundary();

    ProcessorOptions opts_;
    std::set<std::string> filter_;
    TransactionTracker tracker_;
    std::map<std::uint64_t, TableState> tables_;  // table_id -> the map's state
    std::map<std::string, Definition> defs_;      // "db.table" -> definition in force
    std::string unrecorded_;                      // why an unlisted table's is not known
    std::string all_unknown_;  // set by a DDL statement whose tables were not named
    bool xa_group_{false};     // inside XA START ... XA PREPARE
    std::string file_;
    std::uint64_t pos_{4};
    std::string committed_file_;
    std::uint64_t committed_pos_{4};
    std::int64_t xid_{0};
    std::vector<DroppedEvent> dropped_;
    std::uint64_t filtered_{0};
};

}  // namespace clink::mysql
