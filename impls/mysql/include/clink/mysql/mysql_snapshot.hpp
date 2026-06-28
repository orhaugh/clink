#pragma once

// Pure (no libmariadb) helpers for the MySQL CDC initial-snapshot phase, split out
// so they are unit-testable without a live server: qualify a configured table name
// into (db, table), build the snapshot SELECT with safe identifier quoting, and
// turn one fetched row into an Insert CdcEvent (so a snapshot row is byte-identical
// to a streamed insert through clink::cdc::cdc_event_to_json_row).

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/cdc_event.hpp"
#include "clink/mysql/mysql_sql.hpp"  // quote_ident (backtick, validated)

namespace clink::mysql {

struct QualifiedTable {
    std::string db;
    std::string table;
};

// Split a configured table entry into (db, table). "db.tbl" -> {db,tbl}; a bare
// "tbl" is qualified with default_db (the connection's database) - and is an error
// if default_db is empty (the snapshot SELECT must be schema-qualified). Throws on
// an empty/garbled entry.
inline QualifiedTable qualify_table(const std::string& entry, const std::string& default_db) {
    const std::size_t dot = entry.find('.');
    QualifiedTable q;
    if (dot == std::string::npos) {
        if (default_db.empty()) {
            throw std::runtime_error("mysql_cdc snapshot: table '" + entry +
                                     "' is not schema-qualified and no connection database is set "
                                     "(use 'db.table' or set database=)");
        }
        q.db = default_db;
        q.table = entry;
    } else {
        q.db = entry.substr(0, dot);
        q.table = entry.substr(dot + 1);
    }
    if (q.db.empty() || q.table.empty()) {
        throw std::runtime_error("mysql_cdc snapshot: invalid table name '" + entry + "'");
    }
    return q;
}

// SELECT * FROM `db`.`table`, identifiers backtick-quoted + validated by
// quote_ident (rejects anything outside [A-Za-z0-9_$]).
inline std::string build_snapshot_select(const QualifiedTable& t) {
    return "SELECT * FROM " + quote_ident(t.db) + "." + quote_ident(t.table);
}

// Build the Insert CdcEvent for one snapshot row. `names` are the column names (in
// SELECT order); `cells` are the column values, std::nullopt for SQL NULL. The
// event's lsn is the captured snapshot binlog coordinate (constant for the whole
// snapshot), xid is 0 (no transaction id for a snapshot read). Throws on a
// names/cells length mismatch (a programming error, never expected at runtime).
inline CdcEvent snapshot_row_to_event(const std::string& table,
                                      const std::string& lsn,
                                      const std::vector<std::string>& names,
                                      const std::vector<std::optional<std::string>>& cells) {
    if (names.size() != cells.size()) {
        throw std::runtime_error("mysql_cdc snapshot: column count mismatch for " + table);
    }
    CdcEvent ev;
    ev.op = CdcEvent::Op::Insert;
    ev.table = table;
    ev.lsn = lsn;
    ev.xid = 0;
    ev.values.reserve(names.size());
    for (std::size_t i = 0; i < names.size(); ++i) {
        CdcField f;
        f.name = names[i];
        if (cells[i].has_value()) {
            f.value = *cells[i];
            f.is_null = false;
        } else {
            f.is_null = true;
        }
        ev.values.push_back(std::move(f));
    }
    return ev;
}

}  // namespace clink::mysql
