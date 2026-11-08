// The MySQL sinks' pure rules. See mysql_sink_rules.hpp.

#include "clink/mysql/mysql_sink_rules.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <set>
#include <string>
#include <system_error>
#include <utility>

#include "clink/mysql/mysql_sql.hpp"

namespace clink::mysql {

namespace {
bool contains(const std::vector<std::string>& v, std::string_view x) {
    return std::find(v.begin(), v.end(), x) != v.end();
}

std::string joined(const std::vector<std::string>& v) {
    std::string out;
    for (const auto& x : v) {
        out += (out.empty() ? "" : ", ") + x;
    }
    return out;
}

}  // namespace

// ---- the destination table ------------------------------------------------------------

const ColumnInfo* TableSchema::column(std::string_view name) const {
    for (const auto& c : columns) {
        if (c.name == name) {
            return &c;
        }
    }
    return nullptr;
}

bool key_identity_exact(const ColumnInfo& c, bool epoch_ms) {
    static const std::set<std::string, std::less<>> kIntegers = {
        "tinyint", "smallint", "mediumint", "int", "integer", "bigint"};
    if (kIntegers.count(c.data_type) > 0 || c.data_type == "decimal" || c.data_type == "numeric" ||
        c.data_type == "varbinary") {
        return true;
    }
    if (c.data_type == "varchar") {
        const auto& coll = c.collation;
        return c.collation_no_pad && coll.size() > 4 &&
               coll.compare(coll.size() - 4, 4, "_bin") == 0;
    }
    if (c.data_type == "datetime" || c.data_type == "timestamp") {
        return epoch_ms && c.datetime_precision >= 3;
    }
    return false;
}

std::vector<std::string> check_target(const SinkTarget& t, const TableSchema& s) {
    std::vector<std::string> why;
    if (!s.exists) {
        why.push_back("table " + t.table + " does not exist, or the sink's user cannot see it");
        return why;
    }
    const bool keyed = t.mode != SinkMode::Append;
    if (s.view && keyed) {
        why.push_back("table " + t.table + " is a view, which has no keys to write by");
        return why;
    }
    for (const auto& name : t.columns) {
        const auto* c = s.column(name);
        if (c == nullptr) {
            why.push_back("table " + t.table + " has no column '" + name + "'");
        } else if (c->generated) {
            why.push_back("column '" + name + "' is a generated column, which takes no value");
        }
    }
    for (const auto& [name, kind] : t.timestamp_columns) {
        (void)kind;
        const auto* c = s.column(name);
        if (c != nullptr && contains(t.columns, name) && c->data_type != "datetime" &&
            c->data_type != "timestamp") {
            why.push_back("column '" + name + "' carries a TIMESTAMP and is " + c->column_type +
                          " in the table; it must be DATETIME or TIMESTAMP");
        }
    }
    if (!why.empty()) {
        return why;
    }
    for (const auto& idx : s.unique_indexes) {
        if (keyed && idx.prefix) {
            why.push_back("unique index " + idx.name +
                          " covers a prefix of a column's values, so rows whose values differ "
                          "past that prefix collide on it");
        }
    }
    if (t.mode == SinkMode::InsertOrUpdate) {
        for (const auto& u : t.update_columns) {
            if (!contains(t.columns, u)) {
                why.push_back("update column '" + u +
                              "' is not one of the columns the sink writes");
            }
        }
        if (s.unique_indexes.empty()) {
            why.push_back("on_duplicate='update' needs a PRIMARY KEY or UNIQUE index on table " +
                          t.table + ": without one a replay inserts every row again");
        } else if (s.unique_indexes.size() > 1) {
            std::vector<std::string> names;
            for (const auto& idx : s.unique_indexes) {
                names.push_back(idx.name);
            }
            why.push_back("table " + t.table + " has " + std::to_string(names.size()) +
                          " unique indexes (" + joined(names) +
                          "): ON DUPLICATE KEY UPDATE updates whichever row a record collides "
                          "with first, so on_duplicate='update' needs exactly one");
        } else {
            for (const auto& c : s.unique_indexes.front().columns) {
                if (!contains(t.columns, c)) {
                    why.push_back("on_duplicate='update' keys rows by index " +
                                  s.unique_indexes.front().name + ", whose column '" + c +
                                  "' the sink does not write");
                }
            }
        }
    }
    if (t.mode == SinkMode::Changelog) {
        if (!s.transactional) {
            why.push_back("table " + t.table + " is " +
                          (s.engine.empty() ? std::string{"of an unknown engine"}
                                            : "a " + s.engine + " table") +
                          ", which has no transactions; mysql_upsert_sink applies each flush's "
                          "upserts and deletes as one transaction");
        }
        for (const auto& k : t.key_columns) {
            if (!contains(t.columns, k)) {
                why.push_back("key column '" + k + "' is not one of the columns the sink writes");
            }
        }
        const std::set<std::string> key(t.key_columns.begin(), t.key_columns.end());
        const UniqueIndex* identity = nullptr;
        for (const auto& idx : s.unique_indexes) {
            if (std::set<std::string>(idx.columns.begin(), idx.columns.end()) == key &&
                idx.columns.size() == key.size()) {
                identity = &idx;
            }
        }
        if (identity == nullptr) {
            why.push_back("table " + t.table + " has no PRIMARY KEY or UNIQUE index on exactly (" +
                          joined(t.key_columns) + "), the key the sink writes rows by");
        }
        for (const auto& idx : s.unique_indexes) {
            if (&idx != identity) {
                why.push_back("table " + t.table + " also has unique index " + idx.name + " (" +
                              joined(idx.columns) +
                              "): an upsert colliding with it would update the row it names "
                              "rather than the key's");
            }
        }
        if (!t.update_columns.empty()) {
            for (const auto& c : t.columns) {
                if (!contains(t.key_columns, c) && !contains(t.update_columns, c)) {
                    why.push_back("update_columns leaves out column '" + c +
                                  "': an upsert would keep its value from an earlier record, "
                                  "and the table would not hold each key's last record");
                }
            }
        }
        if (t.parallelism > 1) {
            for (const auto& k : t.key_columns) {
                const auto* c = s.column(k);
                if (c == nullptr) {
                    continue;
                }
                const bool epoch_ms = t.timestamp_columns.count(k) > 0;
                if (!key_identity_exact(*c, epoch_ms)) {
                    why.push_back(
                        "key column '" + k + "' is " + c->column_type +
                        (c->collation.empty() ? std::string{} : " COLLATE " + c->collation) +
                        ", under which values the stream tells apart can be one row; with " +
                        std::to_string(t.parallelism) +
                        " subtasks such values can reach the table from two of them in either "
                        "order. Run the sink at parallelism 1, or key the table by an exact "
                        "type (an integer, DECIMAL, VARBINARY, VARCHAR with a binary NO PAD "
                        "collation such as utf8mb4_0900_bin, or a DATETIME(3) or TIMESTAMP(3) "
                        "the stream writes as a TIMESTAMP)");
                }
            }
        }
    }
    return why;
}

// ---- rendering --------------------------------------------------------------------------

std::string render_literal(const JsonMember& v,
                           const ColumnInfo* col,
                           sqljson::TimeKind time,
                           const sqljson::EscapeFn& esc) {
    switch (v.kind) {
        case JsonMember::Kind::Null:
            return "NULL";
        case JsonMember::Kind::Bool:
            return v.text == "true" ? "1" : "0";
        case JsonMember::Kind::String:
            return "'" + esc(v.text) + "'";
        case JsonMember::Kind::Composite:
            return "'" + esc(v.text) + "'";
        case JsonMember::Kind::Number:
            break;
    }
    // An approximate column holds the number the server reads from the token,
    // which JSON spells as MySQL does. Only a value past a DOUBLE's range is
    // refused here, where the server's refusal would quote it.
    if (time == sqljson::TimeKind::None && col != nullptr &&
        (col->data_type == "double" || col->data_type == "float")) {
        if (!std::isfinite(std::strtod(v.text.c_str(), nullptr))) {
            throw RecordRefused("a number outside the range of a DOUBLE");
        }
        return v.text;
    }
    const auto plain = plain_decimal(v.text, kMaxNumberChars);
    if (plain.empty()) {
        throw RecordRefused("a number whose plain decimal form is longer than " +
                            std::to_string(kMaxNumberChars) + " characters");
    }
    if (time == sqljson::TimeKind::None) {
        return plain;
    }
    std::int64_t ms = 0;
    const auto [end, ec] = std::from_chars(plain.data(), plain.data() + plain.size(), ms);
    if (plain.find('.') != std::string::npos || ec != std::errc{} ||
        end != plain.data() + plain.size()) {
        throw RecordRefused("a TIMESTAMP that is not a whole number of milliseconds");
    }
    const int fsp = col != nullptr ? col->datetime_precision : 3;
    if (fsp >= 0 && fsp < 3) {
        std::int64_t unit = 1;
        for (int k = fsp; k < 3; ++k) {
            unit *= 10;
        }
        if (ms % unit != 0) {
            throw RecordRefused("a TIMESTAMP with milliseconds that column " +
                                (col != nullptr ? "'" + col->name + "' " : std::string{}) +
                                "would round away (it keeps " + std::to_string(fsp) +
                                " fractional digits)");
        }
    }
    try {
        return sqljson::epoch_ms_to_sql_timestamp(ms, time, sqljson::kMysql);
    } catch (const std::runtime_error&) {
        throw RecordRefused("a TIMESTAMP outside years 1 to 9999");
    }
}

std::string render_row(const JsonMembers& m,
                       const std::vector<std::string>& columns,
                       const TableSchema& schema,
                       const sqljson::TimeColumns& times,
                       const sqljson::EscapeFn& esc) {
    std::string out = "(";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        const auto it = m.find(columns[i]);
        if (it == m.end()) {
            out += "NULL";
            continue;
        }
        try {
            out += render_literal(it->second,
                                  schema.column(columns[i]),
                                  sqljson::time_kind_of(times, columns[i]),
                                  esc);
        } catch (const RecordRefused& e) {
            throw RecordRefused("member \"" + columns[i] + "\" is " + e.what());
        }
    }
    out.push_back(')');
    return out;
}

std::string render_key(const JsonMembers& m,
                       const std::vector<std::string>& key_columns,
                       const TableSchema& schema,
                       const sqljson::TimeColumns& times,
                       const sqljson::EscapeFn& esc) {
    std::string out = key_columns.size() > 1 ? "(" : "";
    for (std::size_t i = 0; i < key_columns.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        const auto it = m.find(key_columns[i]);
        if (it == m.end() || it->second.kind == JsonMember::Kind::Null) {
            throw RecordRefused("its key member \"" + key_columns[i] +
                                "\" is missing or null, so it names no row");
        }
        try {
            out += render_literal(it->second,
                                  schema.column(key_columns[i]),
                                  sqljson::time_kind_of(times, key_columns[i]),
                                  esc);
        } catch (const RecordRefused& e) {
            throw RecordRefused("key member \"" + key_columns[i] + "\" is " + e.what());
        }
    }
    if (key_columns.size() > 1) {
        out.push_back(')');
    }
    return out;
}

void check_key_spelling(const JsonMembers& m,
                        const std::vector<std::string>& key_columns,
                        const TableSchema& schema) {
    for (const auto& k : key_columns) {
        const auto it = m.find(k);
        const auto* c = schema.column(k);
        if (it == m.end() || c == nullptr || it->second.kind == JsonMember::Kind::Null) {
            continue;  // stream_key refuses a missing or null key
        }
        const bool text = c->data_type == "varchar" || c->data_type == "varbinary";
        if (it->second.kind != (text ? JsonMember::Kind::String : JsonMember::Kind::Number)) {
            throw RecordRefused("its key member \"" + k + "\" is not a JSON " +
                                (text ? "string" : "number") + ", as column '" + k + "' (" +
                                c->column_type +
                                ") needs with more than one subtask: a key written two ways "
                                "can reach two subtasks, which write one row in either order");
        }
    }
}

// ---- statements -------------------------------------------------------------------------

std::string insert_head(const std::string& table, const std::vector<std::string>& columns) {
    std::string sql = "INSERT INTO " + quote_table_ident(table) + " (";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        sql += (i > 0 ? "," : "") + quote_ident(columns[i]);
    }
    return sql + ") VALUES ";
}

std::string upsert_tail(const std::vector<std::string>& set_columns) {
    std::string sql = " AS clink_new ON DUPLICATE KEY UPDATE ";
    for (std::size_t i = 0; i < set_columns.size(); ++i) {
        const auto q = quote_ident(set_columns[i]);
        sql += (i > 0 ? "," : "") + q + "=clink_new." + q;
    }
    return sql;
}

std::string delete_head(const std::string& table, const std::vector<std::string>& key_columns) {
    std::string sql = "DELETE FROM " + quote_table_ident(table) + " WHERE ";
    if (key_columns.size() == 1) {
        return sql + quote_ident(key_columns.front()) + " IN (";
    }
    sql += "(";
    for (std::size_t i = 0; i < key_columns.size(); ++i) {
        sql += (i > 0 ? "," : "") + quote_ident(key_columns[i]);
    }
    return sql + ") IN (";
}

// ---- server messages ------------------------------------------------------------------

std::string redact_server_message(std::string_view message) {
    std::string out{message};
    static constexpr std::string_view kMarkers[] = {"entry '", "value: '", "value '"};
    for (const auto marker : kMarkers) {
        std::size_t from = 0;
        while ((from = out.find(marker, from)) != std::string::npos) {
            const auto begin = from + marker.size();
            auto end = out.find("' for ", begin);
            if (end == std::string::npos) {
                end = out.rfind('\'');
                if (end == std::string::npos || end < begin) {
                    break;
                }
            }
            out.replace(begin, end - begin, "<value>");
            from = begin + 7;
        }
    }
    return out;
}

}  // namespace clink::mysql
