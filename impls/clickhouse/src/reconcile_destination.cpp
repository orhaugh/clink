#include "clink/clickhouse/reconcile_destination.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdio>
#include <optional>
#include <stdexcept>
#include <utility>

#include <clickhouse/client.h>
#include <clickhouse/columns/string.h>

#include "native/sql_text.hpp"

namespace clink::clickhouse {

namespace rc = incident::reconcile;

namespace {

using native::quote_identifier;
using native::quote_string;

// A String literal for any bytes: ' and \ escaped, and every control byte as
// \xHH, so the statement's text holds nothing but printable bytes and the
// escapes the server reads back to the same bytes.
std::string literal_bytes(std::string_view bytes) {
    std::string out = "'";
    for (const char ch : bytes) {
        const auto u = static_cast<unsigned char>(ch);
        if (ch == '\'' || ch == '\\') {
            out += '\\';
            out += ch;
        } else if (u < 0x20 || u == 0x7f) {
            std::array<char, 8> esc{};
            std::snprintf(esc.data(), esc.size(), "\\x%02X", u);
            out += esc.data();
        } else {
            out += ch;
        }
    }
    return out + "'";
}

template <typename T>
std::optional<T> parse_number(std::string_view text) {
    T v{};
    const auto r = std::from_chars(text.data(), text.data() + text.size(), v);
    if (r.ec != std::errc{} || r.ptr != text.data() + text.size()) {
        return std::nullopt;
    }
    return v;
}

// What reconcile_value_expr selected for a column, as its canonical cell.
std::string canon_observed(bool is_null, std::string_view text, const rc::ColumnType& t) {
    if (is_null) {
        return rc::canon::null_cell();
    }
    switch (t.kind) {
        case rc::TypeKind::Int:
        case rc::TypeKind::UInt:
            return rc::canon::integer(text);
        case rc::TypeKind::Float64: {
            const auto bits = parse_number<std::uint64_t>(text);
            return bits ? rc::canon::float64_bits(*bits) : "!unreadable Float64 bits";
        }
        case rc::TypeKind::Float32: {
            const auto bits = parse_number<std::uint32_t>(text);
            return bits ? rc::canon::float32_bits(*bits) : "!unreadable Float32 bits";
        }
        case rc::TypeKind::Bool:
            return text == "1" ? rc::canon::boolean(true)
                               : (text == "0" ? rc::canon::boolean(false) : "!unreadable Bool");
        case rc::TypeKind::String:
            return rc::canon::string(text);
        case rc::TypeKind::Decimal:
            return rc::canon::decimal(text, t.scale);
        case rc::TypeKind::DateTime64:
        case rc::TypeKind::DateTime: {
            const auto n = parse_number<std::int64_t>(text);
            return n ? rc::canon::ticks(*n, t.kind == rc::TypeKind::DateTime ? 0 : t.precision)
                     : "!unreadable time";
        }
        case rc::TypeKind::Date: {
            const auto n = parse_number<std::int64_t>(text);
            return n ? rc::canon::days(*n) : "!unreadable date";
        }
        case rc::TypeKind::Unsupported:
            break;
    }
    return "!" + t.spelling + " is not compared";
}

std::string server_error_text(const std::exception& e) {
    return e.what();
}

class ClickHouseDestination final : public rc::Destination {
public:
    ClickHouseDestination(ReconcileConnection c, rc::ReadBounds b)
        : conn_(std::move(c)), bounds_(b) {}

    rc::DestinationDescription describe() override {
        rc::DestinationDescription d;
        d.database = conn_.database;
        d.table = conn_.table;
        const auto ident = select(
            "SELECT CAST(toString(serverUUID()) AS String), CAST(hostName() AS String), "
            "CAST(version() AS String), CAST(toString(now64(3, 'UTC')) AS String) " +
                reconcile_settings(bounds_),
            4);
        if (ident.size() != 1) {
            throw rc::DestinationUnavailable("the server did not say who it is");
        }
        d.server_uuid = ident[0][0];
        d.host_name = ident[0][1];
        d.version = ident[0][2];
        d.now = ident[0][3];
        const auto table = select(
            "SELECT CAST(toString(uuid) AS String), CAST(engine AS String), "
            "CAST(engine_full AS String), CAST(create_table_query AS String), "
            "CAST(toString(metadata_modification_time) AS String) FROM system.tables WHERE "
            "database = " +
                quote_string(conn_.database) + " AND name = " + quote_string(conn_.table) + " " +
                reconcile_settings(bounds_),
            5);
        if (table.empty()) {
            // No such table here: the identity check refuses it as another destination.
            return d;
        }
        d.table_uuid = table[0][0];
        d.engine = table[0][1];
        d.engine_full = table[0][2];
        d.create_table_query = table[0][3];
        d.metadata_modification_time = table[0][4];
        for (auto& row :
             select("SELECT CAST(name AS String), CAST(type AS String) FROM "
                    "system.columns WHERE database = " +
                        quote_string(conn_.database) + " AND table = " + quote_string(conn_.table) +
                        " ORDER BY position " + reconcile_settings(bounds_),
                    2)) {
            d.columns.emplace_back(std::move(row[0]), std::move(row[1]));
        }
        return d;
    }

    std::vector<rc::Mutation> mutations() override {
        std::vector<rc::Mutation> out;
        for (auto& row :
             select("SELECT CAST(mutation_id AS String), CAST(command AS String), "
                    "CAST(toString(is_done) AS String), "
                    "CAST(toString(create_time) AS String) FROM system.mutations WHERE "
                    "database = " +
                        quote_string(conn_.database) + " AND table = " + quote_string(conn_.table) +
                        " ORDER BY create_time, mutation_id " + reconcile_settings(bounds_),
                    4)) {
            out.push_back(rc::Mutation{.id = std::move(row[0]),
                                       .command = std::move(row[1]),
                                       .done = row[2] == "1",
                                       .created = std::move(row[3])});
        }
        return out;
    }

    std::vector<rc::Row> read(const std::vector<rc::Column>& columns,
                              const std::vector<std::size_t>& key,
                              const std::vector<rc::Row>& keys) override {
        std::vector<rc::Row> out;
        if (keys.empty()) {
            return out;
        }
        const auto rows = select(
            reconcile_read_statement(conn_.database, conn_.table, columns, key, keys, bounds_),
            columns.size() * 2);
        out.reserve(rows.size());
        for (const auto& r : rows) {
            rc::Row row;
            row.reserve(columns.size());
            for (std::size_t i = 0; i < columns.size(); ++i) {
                row.push_back(canon_observed(r[2 * i] == "1", r[2 * i + 1], columns[i].type));
            }
            out.push_back(std::move(row));
        }
        return out;
    }

private:
    // One SELECT, whole or not at all: every exception the client raises is
    // turned into DestinationUnavailable, and the client is rebuilt after it.
    std::vector<std::vector<std::string>> select(const std::string& sql, std::size_t width) {
        std::vector<std::vector<std::string>> rows;
        try {
            if (!client_) {
                ::clickhouse::ClientOptions o;
                o.SetHost(conn_.host)
                    .SetPort(conn_.port)
                    .SetUser(conn_.user)
                    .SetPassword(conn_.password)
                    .SetDefaultDatabase("system")
                    .SetPingBeforeQuery(false)
                    .SetSendRetries(1)
                    .SetConnectionConnectTimeout(conn_.connect_timeout)
                    // The server enforces max_execution_time; this bounds a server
                    // that stops answering at all.
                    .SetConnectionRecvTimeout(bounds_.query_timeout + std::chrono::seconds{10})
                    .SetConnectionSendTimeout(bounds_.query_timeout + std::chrono::seconds{10});
                if (conn_.secure) {
                    ::clickhouse::ClientOptions::SSLOptions ssl;
                    if (!conn_.tls_ca_file.empty()) {
                        ssl.SetPathToCAFiles({conn_.tls_ca_file}).SetUseDefaultCALocations(false);
                    }
                    ssl.SetSkipVerification(!conn_.tls_verify);
                    o.SetSSLOptions(std::move(ssl));
                }
                client_ = std::make_unique<::clickhouse::Client>(o);
            }
            client_->Select(sql, [&](const ::clickhouse::Block& block) {
                if (block.GetRowCount() == 0) {
                    return;
                }
                if (block.GetColumnCount() != width) {
                    throw std::runtime_error(
                        "a result of " + std::to_string(block.GetColumnCount()) +
                        " columns where " + std::to_string(width) + " were selected");
                }
                std::vector<std::shared_ptr<::clickhouse::ColumnString>> cols;
                for (std::size_t c = 0; c < width; ++c) {
                    auto s = block[c]->As<::clickhouse::ColumnString>();
                    if (!s) {
                        throw std::runtime_error("a result column that is not String");
                    }
                    cols.push_back(std::move(s));
                }
                for (std::size_t r = 0; r < block.GetRowCount(); ++r) {
                    std::vector<std::string> row;
                    row.reserve(width);
                    for (const auto& s : cols) {
                        row.emplace_back(s->At(r));
                    }
                    rows.push_back(std::move(row));
                }
            });
        } catch (const std::exception& e) {
            client_.reset();
            throw rc::DestinationUnavailable(server_error_text(e));
        }
        return rows;
    }

    ReconcileConnection conn_;
    rc::ReadBounds bounds_;
    std::unique_ptr<::clickhouse::Client> client_;
};

}  // namespace

bool reconcile_destination_available() noexcept {
    return true;
}

bool reconcile_destination_tls_available() noexcept {
#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
    return true;
#else
    return false;
#endif
}

std::unique_ptr<rc::Destination> make_reconcile_destination(ReconcileConnection connection,
                                                            rc::ReadBounds bounds) {
    if (connection.secure && !reconcile_destination_tls_available()) {
        throw std::runtime_error("this build's ClickHouse client has no TLS");
    }
    if (connection.database.empty() || connection.table.empty()) {
        throw std::runtime_error("a reconciliation needs the database and the table");
    }
    return std::make_unique<ClickHouseDestination>(std::move(connection), bounds);
}

std::string reconcile_settings(const rc::ReadBounds& bounds) {
    const auto ms = std::max<std::int64_t>(bounds.query_timeout.count(), 1);
    std::array<char, 32> secs{};
    std::snprintf(secs.data(),
                  secs.size(),
                  "%lld.%03lld",
                  static_cast<long long>(ms / 1000),
                  static_cast<long long>(ms % 1000));
    return "SETTINGS readonly = 2, max_execution_time = " + std::string{secs.data()} +
           ", timeout_overflow_mode = 'throw', max_result_rows = " +
           std::to_string(std::max<std::uint64_t>(bounds.max_rows_per_query, 1)) +
           ", result_overflow_mode = 'throw'";
}

std::string reconcile_value_expr(std::string_view quoted_column, const rc::ColumnType& t) {
    const std::string c = t.nullable ? "assumeNotNull(" + std::string{quoted_column} + ")"
                                     : std::string{quoted_column};
    switch (t.kind) {
        case rc::TypeKind::Int:
        case rc::TypeKind::UInt:
        case rc::TypeKind::Decimal:
            return "toString(" + c + ")";
        case rc::TypeKind::Float64:
            return "toString(reinterpretAsUInt64(" + c + "))";
        case rc::TypeKind::Float32:
            return "toString(reinterpretAsUInt32(" + c + "))";
        case rc::TypeKind::Bool:
            return "toString(toUInt8(" + c + "))";
        case rc::TypeKind::String:
            return "CAST(" + c + " AS String)";
        case rc::TypeKind::DateTime64:
            return "toString(reinterpretAsInt64(" + c + "))";
        case rc::TypeKind::DateTime:
            return "toString(toUnixTimestamp(" + c + "))";
        case rc::TypeKind::Date:
            return "toString(toInt32(" + c + "))";
        case rc::TypeKind::Unsupported:
            break;
    }
    throw std::invalid_argument("column type " + t.spelling + " is not read for a reconciliation");
}

std::string reconcile_literal(std::string_view cell, const rc::ColumnType& t) {
    if (cell.empty()) {
        throw std::invalid_argument("an empty key cell");
    }
    const std::string_view body = cell.substr(1);
    if ((t.kind == rc::TypeKind::Int || t.kind == rc::TypeKind::UInt) && cell.front() == 'I') {
        if (body.empty() || body.find_first_not_of("-0123456789") != std::string_view::npos) {
            throw std::invalid_argument("a key cell that is not an integer");
        }
        return std::string{body};
    }
    if (t.kind == rc::TypeKind::String && cell.front() == 'S') {
        return literal_bytes(body);
    }
    throw std::invalid_argument("a key of type " + t.spelling + " cannot be read by");
}

std::string reconcile_read_statement(std::string_view database,
                                     std::string_view table,
                                     const std::vector<rc::Column>& columns,
                                     const std::vector<std::size_t>& key,
                                     const std::vector<rc::Row>& keys,
                                     const rc::ReadBounds& bounds) {
    std::string sql = "SELECT ";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        const std::string q = quote_identifier(columns[i].name);
        sql += (i == 0 ? "" : ", ");
        sql += "CAST(toString(isNull(" + q + ")) AS String), " +
               reconcile_value_expr(q, columns[i].type);
    }
    sql += " FROM " + native::qualified_table(database, table) + " WHERE ";
    const bool composite = key.size() > 1;
    if (composite) {
        sql += "(";
    }
    for (std::size_t i = 0; i < key.size(); ++i) {
        sql += (i == 0 ? "" : ", ") + quote_identifier(columns.at(key[i]).name);
    }
    sql += composite ? ") IN (" : " IN (";
    for (std::size_t r = 0; r < keys.size(); ++r) {
        sql += (r == 0 ? "" : ", ");
        if (composite) {
            sql += "(";
        }
        for (std::size_t i = 0; i < key.size(); ++i) {
            sql += (i == 0 ? "" : ", ") + reconcile_literal(keys[r].at(i), columns.at(key[i]).type);
        }
        if (composite) {
            sql += ")";
        }
    }
    sql += ") " + reconcile_settings(bounds);
    return sql;
}

}  // namespace clink::clickhouse
