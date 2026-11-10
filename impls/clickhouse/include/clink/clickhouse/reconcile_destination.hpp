#pragma once

// ClickHouse as a destination for `clink incident-reconcile`
// (clink/incident/reconcile.hpp): read-only, bounded reads over the native
// protocol, on a connection the operator gives. Nothing it connects with or
// runs comes from a bundle: the bundle supplies only the database, table and
// column names the evidence recorded, which are quoted as identifiers, and the
// key values, which are rendered as literals from their canonical cells.
//
// Every statement is a SELECT carrying readonly = 2, under which the server
// refuses any write in it, with max_execution_time, max_result_rows and
// max_rows_to_read set to throw, so a read past a bound fails rather than
// returning part of its answer. Give it a user that can only read:
// SELECT on the table, system.tables, system.columns and system.mutations.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "clink/incident/reconcile.hpp"

namespace clink::clickhouse {

// Where a reconciliation reads: the operator's to give.
struct ReconcileConnection {
    std::string host{"localhost"};
    std::uint16_t port{9000};
    std::string user{"default"};
    std::string password;
    std::string database;
    std::string table;
    bool secure{false};
    std::string tls_ca_file;  // only this CA, when set
    bool tls_verify{true};
    std::chrono::milliseconds connect_timeout{std::chrono::seconds{10}};
};

// Whether this build can read ClickHouse for a reconciliation, and over TLS.
[[nodiscard]] bool reconcile_destination_available() noexcept;
[[nodiscard]] bool reconcile_destination_tls_available() noexcept;

// Throws std::runtime_error when the build cannot read ClickHouse, or TLS is
// asked for and the client has none. Connects on first use.
[[nodiscard]] std::unique_ptr<incident::reconcile::Destination> make_reconcile_destination(
    ReconcileConnection connection, incident::reconcile::ReadBounds bounds);

// The statements it sends, exposed for tests. A column's value expression
// selects, as String, what canon:: needs for its type; a row read selects, per
// column, isNull and that expression.
[[nodiscard]] std::string reconcile_value_expr(std::string_view quoted_column,
                                               const incident::reconcile::ColumnType& type);
[[nodiscard]] std::string reconcile_literal(std::string_view canonical_cell,
                                            const incident::reconcile::ColumnType& type);
[[nodiscard]] std::string reconcile_read_statement(
    std::string_view database,
    std::string_view table,
    const std::vector<incident::reconcile::Column>& columns,
    const std::vector<std::size_t>& key,
    const std::vector<incident::reconcile::Row>& keys,
    const incident::reconcile::ReadBounds& bounds);
[[nodiscard]] std::string reconcile_settings(const incident::reconcile::ReadBounds& bounds);

}  // namespace clink::clickhouse
