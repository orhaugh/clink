#pragma once

#include <memory>
#include <string>

#include "clink/sql/ast.hpp"

#include "arrow/api.h"

// SQL type to Arrow DataType bridge.
//
// PG normalizes the keyword spellings users write (BIGINT, INTEGER,
// TEXT, TIMESTAMP) to lowercase canonical names with a pg_catalog
// schema prefix (int8, int4, text, timestamp). We map those canonical
// names to Arrow's type system, which is also clink's type system on
// the wire and in storage.
//
// Phase 1 scope: integer family, float family, string family, bool,
// timestamp, date. Decimal (numeric) carries through but defaults to
// (38, 9) when precision/scale aren't supplied. Arrays / structs /
// maps land in a later phase.

namespace clink::sql {

// Throws TranslationError with the AST node's source position when
// the type name is outside the supported subset.
std::shared_ptr<arrow::DataType> sql_type_to_arrow(const ast::TypeName& type);

// For EXPLAIN / diagnostics: print a SQL-shaped string for an Arrow
// type (e.g. "BIGINT", "VARCHAR", "TIMESTAMP(3)"). Returns the Arrow
// type's own debug string for shapes that don't have a canonical SQL
// rendering yet.
std::string arrow_to_sql_type_string(const arrow::DataType& type);

// Parse a SQL type expression like "BIGINT" or "TIMESTAMP(3)" by
// synthesizing a single-column CREATE TABLE and pushing it through
// libpg_query. Throws TranslationError on invalid input. Used by the
// persistent catalog loader to recover Arrow types from the SQL-
// shaped strings produced by arrow_to_sql_type_string.
ast::TypeName parse_sql_type_expression(const std::string& expr);

}  // namespace clink::sql
