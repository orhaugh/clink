#pragma once

#include <string_view>

#include "clink/sql/ast.hpp"

// libpg_query JSON -> clink::sql::ast translation. Private surface;
// callers should reach the AST via clink::sql::parse() in parser.hpp.
// Exposed only so it can be unit-tested without re-running libpg_query.

namespace clink::sql {

ast::Script translate_to_ast(std::string_view pg_json);

}  // namespace clink::sql
