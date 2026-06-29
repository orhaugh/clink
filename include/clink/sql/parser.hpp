#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

#include "clink/sql/ast.hpp"

// clink SQL parser: thin C++ wrapper around vendored libpg_query.
//
// parse() returns a clink-owned ast::Script. libpg_query's raw JSON
// parse tree is consumed inside src/sql/ast_builder.cpp and never
// escapes - so an upstream upgrade or parser swap stays contained.

namespace clink::sql {

// Postgres-level syntax error (libpg_query rejected the input).
class ParseError : public std::runtime_error {
public:
    ParseError(const std::string& message, int cursor_position)
        : std::runtime_error(message), cursor_position_(cursor_position) {}

    [[nodiscard]] int cursor_position() const noexcept { return cursor_position_; }

private:
    int cursor_position_;
};

// Translation error: PG accepted the syntax but the construct is
// outside clink's supported subset (the subset is intentionally narrow;
// this class is how we surface a construct we do not yet handle).
// cursor_position is 1-based byte offset within the source, or 0 if
// libpg_query didn't localize the offending node.
class TranslationError : public std::runtime_error {
public:
    TranslationError(const std::string& message, int cursor_position)
        : std::runtime_error(message), cursor_position_(cursor_position) {}

    [[nodiscard]] int cursor_position() const noexcept { return cursor_position_; }

private:
    int cursor_position_;
};

// Parse a SQL string into a clink AST. Throws ParseError for syntax
// errors and TranslationError for syntactically-valid constructs
// outside the supported subset.
ast::Script parse(std::string_view sql);

}  // namespace clink::sql
