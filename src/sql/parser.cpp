#include "clink/sql/parser.hpp"

#include <chrono>
#include <pg_query.h>
#include <string>
#include <string_view>

#include "clink/metrics/sql_metrics.hpp"
#include "clink/sql/ast_builder.hpp"
#include "clink/sql/preparse.hpp"

namespace clink::sql {

namespace {

ast::Script parse_piece(std::string_view sql);

}  // namespace

// A script's statement sets are cut out first and each piece parsed on its
// own, in order; a statement inside a set is tagged with it. Positions in
// errors from a set's statements count from the start of the set's body.
ast::Script parse(std::string_view sql) {
    const auto pieces = preparse::split_statement_sets(sql);
    if (pieces.size() == 1 && pieces.front().set < 0) {
        return parse_piece(sql);
    }
    ast::Script out;
    bool any_set = false;
    for (const auto& piece : pieces) {
        ast::Script part;
        try {
            part = parse_piece(piece.text);
        } catch (const ParseError& e) {
            throw ParseError(e.what(), e.cursor_position() + static_cast<int>(piece.offset));
        }
        if (piece.set >= 0) {
            any_set = true;
        }
        for (auto& st : part.statements) {
            out.statements.push_back(std::move(st));
            out.statement_sets.push_back(piece.set);
        }
        out.allow_unbounded_state = out.allow_unbounded_state || part.allow_unbounded_state;
    }
    if (!any_set) {
        out.statement_sets.clear();
    }
    return out;
}

namespace {

ast::Script parse_piece(std::string_view sql) {
    const auto t0 = std::chrono::steady_clock::now();
    // #61: pre-parser shim rewrites PG-ungrammatical constructs (composite DDL
    // types today) to placeholders before libpg_query, then a reattach pass
    // restores them. A no-op for SQL without those constructs.
    preparse::PreparseResult pre = preparse::preparse(sql);
    // libpg_query owns the returned buffers via PgQueryParseResult;
    // free unconditionally before returning or throwing.
    std::string sql_copy(pre.rewritten_sql);
    PgQueryParseResult result = pg_query_parse(sql_copy.c_str());

    if (result.error != nullptr) {
        std::string msg =
            result.error->message != nullptr ? result.error->message : "unknown parse error";
        int cursor = result.error->cursorpos;
        pg_query_free_parse_result(result);
        clink::metrics::sql::parse_failed();
        throw ParseError(msg, cursor);
    }

    std::string_view pg_json;
    if (result.parse_tree != nullptr) {
        pg_json = result.parse_tree;
    }

    try {
        auto script = translate_to_ast(pg_json);
        preparse::reattach_composite_types(script, pre.composite_types);
        preparse::reattach_match_recognize(script, pre.match_recognize);
        preparse::reattach_process_table_functions(script, pre.table_functions);
        preparse::reattach_create_models(script, pre.models);
        preparse::reattach_ml_predicts(script, pre.ml_predicts);
        preparse::reattach_vector_searches(script, pre.vector_searches);
        // Carry the bounded-state opt-out through to the planner. Stripped
        // at text level, so this is the only route it has.
        script.allow_unbounded_state = pre.allow_unbounded_state;
        pg_query_free_parse_result(result);
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::sql::parse_completed(static_cast<std::uint64_t>(dt));
        return script;
    } catch (...) {
        pg_query_free_parse_result(result);
        clink::metrics::sql::parse_failed();
        throw;
    }
}

}  // namespace

}  // namespace clink::sql
