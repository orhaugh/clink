#include "clink/sql/parser.hpp"

#include <chrono>
#include <pg_query.h>
#include <string>
#include <string_view>

#include "clink/metrics/sql_metrics.hpp"
#include "clink/sql/ast_builder.hpp"
#include "clink/sql/preparse.hpp"

namespace clink::sql {

ast::Script parse(std::string_view sql) {
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

}  // namespace clink::sql
