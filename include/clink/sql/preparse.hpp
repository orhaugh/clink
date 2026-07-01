#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "clink/sql/ast.hpp"

// #61: pre-parser shim. libpg_query (the PostgreSQL 16 grammar clink parses
// with) cannot parse a handful of streaming-SQL constructs - MAP<k,v> / ROW<..>
// / MULTISET<t> DDL column types and MATCH_RECOGNIZE. The shim runs over the
// raw SQL text BEFORE libpg_query, locates those constructs as balanced-bracket
// "islands" (skipping string literals, dollar-quotes, and comments so it never
// false-matches text inside them), replaces each with a PG-parseable
// placeholder, and records the parsed clink fragment. After libpg_query +
// ast_builder produce the AST, a reattach pass swaps the placeholders for the
// recorded fragments. libpg_query and ast_builder are otherwise untouched.

namespace clink::sql::preparse {

// Composite DDL types are replaced by a placeholder type name "__clink_ctype_N"
// (N = index into composite_types). The placeholder is a plain identifier that
// libpg_query accepts as an (unknown) type name; the reattach pass restores the
// real type.
inline constexpr const char* kCompositeTypePrefix = "__clink_ctype_";

// A `<table> MATCH_RECOGNIZE (...)` FROM-clause island is replaced by a
// placeholder table reference "__clink_mr_N" (N = index into match_recognize).
inline constexpr const char* kMatchRecognizePrefix = "__clink_mr_";

// A `name(TABLE t PARTITION BY ...)` process-table-function FROM-clause island
// is replaced by a placeholder table reference "__clink_ptf_N" (N = index into
// table_functions). libpg_query cannot grammar-parse the TABLE/PARTITION BY
// form, so the clause is recognised structurally in the shim.
inline constexpr const char* kProcessTableFunctionPrefix = "__clink_ptf_";

// SQL-native AI islands (libpg_query has no MODEL / DESCRIPTOR grammar):
//  - a whole `CREATE MODEL ...` statement is replaced by a placeholder
//    `CREATE TABLE __clink_model_N (...)` statement and the parsed CreateModelStmt
//    recorded (N = index into models); reattach swaps the whole Statement.
//  - an `ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(...))` FROM island becomes a
//    placeholder table ref "__clink_mlp_N" (N = index into ml_predicts).
//  - a `VECTOR_SEARCH(TABLE t, ...)` FROM island becomes "__clink_vs_N"
//    (N = index into vector_searches).
inline constexpr const char* kCreateModelPrefix = "__clink_model_";
inline constexpr const char* kMlPredictPrefix = "__clink_mlp_";
inline constexpr const char* kVectorSearchPrefix = "__clink_vs_";

struct PreparseResult {
    std::string rewritten_sql;                               // PG-parseable SQL, islands replaced
    std::vector<ast::TypeName> composite_types;              // indexed by the placeholder suffix
    std::vector<ast::MatchRecognizeClause> match_recognize;  // indexed by placeholder suffix
    std::vector<ast::ProcessTableFunctionClause> table_functions;  // indexed by placeholder suffix
    std::vector<ast::CreateModelStmt> models;                      // indexed by placeholder suffix
    std::vector<ast::MlPredictClause> ml_predicts;                 // indexed by placeholder suffix
    std::vector<ast::VectorSearchClause> vector_searches;          // indexed by placeholder suffix
};

// Scan + rewrite. Throws TranslationError on a malformed island (e.g. an
// unbalanced MAP<...> or a ROW field without a type).
PreparseResult preparse(std::string_view sql);

// Parse a composite type expression ("MAP<TEXT,BIGINT>", "ROW<a INT, b TEXT>",
// "MULTISET<INT>", "ARRAY<...>", with nesting) into an ast::TypeName. Scalar
// leaves delegate to parse_sql_type_expression so they get PG-canonical names.
// Exposed for unit testing.
ast::TypeName parse_composite_type(std::string_view type_expr);

// Post-parse reattach: replace any CREATE TABLE column whose type is a
// "__clink_ctype_N" placeholder with composite_types[N] (carrying through any
// trailing array dimensions, e.g. MAP<..>[]).
void reattach_composite_types(ast::Script& script,
                              const std::vector<ast::TypeName>& composite_types);

// Parse a MATCH_RECOGNIZE clause body (the text between the outer parens) plus
// its input table text into a structural ast::MatchRecognizeClause. Expression
// sub-fragments (DEFINE predicates, MEASURES exprs) are kept as raw SQL.
// Exposed for unit testing. Throws TranslationError on malformed input.
ast::MatchRecognizeClause parse_match_recognize(std::string_view input_table,
                                                std::string_view body);

// Post-parse reattach: replace any FROM-clause "__clink_mr_N" placeholder table
// reference with match_recognize[N].
void reattach_match_recognize(ast::Script& script,
                              std::vector<ast::MatchRecognizeClause>& match_recognize);

// Parse a process-table-function clause body (the text inside the parens, after
// the leading `name(`) plus the function name into a structural
// ast::ProcessTableFunctionClause. v1: `TABLE t [PARTITION BY cols] [ORDER BY
// col]`, no scalar args. Exposed for unit testing. Throws on malformed input.
ast::ProcessTableFunctionClause parse_process_table_function(std::string_view fn_name,
                                                             std::string_view body);

// Post-parse reattach: replace any FROM-clause "__clink_ptf_N" placeholder table
// reference with table_functions[N] (carrying the placeholder's alias).
void reattach_process_table_functions(
    ast::Script& script, std::vector<ast::ProcessTableFunctionClause>& table_functions);

// Parse a whole `CREATE MODEL ...` statement structurally into a CreateModelStmt.
// Exposed for unit testing. Throws TranslationError on malformed input.
ast::CreateModelStmt parse_create_model(std::string_view stmt_text);

// Parse an ML_PREDICT / VECTOR_SEARCH FROM-island body (the text inside the outer
// parens) into its structural clause. Exposed for unit testing. Throws on malformed
// input.
ast::MlPredictClause parse_ml_predict(std::string_view body);
ast::VectorSearchClause parse_vector_search(std::string_view body);

// Post-parse reattach: swap the placeholder CreateTableStmt for the recorded
// CreateModelStmt, and the "__clink_mlp_N" / "__clink_vs_N" placeholder table refs
// for the recorded ML_PREDICT / VECTOR_SEARCH clauses (carrying the alias).
void reattach_create_models(ast::Script& script, std::vector<ast::CreateModelStmt>& models);
void reattach_ml_predicts(ast::Script& script, std::vector<ast::MlPredictClause>& ml_predicts);
void reattach_vector_searches(ast::Script& script,
                              std::vector<ast::VectorSearchClause>& vector_searches);

}  // namespace clink::sql::preparse
