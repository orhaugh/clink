#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

// clink SQL AST: normalized, parser-agnostic syntax tree.
//
// Consumers (binder, planner, EXPLAIN) work against this layer.
// libpg_query's JSON parse tree never escapes src/sql/ast_builder.cpp -
// keeps the upstream dependency contained and lets us swap parsers
// later without disturbing higher layers.

namespace clink::sql::ast {

// 1-based byte offset into the original SQL string. 0 means
// "unknown" (libpg_query couldn't localize this node).
struct Loc {
    int pos = 0;
};

// --- Expressions ---------------------------------------------------

struct ColumnRef {
    std::vector<std::string> parts;  // ["t", "col"] for t.col; ["col"] for col
    bool is_star = false;            // SELECT *
    Loc loc;
};

struct IntLiteral {
    std::int64_t value = 0;
    Loc loc;
};

// Numeric literal with a fractional part (PG A_Const fval), e.g. 0.5.
// Integer literals stay IntLiteral; this is for non-integral constants.
// `text` keeps the raw PG decimal token so the binder can lower it to an
// EXACT decimal (#56); `value` is the lossy double for legacy callers.
struct FloatLiteral {
    double value = 0.0;
    std::string text;
    Loc loc;
};

struct StringLiteral {
    std::string value;
    Loc loc;
};

struct BoolLiteral {
    bool value = false;
    Loc loc;
};

struct NullLiteral {
    Loc loc;
};

// The recursive expression kinds (BinaryOp / LogicalOp / NotOp /
// IsNullOp). Recursive cases are wrapped in unique_ptr so
// the variant template only sees a forward declaration; bodies are
// defined below the Expression alias.

enum class BinOp { Eq, Ne, Lt, Le, Gt, Ge };
enum class LogicalKind { And, Or };
enum class ArithKind { Plus, Minus, Mul, Div, Mod, Concat, Neg };

struct BinaryOp;
struct LogicalOp;
struct NotOp;
struct IsNullOp;
struct ArithOp;
struct FunctionCall;
struct CastOp;
struct CaseExpr;
struct OverClause;
struct SortItem;
struct SubLink;
struct ArrayLiteral;
struct Subscript;
struct RowConstructor;
struct FieldAccess;
struct SelectStmt;

using Expression = std::variant<ColumnRef,
                                IntLiteral,
                                FloatLiteral,
                                StringLiteral,
                                BoolLiteral,
                                NullLiteral,
                                std::unique_ptr<BinaryOp>,
                                std::unique_ptr<LogicalOp>,
                                std::unique_ptr<NotOp>,
                                std::unique_ptr<IsNullOp>,
                                std::unique_ptr<ArithOp>,
                                std::unique_ptr<FunctionCall>,
                                std::unique_ptr<CastOp>,
                                std::unique_ptr<CaseExpr>,
                                std::unique_ptr<SubLink>,
                                std::unique_ptr<ArrayLiteral>,
                                std::unique_ptr<Subscript>,
                                std::unique_ptr<RowConstructor>,
                                std::unique_ptr<FieldAccess>>;

// Binary comparison: left <op> right. Both operands are Expressions
// so they can themselves recurse (constant folding etc.). The binder
// only allows {ColumnRef, Literal} leaves.
struct BinaryOp {
    BinOp op;
    Expression left;
    Expression right;
    Loc loc;
};

// AND / OR. Variadic in PG; we keep that since associative folding
// over arbitrary args is cheap and matches the PG parse tree.
struct LogicalOp {
    LogicalKind op;
    std::vector<Expression> args;
    Loc loc;
};

struct NotOp {
    Expression arg;
    Loc loc;
};

// IS NULL / IS NOT NULL.
struct IsNullOp {
    Expression arg;
    bool negated = false;  // true for IS NOT NULL
    Loc loc;
};

// Arithmetic + concat. Plus/Minus/Mul/Div/Mod take
// two args; Neg takes one; Concat is variadic in the AST (PG parses
// 'a' || 'b' || 'c' as left-associative pairs, but we keep the variant
// flexible for the binder to flatten).
struct ArithOp {
    ArithKind op;
    std::vector<Expression> args;
    Loc loc;
};

// Scalar function call: UPPER(x), LENGTH(s), COALESCE(a, b, c), etc.
// Name is lowercase canonical (PG normalizes the user's casing).
// Optional over_clause is set when PG emits a windowed
// FuncCall such as `ROW_NUMBER() OVER (PARTITION BY p ORDER BY o)`.
struct FunctionCall {
    std::string name;
    std::vector<Expression> args;
    std::unique_ptr<OverClause> over_clause;  // null for non-windowed calls
    bool agg_distinct = false;                // true for agg(DISTINCT ...)
    Loc loc;
};

// CASE WHEN <expr> THEN <expr> [WHEN ... THEN ...] [ELSE <expr>] END.
// Searched-CASE only (no simple-CASE; users rewrite
// `CASE x WHEN 1 ...` as `CASE WHEN x = 1 ...`). At least one WHEN
// branch is required; ELSE is optional and defaults to NULL.
struct CaseBranch {
    Expression when_expr;
    Expression then_expr;
    Loc loc;
};

struct CaseExpr {
    std::vector<CaseBranch> branches;
    std::optional<Expression> else_expr;
    Loc loc;
};

// A subquery appearing in a predicate or value position (Inc 4):
//   x IN (subquery)      -> Kind::In,    test_expr = x
//   x NOT IN (subquery)  -> Kind::NotIn, test_expr = x
//   EXISTS (subquery)    -> Kind::Exists
//   (subquery)           -> Kind::Scalar (a single-value expression,
//                           e.g. the RHS of `col > (SELECT avg(y)...)`)
// NOT EXISTS arrives as NotOp(SubLink{Exists}); the binder folds the
// negation. subselect is forward-declared (unique_ptr) so this stays
// definable before SelectStmt; the binder binds the subquery body.
struct SubLink {
    enum class Kind { In, NotIn, Exists, Scalar };
    Kind kind = Kind::In;
    std::optional<Expression> test_expr;  // single-column In / NotIn
    // Multi-column `(a, b) IN (SELECT x, y ...)`: the LHS RowExpr's elements.
    // When non-empty this is a composite IN and test_expr is unset.
    std::vector<Expression> test_exprs;
    std::unique_ptr<SelectStmt> subselect;
    Loc loc;
};

// ARRAY[e1, e2, ...] constructor (Wave 5). Elements are arbitrary
// expressions; the binder lowers this to a `make_array` value-op and
// infers the list element type from the first element (an empty
// ARRAY[] yields list<null>). SQL arrays may contain NULL elements.
struct ArrayLiteral {
    std::vector<Expression> elements;
    Loc loc;
};

// a[i] element access (Wave 5). SQL subscripts are 1-based; the binder
// lowers this to an `element_at` value-op which returns NULL for an
// out-of-range index (SQL-standard: no error). `base` is the array
// expression, `index` the 1-based subscript expression.
struct Subscript {
    Expression base;
    Expression index;
    Loc loc;
};

// ROW(e1, e2, ...) constructor (Wave 5c). PG parses explicit ROW(...) as
// a RowExpr with row_format=COERCE_EXPLICIT_CALL; the implicit (a, b)
// form (COERCE_IMPLICIT_CAST) is rejected outside IN-subqueries (it
// collides with multi-column IN paren-lists). The binder lowers this to
// a `make_row` value-op whose runtime value is a JSON object keyed by
// field name. Field names come from an explicit alias (`ROW(a AS x)`),
// else the field expression's column name, else a positional `fN`.
struct RowConstructor {
    std::vector<Expression> fields;
    std::vector<std::optional<std::string>> field_names;  // parallel to fields; nullopt = derive
    Loc loc;
};

// (r).f field access (Wave 5c). PG parses this as an A_Indirection whose
// indirection element is a String. The binder lowers it to a `field_at`
// value-op which returns NULL for a missing field or a non-row base
// (SQL-standard: no error), mirroring Subscript/element_at.
struct FieldAccess {
    Expression base;
    std::string field;
    Loc loc;
};

// CAST(expr AS type). PG also parses expr::type as the same node.
// The target type is stored as a canonical SQL type string ("BIGINT",
// "VARCHAR", "TIMESTAMP(3)") since the value-expression evaluator
// only cares about a handful of broad type families.
struct CastOp {
    Expression arg;
    std::string target_type;
    std::vector<int> typmods;  // {precision, scale} for CAST(.. AS DECIMAL(p,s))
    Loc loc;
};

// --- Statement components ------------------------------------------

struct SelectItem {
    Expression expr;
    std::optional<std::string> alias;  // SELECT expr AS alias
    Loc loc;
};

struct TableRef {
    std::string name;
    std::optional<std::string> schema;
    std::optional<std::string> alias;
    Loc loc;
};

// FROM-clause entry: either a table reference or a join over two
// sources. Captures the structure; the binder pattern-
// matches specific join shapes (interval join, equi-join) into
// LogicalIntervalJoin / LogicalJoin nodes.
struct JoinClause;
struct SubqueryItem;
struct MatchRecognizeClause;
struct ProcessTableFunctionClause;
struct SelectStmt;
using FromItem = std::variant<TableRef,
                              std::unique_ptr<JoinClause>,
                              std::unique_ptr<SubqueryItem>,
                              std::unique_ptr<MatchRecognizeClause>,
                              std::unique_ptr<ProcessTableFunctionClause>>;

enum class JoinKind { Inner, Left, Right, Full };

struct JoinClause {
    JoinKind kind = JoinKind::Inner;
    FromItem left;
    FromItem right;
    std::optional<Expression> on_clause;
    Loc loc;
};

// FROM (SELECT ...) AS sub. PG requires an alias on a
// subquery in FROM and our binder enforces it too. The body is
// pre-bound in the binder; this AST node just carries the textual
// structure.
struct SubqueryItem {
    std::unique_ptr<SelectStmt> body;
    std::string alias;
    Loc loc;
};

// MATCH_RECOGNIZE row-pattern matching over a source (#61). The
// pre-parser shim parses the (PG-ungrammatical) clause body STRUCTURALLY and
// stores the expression sub-fragments (DEFINE predicates, MEASURES exprs) as
// raw SQL text; the binder parses those via the normal expression path. v1
// subset: PARTITION BY columns, ORDER BY one event-time column, MEASURES,
// linear PATTERN with greedy + * ? {n} {n,m} quantifiers, DEFINE simple
// per-row predicates, ONE ROW PER MATCH, AFTER MATCH SKIP PAST LAST ROW.
struct PatternVar {
    std::string name;
    std::uint32_t min_count = 1;  // {1,1} = no quantifier
    std::uint32_t max_count = 1;
};
struct MrMeasure {
    std::string expr_sql;  // e.g. "LAST(a.price)"; a pattern-var ref is "var.col"
    std::string alias;
};
struct MrDefine {
    std::string var;
    std::string predicate_sql;  // e.g. "price > 100"
};
struct MatchRecognizeClause {
    TableRef input;                         // the source table
    std::vector<std::string> partition_by;  // column names
    std::string order_by;                   // single event-time column (v1)
    std::vector<MrMeasure> measures;
    std::vector<PatternVar> pattern;
    std::vector<MrDefine> define;
    std::optional<std::string> alias;  // FROM ... MATCH_RECOGNIZE (...) AS m
    Loc loc;
};

// Process-table-function call in FROM (SQLOPT PTF). The pre-parser shim parses
// the PG-ungrammatical `name(TABLE t PARTITION BY cols[, args])` clause
// STRUCTURALLY (libpg_query cannot grammar-parse the TABLE/PARTITION BY form)
// and stores it here; the binder resolves the input table, validates the
// partition columns, and reads the function's declared output columns from the
// PtfRegistry to build the synthetic derived table. v1: single TABLE input,
// PARTITION BY columns, no scalar args, no ORDER BY use (reserved).
struct ProcessTableFunctionClause {
    std::string fn_name;                    // registered PtfRegistry name
    TableRef input;                         // the TABLE(...) argument
    std::vector<std::string> partition_by;  // PARTITION BY column names
    std::string order_by;                   // reserved (parsed, unused in v1)
    std::vector<std::string> arg_sql;       // raw-text scalar args (reserved, v1 rejects non-empty)
    std::optional<std::string> alias;       // FROM my_ptf(...) AS p
    Loc loc;
};

// SQL type as the parser saw it. PG normalizes "BIGINT" to
// "pg_catalog.int8"; we keep both schema (may be empty) and name so
// the type-system bridge (task #72) can resolve either spelling.
struct TypeName {
    std::string schema;  // "pg_catalog" or empty
    std::string name;    // "int8", "timestamp", "text", ...; "map"/"row"/"multiset" composites
    std::vector<int> typmods;  // {3} for TIMESTAMP(3)
    int array_ndims = 0;       // 0 = scalar; N = `elem[]...[]` (N pairs), Wave 5
    // Composite-type structure, populated by the pre-parser shim (#61) for the
    // PG-ungrammatical MAP<k,v> / ROW<f t, ...> / MULTISET<t> spellings; empty
    // for scalar / array types. MAP: params = [key, value]. ROW: params =
    // field types, field_names parallel. MULTISET: params = [element].
    std::vector<TypeName> params;
    std::vector<std::string> field_names;
    Loc loc;
};

struct ColumnDef {
    std::string name;
    TypeName type;
    Loc loc;
};

// CREATE TABLE ... WITH (key='value', ...). String-typed
// values only; the connector configs we care about (connector,
// topic, bootstrap, path) are all strings.
struct StorageOption {
    std::string key;
    std::string value;
    Loc loc;
};

// --- Statements ----------------------------------------------------

struct CreateTableStmt {
    std::string table_name;
    std::optional<std::string> schema;
    std::vector<ColumnDef> columns;
    std::vector<StorageOption> options;
    bool if_not_exists = false;  // CREATE TABLE IF NOT EXISTS - skip when present
    Loc loc;
};

// One command in an ALTER TABLE statement. v1 supports column add / drop,
// ALTER COLUMN TYPE, and table SET / RESET (...) options; other AlterTableCmd
// subtypes (constraints, SET/DROP NOT NULL, DEFAULT) are rejected at parse time.
struct AlterTableCmd {
    enum class Kind { AddColumn, DropColumn, AlterColumnType, SetOptions, ResetOptions };
    Kind kind;
    std::string column_name;  // ADD: the new column; DROP / ALTER TYPE: the target column
    TypeName type;            // ADD COLUMN / ALTER COLUMN TYPE: the (new) column type
    bool missing_ok = false;  // ADD COLUMN IF NOT EXISTS / DROP COLUMN IF EXISTS
    // SET (k='v', ...) options (key + value) / RESET (k, ...) options (key only,
    // value empty). The table's WITH-option bag is mutated in place.
    std::vector<StorageOption> options;
    Loc loc;
};

// ALTER TABLE [IF EXISTS] <name> <cmd> [, <cmd> ...]. A streaming table is a
// catalog declaration over an external source/sink (no stored data to rewrite),
// so this mutates the catalog TableDef's columns. Applied to a base table only
// (ALTER VIEW / ALTER MATERIALIZED VIEW are rejected at parse time via objtype).
struct AlterTableStmt {
    std::string table_name;
    std::optional<std::string> schema;
    std::vector<AlterTableCmd> cmds;
    bool if_exists = false;  // ALTER TABLE IF EXISTS - silent when the table is absent
    Loc loc;
};

// ALTER TABLE <name> RENAME TO <new> | RENAME COLUMN <old> TO <new>. libpg_query
// parses both as a RenameStmt (renameType OBJECT_TABLE vs OBJECT_COLUMN). A
// catalog rename: the table key / column name in the TableDef changes. v1 covers
// base tables only (ALTER VIEW / ALTER MATERIALIZED VIEW RENAME are rejected).
struct RenameStmt {
    enum class Kind { Table, Column };
    Kind kind;
    std::string table_name;  // the table being altered
    std::optional<std::string> schema;
    std::string old_column;  // Column: the existing column name
    std::string new_name;    // Table: the new table name; Column: the new column name
    bool if_exists = false;  // ALTER TABLE IF EXISTS ... RENAME
    Loc loc;
};

// Set-op kind on a SelectStmt. None = normal SELECT.
// Otherwise the statement is (larg) <set-op> (rarg); target_list /
// from_* / where / group / having on the outer are unused. UnionAll
// keeps duplicates; UnionDistinct / Intersect / Except are set
// (duplicate-eliminating) operations. The multiset INTERSECT ALL /
// EXCEPT ALL forms are not modeled (rejected at parse time).
enum class SelectSetOp {
    None,
    UnionAll,
    UnionDistinct,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll
};

// One CommonTableExpression in a WITH clause. The binder
// pre-binds each ctequery to a LogicalPlan and registers it as a
// virtual table for the lifetime of the outer SELECT.
struct CommonTableExpr;
struct SelectStmt;

struct CommonTableExpr {
    std::string name;
    std::unique_ptr<SelectStmt> body;
    Loc loc;
};

// One entry in an ORDER BY clause. v1 only
// accepts a bare column reference; expressions in sort keys land
// in a follow-up.
struct SortItem {
    Expression expr;
    bool descending = false;
    Loc loc;
};

// The OVER (...) clause on a window function call.
// Carries PARTITION BY columns (any Expression for now; binder
// restricts to ColumnRef) and ORDER BY entries.
// OVER window frame (Wave 7). Running is the default
// (UNBOUNDED PRECEDING ... CURRENT ROW). Rows/Range are bounded
// `<n> PRECEDING ... CURRENT ROW` frames: Rows counts physical rows,
// Range bounds by the ORDER BY (event-time) value in milliseconds.
enum class FrameMode { Running, Rows, Range };

struct OverClause {
    std::vector<Expression> partition_by;
    std::vector<SortItem> order_by;
    FrameMode frame_mode = FrameMode::Running;
    // For Rows/Range: the `<n> PRECEDING` start bound (row count for Rows,
    // ms for Range). Unused (0) for Running.
    std::int64_t frame_start_preceding = 0;
    Loc loc;
};

struct SelectStmt {
    std::vector<SelectItem> target_list;
    std::vector<TableRef> from_clause;         // 0 (SELECT 1) or 1 table
    std::vector<FromItem> from_items;          // tables and joins
    std::optional<Expression> where_clause;    // optional WHERE predicate
    std::vector<Expression> group_clause;      // GROUP BY entries
    std::optional<Expression> having_clause;   // HAVING predicate
    bool distinct = false;                     // SELECT DISTINCT
    std::optional<std::int64_t> limit_count;   // LIMIT n
    std::optional<std::int64_t> offset_count;  // OFFSET n
    SelectSetOp set_op = SelectSetOp::None;    // UNION ALL
    std::unique_ptr<SelectStmt> larg;          // left branch when set_op != None
    std::unique_ptr<SelectStmt> rarg;          // right branch when set_op != None
    std::vector<CommonTableExpr> with_clause;  // WITH ctes
    std::vector<SortItem> sort_clause;         // ORDER BY
    Loc loc;
};

struct InsertStmt {
    TableRef target;
    SelectStmt select;
    // Optional explicit column list `INSERT INTO t (a, b)
    // SELECT ...`. When present, each name must be a column of `target`
    // and the SELECT projects in column-list order. Empty means the
    // legacy positional form: SELECT must project sink columns in
    // declaration order.
    std::vector<std::string> column_list;
    Loc loc;
};

// The object kind a DROP statement targets. Postgres uses a distinct DROP for
// each (DROP TABLE / DROP MATERIALIZED VIEW / DROP VIEW) and rejects a mismatch
// (DROP TABLE on a materialized view errors, and vice versa); execution enforces
// the same. View is parsed but currently rejected (CREATE VIEW is not supported).
enum class DropKind { Table, MaterializedView, View };

struct DropTableStmt {
    // One or more objects: DROP TABLE a, b, c. Dropped left to right.
    std::vector<std::string> table_names;
    bool if_exists = false;  // DROP ... IF EXISTS - silent when absent
    DropKind object_kind = DropKind::Table;
    Loc loc;
};

// SHOW TABLES - prints the catalog contents. Carries no payload;
// the CLI / binder handles execution against the current catalog.
struct ShowTablesStmt {
    Loc loc;
};

// ANALYZE [TABLE] <name> [(col, ...)] - scan a bounded table and compute its
// column statistics (row_count / NDV / histogram / MCV), writing them into the
// catalog so the optimizer's selectivity estimator uses them. libpg_query parses
// it as a VacuumStmt (the optional `TABLE` keyword is stripped by the
// pre-parser); `columns` empty means all columns. Driver/binder executes it.
struct AnalyzeStmt {
    std::string table;
    std::vector<std::string> columns;  // empty = all columns
    Loc loc;
};

// MATTBL: CREATE MATERIALIZED VIEW <name> WITH (freshness='...', connector=...,
// ...) AS <SELECT>. libpg_query parses this as a CreateTableAsStmt with
// objtype=OBJECT_MATVIEW; the WITH-options reuse StorageOption verbatim. The
// driver desugars it into a backing TableDef plus a continuous maintenance
// INSERT (see clink/sql/materialized_view.hpp). FRESHNESS is carried as a plain
// quoted-string WITH-option (freshness='0'), never a bespoke keyword.
struct CreateMaterializedViewStmt {
    std::string view_name;
    std::optional<std::string> schema;
    SelectStmt query;
    std::vector<StorageOption> options;
    Loc loc;
};

// CREATE [OR REPLACE] VIEW <name> AS <SELECT>. A logical (non-materialized)
// view: no backing table and no maintenance job - the catalog stores the
// defining query, and a reference to the view is expanded inline (re-bound as a
// sub-plan) at bind time. libpg_query parses it as a ViewStmt. v1 does not
// support a column-alias list `CREATE VIEW v (a, b) AS ...` (name the columns in
// the SELECT) or WITH CHECK OPTION.
struct CreateViewStmt {
    std::string view_name;
    std::optional<std::string> schema;
    SelectStmt query;
    bool or_replace = false;
    Loc loc;
};

struct ExplainStmt;

using Statement = std::variant<CreateTableStmt,
                               SelectStmt,
                               InsertStmt,
                               DropTableStmt,
                               ShowTablesStmt,
                               CreateMaterializedViewStmt,
                               CreateViewStmt,
                               AlterTableStmt,
                               RenameStmt,
                               AnalyzeStmt,
                               std::unique_ptr<ExplainStmt>>;

// EXPLAIN <query> - emits the bound LogicalPlan tree rather than
// executing. The inner statement is one of SelectStmt or InsertStmt;
// other shapes are rejected at bind time.
struct ExplainStmt {
    Statement query;
    Loc loc;
};

// A parsed input is a list of statements (PG allows semicolon-
// separated batches). The CLI typically expects exactly one but
// the AST captures whatever the parser saw.
struct Script {
    std::vector<Statement> statements;
};

}  // namespace clink::sql::ast
