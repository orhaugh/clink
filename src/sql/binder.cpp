#include "clink/sql/binder.hpp"

#include <chrono>
#include <exception>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/metrics/sql_metrics.hpp"
#include "clink/operators/agg_function_registry.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/sql/async_function_registry.hpp"
#include "clink/sql/expr_lowering.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/ptf_registry.hpp"
#include "clink/sql/type.hpp"

namespace clink::sql {

namespace {

[[noreturn]] void bind_error(const std::string& msg, int pos) {
    throw TranslationError(msg, pos);
}

// Phase 28c-frontend: detect `SELECT <fn>(*) FROM <table>` where <fn>
// is a registered async lookup. Returns (function_name, source table
// ref) so bind_insert can lower it to Scan -> AsyncMap -> Sink. The
// args are intentionally ignored - the runtime passes the whole row to
// the registered Row -> async::Task<Row> coroutine; the `<fn>(*)`
// convention just signals "enrich this row". Only the simple
// single-table, no-clause shape is recognized; anything richer falls
// through to the ordinary projection binder.
std::optional<std::pair<std::string, const ast::TableRef*>> detect_async_lookup(
    const ast::SelectStmt& stmt) {
    if (stmt.target_list.size() != 1) {
        return std::nullopt;
    }
    if (stmt.where_clause.has_value() || !stmt.group_clause.empty() ||
        stmt.having_clause.has_value() || stmt.distinct || stmt.set_op != ast::SelectSetOp::None ||
        !stmt.with_clause.empty() || !stmt.sort_clause.empty() || stmt.limit_count.has_value()) {
        return std::nullopt;
    }
    // Single plain table; joins / derived tables live in from_items.
    if (stmt.from_clause.size() != 1) {
        return std::nullopt;
    }
    const auto& item = stmt.target_list[0];
    if (!std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(item.expr)) {
        return std::nullopt;
    }
    const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(item.expr);
    if (!AsyncFunctionRegistry::global().contains(fc.name)) {
        return std::nullopt;
    }
    return std::make_pair(fc.name, &stmt.from_clause[0]);
}

const TableDef& resolve_table(const Catalog& cat,
                              const ast::TableRef& ref,
                              const std::unordered_map<std::string, TableDef>& cte_overlay) {
    if (ref.schema.has_value()) {
        bind_error("schema-qualified table names not supported in Phase 1: " + *ref.schema + "." +
                       ref.name,
                   ref.loc.pos);
    }
    auto it = cte_overlay.find(ref.name);
    if (it != cte_overlay.end()) {
        return it->second;
    }
    const auto* def = cat.get_table(ref.name);
    if (def == nullptr) {
        bind_error("table not found: " + ref.name, ref.loc.pos);
    }
    return *def;
}

// Find a column by name in a TableDef. Returns the 0-based column
// index. Throws if the name doesn't match exactly one column.
int find_column(const TableDef& table, const std::string& name, int pos) {
    int found = -1;
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        if (table.columns[i].name == name) {
            if (found >= 0) {
                bind_error("column name is ambiguous in table " + table.name + ": " + name, pos);
            }
            found = static_cast<int>(i);
        }
    }
    if (found < 0) {
        bind_error("column not found in " + table.name + ": " + name, pos);
    }
    return found;
}

// --- Value-expression lowering (Phase 3.3) -------------------------

clink::config::JsonValue lower_value_expr(const ast::Expression& expr,
                                          const TableDef& source,
                                          const std::string& source_alias);
clink::config::JsonValue lower_predicate(const ast::Expression& expr, const TableDef& source);
// Defined below; lower_value_expr's Subscript case needs the base type to
// pick element_at (array) vs map_get (map) at bind time.
std::shared_ptr<arrow::DataType> infer_expr_type(const ast::Expression& expr,
                                                 const TableDef& source);

const char* arith_op_name(ast::ArithKind k) {
    switch (k) {
        case ast::ArithKind::Plus:
            return "add";
        case ast::ArithKind::Minus:
            return "sub";
        case ast::ArithKind::Mul:
            return "mul";
        case ast::ArithKind::Div:
            return "div";
        case ast::ArithKind::Mod:
            return "mod";
        case ast::ArithKind::Concat:
            return "concat";
        case ast::ArithKind::Neg:
            return "neg";
    }
    return "add";
}

std::string resolve_value_column_name(const ast::ColumnRef& ref,
                                      const TableDef& source,
                                      const std::string& source_alias) {
    if (ref.is_star || ref.parts.empty()) {
        bind_error("empty / star column reference in expression", ref.loc.pos);
    }
    std::string col_name;
    if (ref.parts.size() == 1) {
        col_name = ref.parts[0];
    } else if (ref.parts.size() == 2) {
        const std::string& qualifier = ref.parts[0];
        if (qualifier != source.name && qualifier != source_alias) {
            bind_error("unknown table qualifier: " + qualifier, ref.loc.pos);
        }
        col_name = ref.parts[1];
    } else {
        bind_error("schema-qualified column references not supported", ref.loc.pos);
    }
    (void)find_column(source, col_name, ref.loc.pos);
    return col_name;
}

// Non-throwing column lookup (find_column throws on a miss). Used to decide
// whether window_start / window_end name a real source column or the synthetic
// window bound a windowed GROUP BY projects.
bool source_has_column(const TableDef& source, const std::string& name) {
    for (const auto& c : source.columns) {
        if (c.name == name)
            return true;
    }
    return false;
}

// True when a bare ColumnRef is window_start / window_end AND the source has no
// real column of that name (so it must be the synthetic windowed-GROUP-BY bound).
bool is_synthetic_window_bound(const ast::ColumnRef& cr, const TableDef& source) {
    return cr.parts.size() == 1 && !cr.is_star &&
           (cr.parts[0] == "window_start" || cr.parts[0] == "window_end") &&
           !source_has_column(source, cr.parts[0]);
}

// Derive the name of the i-th field of a ROW(...) constructor: an
// explicit alias (`ROW(a AS x)`) wins; else the field's own column name
// (`ROW(amount)` -> "amount"); else a positional `fN` (1-based, matching
// PG's default RowExpr field naming). Named-field access only, so these
// names are the contract for `(r).f`.
std::string row_field_name(const ast::RowConstructor& rc, std::size_t i) {
    if (i < rc.field_names.size() && rc.field_names[i].has_value()) {
        return *rc.field_names[i];
    }
    const auto& f = rc.fields[i];
    if (std::holds_alternative<ast::ColumnRef>(f)) {
        const auto& ref = std::get<ast::ColumnRef>(f);
        if (!ref.is_star && !ref.parts.empty()) {
            return ref.parts.back();
        }
    }
    return "f" + std::to_string(i + 1);
}

clink::config::JsonValue lower_value_expr(const ast::Expression& expr,
                                          const TableDef& source,
                                          const std::string& source_alias) {
    using clink::config::JsonArray;
    using clink::config::JsonObject;
    using clink::config::JsonValue;

    if (std::holds_alternative<ast::ColumnRef>(expr)) {
        JsonObject obj;
        obj["col"] = JsonValue{
            resolve_value_column_name(std::get<ast::ColumnRef>(expr), source, source_alias)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<ast::IntLiteral>(expr)) {
        JsonObject obj;
        obj["lit"] = JsonValue{static_cast<std::int64_t>(std::get<ast::IntLiteral>(expr).value)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<ast::FloatLiteral>(expr)) {
        // #56: a fractional literal lowers to an EXACT sentinel-tagged
        // dec-string from the raw token, not the lossy double. The dec-string
        // (a 0x01-prefixed String) survives the JSON-IR serialize/parse.
        const auto& fl = std::get<ast::FloatLiteral>(expr);
        JsonObject obj;
        if (auto d = clink::config::dec_parse(fl.text)) {
            obj["lit"] = clink::config::make_dec_value(*d);
        } else {
            obj["lit"] = JsonValue{fl.value};  // unparseable token: fall back to double
        }
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<ast::StringLiteral>(expr)) {
        JsonObject obj;
        obj["lit"] = JsonValue{std::get<ast::StringLiteral>(expr).value};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<ast::BoolLiteral>(expr)) {
        JsonObject obj;
        obj["lit"] = JsonValue{std::get<ast::BoolLiteral>(expr).value};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<ast::NullLiteral>(expr)) {
        JsonObject obj;
        obj["lit"] = JsonValue{nullptr};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::ArithOp>>(expr)) {
        const auto& a = *std::get<std::unique_ptr<ast::ArithOp>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{arith_op_name(a.op)}};
        JsonArray args;
        for (const auto& sub : a.args) {
            args.emplace_back(lower_value_expr(sub, source, source_alias));
        }
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(expr)) {
        const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(expr);
        // Wall-clock functions are nondeterministic: clink keeps SQL
        // results reproducible (a job replays to the same output), so
        // NOW() and friends are rejected. Use the event-time column or
        // a windowing TVF for time-based logic. (CURRENT_TIMESTAMP is
        // already rejected earlier as a SQLValueFunction.)
        if (fc.name == "now" || fc.name == "current_timestamp" || fc.name == "localtimestamp" ||
            fc.name == "current_date" || fc.name == "current_time" || fc.name == "localtime") {
            bind_error(fc.name +
                           "() is not supported: clink keeps SQL deterministic; "
                           "use the event-time column or a windowing TVF",
                       0);
        }
        JsonObject obj;
        obj["op"] = JsonValue{fc.name};  // lowercase by ast_builder
        JsonArray args;
        for (const auto& sub : fc.args) {
            args.emplace_back(lower_value_expr(sub, source, source_alias));
        }
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::CastOp>>(expr)) {
        const auto& c = *std::get<std::unique_ptr<ast::CastOp>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{"cast_"} + c.target_type};
        if (c.target_type == "decimal") {
            // #56: thread precision/scale so cast_decimal can re-quantise.
            int precision = !c.typmods.empty() ? c.typmods[0] : 38;
            int scale = c.typmods.size() > 1 ? c.typmods[1] : 9;
            if (precision < 1 || precision > 38) {
                bind_error("DECIMAL precision must be in [1, 38]", c.loc.pos);
            }
            if (scale < 0 || scale > precision) {
                bind_error("DECIMAL scale must be in [0, precision]", c.loc.pos);
            }
            obj["precision"] = JsonValue{static_cast<std::int64_t>(precision)};
            obj["scale"] = JsonValue{static_cast<std::int64_t>(scale)};
        }
        JsonArray args;
        args.emplace_back(lower_value_expr(c.arg, source, source_alias));
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::ArrayLiteral>>(expr)) {
        // Wave 5: ARRAY[e1, e2, ...] -> {"op":"make_array","args":[...]}.
        const auto& al = *std::get<std::unique_ptr<ast::ArrayLiteral>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{"make_array"}};
        JsonArray args;
        for (const auto& el : al.elements) {
            args.emplace_back(lower_value_expr(el, source, source_alias));
        }
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::Subscript>>(expr)) {
        // Wave 5: a[i] -> {"op":"element_at","args":[base, index]} for an
        // array base; m['k'] -> {"op":"map_get",...} for a map base. The
        // op is chosen at BIND time from the base's inferred type so the
        // lowered JSON is unambiguous (a runtime-polymorphic element_at
        // would silently change meaning if a column's type evolved). A
        // non-array, non-map base keeps element_at (returns NULL).
        const auto& sub = *std::get<std::unique_ptr<ast::Subscript>>(expr);
        auto base_type = infer_expr_type(sub.base, source);
        const bool is_map = base_type && base_type->id() == arrow::Type::MAP;
        JsonObject obj;
        obj["op"] = JsonValue{std::string{is_map ? "map_get" : "element_at"}};
        JsonArray args;
        args.emplace_back(lower_value_expr(sub.base, source, source_alias));
        args.emplace_back(lower_value_expr(sub.index, source, source_alias));
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::RowConstructor>>(expr)) {
        // Wave 5c: ROW(...) -> {"op":"make_row","fields":[names],"args":[vals]}.
        // Every value expression rides under "args" (only bare name
        // strings live under "fields") so the projection-pushdown column
        // collector, which recurses into "args", sees every referenced
        // source column - a column used only inside ROW(...) is not pruned.
        const auto& rc = *std::get<std::unique_ptr<ast::RowConstructor>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{"make_row"}};
        JsonArray names;
        JsonArray args;
        for (std::size_t i = 0; i < rc.fields.size(); ++i) {
            names.emplace_back(JsonValue{row_field_name(rc, i)});
            args.emplace_back(lower_value_expr(rc.fields[i], source, source_alias));
        }
        obj["fields"] = JsonValue{std::move(names)};
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::FieldAccess>>(expr)) {
        // Wave 5c: (r).f -> {"op":"field_at","field":"f","args":[base]}.
        const auto& fa = *std::get<std::unique_ptr<ast::FieldAccess>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{"field_at"}};
        obj["field"] = JsonValue{fa.field};
        JsonArray args;
        args.emplace_back(lower_value_expr(fa.base, source, source_alias));
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::CaseExpr>>(expr)) {
        // Phase 12: lower searched-CASE to
        //   {"op":"case","branches":[{"when":<pred>,"then":<val>}],
        //    "else":<val>}.
        // when-predicates ride the lower_predicate JSON shape (so
        // existing three-valued logic applies); then-values + else
        // ride the value-expression shape.
        const auto& ce = *std::get<std::unique_ptr<ast::CaseExpr>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{"case"}};
        JsonArray branches;
        for (const auto& br : ce.branches) {
            JsonObject branch;
            branch["when"] = lower_predicate(br.when_expr, source);
            branch["then"] = lower_value_expr(br.then_expr, source, source_alias);
            branches.emplace_back(std::move(branch));
        }
        obj["branches"] = JsonValue{std::move(branches)};
        if (ce.else_expr.has_value()) {
            obj["else"] = lower_value_expr(*ce.else_expr, source, source_alias);
        }
        return JsonValue{std::move(obj)};
    }
    // Boolean expressions in SELECT clause: lower the predicate via
    // the predicate-shaped lower_predicate helper above so the runtime
    // can produce a bool value. Phase 3.5 will reconcile this with
    // three-valued semantics.
    bind_error("expression kind not supported in SELECT", 0);
}

// #56 decimal type helpers ---------------------------------------------------

bool is_decimal_type(const std::shared_ptr<arrow::DataType>& t) {
    return t && t->id() == arrow::Type::DECIMAL128;
}

std::shared_ptr<arrow::DataType> make_decimal(int precision, int scale) {
    precision = std::min(std::max(precision, 1), 38);
    scale = std::min(std::max(scale, 0), precision);
    return arrow::decimal128(precision, scale);
}

// Decimal (precision, scale) view of an operand: a decimal yields its own; an
// integer is decimal(19,0); anything else (float / string) yields none, which
// forces the arithmetic result back to the legacy double path.
std::optional<std::pair<int, int>> decimal_ps_of(const std::shared_ptr<arrow::DataType>& t) {
    if (is_decimal_type(t)) {
        const auto& d = static_cast<const arrow::Decimal128Type&>(*t);
        return std::pair<int, int>{d.precision(), d.scale()};
    }
    if (t && (t->id() == arrow::Type::INT64 || t->id() == arrow::Type::INT32 ||
              t->id() == arrow::Type::INT16 || t->id() == arrow::Type::INT8)) {
        return std::pair<int, int>{19, 0};
    }
    return std::nullopt;
}

// SQL-standard result decimal type of `op` over two operands, or nullptr when
// the expression is not a decimal context (no decimal operand, or a
// non-decimal-non-int operand -> demote to double).
std::shared_ptr<arrow::DataType> arith_decimal_type(ast::ArithKind op,
                                                    const std::shared_ptr<arrow::DataType>& lt,
                                                    const std::shared_ptr<arrow::DataType>& rt) {
    auto a = decimal_ps_of(lt);
    auto b = decimal_ps_of(rt);
    if (!a || !b) {
        return nullptr;  // a float / other operand -> double path
    }
    if (!is_decimal_type(lt) && !is_decimal_type(rt)) {
        return nullptr;  // both integer -> keep the existing int/float path
    }
    auto [p1, s1] = *a;
    auto [p2, s2] = *b;
    int p = 0;
    int s = 0;
    switch (op) {
        case ast::ArithKind::Plus:
        case ast::ArithKind::Minus:
            s = std::max(s1, s2);
            p = std::max(p1 - s1, p2 - s2) + s + 1;
            break;
        case ast::ArithKind::Mul:
            s = s1 + s2;
            p = p1 + p2 + 1;
            break;
        case ast::ArithKind::Div:
            s = std::max({s1, s2, 6});  // clink division-scale rule (documented)
            p = p1 + s2 + s;
            break;
        case ast::ArithKind::Mod:
            s = std::max(s1, s2);
            p = std::min(p1, p2);
            break;
        default:
            return nullptr;
    }
    return make_decimal(p, s);
}

// Decimal type of a fractional literal: scale = fractional digit count,
// precision = total digit count (both clamped to [1, 38]).
std::shared_ptr<arrow::DataType> decimal_literal_type(const std::string& text) {
    int digits = 0;
    int scale = 0;
    bool after_dot = false;
    for (char ch : text) {
        if (ch == '.') {
            after_dot = true;
        } else if (ch >= '0' && ch <= '9') {
            ++digits;
            if (after_dot) {
                ++scale;
            }
        }
    }
    return make_decimal(std::max(digits, scale), scale);
}

// Infer the Arrow type of an expression at bind time. Phase 3.3 keeps
// this coarse: column refs adopt the source column's type; arithmetic
// promotes to float64 (or decimal when an operand is decimal); string
// functions stay utf8; everything else falls back to utf8.
std::shared_ptr<arrow::DataType> infer_expr_type(const ast::Expression& expr,
                                                 const TableDef& source) {
    if (std::holds_alternative<ast::ColumnRef>(expr)) {
        const auto& ref = std::get<ast::ColumnRef>(expr);
        if (!ref.is_star && !ref.parts.empty()) {
            const std::string& col_name = ref.parts.back();
            for (const auto& c : source.columns) {
                if (c.name == col_name)
                    return c.type;
            }
        }
        return arrow::utf8();
    }
    if (std::holds_alternative<ast::IntLiteral>(expr))
        return arrow::int64();
    if (std::holds_alternative<ast::FloatLiteral>(expr))
        return decimal_literal_type(std::get<ast::FloatLiteral>(expr).text);
    if (std::holds_alternative<ast::StringLiteral>(expr))
        return arrow::utf8();
    if (std::holds_alternative<ast::BoolLiteral>(expr))
        return arrow::boolean();
    if (std::holds_alternative<std::unique_ptr<ast::ArithOp>>(expr)) {
        const auto& a = *std::get<std::unique_ptr<ast::ArithOp>>(expr);
        if (a.op == ast::ArithKind::Concat)
            return arrow::utf8();
        // #56: unary negation keeps a decimal operand's type.
        if (a.op == ast::ArithKind::Neg && a.args.size() == 1) {
            auto t = infer_expr_type(a.args[0], source);
            return is_decimal_type(t) ? t : arrow::float64();
        }
        if (a.args.size() == 2) {
            auto dt = arith_decimal_type(
                a.op, infer_expr_type(a.args[0], source), infer_expr_type(a.args[1], source));
            if (dt != nullptr)
                return dt;
        }
        return arrow::float64();
    }
    if (std::holds_alternative<std::unique_ptr<ast::CastOp>>(expr)) {
        const auto& c = *std::get<std::unique_ptr<ast::CastOp>>(expr);
        if (c.target_type == "int")
            return arrow::int64();
        if (c.target_type == "float")
            return arrow::float64();
        if (c.target_type == "bool")
            return arrow::boolean();
        if (c.target_type == "decimal") {
            int precision = !c.typmods.empty() ? c.typmods[0] : 38;
            int scale = c.typmods.size() > 1 ? c.typmods[1] : 9;
            return make_decimal(precision, scale);
        }
        return arrow::utf8();
    }
    if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(expr)) {
        const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(expr);
        if (fc.name == "length" || fc.name == "position" || fc.name == "char_length" ||
            fc.name == "character_length" || fc.name == "ascii")
            return arrow::int64();
        if (fc.name == "abs" || fc.name == "floor" || fc.name == "ceil" || fc.name == "ceiling" ||
            fc.name == "round" || fc.name == "sqrt" || fc.name == "exp" || fc.name == "ln" ||
            fc.name == "log10" || fc.name == "sign" || fc.name == "power" || fc.name == "pow" ||
            fc.name == "trunc" || fc.name == "mod") {
            return arrow::float64();
        }
        if (fc.name == "starts_with")
            return arrow::boolean();
        if (fc.name == "substring" || fc.name == "replace" || fc.name == "btrim" ||
            fc.name == "ltrim" || fc.name == "rtrim" || fc.name == "regexp_extract" ||
            fc.name == "split_index") {
            return arrow::utf8();
        }
        if ((fc.name == "nullif" || fc.name == "greatest" || fc.name == "least") &&
            !fc.args.empty()) {
            // Result type follows the first argument; binder does no
            // type unification across siblings in Phase 15.
            return infer_expr_type(fc.args[0], source);
        }
        // Date/time: EXTRACT / DATE_TRUNC / TO_TIMESTAMP return epoch-
        // millis or numeric fields (int64); DATE_FORMAT returns text.
        if (fc.name == "extract" || fc.name == "date_trunc" || fc.name == "to_timestamp")
            return arrow::int64();
        if (fc.name == "date_format")
            return arrow::utf8();
        // JSON: extraction / construction return text; JSON_EXISTS bool.
        if (fc.name == "json_value" || fc.name == "json_query" || fc.name == "json_object")
            return arrow::utf8();
        if (fc.name == "json_exists")
            return arrow::boolean();
        if (fc.name == "map") {
            // Wave 5b: MAP(k1,v1,...) -> arrow::map(key type, value type)
            // from the first pair; the type lets m['k'] dispatch to map_get
            // and peel the value type. An odd argument count is a malformed
            // map - reject at bind for a clean error (the runtime op also
            // guards, but a constant arg count is best caught here).
            if (fc.args.size() % 2 != 0) {
                bind_error("MAP(...) requires an even number of arguments (key/value pairs)",
                           fc.loc.pos);
            }
            if (fc.args.size() >= 2) {
                return arrow::map(infer_expr_type(fc.args[0], source),
                                  infer_expr_type(fc.args[1], source));
            }
            return arrow::map(arrow::utf8(), arrow::null());
        }
        // SQLOPT-3: a registered scalar UDF types as its declared return type,
        // so a UDF result feeds a typed sink correctly.
        if (auto rt = ScalarFunctionRegistry::global().return_type(fc.name); rt != nullptr) {
            return rt;
        }
        // upper/lower/concat/coalesce default to utf8.
        return arrow::utf8();
    }
    if (std::holds_alternative<std::unique_ptr<ast::ArrayLiteral>>(expr)) {
        // Wave 5: ARRAY[...] is a list whose element type follows the
        // first element. An empty ARRAY[] yields list<null>. We don't
        // unify element types across siblings (the row wire is JSON, so
        // heterogeneous arrays ride fine); the type is for bookkeeping.
        const auto& al = *std::get<std::unique_ptr<ast::ArrayLiteral>>(expr);
        if (al.elements.empty())
            return arrow::list(arrow::null());
        return arrow::list(infer_expr_type(al.elements[0], source));
    }
    if (std::holds_alternative<std::unique_ptr<ast::Subscript>>(expr)) {
        // Wave 5: a[i] yields the element type of the array. If the base
        // is a known list type, peel one level; otherwise fall back to
        // utf8 (the runtime returns NULL for non-array bases anyway).
        const auto& sub = *std::get<std::unique_ptr<ast::Subscript>>(expr);
        auto base_type = infer_expr_type(sub.base, source);
        if (base_type->id() == arrow::Type::LIST) {
            return std::static_pointer_cast<arrow::ListType>(base_type)->value_type();
        }
        if (base_type->id() == arrow::Type::MAP) {
            // m['k'] yields the map's value type (the runtime map_get op
            // returns NULL for an absent key).
            return std::static_pointer_cast<arrow::MapType>(base_type)->item_type();
        }
        return arrow::utf8();
    }
    if (std::holds_alternative<std::unique_ptr<ast::RowConstructor>>(expr)) {
        // Wave 5c: ROW(...) is a struct whose fields are named per
        // row_field_name and typed from each field expression. Used for
        // bind-time field-access typing and the sink-compat rejection.
        const auto& rc = *std::get<std::unique_ptr<ast::RowConstructor>>(expr);
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(rc.fields.size());
        for (std::size_t i = 0; i < rc.fields.size(); ++i) {
            fields.push_back(
                arrow::field(row_field_name(rc, i), infer_expr_type(rc.fields[i], source)));
        }
        return arrow::struct_(std::move(fields));
    }
    if (std::holds_alternative<std::unique_ptr<ast::FieldAccess>>(expr)) {
        // Wave 5c: (r).f yields the named field's type when the base is a
        // known struct; otherwise utf8 (the runtime returns NULL for a
        // non-row base or a missing field anyway).
        const auto& fa = *std::get<std::unique_ptr<ast::FieldAccess>>(expr);
        auto base_type = infer_expr_type(fa.base, source);
        if (base_type->id() == arrow::Type::STRUCT) {
            const auto& st = static_cast<const arrow::StructType&>(*base_type);
            auto f = st.GetFieldByName(fa.field);
            if (f) {
                return f->type();
            }
        }
        return arrow::utf8();
    }
    if (std::holds_alternative<std::unique_ptr<ast::CaseExpr>>(expr)) {
        // CASE returns the type of the first THEN branch. We don't
        // do full type unification here; the binder validates that
        // every other THEN (and ELSE, if present) has the same coarse
        // Arrow type, so the wire shape stays predictable.
        const auto& ce = *std::get<std::unique_ptr<ast::CaseExpr>>(expr);
        auto first = infer_expr_type(ce.branches[0].then_expr, source);
        for (std::size_t i = 1; i < ce.branches.size(); ++i) {
            auto other = infer_expr_type(ce.branches[i].then_expr, source);
            if (!other->Equals(*first)) {
                bind_error("CASE branches must have the same result type", ce.branches[i].loc.pos);
            }
        }
        if (ce.else_expr.has_value()) {
            auto else_t = infer_expr_type(*ce.else_expr, source);
            if (!else_t->Equals(*first)) {
                bind_error("CASE ELSE branch must have the same result type as the THEN branches",
                           ce.loc.pos);
            }
        }
        return first;
    }
    return arrow::utf8();
}

// Phase 17: wrap a just-bound plan in LogicalTopN when ORDER BY is
// present, else LogicalLimit if just LIMIT is set, else passthrough.
// ORDER BY without LIMIT is rejected here: a streaming engine has no
// "end of input" except at bounded sources, and an unbounded sort
// buffer is a footgun. Sort keys must be bare column refs against
// the plan's emitted schema (Phase 17 v1).
std::unique_ptr<LogicalPlan> wrap_top_n_or_limit(std::unique_ptr<LogicalPlan> plan,
                                                 const ast::SelectStmt& stmt) {
    if (!stmt.sort_clause.empty()) {
        if (!stmt.limit_count.has_value()) {
            bind_error("ORDER BY requires LIMIT in a streaming SELECT (unbounded sort buffer)",
                       stmt.loc.pos);
        }
        auto schema = plan->schema();
        std::vector<std::string> sort_columns;
        std::vector<bool> sort_descending;
        sort_columns.reserve(stmt.sort_clause.size());
        sort_descending.reserve(stmt.sort_clause.size());
        for (const auto& item : stmt.sort_clause) {
            if (!std::holds_alternative<ast::ColumnRef>(item.expr)) {
                bind_error("ORDER BY entry must be a column reference (Phase 17 v1)", item.loc.pos);
            }
            const auto& cr = std::get<ast::ColumnRef>(item.expr);
            if (cr.is_star || cr.parts.empty()) {
                bind_error("ORDER BY entry must be a bare column name", item.loc.pos);
            }
            const std::string& col = cr.parts.back();
            bool found = false;
            for (int i = 0; i < schema->num_fields(); ++i) {
                if (schema->field(i)->name() == col) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                bind_error("ORDER BY column '" + col + "' is not in the SELECT output",
                           item.loc.pos);
            }
            sort_columns.push_back(col);
            sort_descending.push_back(item.descending);
        }
        return std::make_unique<LogicalTopN>(std::move(plan),
                                             std::move(sort_columns),
                                             std::move(sort_descending),
                                             *stmt.limit_count,
                                             stmt.offset_count.value_or(0));
    }
    if (stmt.limit_count.has_value()) {
        return std::make_unique<LogicalLimit>(
            std::move(plan), *stmt.limit_count, stmt.offset_count.value_or(0));
    }
    if (stmt.offset_count.has_value()) {
        // OFFSET without LIMIT in a streaming SELECT has no defined
        // emit boundary; reject explicitly.
        bind_error("OFFSET requires LIMIT in a streaming SELECT", stmt.loc.pos);
    }
    return plan;
}

// Resolve a SELECT-item expression to a ProjectOutput. The expression
// itself can be a bare ColumnRef (Phase 1 path), SELECT * (expanded
// here), or a richer expression tree handled by lower_value_expr.
std::vector<ProjectOutput> resolve_select_items(const std::vector<ast::SelectItem>& items,
                                                const TableDef& source,
                                                const std::string& source_alias) {
    std::vector<ProjectOutput> out;
    for (const auto& item : items) {
        const auto& expr = item.expr;
        if (std::holds_alternative<ast::ColumnRef>(expr) &&
            std::get<ast::ColumnRef>(expr).is_star) {
            if (item.alias.has_value()) {
                bind_error("SELECT * cannot have an alias", item.loc.pos);
            }
            for (const auto& c : source.columns) {
                clink::config::JsonObject ref;
                ref["col"] = clink::config::JsonValue{c.name};
                out.push_back(ProjectOutput{
                    c.name, clink::config::JsonValue{std::move(ref)}.serialize(0), c.type});
            }
            continue;
        }
        auto json = lower_value_expr(expr, source, source_alias);
        std::string output_name;
        if (item.alias.has_value()) {
            output_name = *item.alias;
        } else if (std::holds_alternative<ast::ColumnRef>(expr)) {
            output_name = std::get<ast::ColumnRef>(expr).parts.back();
        } else {
            output_name = "_col" + std::to_string(out.size());
        }
        out.push_back(ProjectOutput{
            std::move(output_name), json.serialize(0), infer_expr_type(expr, source)});
    }
    return out;
}

// --- WHERE predicate -> JSON (Phase 3.1) ---------------------------

const char* bin_op_to_predicate_op(ast::BinOp op) {
    switch (op) {
        case ast::BinOp::Eq:
            return "eq";
        case ast::BinOp::Ne:
            return "ne";
        case ast::BinOp::Lt:
            return "lt";
        case ast::BinOp::Le:
            return "le";
        case ast::BinOp::Gt:
            return "gt";
        case ast::BinOp::Ge:
            return "ge";
    }
    return "eq";  // unreachable; switch is exhaustive
}

// Resolve a column-ref Expression to the column name in the source
// table. Throws on anything else (Phase 3.1 binary operands are
// limited to column refs and string literals).
std::string resolve_column_name(const ast::Expression& expr, const TableDef& source) {
    if (!std::holds_alternative<ast::ColumnRef>(expr)) {
        bind_error("WHERE predicate must compare a column reference (Phase 3.1)", 0);
    }
    const auto& ref = std::get<ast::ColumnRef>(expr);
    if (ref.is_star || ref.parts.empty()) {
        bind_error("WHERE predicate column reference is malformed", ref.loc.pos);
    }
    const std::string& col_name = ref.parts.back();
    int idx = -1;
    for (std::size_t i = 0; i < source.columns.size(); ++i) {
        if (source.columns[i].name == col_name) {
            idx = static_cast<int>(i);
            break;
        }
    }
    if (idx < 0) {
        bind_error("WHERE predicate references unknown column: " + col_name, ref.loc.pos);
    }
    return col_name;
}

clink::config::JsonValue literal_to_json(const ast::Expression& expr) {
    if (std::holds_alternative<ast::StringLiteral>(expr)) {
        return clink::config::JsonValue{std::get<ast::StringLiteral>(expr).value};
    }
    if (std::holds_alternative<ast::IntLiteral>(expr)) {
        return clink::config::JsonValue{
            static_cast<std::int64_t>(std::get<ast::IntLiteral>(expr).value)};
    }
    if (std::holds_alternative<ast::FloatLiteral>(expr)) {
        return clink::config::JsonValue{std::get<ast::FloatLiteral>(expr).value};
    }
    if (std::holds_alternative<ast::BoolLiteral>(expr)) {
        return clink::config::JsonValue{std::get<ast::BoolLiteral>(expr).value};
    }
    if (std::holds_alternative<ast::NullLiteral>(expr)) {
        return clink::config::JsonValue{nullptr};
    }
    bind_error("WHERE predicate RHS must be a literal (Phase 3.4)", 0);
}

// Walk the WHERE expression and produce a JSON predicate in the
// shape consumed by filter_string_predicate at runtime. See
// include/clink/operators/json_predicate.hpp for the format.
clink::config::JsonValue lower_predicate(const ast::Expression& expr, const TableDef& source) {
    using clink::config::JsonArray;
    using clink::config::JsonObject;
    using clink::config::JsonValue;

    if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(expr)) {
        const auto& bin = *std::get<std::unique_ptr<ast::BinaryOp>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{bin_op_to_predicate_op(bin.op)}};
        obj["col"] = JsonValue{resolve_column_name(bin.left, source)};
        // The RHS is a literal, or another column of the same row
        // (column-vs-column, e.g. a post-join residual a.x >= b.y).
        if (std::holds_alternative<ast::ColumnRef>(bin.right)) {
            obj["rhs_col"] = JsonValue{resolve_column_name(bin.right, source)};
        } else {
            obj["literal"] = literal_to_json(bin.right);
        }
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::LogicalOp>>(expr)) {
        const auto& logical = *std::get<std::unique_ptr<ast::LogicalOp>>(expr);
        JsonObject obj;
        obj["op"] =
            JsonValue{logical.op == ast::LogicalKind::And ? std::string{"and"} : std::string{"or"}};
        JsonArray args;
        args.reserve(logical.args.size());
        for (const auto& a : logical.args) {
            args.emplace_back(lower_predicate(a, source));
        }
        obj["args"] = JsonValue{std::move(args)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::NotOp>>(expr)) {
        const auto& n = *std::get<std::unique_ptr<ast::NotOp>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{std::string{"not"}};
        obj["arg"] = lower_predicate(n.arg, source);
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::IsNullOp>>(expr)) {
        const auto& n = *std::get<std::unique_ptr<ast::IsNullOp>>(expr);
        JsonObject obj;
        obj["op"] = JsonValue{n.negated ? std::string{"is_not_null"} : std::string{"is_null"}};
        obj["col"] = JsonValue{resolve_column_name(n.arg, source)};
        return JsonValue{std::move(obj)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(expr)) {
        const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(expr);
        if (fc.name == "like") {
            if (fc.args.size() != 2 || !std::holds_alternative<ast::StringLiteral>(fc.args[1])) {
                bind_error("LIKE: pattern must be a string literal", fc.loc.pos);
            }
            JsonObject obj;
            obj["op"] = JsonValue{std::string{"like"}};
            obj["col"] = JsonValue{resolve_column_name(fc.args[0], source)};
            obj["pattern"] = JsonValue{std::get<ast::StringLiteral>(fc.args[1]).value};
            return JsonValue{std::move(obj)};
        }
        if (fc.name == "in") {
            // Phase 19a: WHERE x IN (lit, lit, ...). First arg is the
            // column reference; the rest are literal values (subquery
            // forms come through as SubLink, which we reject elsewhere).
            if (fc.args.size() < 2) {
                bind_error("IN requires a column and at least one literal value", fc.loc.pos);
            }
            JsonObject obj;
            obj["op"] = JsonValue{std::string{"in"}};
            obj["col"] = JsonValue{resolve_column_name(fc.args[0], source)};
            JsonArray values;
            values.reserve(fc.args.size() - 1);
            for (std::size_t i = 1; i < fc.args.size(); ++i) {
                values.emplace_back(literal_to_json(fc.args[i]));
            }
            obj["values"] = JsonValue{std::move(values)};
            return JsonValue{std::move(obj)};
        }
    }
    bind_error("WHERE predicate kind not supported in Phase 3.1", 0);
}

// Inc 4: flatten a top-level AND into its conjuncts. A non-AND
// expression yields a single conjunct.
void flatten_and(const ast::Expression& e, std::vector<const ast::Expression*>& out) {
    if (std::holds_alternative<std::unique_ptr<ast::LogicalOp>>(e)) {
        const auto& lo = *std::get<std::unique_ptr<ast::LogicalOp>>(e);
        if (lo.op == ast::LogicalKind::And) {
            for (const auto& a : lo.args)
                flatten_and(a, out);
            return;
        }
    }
    out.push_back(&e);
}

// Inc 4: does this expression contain a SubLink anywhere the binder
// inspects (top-level, under NOT, or as a comparison operand)?
bool expr_contains_sublink(const ast::Expression& e) {
    if (std::holds_alternative<std::unique_ptr<ast::SubLink>>(e))
        return true;
    if (std::holds_alternative<std::unique_ptr<ast::NotOp>>(e))
        return expr_contains_sublink(std::get<std::unique_ptr<ast::NotOp>>(e)->arg);
    if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(e)) {
        const auto& b = *std::get<std::unique_ptr<ast::BinaryOp>>(e);
        return expr_contains_sublink(b.left) || expr_contains_sublink(b.right);
    }
    if (std::holds_alternative<std::unique_ptr<ast::LogicalOp>>(e)) {
        for (const auto& a : std::get<std::unique_ptr<ast::LogicalOp>>(e)->args) {
            if (expr_contains_sublink(a))
                return true;
        }
    }
    return false;
}

// #55: return the first top-level scalar SubLink that appears as a
// SELECT item (the whole item is `(SELECT <agg> ...)`), else nullptr.
// A SubLink nested inside a larger expression is not matched here; it
// falls through to lower_value_expr, which rejects it cleanly.
const ast::SubLink* target_list_scalar_sublink(const std::vector<ast::SelectItem>& items) {
    for (const auto& item : items) {
        if (std::holds_alternative<std::unique_ptr<ast::SubLink>>(item.expr)) {
            const auto* sl = std::get<std::unique_ptr<ast::SubLink>>(item.expr).get();
            if (sl->kind == ast::SubLink::Kind::Scalar)
                return sl;
        }
    }
    return nullptr;
}

// Inc 4: if e is a two-part qualified column ref `alias.col`, return
// (alias, col); else nullopt.
std::optional<std::pair<std::string, std::string>> two_part_col(const ast::Expression& e) {
    if (!std::holds_alternative<ast::ColumnRef>(e))
        return std::nullopt;
    const auto& cr = std::get<ast::ColumnRef>(e);
    if (cr.is_star || cr.parts.size() != 2)
        return std::nullopt;
    return std::make_pair(cr.parts[0], cr.parts[1]);
}

// Inc 4: AND-combine a list of predicate conjuncts into one JSON.
clink::config::JsonValue lower_predicate_conjuncts(
    const std::vector<const ast::Expression*>& conjuncts, const TableDef& source) {
    if (conjuncts.size() == 1)
        return lower_predicate(*conjuncts[0], source);
    clink::config::JsonObject obj;
    obj["op"] = clink::config::JsonValue{std::string{"and"}};
    clink::config::JsonArray args;
    for (const auto* c : conjuncts)
        args.emplace_back(lower_predicate(*c, source));
    obj["args"] = clink::config::JsonValue{std::move(args)};
    return clink::config::JsonValue{std::move(obj)};
}

void check_sink_compatibility(const TableDef& sink, const arrow::Schema& source_schema, int loc) {
    if (static_cast<int>(sink.columns.size()) != source_schema.num_fields()) {
        std::ostringstream msg;
        msg << "INSERT INTO " << sink.name << " expects " << sink.columns.size()
            << " columns, SELECT projects " << source_schema.num_fields();
        bind_error(msg.str(), loc);
    }
    for (int i = 0; i < source_schema.num_fields(); ++i) {
        const auto& src = source_schema.field(i)->type();
        const auto& dst = sink.columns[static_cast<std::size_t>(i)].type;
        // #61: a whole MAP/ROW/MULTISET value CAN now be written to a sink
        // column - the pre-parser shim makes MAP<k,v>/ROW<...>/MULTISET<t>
        // declarable as DDL column types. A composite source is assignable to a
        // composite sink of the SAME Arrow shape (the Equals check below
        // governs: struct field names + types, map key/item types, list
        // elements must match). A composite into a non-matching column falls to
        // the generic mismatch message, which renders both sides in SQL terms.
        // #56: implicit assignment coercions for DECIMAL. A DECIMAL source is
        // assignable to a DECIMAL sink column of ANY precision/scale (the value
        // keeps its own scale in v1; no sink-side re-quantise) or to a
        // DOUBLE/FLOAT column (lossy). An integer source is assignable to a
        // DECIMAL column. Everything else stays strict.
        auto is_dec = [](const arrow::DataType& t) { return t.id() == arrow::Type::DECIMAL128; };
        auto is_real = [](const arrow::DataType& t) {
            return t.id() == arrow::Type::DOUBLE || t.id() == arrow::Type::FLOAT;
        };
        auto is_integral = [](const arrow::DataType& t) {
            return t.id() == arrow::Type::INT64 || t.id() == arrow::Type::INT32 ||
                   t.id() == arrow::Type::INT16 || t.id() == arrow::Type::INT8;
        };
        const bool decimal_assignable = (is_dec(*src) && (is_dec(*dst) || is_real(*dst))) ||
                                        (is_dec(*dst) && (is_dec(*src) || is_integral(*src)));
        if (!src->Equals(*dst) && !decimal_assignable) {
            std::ostringstream msg;
            msg << "INSERT INTO " << sink.name << " column " << i << " ("
                << sink.columns[static_cast<std::size_t>(i)].name << ") expects "
                << arrow_to_sql_type_string(*dst) << ", SELECT projects "
                << arrow_to_sql_type_string(*src);
            bind_error(msg.str(), loc);
        }
    }
}

}  // namespace

// --- Phase 4: aggregate-aware SELECT binding -----------------------

namespace {

bool is_builtin_aggregate_name(const std::string& name) {
    return name == "sum" || name == "count" || name == "min" || name == "max" || name == "avg" ||
           name == "stddev" || name == "stddev_pop" || name == "stddev_samp" ||
           name == "variance" || name == "var_pop" || name == "var_samp" || name == "string_agg" ||
           name == "listagg" || name == "percentile" || name == "approx_percentile" ||
           name == "array_agg";
}

bool is_aggregate_fn_name(const std::string& name) {
    // Built-ins first, then a registered aggregate UDF (SQLOPT-3). Built-ins
    // take precedence so a UDAF can never shadow one; an unregistered name is
    // still not an aggregate and falls through to the scalar path unchanged.
    return is_builtin_aggregate_name(name) || AggFunctionRegistry::global().contains(name);
}

// A name that is an aggregate only by virtue of UDAF registration (not a
// built-in). Used to reject UDAFs in contexts the runtime cannot resolve them
// (the OVER window operator builds its specs without the registry seam).
bool is_registered_udaf_name(const std::string& name) {
    return !is_builtin_aggregate_name(name) && AggFunctionRegistry::global().contains(name);
}

// Decode a TUMBLE/HOP/SESSION argument like `INTERVAL '5' SECOND`
// (PG-parsed as TypeCast{A_Const, TypeName{pg_catalog.interval, typmod}})
// or a plain integer literal (treated as milliseconds).
std::int64_t interval_to_ms(const ast::Expression& expr) {
    if (std::holds_alternative<ast::IntLiteral>(expr)) {
        return std::get<ast::IntLiteral>(expr).value;
    }
    if (std::holds_alternative<std::unique_ptr<ast::CastOp>>(expr)) {
        // The cast lowers PG's INTERVAL TypeCast. The inner arg is a
        // StringLiteral carrying the numeric quantity; the cast's
        // target carries the unit (currently we encode all of int /
        // float / str — the interval-specific unit needs more parser
        // work). For Phase 4 we accept INTERVAL '<n>' [SECOND|MINUTE|
        // HOUR|MILLISECOND]; PG's typmod encoding is checked at the
        // ast_builder level for the unit. Until then, the inner
        // string is the numeric quantity in the user's chosen unit.
    }
    // TODO: distinguish SECOND/MINUTE/HOUR units. Phase 4 first cut
    // expects users to use raw milliseconds, e.g. TUMBLE(ts, 5000).
    return 0;
}

// Phase 4 first cut: only support TUMBLE(time_col, <int_ms>) and
// require the size to be a plain integer literal in milliseconds.
// INTERVAL syntax recognition lands once the unit-aware parsing is
// wired through CastOp.
WindowSpec decode_window_call(const ast::FunctionCall& fc,
                              const TableDef& source,
                              const std::string& source_alias) {
    WindowSpec spec;
    if (fc.name == "tumble") {
        spec.kind = WindowSpec::Kind::Tumble;
    } else if (fc.name == "hop") {
        spec.kind = WindowSpec::Kind::Hop;
    } else if (fc.name == "session") {
        spec.kind = WindowSpec::Kind::Session;
    } else if (fc.name == "cumulate") {
        spec.kind = WindowSpec::Kind::Cumulate;
    } else {
        bind_error(
            "only TUMBLE / HOP / SESSION / CUMULATE are valid window TVFs in GROUP BY: " + fc.name,
            fc.loc.pos);
    }

    const bool three_arg =
        spec.kind == WindowSpec::Kind::Hop || spec.kind == WindowSpec::Kind::Cumulate;
    auto required_args = three_arg ? 3u : 2u;
    if (fc.args.size() != required_args) {
        bind_error(fc.name + " takes " + std::to_string(required_args) + " args", fc.loc.pos);
    }
    // arg 0: event-time column. The remaining integer-literal args are
    // milliseconds: TUMBLE(ts, size); HOP(ts, size, slide);
    // SESSION(ts, gap); CUMULATE(ts, step, size).
    if (!std::holds_alternative<ast::ColumnRef>(fc.args[0])) {
        bind_error(fc.name + ": first argument must be the event-time column", fc.loc.pos);
    }
    spec.time_column =
        resolve_value_column_name(std::get<ast::ColumnRef>(fc.args[0]), source, source_alias);
    if (!std::holds_alternative<ast::IntLiteral>(fc.args[1])) {
        bind_error(fc.name + ": second argument must be an integer literal (milliseconds)",
                   fc.loc.pos);
    }
    auto arg1_ms = std::get<ast::IntLiteral>(fc.args[1]).value;
    if (spec.kind == WindowSpec::Kind::Session) {
        spec.gap_ms = arg1_ms;
    } else if (spec.kind == WindowSpec::Kind::Cumulate) {
        spec.step_ms = arg1_ms;  // CUMULATE(ts, step, size)
    } else {
        spec.size_ms = arg1_ms;
    }
    if (spec.kind == WindowSpec::Kind::Hop) {
        if (!std::holds_alternative<ast::IntLiteral>(fc.args[2])) {
            bind_error("HOP: slide argument must be an integer literal (milliseconds)", fc.loc.pos);
        }
        spec.slide_ms = std::get<ast::IntLiteral>(fc.args[2]).value;
    }
    if (spec.kind == WindowSpec::Kind::Cumulate) {
        if (!std::holds_alternative<ast::IntLiteral>(fc.args[2])) {
            bind_error("CUMULATE: size argument must be an integer literal (milliseconds)",
                       fc.loc.pos);
        }
        spec.size_ms = std::get<ast::IntLiteral>(fc.args[2]).value;
        if (spec.step_ms <= 0 || spec.size_ms <= 0) {
            bind_error("CUMULATE: step and size must be positive", fc.loc.pos);
        }
        if (spec.size_ms % spec.step_ms != 0) {
            bind_error("CUMULATE: size must be an integer multiple of step", fc.loc.pos);
        }
    }
    if (spec.size_ms < 0 || spec.slide_ms < 0 || spec.gap_ms < 0 || spec.step_ms < 0) {
        bind_error(fc.name + " window parameters must be non-negative", fc.loc.pos);
    }
    return spec;
}

// Inspect a SELECT-item Expression and find any aggregate function
// call. Returns the FunctionCall name + the input column name (empty
// for COUNT(*)). For Phase 4 we accept only the simplest shapes:
// aggregate-of-column-ref or COUNT(*).
struct AggregateExtraction {
    bool found = false;
    std::string fn;
    std::string input_column;
    bool distinct = false;
    std::string separator;
    double percentile = 0.0;  // PERCENTILE / APPROX_PERCENTILE fraction
};

AggregateExtraction extract_aggregate(const ast::Expression& expr,
                                      const TableDef& source,
                                      const std::string& source_alias) {
    AggregateExtraction out;
    if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(expr)) {
        const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(expr);
        if (is_aggregate_fn_name(fc.name)) {
            out.found = true;
            // LISTAGG is an alias for clink's string_agg.
            out.fn = (fc.name == "listagg") ? "string_agg" : fc.name;
            out.distinct = fc.agg_distinct;
            // DISTINCT is only meaningful for COUNT, STRING_AGG and
            // ARRAY_AGG in this cut; reject it elsewhere rather than
            // silently drop.
            if (out.distinct && out.fn != "count" && out.fn != "string_agg" &&
                out.fn != "array_agg") {
                bind_error("DISTINCT is only supported for COUNT, STRING_AGG and ARRAY_AGG",
                           fc.loc.pos);
            }
            if (fc.name == "count" && fc.args.empty()) {
                // COUNT(*) - PG sets agg_star and drops args. We see
                // no args; input_column stays empty as the sentinel.
                return out;
            }
            // STRING_AGG(col [, separator]): the optional second arg is
            // a string-literal separator (default ",").
            if (out.fn == "string_agg") {
                if (fc.args.empty() || fc.args.size() > 2) {
                    bind_error("STRING_AGG takes (expr) or (expr, separator)", fc.loc.pos);
                }
                out.separator = ",";
                if (fc.args.size() == 2) {
                    if (!std::holds_alternative<ast::StringLiteral>(fc.args[1])) {
                        bind_error("STRING_AGG separator must be a string literal", fc.loc.pos);
                    }
                    out.separator = std::get<ast::StringLiteral>(fc.args[1]).value;
                }
                if (!std::holds_alternative<ast::ColumnRef>(fc.args[0])) {
                    bind_error("STRING_AGG: first argument must be a column reference", fc.loc.pos);
                }
                out.input_column = resolve_value_column_name(
                    std::get<ast::ColumnRef>(fc.args[0]), source, source_alias);
                return out;
            }
            // PERCENTILE(expr, fraction) / APPROX_PERCENTILE(expr, fraction):
            // first arg is the value column, the second a constant fraction
            // in [0, 1].
            if (out.fn == "percentile" || out.fn == "approx_percentile") {
                if (fc.args.size() != 2) {
                    bind_error(out.fn + " takes (expr, fraction)", fc.loc.pos);
                }
                if (!std::holds_alternative<ast::ColumnRef>(fc.args[0])) {
                    bind_error(out.fn + ": first argument must be a column reference", fc.loc.pos);
                }
                double frac = 0.0;
                if (std::holds_alternative<ast::FloatLiteral>(fc.args[1])) {
                    frac = std::get<ast::FloatLiteral>(fc.args[1]).value;
                } else if (std::holds_alternative<ast::IntLiteral>(fc.args[1])) {
                    frac = static_cast<double>(std::get<ast::IntLiteral>(fc.args[1]).value);
                } else {
                    bind_error(out.fn + ": fraction must be a numeric literal", fc.loc.pos);
                }
                if (frac < 0.0 || frac > 1.0) {
                    bind_error(out.fn + ": fraction must be in [0, 1]", fc.loc.pos);
                }
                out.percentile = frac;
                out.input_column = resolve_value_column_name(
                    std::get<ast::ColumnRef>(fc.args[0]), source, source_alias);
                return out;
            }
            if (fc.args.size() != 1) {
                bind_error(fc.name + " takes exactly one argument", fc.loc.pos);
            }
            if (!std::holds_alternative<ast::ColumnRef>(fc.args[0])) {
                bind_error(fc.name + ": argument must be a column reference (Phase 4)", fc.loc.pos);
            }
            out.input_column = resolve_value_column_name(
                std::get<ast::ColumnRef>(fc.args[0]), source, source_alias);
            return out;
        }
    }
    return out;
}

// Phase 19c: deep-copy an Expression. Variant arms holding unique_ptr
// need allocation; literals and ColumnRef copy by value.
ast::Expression clone_expression(const ast::Expression& expr);

ast::Expression clone_expression(const ast::Expression& expr) {
    if (std::holds_alternative<ast::ColumnRef>(expr))
        return std::get<ast::ColumnRef>(expr);
    if (std::holds_alternative<ast::IntLiteral>(expr))
        return std::get<ast::IntLiteral>(expr);
    if (std::holds_alternative<ast::FloatLiteral>(expr))
        return std::get<ast::FloatLiteral>(expr);
    if (std::holds_alternative<ast::StringLiteral>(expr))
        return std::get<ast::StringLiteral>(expr);
    if (std::holds_alternative<ast::BoolLiteral>(expr))
        return std::get<ast::BoolLiteral>(expr);
    if (std::holds_alternative<ast::NullLiteral>(expr))
        return std::get<ast::NullLiteral>(expr);
    if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(expr)) {
        const auto& b = *std::get<std::unique_ptr<ast::BinaryOp>>(expr);
        auto nb = std::make_unique<ast::BinaryOp>();
        nb->op = b.op;
        nb->loc = b.loc;
        nb->left = clone_expression(b.left);
        nb->right = clone_expression(b.right);
        return ast::Expression{std::move(nb)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::LogicalOp>>(expr)) {
        const auto& l = *std::get<std::unique_ptr<ast::LogicalOp>>(expr);
        auto nl = std::make_unique<ast::LogicalOp>();
        nl->op = l.op;
        nl->loc = l.loc;
        for (const auto& a : l.args)
            nl->args.push_back(clone_expression(a));
        return ast::Expression{std::move(nl)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::NotOp>>(expr)) {
        const auto& n = *std::get<std::unique_ptr<ast::NotOp>>(expr);
        auto nn = std::make_unique<ast::NotOp>();
        nn->loc = n.loc;
        nn->arg = clone_expression(n.arg);
        return ast::Expression{std::move(nn)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::IsNullOp>>(expr)) {
        const auto& n = *std::get<std::unique_ptr<ast::IsNullOp>>(expr);
        auto nn = std::make_unique<ast::IsNullOp>();
        nn->loc = n.loc;
        nn->negated = n.negated;
        nn->arg = clone_expression(n.arg);
        return ast::Expression{std::move(nn)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::ArithOp>>(expr)) {
        const auto& a = *std::get<std::unique_ptr<ast::ArithOp>>(expr);
        auto na = std::make_unique<ast::ArithOp>();
        na->op = a.op;
        na->loc = a.loc;
        for (const auto& sub : a.args)
            na->args.push_back(clone_expression(sub));
        return ast::Expression{std::move(na)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(expr)) {
        const auto& f = *std::get<std::unique_ptr<ast::FunctionCall>>(expr);
        auto nf = std::make_unique<ast::FunctionCall>();
        nf->name = f.name;
        nf->loc = f.loc;
        for (const auto& sub : f.args)
            nf->args.push_back(clone_expression(sub));
        return ast::Expression{std::move(nf)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::CastOp>>(expr)) {
        const auto& c = *std::get<std::unique_ptr<ast::CastOp>>(expr);
        auto nc = std::make_unique<ast::CastOp>();
        nc->target_type = c.target_type;
        nc->loc = c.loc;
        nc->arg = clone_expression(c.arg);
        return ast::Expression{std::move(nc)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::CaseExpr>>(expr)) {
        const auto& ce = *std::get<std::unique_ptr<ast::CaseExpr>>(expr);
        auto nce = std::make_unique<ast::CaseExpr>();
        nce->loc = ce.loc;
        for (const auto& br : ce.branches) {
            ast::CaseBranch nb;
            nb.loc = br.loc;
            nb.when_expr = clone_expression(br.when_expr);
            nb.then_expr = clone_expression(br.then_expr);
            nce->branches.push_back(std::move(nb));
        }
        if (ce.else_expr.has_value())
            nce->else_expr = clone_expression(*ce.else_expr);
        return ast::Expression{std::move(nce)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::ArrayLiteral>>(expr)) {
        const auto& al = *std::get<std::unique_ptr<ast::ArrayLiteral>>(expr);
        auto na = std::make_unique<ast::ArrayLiteral>();
        na->loc = al.loc;
        for (const auto& el : al.elements)
            na->elements.push_back(clone_expression(el));
        return ast::Expression{std::move(na)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::Subscript>>(expr)) {
        const auto& sub = *std::get<std::unique_ptr<ast::Subscript>>(expr);
        auto ns = std::make_unique<ast::Subscript>();
        ns->loc = sub.loc;
        ns->base = clone_expression(sub.base);
        ns->index = clone_expression(sub.index);
        return ast::Expression{std::move(ns)};
    }
    bind_error("clone_expression: unsupported variant arm", 0);
}

// Phase 19c: rewrite aggregate FunctionCalls in a HAVING expression
// into ColumnRef(output_name) so lower_predicate can resolve them
// against the synthetic post-aggregate schema. Errors when an
// aggregate FunctionCall has no matching slot in `aggregates`.
ast::Expression rewrite_aggregates_for_having(const ast::Expression& expr,
                                              const std::vector<AggregateOutput>& aggregates,
                                              const TableDef& source,
                                              const std::string& source_alias) {
    auto rewrite = [&](const ast::Expression& sub) {
        return rewrite_aggregates_for_having(sub, aggregates, source, source_alias);
    };
    if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(expr)) {
        auto extracted = extract_aggregate(expr, source, source_alias);
        if (extracted.found) {
            for (const auto& a : aggregates) {
                if (a.agg_fn == extracted.fn && a.input_column == extracted.input_column) {
                    ast::ColumnRef cr;
                    cr.parts = {a.output_name};
                    cr.loc = std::get<std::unique_ptr<ast::FunctionCall>>(expr)->loc;
                    return ast::Expression{cr};
                }
            }
            bind_error(
                "HAVING references " + extracted.fn +
                    " but no matching aggregate appears in SELECT (use an alias to reference it)",
                std::get<std::unique_ptr<ast::FunctionCall>>(expr)->loc.pos);
        }
        // Non-aggregate FunctionCall: clone with args rewritten.
        const auto& f = *std::get<std::unique_ptr<ast::FunctionCall>>(expr);
        auto nf = std::make_unique<ast::FunctionCall>();
        nf->name = f.name;
        nf->loc = f.loc;
        for (const auto& sub : f.args)
            nf->args.push_back(rewrite(sub));
        return ast::Expression{std::move(nf)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(expr)) {
        const auto& b = *std::get<std::unique_ptr<ast::BinaryOp>>(expr);
        auto nb = std::make_unique<ast::BinaryOp>();
        nb->op = b.op;
        nb->loc = b.loc;
        nb->left = rewrite(b.left);
        nb->right = rewrite(b.right);
        return ast::Expression{std::move(nb)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::LogicalOp>>(expr)) {
        const auto& l = *std::get<std::unique_ptr<ast::LogicalOp>>(expr);
        auto nl = std::make_unique<ast::LogicalOp>();
        nl->op = l.op;
        nl->loc = l.loc;
        for (const auto& a : l.args)
            nl->args.push_back(rewrite(a));
        return ast::Expression{std::move(nl)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::NotOp>>(expr)) {
        const auto& n = *std::get<std::unique_ptr<ast::NotOp>>(expr);
        auto nn = std::make_unique<ast::NotOp>();
        nn->loc = n.loc;
        nn->arg = rewrite(n.arg);
        return ast::Expression{std::move(nn)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::IsNullOp>>(expr)) {
        const auto& n = *std::get<std::unique_ptr<ast::IsNullOp>>(expr);
        auto nn = std::make_unique<ast::IsNullOp>();
        nn->loc = n.loc;
        nn->negated = n.negated;
        nn->arg = rewrite(n.arg);
        return ast::Expression{std::move(nn)};
    }
    if (std::holds_alternative<std::unique_ptr<ast::ArithOp>>(expr)) {
        const auto& a = *std::get<std::unique_ptr<ast::ArithOp>>(expr);
        auto na = std::make_unique<ast::ArithOp>();
        na->op = a.op;
        na->loc = a.loc;
        for (const auto& sub : a.args)
            na->args.push_back(rewrite(sub));
        return ast::Expression{std::move(na)};
    }
    return clone_expression(expr);
}

std::shared_ptr<arrow::DataType> aggregate_output_type(const std::string& fn,
                                                       const TableDef& source,
                                                       const std::string& input_column) {
    if (fn == "count")
        return arrow::int64();
    if (fn == "string_agg")
        return arrow::utf8();
    if (fn == "avg" || fn == "stddev" || fn == "stddev_pop" || fn == "stddev_samp" ||
        fn == "variance" || fn == "var_pop" || fn == "var_samp" || fn == "percentile" ||
        fn == "approx_percentile")
        return arrow::float64();
    if (input_column.empty())
        return arrow::int64();
    for (const auto& c : source.columns) {
        if (c.name == input_column) {
            if (fn == "sum") {
                // SUM widens int -> int64, float -> float64; reuse the
                // source type otherwise.
                if (c.type->Equals(*arrow::int32()) || c.type->Equals(*arrow::int16())) {
                    return arrow::int64();
                }
                if (c.type->Equals(*arrow::float32()))
                    return arrow::float64();
                return c.type;
            }
            if (fn == "array_agg")
                return arrow::list(c.type);  // collects the input into a list
            return c.type;                   // min/max keep the input type
        }
    }
    // UDAF (SQLOPT-3): a registered aggregate UDF types as its declared return
    // type. Placed after the built-ins so a built-in name can never resolve here.
    if (auto rt = AggFunctionRegistry::global().return_type(fn); rt != nullptr) {
        return rt;
    }
    return arrow::utf8();
}

}  // namespace

// --- Phase 5: interval-join detection ------------------------------

namespace {

// Pulls (qualifier, column_name) out of a 2-part ColumnRef. Throws
// when the ref isn't of the form alias.col.
std::pair<std::string, std::string> qualified_column(const ast::Expression& expr) {
    if (!std::holds_alternative<ast::ColumnRef>(expr)) {
        bind_error("interval join condition expects qualified column refs (alias.col)", 0);
    }
    const auto& cr = std::get<ast::ColumnRef>(expr);
    if (cr.parts.size() != 2) {
        bind_error("interval join condition expects qualified column refs (alias.col)", cr.loc.pos);
    }
    return {cr.parts[0], cr.parts[1]};
}

// Match a 'right_alias.ts + N_ms' or 'right_alias.ts - N_ms' arithmetic
// expression. Returns the (alias, column, signed offset_ms).
struct OffsetMatch {
    std::string alias;
    std::string column;
    std::int64_t offset_ms;
};

std::optional<OffsetMatch> match_ts_plus_offset(const ast::Expression& expr) {
    // Plain column ref: alias.col with no offset.
    if (std::holds_alternative<ast::ColumnRef>(expr)) {
        auto qc = qualified_column(expr);
        return OffsetMatch{qc.first, qc.second, 0};
    }
    if (!std::holds_alternative<std::unique_ptr<ast::ArithOp>>(expr))
        return std::nullopt;
    const auto& a = *std::get<std::unique_ptr<ast::ArithOp>>(expr);
    if (a.args.size() != 2)
        return std::nullopt;
    // First arg must be a 2-part column ref; second arg must be an
    // IntLiteral (after INTERVAL has been folded).
    if (!std::holds_alternative<ast::ColumnRef>(a.args[0]) ||
        !std::holds_alternative<ast::IntLiteral>(a.args[1])) {
        return std::nullopt;
    }
    auto qc = qualified_column(a.args[0]);
    auto ms = std::get<ast::IntLiteral>(a.args[1]).value;
    if (a.op == ast::ArithKind::Plus)
        return OffsetMatch{qc.first, qc.second, ms};
    if (a.op == ast::ArithKind::Minus)
        return OffsetMatch{qc.first, qc.second, -ms};
    return std::nullopt;
}

struct IntervalJoinShape {
    std::string left_alias;
    std::string right_alias;
    std::string left_key;
    std::string right_key;
    std::string left_ts;
    std::string right_ts;
    std::int64_t lower_offset_ms;
    std::int64_t upper_offset_ms;
};

// Walk the join ON clause; the expected shape is
//   AND( eq(a.k, b.k),
//        between(a.ts, b.ts + low, b.ts + high) )
std::optional<IntervalJoinShape> match_interval_join(const ast::Expression& on,
                                                     const std::string& a_alias,
                                                     const std::string& b_alias) {
    if (!std::holds_alternative<std::unique_ptr<ast::LogicalOp>>(on))
        return std::nullopt;
    const auto& and_op = *std::get<std::unique_ptr<ast::LogicalOp>>(on);
    if (and_op.op != ast::LogicalKind::And || and_op.args.size() != 2)
        return std::nullopt;

    // Pick out the eq and the between in either order.
    const ast::Expression* eq = nullptr;
    const ast::Expression* between = nullptr;
    auto classify = [&](const ast::Expression& e) {
        if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(e) &&
            std::get<std::unique_ptr<ast::BinaryOp>>(e)->op == ast::BinOp::Eq) {
            eq = &e;
        } else if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(e) &&
                   std::get<std::unique_ptr<ast::FunctionCall>>(e)->name == "between") {
            between = &e;
        }
    };
    classify(and_op.args[0]);
    classify(and_op.args[1]);
    if (eq == nullptr || between == nullptr)
        return std::nullopt;

    // eq: a_alias.left_key = b_alias.right_key (sides may be reversed).
    const auto& eq_bin = *std::get<std::unique_ptr<ast::BinaryOp>>(*eq);
    auto left_qc = qualified_column(eq_bin.left);
    auto right_qc = qualified_column(eq_bin.right);
    std::string left_key, right_key;
    if (left_qc.first == a_alias && right_qc.first == b_alias) {
        left_key = left_qc.second;
        right_key = right_qc.second;
    } else if (left_qc.first == b_alias && right_qc.first == a_alias) {
        left_key = right_qc.second;
        right_key = left_qc.second;
    } else {
        return std::nullopt;
    }

    // between: a_alias.left_ts BETWEEN b_alias.right_ts + low AND b_alias.right_ts + high
    const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(*between);
    if (fc.args.size() != 3)
        return std::nullopt;
    auto arg_qc = qualified_column(fc.args[0]);
    if (arg_qc.first != a_alias)
        return std::nullopt;
    auto low = match_ts_plus_offset(fc.args[1]);
    auto high = match_ts_plus_offset(fc.args[2]);
    if (!low || !high)
        return std::nullopt;
    if (low->alias != b_alias || high->alias != b_alias)
        return std::nullopt;
    if (low->column != high->column)
        return std::nullopt;

    IntervalJoinShape s;
    s.left_alias = a_alias;
    s.right_alias = b_alias;
    s.left_key = std::move(left_key);
    s.right_key = std::move(right_key);
    s.left_ts = arg_qc.second;
    s.right_ts = low->column;
    s.lower_offset_ms = low->offset_ms;
    s.upper_offset_ms = high->offset_ms;
    return s;
}

// Phase 18: stream-stream equi-join. Recognises a single equality
// `a.k = b.k` with one column from each declared alias, in either
// order. Anything more complex (AND, OR, expressions on either
// side) returns nullopt so the dispatcher can fall through to the
// interval-join recogniser or fail with a clear message.
struct EquiJoinShape {
    std::string left_key;
    std::string right_key;
};

std::optional<EquiJoinShape> match_equi_join(const ast::Expression& on,
                                             const std::string& a_alias,
                                             const std::string& b_alias) {
    if (!std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(on))
        return std::nullopt;
    const auto& bin = *std::get<std::unique_ptr<ast::BinaryOp>>(on);
    if (bin.op != ast::BinOp::Eq)
        return std::nullopt;
    if (!std::holds_alternative<ast::ColumnRef>(bin.left) ||
        !std::holds_alternative<ast::ColumnRef>(bin.right)) {
        return std::nullopt;
    }
    auto left_qc = qualified_column(bin.left);
    auto right_qc = qualified_column(bin.right);
    if (left_qc.first == a_alias && right_qc.first == b_alias) {
        return EquiJoinShape{left_qc.second, right_qc.second};
    }
    if (left_qc.first == b_alias && right_qc.first == a_alias) {
        return EquiJoinShape{right_qc.second, left_qc.second};
    }
    // Same alias on both sides isn't a stream-stream join.
    return std::nullopt;
}

// Phase 22a: walk a bound LogicalPlan tree and decide whether it
// produces changelog records (rows tagged with __row_kind=delete
// at runtime). Today only LogicalTopNPerKey is a producer; this
// helper recurses through pass-through nodes so a Project wrapping
// a TopN still classifies as changelog. Multi-input nodes inherit
// from any child being changelog.
bool is_changelog_plan(const LogicalPlan& node) {
    if (node.kind() == "TopNPerKey") {
        return true;
    }
    for (const auto* in : node.inputs()) {
        if (in != nullptr && is_changelog_plan(*in))
            return true;
    }
    return false;
}

}  // namespace

// Make a plan node for `table_name`: if it matches a CTE in the
// current scope, consume that plan (each CTE is at-most-once);
// otherwise return a plain LogicalScan against the resolved table.
std::unique_ptr<LogicalPlan> Binder::make_table_plan(const std::string& table_name,
                                                     const TableDef& resolved,
                                                     int pos) const {
    auto it = cte_plans_.find(table_name);
    if (it != cte_plans_.end()) {
        if (it->second == nullptr) {
            bind_error("CTE '" + table_name + "' referenced more than once (not yet supported)",
                       pos);
        }
        return std::move(it->second);
    }
    if (resolved.is_lookup()) {
        bind_error("lookup table '" + resolved.name +
                       "' (connector='lookup') is a keyed enrichment source, not a stream; "
                       "use it only as the right side of a JOIN",
                   pos);
    }
    return std::make_unique<LogicalScan>(&resolved);
}

// Recursively bind a (possibly nested) FROM item into a BoundRel: a base table,
// or a nested INNER equi-join of two sub-relations. Builds a left-deep tree of
// binary LogicalEquiJoins. A base-table side contributes its raw scan columns
// and is prefixed by its alias at the parent join (left_alias/right_alias); a
// sub-join side contributes already-flat "<alias>_<col>" names and is passed
// through unprefixed (empty alias) so they are not double-prefixed. Each join's
// keys are resolved to the column name in the corresponding child's OUTPUT
// stream. The 2-base-table top-level join (incl. interval / lookup / outer)
// stays on the inline path in bind_select; this powers the nested case.
Binder::BoundRel Binder::bind_base_table_rel(const ast::TableRef& ref) const {
    const auto& def = resolve_table(catalog_, ref, cte_synth_tables_);
    if (def.is_lookup()) {
        bind_error("lookup table '" + def.name +
                       "' is not supported inside a multi-way join (v1); use it as the right "
                       "side of a single JOIN",
                   ref.loc.pos);
    }
    BoundRel r;
    r.plan = make_table_plan(ref.name, def, ref.loc.pos);
    r.is_base = true;
    r.alias = ref.alias.value_or(def.name);
    r.aliases.insert(r.alias);
    for (const auto& c : def.columns) {
        r.columns.push_back(c);  // a scan's output stream is the raw columns
        r.qual_to_stream[r.alias + "." + c.name] = c.name;
    }
    return r;
}

Binder::BoundRel Binder::bind_join_rel(const ast::FromItem& item) const {
    if (std::holds_alternative<ast::TableRef>(item)) {
        return bind_base_table_rel(std::get<ast::TableRef>(item));
    }
    if (std::holds_alternative<std::unique_ptr<ast::SubqueryItem>>(item)) {
        // A derived table (subquery, e.g. a windowed aggregate) as a join side.
        // Pre-bind its body and register it as a synthetic table exactly like a
        // single-FROM derived table, so make_table_plan returns the sub-plan and
        // the base-table path prefixes its columns as <alias>_<col> in the join
        // output - identical naming to a base-table side.
        const auto& sq = *std::get<std::unique_ptr<ast::SubqueryItem>>(item);
        if (sq.alias.empty()) {
            bind_error("derived table requires an alias", sq.loc.pos);
        }
        if (cte_synth_tables_.count(sq.alias) != 0) {
            bind_error("derived-table alias collides with an in-scope table or CTE: " + sq.alias,
                       sq.loc.pos);
        }
        // Reject an alias that collides with a base table: registering it in the
        // cte overlay would shadow the base table, so another join side that
        // references that base name would silently resolve to this derived table.
        if (catalog_.get_table(sq.alias) != nullptr) {
            bind_error("derived-table alias '" + sq.alias +
                           "' collides with a base table of the same name; rename the alias",
                       sq.loc.pos);
        }
        auto body_plan = bind_select(*sq.body);
        auto body_schema = body_plan->schema();
        TableDef synth;
        synth.name = sq.alias;
        synth.columns.reserve(static_cast<std::size_t>(body_schema->num_fields()));
        for (int i = 0; i < body_schema->num_fields(); ++i) {
            synth.columns.push_back(
                ColumnSpec{body_schema->field(i)->name(), body_schema->field(i)->type()});
        }
        cte_synth_tables_.emplace(sq.alias, std::move(synth));
        cte_plans_.emplace(sq.alias, std::move(body_plan));
        ast::TableRef tr;
        tr.name = sq.alias;
        tr.alias = sq.alias;
        tr.loc = sq.loc;
        return bind_base_table_rel(tr);
    }
    if (std::holds_alternative<std::unique_ptr<ast::JoinClause>>(item)) {
        const auto& jc = *std::get<std::unique_ptr<ast::JoinClause>>(item);
        if (jc.kind != ast::JoinKind::Inner) {
            bind_error("multi-way joins support INNER joins only (v1)", jc.loc.pos);
        }
        if (!jc.on_clause.has_value()) {
            bind_error("JOIN requires an ON clause", jc.loc.pos);
        }
        BoundRel left = bind_join_rel(jc.left);
        BoundRel right = bind_join_rel(jc.right);

        // ON must be a single equality between one column from each joined side.
        const auto& on = *jc.on_clause;
        const ast::BinaryOp* bin = nullptr;
        if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(on)) {
            bin = std::get<std::unique_ptr<ast::BinaryOp>>(on).get();
        }
        if (bin == nullptr || bin->op != ast::BinOp::Eq ||
            !std::holds_alternative<ast::ColumnRef>(bin->left) ||
            !std::holds_alternative<ast::ColumnRef>(bin->right)) {
            bind_error("multi-way JOIN ON must be 'left.col = right.col' (v1)", jc.loc.pos);
        }
        const std::pair<std::string, std::string> a = qualified_column(bin->left);
        const std::pair<std::string, std::string> b = qualified_column(bin->right);

        const auto* lref = &a;
        const auto* rref = &b;
        const bool ab = left.aliases.count(a.first) != 0 && right.aliases.count(b.first) != 0;
        const bool ba = left.aliases.count(b.first) != 0 && right.aliases.count(a.first) != 0;
        if (ab) {
            lref = &a;
            rref = &b;
        } else if (ba) {
            lref = &b;
            rref = &a;
        } else {
            bind_error(
                "multi-way JOIN ON must equate one qualified column from each joined side "
                "(e.g. a.k = b.k)",
                jc.loc.pos);
        }

        auto lit = left.qual_to_stream.find(lref->first + "." + lref->second);
        auto rit = right.qual_to_stream.find(rref->first + "." + rref->second);
        if (lit == left.qual_to_stream.end() || rit == right.qual_to_stream.end()) {
            bind_error("multi-way JOIN ON references a column not in scope", jc.loc.pos);
        }
        const std::string left_key = lit->second;
        const std::string right_key = rit->second;

        BoundRel out;
        out.is_base = false;
        out.aliases = left.aliases;
        out.aliases.insert(right.aliases.begin(), right.aliases.end());
        auto add_side = [&out](const BoundRel& side) {
            for (const auto& c : side.columns) {
                out.columns.push_back(side.is_base ? ColumnSpec{side.alias + "_" + c.name, c.type}
                                                   : c);
            }
            for (const auto& [q, s] : side.qual_to_stream) {
                out.qual_to_stream[q] = side.is_base ? (side.alias + "_" + s) : s;
            }
        };
        add_side(left);
        add_side(right);

        arrow::FieldVector fields;
        fields.reserve(out.columns.size());
        for (const auto& c : out.columns) {
            fields.push_back(arrow::field(c.name, c.type));
        }
        const std::string left_alias_param = left.is_base ? left.alias : std::string{};
        const std::string right_alias_param = right.is_base ? right.alias : std::string{};
        out.plan = std::make_unique<LogicalEquiJoin>(std::move(left.plan),
                                                     std::move(right.plan),
                                                     left_alias_param,
                                                     right_alias_param,
                                                     left_key,
                                                     right_key,
                                                     arrow::schema(std::move(fields)),
                                                     JoinType::Inner);
        return out;
    }
    bind_error("only base tables and nested INNER joins are supported inside a multi-way join (v1)",
               0);
}

namespace {

bool is_pattern_var(const std::vector<std::string>& vars, const std::string& name) {
    return std::find(vars.begin(), vars.end(), name) != vars.end();
}

// Rewrite `var.col` -> `col` for any column ref whose qualifier is a pattern
// variable (#61). A DEFINE predicate / MEASURES expr references the current
// event's columns; the pattern-var qualifier is a label, not a table alias, so
// the binder's column resolver (which only knows the source table) needs it
// stripped. v1 predicate shapes only (comparisons / and / or / not / arithmetic
// / function calls).
void strip_pattern_var_qualifiers(ast::Expression& e, const std::vector<std::string>& vars) {
    if (auto* cr = std::get_if<ast::ColumnRef>(&e)) {
        if (cr->parts.size() == 2 && is_pattern_var(vars, cr->parts[0])) {
            cr->parts = {cr->parts[1]};
        }
        return;
    }
    if (auto* b = std::get_if<std::unique_ptr<ast::BinaryOp>>(&e)) {
        strip_pattern_var_qualifiers((*b)->left, vars);
        strip_pattern_var_qualifiers((*b)->right, vars);
        return;
    }
    if (auto* l = std::get_if<std::unique_ptr<ast::LogicalOp>>(&e)) {
        for (auto& a : (*l)->args) {
            strip_pattern_var_qualifiers(a, vars);
        }
        return;
    }
    if (auto* n = std::get_if<std::unique_ptr<ast::NotOp>>(&e)) {
        strip_pattern_var_qualifiers((*n)->arg, vars);
        return;
    }
    if (auto* in = std::get_if<std::unique_ptr<ast::IsNullOp>>(&e)) {
        strip_pattern_var_qualifiers((*in)->arg, vars);
        return;
    }
    if (auto* a = std::get_if<std::unique_ptr<ast::ArithOp>>(&e)) {
        for (auto& x : (*a)->args) {
            strip_pattern_var_qualifiers(x, vars);
        }
        return;
    }
    if (auto* fc = std::get_if<std::unique_ptr<ast::FunctionCall>>(&e)) {
        for (auto& x : (*fc)->args) {
            strip_pattern_var_qualifiers(x, vars);
        }
        return;
    }
    if (auto* c = std::get_if<std::unique_ptr<ast::CastOp>>(&e)) {
        strip_pattern_var_qualifiers((*c)->arg, vars);
        return;
    }
    // literals and other leaves: nothing to rewrite.
}

// Parse a single SQL expression fragment ("price > 100", "LAST(a.price)") by
// wrapping it in a SELECT and extracting the projected expression.
ast::Expression parse_mr_expr(const std::string& expr_sql, int pos) {
    auto script = parse("SELECT " + expr_sql);
    if (script.statements.size() != 1 ||
        !std::holds_alternative<ast::SelectStmt>(script.statements[0])) {
        bind_error("MATCH_RECOGNIZE: could not parse expression: " + expr_sql, pos);
    }
    auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    if (sel.target_list.size() != 1) {
        bind_error("MATCH_RECOGNIZE: expected a single expression: " + expr_sql, pos);
    }
    return std::move(sel.target_list[0].expr);
}

}  // namespace

std::unique_ptr<LogicalPlan> Binder::bind_match_recognize(
    const ast::MatchRecognizeClause& mrc) const {
    const TableDef& source = resolve_table(catalog_, mrc.input, cte_synth_tables_);

    std::vector<std::string> vars;
    vars.reserve(mrc.pattern.size());
    for (const auto& pv : mrc.pattern) {
        vars.push_back(pv.name);
    }

    auto col_type_in = [&](const std::string& name) -> std::shared_ptr<arrow::DataType> {
        for (const auto& col : source.columns) {
            if (col.name == name) {
                return col.type;
            }
        }
        return nullptr;
    };

    for (const auto& pc : mrc.partition_by) {
        if (!col_type_in(pc)) {
            bind_error("MATCH_RECOGNIZE: PARTITION BY column not found: " + pc, mrc.loc.pos);
        }
    }
    if (!col_type_in(mrc.order_by)) {
        bind_error("MATCH_RECOGNIZE: ORDER BY column not found: " + mrc.order_by, mrc.loc.pos);
    }

    std::vector<MrPatternStep> steps;
    steps.reserve(mrc.pattern.size());
    for (const auto& pv : mrc.pattern) {
        steps.push_back(MrPatternStep{pv.name, pv.min_count, pv.max_count});
    }

    std::vector<MrDefineSpec> defines;
    for (const auto& d : mrc.define) {
        if (!is_pattern_var(vars, d.var)) {
            bind_error("MATCH_RECOGNIZE: DEFINE references unknown pattern variable: " + d.var,
                       mrc.loc.pos);
        }
        ast::Expression pred = parse_mr_expr(d.predicate_sql, mrc.loc.pos);
        strip_pattern_var_qualifiers(pred, vars);
        defines.push_back(MrDefineSpec{d.var, lower_predicate(pred, source).serialize(0)});
    }

    std::vector<MrMeasureSpec> measures;
    for (const auto& m : mrc.measures) {
        ast::Expression e = parse_mr_expr(m.expr_sql, mrc.loc.pos);
        if (!std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(e)) {
            bind_error("MATCH_RECOGNIZE: MEASURES v1 supports FIRST(var.col) / LAST(var.col): " +
                           m.expr_sql,
                       mrc.loc.pos);
        }
        const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(e);
        std::string fn = fc.name;  // PG lowercases
        if (fn == "first_value") {
            fn = "first";
        } else if (fn == "last_value") {
            fn = "last";
        }
        if (fn != "first" && fn != "last") {
            bind_error("MATCH_RECOGNIZE: MEASURES v1 supports only FIRST / LAST, got: " + fc.name,
                       mrc.loc.pos);
        }
        if (fc.args.size() != 1 || !std::holds_alternative<ast::ColumnRef>(fc.args[0])) {
            bind_error("MATCH_RECOGNIZE: MEASURES function expects one 'var.col' argument",
                       mrc.loc.pos);
        }
        const auto& cr = std::get<ast::ColumnRef>(fc.args[0]);
        if (cr.parts.size() != 2) {
            bind_error("MATCH_RECOGNIZE: MEASURES must qualify the column (var.col)", mrc.loc.pos);
        }
        const std::string& var = cr.parts[0];
        const std::string& column = cr.parts[1];
        if (!is_pattern_var(vars, var)) {
            bind_error("MATCH_RECOGNIZE: MEASURES references unknown pattern variable: " + var,
                       mrc.loc.pos);
        }
        auto ct = col_type_in(column);
        if (!ct) {
            bind_error("MATCH_RECOGNIZE: MEASURES column not found: " + column, mrc.loc.pos);
        }
        measures.push_back(MrMeasureSpec{m.alias, fn, var, column, ct});
    }

    // Output schema: the partition-key columns (ONE ROW PER MATCH) then the
    // measure columns, in declaration order.
    arrow::FieldVector fields;
    fields.reserve(mrc.partition_by.size() + measures.size());
    for (const auto& pc : mrc.partition_by) {
        fields.push_back(arrow::field(pc, col_type_in(pc)));
    }
    for (const auto& ms : measures) {
        fields.push_back(arrow::field(ms.output_name, ms.type));
    }
    auto out_schema = arrow::schema(fields);

    auto input = make_table_plan(mrc.input.name, source, mrc.loc.pos);
    return std::make_unique<LogicalMatchRecognize>(std::move(input),
                                                   mrc.partition_by,
                                                   mrc.order_by,
                                                   std::move(steps),
                                                   std::move(defines),
                                                   std::move(measures),
                                                   out_schema);
}

std::unique_ptr<LogicalPlan> Binder::bind_process_table_function(
    const ast::ProcessTableFunctionClause& ptf) const {
    const TableDef& source = resolve_table(catalog_, ptf.input, cte_synth_tables_);
    if (!ptf.arg_sql.empty()) {
        bind_error(
            "process table function '" + ptf.fn_name +
                "': scalar arguments are not supported in v1 (only TABLE t PARTITION BY ...)",
            ptf.loc.pos);
    }
    // ORDER BY is reserved syntax but not yet enforced: v1 delivers each key its
    // rows in source/shuffle arrival order, not sorted. Rejecting rather than
    // silently ignoring keeps the ordering contract honest (a future version can
    // buffer + sort per key under a watermark).
    if (!ptf.order_by.empty()) {
        bind_error("process table function '" + ptf.fn_name +
                       "': ORDER BY is not supported in v1 (rows arrive in source order; "
                       "use PARTITION BY only)",
                   ptf.loc.pos);
    }
    // Validate every PARTITION BY column against the input table.
    for (const auto& pc : ptf.partition_by) {
        bool found = false;
        for (const auto& col : source.columns) {
            if (col.name == pc) {
                found = true;
                break;
            }
        }
        if (!found) {
            bind_error("process table function '" + ptf.fn_name +
                           "': PARTITION BY column not found: " + pc,
                       ptf.loc.pos);
        }
    }
    // Output columns are declared at registration; the binder needs them here to
    // build the synthetic derived table the outer SELECT binds against.
    auto out_schema = PtfRegistry::global().output_schema(ptf.fn_name);
    if (!out_schema) {
        bind_error("process table function '" + ptf.fn_name +
                       "' is not registered in PtfRegistry::global()",
                   ptf.loc.pos);
    }
    auto input = make_table_plan(ptf.input.name, source, ptf.loc.pos);
    return std::make_unique<LogicalProcessTableFunction>(
        std::move(input), ptf.partition_by, ptf.fn_name, std::move(out_schema));
}

namespace {

// RAII guard that tears down a Binder's per-statement CTE scope on
// exit. The scope is set up by the outermost bind_select with a
// non-empty with_clause; nested bind_select calls (for the CTE
// bodies themselves) leave it alone.
struct CteScopeGuard {
    std::unordered_map<std::string, TableDef>* synth;
    std::unordered_map<std::string, std::unique_ptr<LogicalPlan>>* plans;
    bool owns;
    // CTE names already in scope when this guard was created. On exit we
    // erase only the names THIS scope added, leaving any enclosing scope's
    // CTEs intact - that is what makes a nested WITH (a CTE body that
    // itself has a WITH) safe. A flat clear() would wipe the parent
    // scope's CTEs when the inner WITH unwinds. (The plans map holds
    // move-only unique_ptrs, so we snapshot keys, not values.) For the
    // top-level / non-nested case the entry set is empty, so this is
    // identical to the historic clear().
    std::vector<std::string> entry_names;
    CteScopeGuard(std::unordered_map<std::string, TableDef>* s,
                  std::unordered_map<std::string, std::unique_ptr<LogicalPlan>>* p,
                  bool o)
        : synth(s), plans(p), owns(o) {
        if (owns) {
            entry_names.reserve(synth->size());
            for (const auto& [name, _] : *synth) {
                entry_names.push_back(name);
            }
        }
    }
    ~CteScopeGuard() {
        if (!owns) {
            return;
        }
        const std::unordered_set<std::string> keep(entry_names.begin(), entry_names.end());
        for (auto it = synth->begin(); it != synth->end();) {
            it = keep.contains(it->first) ? std::next(it) : synth->erase(it);
        }
        for (auto it = plans->begin(); it != plans->end();) {
            it = keep.contains(it->first) ? std::next(it) : plans->erase(it);
        }
    }
};

// RAII guard that observes SQL bind timing into the global metrics
// registry. Records bind_completed(duration_ns) on normal exit; if
// std::uncaught_exceptions() advanced during the guard's lifetime
// the exit is a throw and the guard records bind_failed instead.
// Counts include nested bind_select calls (CTE bodies); over-
// counting is acceptable for a "bind cost" load signal.
struct BindTimingGuard {
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
    int uncaught_at_entry = std::uncaught_exceptions();
    ~BindTimingGuard() {
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        if (std::uncaught_exceptions() > uncaught_at_entry) {
            clink::metrics::sql::bind_failed();
        } else {
            clink::metrics::sql::bind_completed(static_cast<std::uint64_t>(dt));
        }
    }
};

}  // namespace

std::unique_ptr<LogicalPlan> Binder::bind_over_aggregate(
    const ast::SelectStmt& stmt,
    const TableDef& source,
    const std::string& alias,
    const ast::TableRef& ref,
    const std::vector<std::size_t>& window_targets) const {
    // Shape: `SELECT *, agg() OVER (...) [, ...]`. Non-windowed items
    // must be `*`; GROUP BY / HAVING / DISTINCT / ORDER BY / LIMIT do
    // not combine with OVER aggregates in this phase.
    if (!stmt.group_clause.empty() || stmt.having_clause.has_value()) {
        bind_error("OVER aggregates are incompatible with GROUP BY / HAVING", stmt.loc.pos);
    }
    if (stmt.distinct || !stmt.sort_clause.empty() || stmt.limit_count.has_value() ||
        stmt.offset_count.has_value()) {
        bind_error(
            "OVER aggregates must appear in a `SELECT *, ...` shape (no DISTINCT / "
            "ORDER BY / LIMIT on this level)",
            stmt.loc.pos);
    }
    std::vector<bool> is_window(stmt.target_list.size(), false);
    for (auto idx : window_targets)
        is_window[idx] = true;
    bool saw_star = false;
    for (std::size_t i = 0; i < stmt.target_list.size(); ++i) {
        if (is_window[i])
            continue;
        const auto& item = stmt.target_list[i];
        if (std::holds_alternative<ast::ColumnRef>(item.expr) &&
            std::get<ast::ColumnRef>(item.expr).is_star) {
            saw_star = true;
            continue;
        }
        bind_error("SELECT items beyond OVER aggregates must be `SELECT *`", item.loc.pos);
    }
    if (!saw_star) {
        bind_error("OVER aggregates require a `SELECT *, agg() OVER (...)` shape", stmt.loc.pos);
    }

    // The shared OVER spec, taken from the first windowed target.
    const auto& first =
        *std::get<std::unique_ptr<ast::FunctionCall>>(stmt.target_list[window_targets[0]].expr);
    const auto& over0 = *first.over_clause;
    if (over0.order_by.size() != 1) {
        bind_error("OVER aggregate requires ORDER BY on a single event-time column", first.loc.pos);
    }
    if (over0.order_by[0].descending) {
        bind_error("OVER aggregate ORDER BY must be ascending (event-time order)", first.loc.pos);
    }
    if (!std::holds_alternative<ast::ColumnRef>(over0.order_by[0].expr)) {
        bind_error("OVER aggregate ORDER BY must be a column reference", first.loc.pos);
    }
    const std::string order_col =
        resolve_value_column_name(std::get<ast::ColumnRef>(over0.order_by[0].expr), source, alias);
    // The order column must be the source's event-time column so the
    // operator's per-row frame closes exactly when the watermark passes.
    auto etc = source.properties.find("event_time_column");
    if (etc == source.properties.end() || etc->second.empty()) {
        bind_error("OVER aggregate requires the source table to declare event_time_column",
                   first.loc.pos);
    }
    if (etc->second != order_col) {
        bind_error("OVER aggregate ORDER BY column must be the source event_time_column ('" +
                       etc->second + "')",
                   first.loc.pos);
    }
    std::vector<std::string> partition_cols;
    for (const auto& e : over0.partition_by) {
        if (!std::holds_alternative<ast::ColumnRef>(e)) {
            bind_error("PARTITION BY entry must be a column reference", first.loc.pos);
        }
        partition_cols.push_back(
            resolve_value_column_name(std::get<ast::ColumnRef>(e), source, alias));
    }

    // True when two OVER clauses resolve to the same partition + order.
    auto same_spec = [&](const ast::OverClause& a, const ast::OverClause& b) -> bool {
        if (a.partition_by.size() != b.partition_by.size() ||
            a.order_by.size() != b.order_by.size()) {
            return false;
        }
        auto col_name = [&](const ast::Expression& e) -> std::string {
            return std::holds_alternative<ast::ColumnRef>(e)
                       ? resolve_value_column_name(std::get<ast::ColumnRef>(e), source, alias)
                       : std::string{"\x01"};  // never matches a real column
        };
        for (std::size_t i = 0; i < a.partition_by.size(); ++i) {
            if (col_name(a.partition_by[i]) != col_name(b.partition_by[i]))
                return false;
        }
        for (std::size_t i = 0; i < a.order_by.size(); ++i) {
            if (a.order_by[i].descending != b.order_by[i].descending ||
                col_name(a.order_by[i].expr) != col_name(b.order_by[i].expr)) {
                return false;
            }
        }
        return true;
    };

    auto column_type = [&](const std::string& col) -> std::shared_ptr<arrow::DataType> {
        for (const auto& c : source.columns) {
            if (c.name == col)
                return c.type;
        }
        return arrow::utf8();
    };

    std::vector<OverOutput> outputs;
    for (auto idx : window_targets) {
        const auto& item = stmt.target_list[idx];
        const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(item.expr);
        if (!same_spec(*fc.over_clause, over0)) {
            bind_error(
                "all OVER aggregates in a SELECT must share one PARTITION BY / ORDER BY "
                "window",
                fc.loc.pos);
        }
        if (fc.name == "lead" || fc.name == "ntile") {
            bind_error(fc.name +
                           " over an unbounded stream is not supported (needs look-ahead or the "
                           "full partition)",
                       fc.loc.pos);
        }
        if (fc.agg_distinct) {
            bind_error("DISTINCT is not supported inside an OVER aggregate", fc.loc.pos);
        }
        const bool is_agg = is_aggregate_fn_name(fc.name);
        const bool is_nav = fc.name == "first_value" || fc.name == "last_value" || fc.name == "lag";
        if (!is_agg && !is_nav) {
            bind_error("unsupported window function in OVER: " + fc.name, fc.loc.pos);
        }
        // SQLOPT-3: the OVER window operator builds its aggregate specs without
        // the registry seam (no decode_agg_extras) and only knows built-in
        // running aggregates, so a UDAF in OVER would bind but silently emit a
        // NULL column. Reject it loudly instead. (Use it in GROUP BY or a
        // TUMBLE/HOP/CUMULATE/SESSION window, which do resolve UDAFs.)
        if (is_registered_udaf_name(fc.name)) {
            bind_error("UDAF '" + fc.name +
                           "' is not supported in an OVER window; use it in GROUP BY or a "
                           "TUMBLE/HOP/CUMULATE/SESSION window",
                       fc.loc.pos);
        }
        OverOutput out;
        out.fn = fc.name;
        if (fc.name == "count" && fc.args.empty()) {
            out.input_column = "";  // COUNT(*) OVER (...)
        } else if (fc.name == "lag") {
            if (fc.args.empty() || !std::holds_alternative<ast::ColumnRef>(fc.args[0])) {
                bind_error("LAG requires a column argument", fc.loc.pos);
            }
            out.input_column =
                resolve_value_column_name(std::get<ast::ColumnRef>(fc.args[0]), source, alias);
            if (fc.args.size() >= 2) {
                if (!std::holds_alternative<ast::IntLiteral>(fc.args[1])) {
                    bind_error("LAG offset must be an integer literal", fc.loc.pos);
                }
                out.lag_offset = std::get<ast::IntLiteral>(fc.args[1]).value;
                if (out.lag_offset < 1) {
                    bind_error("LAG offset must be >= 1", fc.loc.pos);
                }
            }
            if (fc.args.size() > 2) {
                bind_error("LAG takes at most 2 arguments", fc.loc.pos);
            }
        } else {
            if (fc.args.size() != 1 || !std::holds_alternative<ast::ColumnRef>(fc.args[0])) {
                bind_error(fc.name + " OVER requires a single column argument", fc.loc.pos);
            }
            out.input_column =
                resolve_value_column_name(std::get<ast::ColumnRef>(fc.args[0]), source, alias);
        }
        out.output_name = item.alias.value_or(
            fc.name + (out.input_column.empty() ? std::string{"_star"} : "_" + out.input_column));
        out.type = is_agg ? aggregate_output_type(fc.name, source, out.input_column)
                          : column_type(out.input_column);
        // Window frame (Wave 7). Bounded ROWS / RANGE frames are supported
        // only for the plain running aggregates; the navigation functions
        // (first_value / last_value / lag) keep the running frame so their
        // semantics stay well-defined.
        if (fc.over_clause->frame_mode != ast::FrameMode::Running) {
            if (!is_agg) {
                bind_error("bounded window frames (ROWS/RANGE) are not supported for " + fc.name +
                               "; use the default frame",
                           fc.loc.pos);
            }
            out.frame_mode = fc.over_clause->frame_mode == ast::FrameMode::Rows ? 1 : 2;
            out.frame_start = fc.over_clause->frame_start_preceding;
        }
        outputs.push_back(std::move(out));
    }

    // Inner base: scan + optional WHERE filter (mirrors the ranking path).
    std::unique_ptr<LogicalPlan> inner = make_table_plan(ref.name, source, ref.loc.pos);
    if (stmt.where_clause.has_value()) {
        auto predicate_json = lower_predicate(*stmt.where_clause, source).serialize(0);
        inner = std::make_unique<LogicalFilter>(std::move(inner), std::move(predicate_json));
    }

    // Output schema = input columns + appended OVER output columns.
    auto in_schema = inner->schema();
    arrow::FieldVector fields;
    fields.reserve(static_cast<std::size_t>(in_schema->num_fields()) + outputs.size());
    for (int i = 0; i < in_schema->num_fields(); ++i) {
        fields.push_back(in_schema->field(i));
    }
    for (const auto& o : outputs) {
        fields.push_back(arrow::field(o.output_name, o.type));
    }
    auto schema = arrow::schema(std::move(fields));

    return std::make_unique<LogicalOverAggregate>(std::move(inner),
                                                  std::move(partition_cols),
                                                  order_col,
                                                  std::move(outputs),
                                                  std::move(schema));
}

std::unique_ptr<LogicalPlan> Binder::bind_subquery_select(const ast::SelectStmt& stmt,
                                                          const TableDef& source,
                                                          const std::string& alias,
                                                          const ast::TableRef& ref) const {
    if (!stmt.group_clause.empty() || stmt.having_clause.has_value()) {
        bind_error("subquery predicates with GROUP BY / HAVING are not supported yet",
                   stmt.loc.pos);
    }
    if (stmt.set_op != ast::SelectSetOp::None) {
        bind_error("subquery predicates with set operations are not supported yet", stmt.loc.pos);
    }

    // Flatten the WHERE: one conjunct is the subquery predicate; the rest
    // are regular predicates that filter the outer scan.
    std::vector<const ast::Expression*> conjuncts;
    flatten_and(*stmt.where_clause, conjuncts);
    std::vector<const ast::Expression*> regular;
    const ast::Expression* subq = nullptr;
    for (const auto* c : conjuncts) {
        if (expr_contains_sublink(*c)) {
            if (subq != nullptr) {
                bind_error("only one subquery predicate per WHERE is supported", stmt.loc.pos);
            }
            subq = c;
        } else {
            regular.push_back(c);
        }
    }

    // Outer base: scan + filter(regular conjuncts).
    std::unique_ptr<LogicalPlan> outer = make_table_plan(ref.name, source, ref.loc.pos);
    if (!regular.empty()) {
        outer = std::make_unique<LogicalFilter>(
            std::move(outer), lower_predicate_conjuncts(regular, source).serialize(0));
    }

    // Unwrap an outer NOT (NOT EXISTS, NOT (x IN ...)).
    const ast::Expression* pred = subq;
    bool negate = false;
    if (std::holds_alternative<std::unique_ptr<ast::NotOp>>(*pred)) {
        negate = true;
        pred = &std::get<std::unique_ptr<ast::NotOp>>(*pred)->arg;
    }

    std::unique_ptr<LogicalPlan> base;
    if (std::holds_alternative<std::unique_ptr<ast::SubLink>>(*pred)) {
        const auto& sl = *std::get<std::unique_ptr<ast::SubLink>>(*pred);
        if (sl.kind == ast::SubLink::Kind::In || sl.kind == ast::SubLink::Kind::NotIn) {
            bool anti = sl.kind == ast::SubLink::Kind::NotIn;
            if (negate) {
                anti = !anti;
            }
            auto sub_plan = bind_select(*sl.subselect);
            if (!sl.test_exprs.empty()) {
                // Multi-column `(a, b) [NOT] IN (SELECT x, y ...)` ->
                // composite-key semi / anti join. The anti (NOT IN) case uses
                // the SQL-standard per-position NULL poison (#49): a right
                // tuple with a NULL in one position only poisons probes that
                // agree on its other, non-null positions. That is implemented
                // in SemiAntiJoinRowOp's null-aware path.
                if (static_cast<int>(sl.test_exprs.size()) != sub_plan->schema()->num_fields()) {
                    bind_error(
                        "multi-column IN: the left tuple and the subquery must have the "
                        "same number of columns",
                        sl.loc.pos);
                }
                std::vector<std::string> left_keys;
                std::vector<std::string> right_keys;
                for (std::size_t i = 0; i < sl.test_exprs.size(); ++i) {
                    if (!std::holds_alternative<ast::ColumnRef>(sl.test_exprs[i])) {
                        bind_error(
                            "multi-column IN: each left tuple element must be a column "
                            "reference",
                            sl.loc.pos);
                    }
                    left_keys.push_back(resolve_value_column_name(
                        std::get<ast::ColumnRef>(sl.test_exprs[i]), source, alias));
                    right_keys.push_back(sub_plan->schema()->field(static_cast<int>(i))->name());
                }
                base = std::make_unique<LogicalSemiJoin>(std::move(outer),
                                                         std::move(sub_plan),
                                                         std::move(left_keys),
                                                         std::move(right_keys),
                                                         /*anti=*/anti,
                                                         /*null_aware=*/true);
            } else {
                if (!sl.test_expr.has_value() ||
                    !std::holds_alternative<ast::ColumnRef>(*sl.test_expr)) {
                    bind_error("IN-subquery test expression must be a column reference",
                               sl.loc.pos);
                }
                std::string left_key = resolve_value_column_name(
                    std::get<ast::ColumnRef>(*sl.test_expr), source, alias);
                if (sub_plan->schema()->num_fields() != 1) {
                    bind_error("IN-subquery must select exactly one column", sl.loc.pos);
                }
                std::string right_key = sub_plan->schema()->field(0)->name();
                base = std::make_unique<LogicalSemiJoin>(
                    std::move(outer),
                    std::move(sub_plan),
                    std::vector<std::string>{std::move(left_key)},
                    std::vector<std::string>{std::move(right_key)},
                    anti);
            }
        } else if (sl.kind == ast::SubLink::Kind::Exists) {
            // [NOT] EXISTS: decorrelate the `outer.col = inner.col` equality
            // correlations from the subquery WHERE. One or more equalities
            // form a composite key (EXISTS -> semi); remaining conjuncts
            // filter the inner scan.
            const bool anti = negate;
            const ast::SelectStmt& sub = *sl.subselect;
            if (sub.from_clause.size() != 1 || sub.set_op != ast::SelectSetOp::None ||
                !sub.group_clause.empty()) {
                bind_error("EXISTS subquery must be a single-table SELECT (decorrelation-lite)",
                           sl.loc.pos);
            }
            const ast::TableRef& inner_ref = sub.from_clause[0];
            const TableDef& inner = resolve_table(catalog_, inner_ref, cte_synth_tables_);
            const std::string inner_alias = inner_ref.alias.value_or(inner.name);
            std::vector<const ast::Expression*> sub_conjuncts;
            if (sub.where_clause.has_value()) {
                flatten_and(*sub.where_clause, sub_conjuncts);
            }
            std::vector<std::string> left_keys;
            std::vector<std::string> right_keys;
            std::vector<const ast::Expression*> inner_preds;
            for (const auto* c : sub_conjuncts) {
                bool is_corr = false;
                if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(*c)) {
                    const auto& cb = *std::get<std::unique_ptr<ast::BinaryOp>>(*c);
                    if (cb.op == ast::BinOp::Eq) {
                        auto lq = two_part_col(cb.left);
                        auto rq = two_part_col(cb.right);
                        if (lq && rq) {
                            if (lq->first == alias && rq->first == inner_alias) {
                                left_keys.push_back(lq->second);
                                right_keys.push_back(rq->second);
                                is_corr = true;
                            } else if (rq->first == alias && lq->first == inner_alias) {
                                left_keys.push_back(rq->second);
                                right_keys.push_back(lq->second);
                                is_corr = true;
                            }
                        }
                    }
                }
                if (!is_corr) {
                    inner_preds.push_back(c);
                }
            }
            if (left_keys.empty()) {
                bind_error(
                    "EXISTS subquery must have at least one `outer.col = inner.col` "
                    "correlation (decorrelation-lite)",
                    sl.loc.pos);
            }
            std::unique_ptr<LogicalPlan> inner_plan =
                make_table_plan(inner_ref.name, inner, inner_ref.loc.pos);
            if (!inner_preds.empty()) {
                inner_plan = std::make_unique<LogicalFilter>(
                    std::move(inner_plan),
                    lower_predicate_conjuncts(inner_preds, inner).serialize(0));
            }
            // [NOT] EXISTS is a plain anti/semi join (null_aware=false): a
            // correlation key that is NULL simply does not match, so NOT
            // EXISTS includes the probe. This differs from NOT IN's 3VL
            // poison and composes correctly to a multi-equality correlation.
            base = std::make_unique<LogicalSemiJoin>(std::move(outer),
                                                     std::move(inner_plan),
                                                     std::move(left_keys),
                                                     std::move(right_keys),
                                                     anti,
                                                     /*null_aware=*/false);
        } else {
            bind_error("a scalar subquery cannot be used directly as a predicate", sl.loc.pos);
        }
    } else if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(*pred)) {
        if (negate) {
            bind_error("NOT over a scalar-subquery comparison is not supported", stmt.loc.pos);
        }
        const auto& b = *std::get<std::unique_ptr<ast::BinaryOp>>(*pred);
        auto is_scalar = [](const ast::Expression& e) {
            return std::holds_alternative<std::unique_ptr<ast::SubLink>>(e) &&
                   std::get<std::unique_ptr<ast::SubLink>>(e)->kind == ast::SubLink::Kind::Scalar;
        };
        const ast::Expression* value_side = nullptr;
        const ast::SubLink* scalar_sl = nullptr;
        bool scalar_on_right = true;
        if (is_scalar(b.right)) {
            value_side = &b.left;
            scalar_sl = std::get<std::unique_ptr<ast::SubLink>>(b.right).get();
            scalar_on_right = true;
        } else if (is_scalar(b.left)) {
            value_side = &b.right;
            scalar_sl = std::get<std::unique_ptr<ast::SubLink>>(b.left).get();
            scalar_on_right = false;
        } else {
            bind_error("scalar-subquery comparison needs a `(SELECT ...)` operand", b.loc.pos);
        }
        if (!std::holds_alternative<ast::ColumnRef>(*value_side)) {
            bind_error("scalar-subquery comparison: the non-subquery side must be a column",
                       b.loc.pos);
        }
        std::string test_col =
            resolve_value_column_name(std::get<ast::ColumnRef>(*value_side), source, alias);
        // Bind the scalar subquery as a global (ungrouped) aggregate
        // producing one value: SELECT <agg>(col) FROM s [WHERE ...].
        ScalarSubplan sp = bind_scalar_aggregate_subplan(*scalar_sl);
        std::string scalar_col = sp.output_name;
        std::unique_ptr<LogicalPlan> scalar_plan = std::move(sp.plan);
        // Orient the comparison so the main column is the left operand.
        ast::BinOp op = b.op;
        if (!scalar_on_right) {
            switch (op) {
                case ast::BinOp::Lt:
                    op = ast::BinOp::Gt;
                    break;
                case ast::BinOp::Le:
                    op = ast::BinOp::Ge;
                    break;
                case ast::BinOp::Gt:
                    op = ast::BinOp::Lt;
                    break;
                case ast::BinOp::Ge:
                    op = ast::BinOp::Le;
                    break;
                default:
                    break;  // Eq / Ne are symmetric
            }
        }
        base = std::make_unique<LogicalScalarBroadcast>(std::move(outer),
                                                        std::move(scalar_plan),
                                                        std::move(test_col),
                                                        bin_op_to_predicate_op(op),
                                                        std::move(scalar_col));
    } else {
        bind_error("unsupported subquery predicate shape", stmt.loc.pos);
    }

    // Projection on top of the filtered / joined base.
    const bool only_star = stmt.target_list.size() == 1 &&
                           std::holds_alternative<ast::ColumnRef>(stmt.target_list[0].expr) &&
                           std::get<ast::ColumnRef>(stmt.target_list[0].expr).is_star;
    if (only_star) {
        return base;
    }
    auto outs = resolve_select_items(stmt.target_list, source, alias);
    return std::make_unique<LogicalProject>(std::move(base), std::move(outs));
}

Binder::ScalarSubplan Binder::bind_scalar_aggregate_subplan(const ast::SubLink& sl) const {
    const ast::SelectStmt& sub = *sl.subselect;
    if (sub.from_clause.size() != 1 || !sub.group_clause.empty() ||
        sub.set_op != ast::SelectSetOp::None || sub.target_list.size() != 1) {
        bind_error("scalar subquery must be `SELECT <agg>(col) FROM table [WHERE ...]`",
                   sl.loc.pos);
    }
    const ast::TableRef& sref = sub.from_clause[0];
    const TableDef& ssrc = resolve_table(catalog_, sref, cte_synth_tables_);
    const std::string salias = sref.alias.value_or(ssrc.name);
    auto agg = extract_aggregate(sub.target_list[0].expr, ssrc, salias);
    if (!agg.found) {
        bind_error("scalar subquery must select a single aggregate (e.g. avg(x))", sl.loc.pos);
    }
    AggregateOutput ao;
    ao.agg_fn = agg.fn;
    ao.input_column = agg.input_column;
    ao.distinct = agg.distinct;
    ao.separator = agg.separator;
    ao.percentile = agg.percentile;
    ao.output_name = sub.target_list[0].alias.value_or(
        agg.fn + (agg.input_column.empty() ? std::string{"_star"} : "_" + agg.input_column));
    ao.type = aggregate_output_type(agg.fn, ssrc, agg.input_column);
    std::unique_ptr<LogicalPlan> scalar_in = make_table_plan(sref.name, ssrc, sref.loc.pos);
    if (sub.where_clause.has_value()) {
        scalar_in = std::make_unique<LogicalFilter>(
            std::move(scalar_in), lower_predicate(*sub.where_clause, ssrc).serialize(0));
    }
    arrow::FieldVector sfields;
    sfields.push_back(arrow::field(ao.output_name, ao.type));
    auto plan = std::make_unique<LogicalAggregate>(std::move(scalar_in),
                                                   std::vector<std::string>{},
                                                   std::vector<AggregateOutput>{ao},
                                                   arrow::schema(sfields));
    return ScalarSubplan{std::move(plan), ao.output_name, ao.type};
}

std::unique_ptr<LogicalPlan> Binder::bind_scalar_select_projection(
    const ast::SelectStmt& stmt,
    const TableDef& source,
    const std::string& alias,
    const ast::TableRef& ref,
    const ast::SubLink& scalar_sl) const {
    if (!stmt.group_clause.empty() || stmt.having_clause.has_value()) {
        bind_error("a scalar subquery in the SELECT list with GROUP BY / HAVING is not supported",
                   stmt.loc.pos);
    }
    if (stmt.set_op != ast::SelectSetOp::None) {
        bind_error("a scalar subquery in the SELECT list with set operations is not supported",
                   stmt.loc.pos);
    }

    // Outer base: scan + optional WHERE filter. A WHERE carrying its own
    // subquery is handled by bind_subquery_select (which takes priority
    // in the router), so any WHERE reaching here is sublink-free.
    std::unique_ptr<LogicalPlan> outer = make_table_plan(ref.name, source, ref.loc.pos);
    if (stmt.where_clause.has_value()) {
        outer = std::make_unique<LogicalFilter>(
            std::move(outer), lower_predicate(*stmt.where_clause, source).serialize(0));
    }

    // Bind the scalar subquery and append its single value to every
    // outer row under the aggregate's output name.
    ScalarSubplan sp = bind_scalar_aggregate_subplan(scalar_sl);
    const std::string scalar_col = sp.output_name;
    const std::shared_ptr<arrow::DataType> scalar_type = sp.type;
    auto base = std::make_unique<LogicalScalarProject>(
        std::move(outer), std::move(sp.plan), scalar_col, scalar_type);

    // Build the projection directly: the scalar item becomes a column
    // ref to the appended column; other items resolve against the outer
    // source (the appended column is invisible to them, matching SQL
    // scoping where the subquery is a leaf value).
    std::vector<ProjectOutput> outs;
    for (const auto& item : stmt.target_list) {
        const auto& expr = item.expr;
        if (std::holds_alternative<std::unique_ptr<ast::SubLink>>(expr)) {
            if (std::get<std::unique_ptr<ast::SubLink>>(expr).get() != &scalar_sl) {
                bind_error("only one scalar subquery in the SELECT list is supported",
                           item.loc.pos);
            }
            clink::config::JsonObject cref;
            cref["col"] = clink::config::JsonValue{scalar_col};
            outs.push_back(ProjectOutput{item.alias.value_or(scalar_col),
                                         clink::config::JsonValue{std::move(cref)}.serialize(0),
                                         scalar_type});
            continue;
        }
        if (std::holds_alternative<ast::ColumnRef>(expr) &&
            std::get<ast::ColumnRef>(expr).is_star) {
            if (item.alias.has_value()) {
                bind_error("SELECT * cannot have an alias", item.loc.pos);
            }
            for (const auto& c : source.columns) {
                clink::config::JsonObject cref;
                cref["col"] = clink::config::JsonValue{c.name};
                outs.push_back(ProjectOutput{
                    c.name, clink::config::JsonValue{std::move(cref)}.serialize(0), c.type});
            }
            continue;
        }
        auto json = lower_value_expr(expr, source, alias);
        std::string output_name;
        if (item.alias.has_value()) {
            output_name = *item.alias;
        } else if (std::holds_alternative<ast::ColumnRef>(expr)) {
            output_name = std::get<ast::ColumnRef>(expr).parts.back();
        } else {
            output_name = "_col" + std::to_string(outs.size());
        }
        outs.push_back(ProjectOutput{
            std::move(output_name), json.serialize(0), infer_expr_type(expr, source)});
    }
    return std::make_unique<LogicalProject>(std::move(base), std::move(outs));
}

std::unique_ptr<LogicalPlan> Binder::bind_select(const ast::SelectStmt& stmt) const {
    BindTimingGuard bind_timer;
    // Phase 16: WITH clause. Bind each CTE body sequentially so cte2
    // can reference cte1 (PG semantics). Each body gets a synthetic
    // TableDef whose columns come from the bound schema; the bound
    // plan is parked in cte_plans_ and consumed at the LogicalScan-
    // creation site that references the CTE name. Each CTE is
    // referenced at most once - the plan is moved out on use, so a
    // second reference is detected at the consumption point. Multi-
    // use would need LogicalPlan deep-copy, which we don't have yet.
    // CTE bodies cannot themselves carry a WITH clause.
    // Always own the scope: a bind_select must clean up EVERY scratch entry it
    // adds, both WITH CTEs and synthetic FROM tables (join / MATCH_RECOGNIZE /
    // PTF / subquery). Gating cleanup on a WITH clause leaked the synthetic FROM
    // entry into the Binder's maps, so a later statement on a reused Binder (the
    // driver binds a whole script with one Binder) saw a stale entry. The guard
    // snapshots the entries present at construction and erases only what THIS
    // scope adds, so nested binds keep the enclosing scope intact.
    const bool has_with = !stmt.with_clause.empty();
    CteScopeGuard guard{&cte_synth_tables_, &cte_plans_, /*owns=*/true};
    (void)guard;
    if (has_with) {
        for (const auto& cte : stmt.with_clause) {
            // A CTE body may itself carry a WITH: bind_select recurses and
            // its own CteScopeGuard cleans up the inner CTE names on
            // return (erase-added-keys), leaving this scope intact.
            if (cte_synth_tables_.count(cte.name) != 0) {
                bind_error("duplicate CTE name: " + cte.name, cte.loc.pos);
            }
            auto body_plan = bind_select(*cte.body);
            auto schema = body_plan->schema();
            TableDef synth;
            synth.name = cte.name;
            synth.columns.reserve(static_cast<std::size_t>(schema->num_fields()));
            for (int i = 0; i < schema->num_fields(); ++i) {
                synth.columns.push_back(
                    ColumnSpec{schema->field(i)->name(), schema->field(i)->type()});
            }
            cte_synth_tables_.emplace(cte.name, std::move(synth));
            cte_plans_.emplace(cte.name, std::move(body_plan));
        }
    }

    // Phase 13 / set ops: UNION [ALL] / INTERSECT / EXCEPT. Bind each
    // branch independently then check the schemas are union-compatible
    // (same count, same Arrow types). The outer SelectStmt only carries
    // the set-op markers when set_op != None; other fields (where /
    // group / having) belong to the branches. Lowering:
    //   UNION ALL  -> Union (keep duplicates)
    //   UNION      -> Distinct(Union)
    //   INTERSECT  -> SetOp(is_except=false)
    //   EXCEPT     -> SetOp(is_except=true)
    if (stmt.set_op != ast::SelectSetOp::None) {
        if (!stmt.larg || !stmt.rarg) {
            bind_error("set operation is missing a branch", stmt.loc.pos);
        }
        auto left = bind_select(*stmt.larg);
        auto right = bind_select(*stmt.rarg);
        auto l_schema = left->schema();
        auto r_schema = right->schema();
        if (l_schema->num_fields() != r_schema->num_fields()) {
            std::ostringstream msg;
            msg << "set operation: left and right have different column counts ("
                << l_schema->num_fields() << " vs " << r_schema->num_fields() << ")";
            bind_error(msg.str(), stmt.loc.pos);
        }
        for (int i = 0; i < l_schema->num_fields(); ++i) {
            const auto& lf = *l_schema->field(i);
            const auto& rf = *r_schema->field(i);
            if (!lf.type()->Equals(*rf.type())) {
                std::ostringstream msg;
                msg << "set operation: column " << (i + 1) << " type mismatch ("
                    << lf.type()->ToString() << " vs " << rf.type()->ToString() << ")";
                bind_error(msg.str(), stmt.loc.pos);
            }
        }
        switch (stmt.set_op) {
            case ast::SelectSetOp::UnionAll:
                return std::make_unique<LogicalUnion>(std::move(left), std::move(right));
            case ast::SelectSetOp::UnionDistinct:
                return std::make_unique<LogicalDistinct>(
                    std::make_unique<LogicalUnion>(std::move(left), std::move(right)));
            case ast::SelectSetOp::Intersect:
                return std::make_unique<LogicalSetOp>(std::move(left),
                                                      std::move(right),
                                                      /*is_except=*/false,
                                                      /*all=*/false);
            case ast::SelectSetOp::IntersectAll:
                return std::make_unique<LogicalSetOp>(std::move(left),
                                                      std::move(right),
                                                      /*is_except=*/false,
                                                      /*all=*/true);
            case ast::SelectSetOp::Except:
                return std::make_unique<LogicalSetOp>(std::move(left),
                                                      std::move(right),
                                                      /*is_except=*/true,
                                                      /*all=*/false);
            case ast::SelectSetOp::ExceptAll:
                return std::make_unique<LogicalSetOp>(std::move(left),
                                                      std::move(right),
                                                      /*is_except=*/true,
                                                      /*all=*/true);
            case ast::SelectSetOp::None:
                break;  // guarded above; unreachable
        }
    }

    // Set when the FROM is bound to a plan registered under a synthetic ref
    // (join / MATCH_RECOGNIZE / PTF / derived table); the shared projection /
    // WHERE / GROUP BY path below then applies over it.
    std::optional<ast::TableRef> derived_table_ref;

    // Phase 5: handle a JOIN at the top level of from_items.
    if (stmt.from_items.size() == 1 &&
        std::holds_alternative<std::unique_ptr<ast::JoinClause>>(stmt.from_items[0])) {
        const auto& jc = *std::get<std::unique_ptr<ast::JoinClause>>(stmt.from_items[0]);
        JoinType jt = JoinType::Inner;
        switch (jc.kind) {
            case ast::JoinKind::Inner:
                jt = JoinType::Inner;
                break;
            case ast::JoinKind::Left:
                jt = JoinType::LeftOuter;
                break;
            case ast::JoinKind::Right:
                jt = JoinType::RightOuter;
                break;
            case ast::JoinKind::Full:
                jt = JoinType::FullOuter;
                break;
        }
        const bool both_base = std::holds_alternative<ast::TableRef>(jc.left) &&
                               std::holds_alternative<ast::TableRef>(jc.right);
        if (!both_base) {
            // Nested multi-way join: build a left-deep INNER equi-join tree and
            // register its root as the synthetic __join derived table (mirroring
            // the 2-base path below), then fall through to the shared projection
            // / WHERE / GROUP BY path. v1 supports nested sides only for INNER
            // joins; surface the real limitation (an outer/full join with a
            // nested-join side) rather than bind_join_rel's generic INNER-only
            // message.
            if (jt != JoinType::Inner) {
                bind_error(
                    "outer/full joins with a non-base side (a derived table or nested join) are "
                    "not supported yet (v1); only INNER joins may have such a side",
                    jc.loc.pos);
            }
            BoundRel root = bind_join_rel(stmt.from_items[0]);
            const std::string join_alias = "__join";
            if (cte_synth_tables_.count(join_alias) != 0) {
                bind_error("reserved join alias '__join' collides with an in-scope name",
                           jc.loc.pos);
            }
            TableDef synth;
            synth.name = join_alias;
            synth.columns = std::move(root.columns);
            cte_synth_tables_.emplace(join_alias, std::move(synth));
            cte_plans_.emplace(join_alias, std::move(root.plan));
            ast::TableRef tr;
            tr.name = join_alias;
            tr.loc = jc.loc;
            derived_table_ref = std::move(tr);
        } else {
            const auto& left_ref = std::get<ast::TableRef>(jc.left);
            const auto& right_ref = std::get<ast::TableRef>(jc.right);
            const auto& left_def = resolve_table(catalog_, left_ref, cte_synth_tables_);
            const auto& right_def = resolve_table(catalog_, right_ref, cte_synth_tables_);
            std::string left_alias = left_ref.alias.value_or(left_def.name);
            std::string right_alias = right_ref.alias.value_or(right_def.name);
            if (!jc.on_clause.has_value()) {
                bind_error("JOIN requires an ON clause", jc.loc.pos);
            }
            // Phase 18: try the equi-join shape first (simpler, narrower).
            // Falls through to the interval-join recognizer when the ON
            // clause is the eq + BETWEEN AND-pair.
            auto equi_shape = match_equi_join(*jc.on_clause, left_alias, right_alias);
            auto interval_shape = match_interval_join(*jc.on_clause, left_alias, right_alias);
            if (!equi_shape && !interval_shape) {
                bind_error(
                    "JOIN ON must be either 'a.k = b.k' or "
                    "'a.k = b.k AND a.ts BETWEEN b.ts + low AND b.ts + high'",
                    jc.loc.pos);
            }

            // Output schema: every left column qualified by left_alias,
            // then every right column qualified by right_alias. Aliases
            // appear in the column names as "<alias>_<col>" so the sink
            // can be a single flat schema without manual SELECT.
            arrow::FieldVector fields;
            for (const auto& c : left_def.columns) {
                fields.push_back(arrow::field(left_alias + "_" + c.name, c.type));
            }
            for (const auto& c : right_def.columns) {
                fields.push_back(arrow::field(right_alias + "_" + c.name, c.type));
            }
            // The join lowers to one of three plan nodes below; rather than return
            // it directly (which silently bypassed WHERE / projection / GROUP BY -
            // only `SELECT * FROM a JOIN b` worked), it is registered as a synthetic
            // derived table so the outer SELECT applies over the join output, just
            // like the MATCH_RECOGNIZE / PTF / subquery paths. Join output columns
            // are the flat "<alias>_<col>" names; reference them by that flat name
            // in the outer SELECT / WHERE (qualified `alias.col` is a follow-on, and
            // is not supported by the derived-table wrapper today either).
            std::unique_ptr<LogicalPlan> join_plan;
            // Lookup (enrichment) join: one side is a connector='lookup'
            // table. Lower to LogicalLookupJoin - the probe stream enriched
            // per row against the dim's registered async function - rather
            // than a stream-stream join. v1: lookup table on the right,
            // INNER or LEFT only (RIGHT/FULL would require enumerating the
            // dim, which a keyed lookup source cannot do).
            if (left_def.is_lookup() || right_def.is_lookup()) {
                if (left_def.is_lookup() && right_def.is_lookup()) {
                    bind_error("a lookup join needs exactly one lookup table, not two", jc.loc.pos);
                }
                if (left_def.is_lookup()) {
                    bind_error(
                        "the lookup table must be on the right of the JOIN "
                        "(stream JOIN lookup_table); move '" +
                            left_def.name + "' to the right side",
                        jc.loc.pos);
                }
                if (!equi_shape) {
                    bind_error(
                        "a lookup join requires an equality ON clause 'stream.key = lookup.key'",
                        jc.loc.pos);
                }
                if (jt == JoinType::RightOuter || jt == JoinType::FullOuter) {
                    bind_error("only INNER and LEFT joins are supported against a lookup table",
                               jc.loc.pos);
                }
                const std::string fn = right_def.lookup_function();
                if (fn.empty()) {
                    bind_error(
                        "lookup table '" + right_def.name +
                            "' must set a function= property naming a registered async lookup",
                        right_ref.loc.pos);
                }
                std::vector<std::string> probe_cols;
                probe_cols.reserve(left_def.columns.size());
                for (const auto& c : left_def.columns) {
                    probe_cols.push_back(c.name);
                }
                std::vector<std::string> dim_cols;
                dim_cols.reserve(right_def.columns.size());
                for (const auto& c : right_def.columns) {
                    dim_cols.push_back(c.name);
                }
                auto probe_scan = make_table_plan(left_ref.name, left_def, left_ref.loc.pos);
                join_plan = std::make_unique<LogicalLookupJoin>(std::move(probe_scan),
                                                                fn,
                                                                left_alias,
                                                                right_alias,
                                                                std::move(probe_cols),
                                                                std::move(dim_cols),
                                                                equi_shape->right_key,
                                                                jt == JoinType::LeftOuter,
                                                                arrow::schema(std::move(fields)));
            } else {
                auto left_scan = make_table_plan(left_ref.name, left_def, left_ref.loc.pos);
                auto right_scan = make_table_plan(right_ref.name, right_def, right_ref.loc.pos);
                if (interval_shape) {
                    // OUTER interval joins (SQLOPT-1): the runtime IntervalJoinRowOp
                    // null-pads unmatched rows on the kept side at watermark eviction
                    // (the window is finite, so the verdict is final - no retraction).
                    join_plan =
                        std::make_unique<LogicalIntervalJoin>(std::move(left_scan),
                                                              std::move(right_scan),
                                                              std::move(left_alias),
                                                              std::move(right_alias),
                                                              std::move(interval_shape->left_key),
                                                              std::move(interval_shape->right_key),
                                                              std::move(interval_shape->left_ts),
                                                              std::move(interval_shape->right_ts),
                                                              interval_shape->lower_offset_ms,
                                                              interval_shape->upper_offset_ms,
                                                              arrow::schema(std::move(fields)),
                                                              jt);
                } else {
                    join_plan = std::make_unique<LogicalEquiJoin>(std::move(left_scan),
                                                                  std::move(right_scan),
                                                                  std::move(left_alias),
                                                                  std::move(right_alias),
                                                                  std::move(equi_shape->left_key),
                                                                  std::move(equi_shape->right_key),
                                                                  arrow::schema(std::move(fields)),
                                                                  jt);
                }
            }

            // Register the join as a synthetic derived table and fall through to the
            // shared projection / WHERE / GROUP BY path (mirrors MATCH_RECOGNIZE /
            // PTF / subquery). Columns are the flat "<alias>_<col>" names. A query
            // has at most one top-level join (nested joins reference base tables
            // only), so a single reserved alias suffices; the CteScopeGuard erases it
            // at bind exit so it never leaks to a later statement on a reused Binder.
            // Guard against an in-scope name collision (e.g. a user CTE literally
            // named __join), exactly as the subquery / MATCH_RECOGNIZE / PTF paths do.
            const std::string join_alias = "__join";
            if (cte_synth_tables_.count(join_alias) != 0) {
                bind_error(
                    "reserved join alias '" + join_alias + "' collides with an in-scope name",
                    jc.loc.pos);
            }
            auto join_schema = join_plan->schema();
            TableDef synth;
            synth.name = join_alias;
            synth.columns.reserve(static_cast<std::size_t>(join_schema->num_fields()));
            for (int i = 0; i < join_schema->num_fields(); ++i) {
                synth.columns.push_back(
                    ColumnSpec{join_schema->field(i)->name(), join_schema->field(i)->type()});
            }
            cte_synth_tables_.emplace(join_alias, std::move(synth));
            cte_plans_.emplace(join_alias, std::move(join_plan));
            ast::TableRef tr;
            tr.name = join_alias;
            tr.loc = jc.loc;
            derived_table_ref = std::move(tr);
        }  // end else (2-base-table top-level join)
    }

    // Phase 20: FROM (SELECT ...) AS sub. When from_items carries a
    // single SubqueryItem (no JOIN, no base table), pre-bind its body
    // and register a synthetic CTE-like entry under the alias so the
    // existing from_clause path picks it up via cte_synth_tables_ /
    // cte_plans_ + make_table_plan.
    if (stmt.from_items.size() == 1 &&
        std::holds_alternative<std::unique_ptr<ast::SubqueryItem>>(stmt.from_items[0])) {
        const auto& sq = *std::get<std::unique_ptr<ast::SubqueryItem>>(stmt.from_items[0]);
        if (sq.alias.empty()) {
            bind_error("derived table requires an alias", sq.loc.pos);
        }
        if (cte_synth_tables_.count(sq.alias) != 0) {
            bind_error("derived-table alias collides with an in-scope CTE: " + sq.alias,
                       sq.loc.pos);
        }
        auto body_plan = bind_select(*sq.body);

        // Phase 21c: pattern-match TOP-N-per-key.
        //   SELECT * FROM (SELECT *, ROW_NUMBER() OVER (...) AS rn
        //                   FROM t [WHERE ...]) sub
        //   WHERE rn <op> N           with op in {<=, <, =}
        // When matched, replace the bound body's LogicalRowNumber with
        // a LogicalTopNPerKey carrying the same partition / sort spec,
        // and mark the outer WHERE consumed so the projection path
        // doesn't re-apply it. The TopN op's output schema drops the
        // synthetic rn column.
        if (body_plan->kind() == "RowNumber" && stmt.where_clause.has_value()) {
            auto& rn_node = static_cast<LogicalRowNumber&>(*body_plan);
            const auto& wc = *stmt.where_clause;
            if (std::holds_alternative<std::unique_ptr<ast::BinaryOp>>(wc)) {
                const auto& bin = *std::get<std::unique_ptr<ast::BinaryOp>>(wc);
                const bool op_ok = (bin.op == ast::BinOp::Le || bin.op == ast::BinOp::Lt ||
                                    bin.op == ast::BinOp::Eq);
                if (op_ok && std::holds_alternative<ast::ColumnRef>(bin.left) &&
                    std::holds_alternative<ast::IntLiteral>(bin.right)) {
                    const auto& lhs = std::get<ast::ColumnRef>(bin.left);
                    if (lhs.parts.size() == 1 && lhs.parts[0] == rn_node.output_name()) {
                        const std::int64_t literal = std::get<ast::IntLiteral>(bin.right).value;
                        std::int64_t count = literal;
                        if (bin.op == ast::BinOp::Eq && literal != 1) {
                            bind_error("ROW_NUMBER() WHERE rn = N only supports N = 1",
                                       bin.loc.pos);
                        }
                        if (bin.op == ast::BinOp::Lt) {
                            count = literal - 1;
                        }
                        if (count < 1) {
                            bind_error("ROW_NUMBER() WHERE rn <op> N must yield N >= 1",
                                       bin.loc.pos);
                        }
                        auto part = rn_node.partition_columns();
                        auto sort = rn_node.sort_columns();
                        auto sort_desc = rn_node.sort_descending();
                        auto rank_kind = rn_node.rank_kind();
                        auto inner = rn_node.release_input();
                        body_plan = std::make_unique<LogicalTopNPerKey>(std::move(inner),
                                                                        std::move(part),
                                                                        std::move(sort),
                                                                        std::move(sort_desc),
                                                                        count,
                                                                        rank_kind);
                        consumed_topn_where_ = true;
                    }
                }
            }
        }

        auto body_schema = body_plan->schema();
        TableDef synth;
        synth.name = sq.alias;
        synth.columns.reserve(static_cast<std::size_t>(body_schema->num_fields()));
        for (int i = 0; i < body_schema->num_fields(); ++i) {
            synth.columns.push_back(
                ColumnSpec{body_schema->field(i)->name(), body_schema->field(i)->type()});
        }
        cte_synth_tables_.emplace(sq.alias, std::move(synth));
        cte_plans_.emplace(sq.alias, std::move(body_plan));
        ast::TableRef tr;
        tr.name = sq.alias;
        tr.loc = sq.loc;
        derived_table_ref = std::move(tr);
    }

    // #61 phase 2: FROM <table> MATCH_RECOGNIZE (...). Bind the clause and
    // register its output as a synthetic derived table so the outer projection
    // / WHERE apply over the match result (mirrors the derived-table path).
    if (!derived_table_ref.has_value() && stmt.from_items.size() == 1 &&
        std::holds_alternative<std::unique_ptr<ast::MatchRecognizeClause>>(stmt.from_items[0])) {
        const auto& mrc = *std::get<std::unique_ptr<ast::MatchRecognizeClause>>(stmt.from_items[0]);
        const std::string mr_alias = mrc.alias.value_or(std::string{"__match_recognize"});
        if (cte_synth_tables_.count(mr_alias) != 0) {
            bind_error("MATCH_RECOGNIZE alias collides with an in-scope name: " + mr_alias,
                       mrc.loc.pos);
        }
        auto mr_plan = bind_match_recognize(mrc);
        auto body_schema = mr_plan->schema();
        TableDef synth;
        synth.name = mr_alias;
        synth.columns.reserve(static_cast<std::size_t>(body_schema->num_fields()));
        for (int i = 0; i < body_schema->num_fields(); ++i) {
            synth.columns.push_back(
                ColumnSpec{body_schema->field(i)->name(), body_schema->field(i)->type()});
        }
        cte_synth_tables_.emplace(mr_alias, std::move(synth));
        cte_plans_.emplace(mr_alias, std::move(mr_plan));
        ast::TableRef tr;
        tr.name = mr_alias;
        tr.loc = mrc.loc;
        derived_table_ref = std::move(tr);
    }

    // SQLOPT PTF: FROM my_ptf(TABLE t PARTITION BY k). Bind the clause and
    // register its declared output as a synthetic derived table so the outer
    // projection / WHERE apply over the PTF result (mirrors MATCH_RECOGNIZE).
    if (!derived_table_ref.has_value() && stmt.from_items.size() == 1 &&
        std::holds_alternative<std::unique_ptr<ast::ProcessTableFunctionClause>>(
            stmt.from_items[0])) {
        const auto& ptf =
            *std::get<std::unique_ptr<ast::ProcessTableFunctionClause>>(stmt.from_items[0]);
        const std::string ptf_alias = ptf.alias.value_or(std::string{"__process_table_function"});
        if (cte_synth_tables_.count(ptf_alias) != 0) {
            bind_error("process table function alias collides with an in-scope name: " + ptf_alias,
                       ptf.loc.pos);
        }
        auto ptf_plan = bind_process_table_function(ptf);
        auto body_schema = ptf_plan->schema();
        TableDef synth;
        synth.name = ptf_alias;
        synth.columns.reserve(static_cast<std::size_t>(body_schema->num_fields()));
        for (int i = 0; i < body_schema->num_fields(); ++i) {
            synth.columns.push_back(
                ColumnSpec{body_schema->field(i)->name(), body_schema->field(i)->type()});
        }
        cte_synth_tables_.emplace(ptf_alias, std::move(synth));
        cte_plans_.emplace(ptf_alias, std::move(ptf_plan));
        ast::TableRef tr;
        tr.name = ptf_alias;
        tr.loc = ptf.loc;
        derived_table_ref = std::move(tr);
    }

    if (!derived_table_ref.has_value()) {
        if (stmt.from_clause.empty()) {
            bind_error("SELECT without FROM not supported in Phase 1", stmt.loc.pos);
        }
        if (stmt.from_clause.size() > 1) {
            bind_error("multi-table FROM (joins / cross product) not supported in Phase 1",
                       stmt.loc.pos);
        }
    }
    const ast::TableRef& ref =
        derived_table_ref.has_value() ? *derived_table_ref : stmt.from_clause[0];
    const auto& source = resolve_table(catalog_, ref, cte_synth_tables_);
    std::string alias = ref.alias.value_or(source.name);

    // Inc 4: a WHERE carrying a subquery predicate (IN / NOT IN /
    // EXISTS / scalar) is rewritten to a semi/anti join or scalar
    // broadcast. Handle it before the ranking / aggregate paths.
    if (stmt.where_clause.has_value() && expr_contains_sublink(*stmt.where_clause)) {
        return bind_subquery_select(stmt, source, alias, ref);
    }

    // #55: a scalar subquery as a top-level SELECT item -> append the
    // single value to every row. Routed after the WHERE-subquery path so
    // a WHERE sublink takes priority (the combination of both is rejected
    // there, via resolve_select_items, for v1).
    if (const ast::SubLink* scalar_item = target_list_scalar_sublink(stmt.target_list)) {
        return bind_scalar_select_projection(stmt, source, alias, ref, *scalar_item);
    }

    // Ranking window functions ROW_NUMBER() / RANK() / DENSE_RANK()
    // OVER (PARTITION BY ... ORDER BY ...). Accept exactly the shape
    // `SELECT *, <fn>() OVER (...) AS rn FROM <table> [WHERE ...]`.
    // Paired with an enclosing `WHERE rn <op> N`, the derived-table
    // path rewrites this into a bounded LogicalTopNPerKey; the rank
    // kind selects ROW_NUMBER / RANK / DENSE_RANK tie semantics.
    {
        std::vector<std::size_t> window_targets;
        for (std::size_t i = 0; i < stmt.target_list.size(); ++i) {
            const auto& item = stmt.target_list[i];
            if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(item.expr) &&
                std::get<std::unique_ptr<ast::FunctionCall>>(item.expr)->over_clause != nullptr) {
                window_targets.push_back(i);
            }
        }
        std::optional<std::size_t> window_idx;
        if (!window_targets.empty()) {
            const auto& first_wfc = *std::get<std::unique_ptr<ast::FunctionCall>>(
                stmt.target_list[window_targets[0]].expr);
            const bool ranking = first_wfc.name == "row_number" || first_wfc.name == "rank" ||
                                 first_wfc.name == "dense_rank";
            if (ranking) {
                if (window_targets.size() > 1) {
                    bind_error("only one ranking window function per SELECT is supported",
                               stmt.target_list[window_targets[1]].loc.pos);
                }
                window_idx = window_targets[0];
            } else {
                // OVER (running) aggregate path: SUM/COUNT/AVG/MIN/MAX,
                // FIRST_VALUE/LAST_VALUE, LAG over the event-time order.
                return bind_over_aggregate(stmt, source, alias, ref, window_targets);
            }
        }
        if (window_idx.has_value()) {
            // Validate the rest of the SELECT shape.
            if (stmt.from_clause.empty() && !derived_table_ref.has_value()) {
                bind_error("ROW_NUMBER() requires a single-table FROM", stmt.loc.pos);
            }
            if (!stmt.group_clause.empty() || stmt.having_clause.has_value()) {
                bind_error("ROW_NUMBER() is incompatible with GROUP BY / HAVING", stmt.loc.pos);
            }
            if (stmt.distinct || !stmt.sort_clause.empty() || stmt.limit_count.has_value() ||
                stmt.offset_count.has_value()) {
                bind_error(
                    "ROW_NUMBER() must appear as the only window in a SELECT *, ... shape "
                    "(no DISTINCT / ORDER BY / LIMIT on this level)",
                    stmt.loc.pos);
            }
            // Target list must be SELECT * plus exactly one row_number
            // resTarget. Other targets aren't supported in this phase.
            bool saw_star = false;
            for (std::size_t i = 0; i < stmt.target_list.size(); ++i) {
                if (i == *window_idx)
                    continue;
                const auto& item = stmt.target_list[i];
                if (std::holds_alternative<ast::ColumnRef>(item.expr) &&
                    std::get<ast::ColumnRef>(item.expr).is_star) {
                    saw_star = true;
                    continue;
                }
                bind_error("SELECT items beyond ROW_NUMBER() must be `SELECT *`", item.loc.pos);
            }
            if (!saw_star) {
                bind_error("ROW_NUMBER() requires `SELECT *, ROW_NUMBER()...` shape", stmt.loc.pos);
            }
            const auto& wfc =
                *std::get<std::unique_ptr<ast::FunctionCall>>(stmt.target_list[*window_idx].expr);
            RankKind rank_kind = RankKind::RowNumber;
            if (wfc.name == "row_number") {
                rank_kind = RankKind::RowNumber;
            } else if (wfc.name == "rank") {
                rank_kind = RankKind::Rank;
            } else if (wfc.name == "dense_rank") {
                rank_kind = RankKind::DenseRank;
            } else {
                bind_error(
                    "only ROW_NUMBER() / RANK() / DENSE_RANK() are supported as window functions",
                    wfc.loc.pos);
            }
            if (!wfc.args.empty()) {
                bind_error("ranking window functions take no arguments", wfc.loc.pos);
            }
            if (wfc.over_clause->order_by.empty()) {
                bind_error("a ranking window function OVER requires a non-empty ORDER BY",
                           wfc.loc.pos);
            }
            std::vector<std::string> partition_cols;
            for (const auto& e : wfc.over_clause->partition_by) {
                if (!std::holds_alternative<ast::ColumnRef>(e)) {
                    bind_error("PARTITION BY entry must be a column reference", wfc.loc.pos);
                }
                partition_cols.push_back(
                    resolve_value_column_name(std::get<ast::ColumnRef>(e), source, alias));
            }
            std::vector<std::string> sort_cols;
            std::vector<bool> sort_desc;
            for (const auto& si : wfc.over_clause->order_by) {
                if (!std::holds_alternative<ast::ColumnRef>(si.expr)) {
                    bind_error("ORDER BY in OVER must be a column reference", si.loc.pos);
                }
                sort_cols.push_back(
                    resolve_value_column_name(std::get<ast::ColumnRef>(si.expr), source, alias));
                sort_desc.push_back(si.descending);
            }
            const auto& rn_name = stmt.target_list[*window_idx].alias.value_or(std::string{"rn"});
            // Inner base: scan + optional WHERE filter.
            std::unique_ptr<LogicalPlan> inner = make_table_plan(ref.name, source, ref.loc.pos);
            if (stmt.where_clause.has_value()) {
                auto predicate_json = lower_predicate(*stmt.where_clause, source).serialize(0);
                inner =
                    std::make_unique<LogicalFilter>(std::move(inner), std::move(predicate_json));
            }
            return std::make_unique<LogicalRowNumber>(std::move(inner),
                                                      std::move(partition_cols),
                                                      std::move(sort_cols),
                                                      std::move(sort_desc),
                                                      rn_name,
                                                      rank_kind);
        }
    }

    // Look for aggregate functions in the SELECT items. If any are
    // present we must produce a LogicalWindowAggregate; otherwise we
    // fall through to the plain projection path.
    std::vector<AggregateOutput> aggregates;
    std::vector<std::pair<std::size_t, std::string>> agg_target_indices;  // (target idx, alias)
    std::vector<std::pair<std::size_t, std::string>> key_target_indices;  // (target idx, col)
    for (std::size_t i = 0; i < stmt.target_list.size(); ++i) {
        const auto& item = stmt.target_list[i];
        auto agg = extract_aggregate(item.expr, source, alias);
        if (agg.found) {
            AggregateOutput a;
            a.agg_fn = std::move(agg.fn);
            a.input_column = std::move(agg.input_column);
            a.distinct = agg.distinct;
            a.separator = std::move(agg.separator);
            a.percentile = agg.percentile;
            a.output_name = item.alias.value_or(
                a.agg_fn + (a.input_column.empty() ? std::string{"_star"} : "_" + a.input_column));
            a.type = aggregate_output_type(a.agg_fn, source, a.input_column);
            aggregates.push_back(std::move(a));
            agg_target_indices.emplace_back(i, aggregates.back().output_name);
        } else if (std::holds_alternative<ast::ColumnRef>(item.expr) &&
                   !std::get<ast::ColumnRef>(item.expr).is_star) {
            const auto& cr = std::get<ast::ColumnRef>(item.expr);
            // window_start / window_end with no real same-named source column are
            // synthetic columns a windowed GROUP BY emits; don't resolve them here
            // (the resolver would throw "column not found"). Carry the literal name
            // and let the validation below gate them on a window TVF being present.
            if (is_synthetic_window_bound(cr, source)) {
                key_target_indices.emplace_back(i, cr.parts[0]);
            } else {
                key_target_indices.emplace_back(i, resolve_value_column_name(cr, source, alias));
            }
        }
    }

    const bool has_aggs = !aggregates.empty();
    const bool has_group = !stmt.group_clause.empty();
    if ((has_aggs || has_group) && !stmt.target_list.empty()) {
        for (const auto& item : stmt.target_list) {
            if (std::holds_alternative<ast::ColumnRef>(item.expr) &&
                std::get<ast::ColumnRef>(item.expr).is_star) {
                bind_error("SELECT * with GROUP BY is not supported in Phase 4", item.loc.pos);
            }
        }
    }
    if (!has_aggs && !has_group) {
        // Phase 1-3 path: pure projection.
        auto resolved = resolve_select_items(stmt.target_list, source, alias);
        std::unique_ptr<LogicalPlan> scan_or_filter =
            make_table_plan(ref.name, source, ref.loc.pos);
        if (stmt.where_clause.has_value() && !consumed_topn_where_) {
            auto predicate_json = lower_predicate(*stmt.where_clause, source).serialize(0);
            scan_or_filter = std::make_unique<LogicalFilter>(std::move(scan_or_filter),
                                                             std::move(predicate_json));
        }
        std::unique_ptr<LogicalPlan> plan =
            std::make_unique<LogicalProject>(std::move(scan_or_filter), std::move(resolved));
        if (stmt.distinct) {
            plan = std::make_unique<LogicalDistinct>(std::move(plan));
        }
        return wrap_top_n_or_limit(std::move(plan), stmt);
    }
    if (has_aggs && !has_group) {
        bind_error(
            "aggregate functions in SELECT require a GROUP BY clause (Phase 4 streaming "
            "semantics)",
            stmt.loc.pos);
    }
    if (has_group && !has_aggs) {
        bind_error("GROUP BY without aggregate functions in SELECT is not yet supported",
                   stmt.loc.pos);
    }

    // Classify GROUP BY entries. One window TVF (TUMBLE / HOP /
    // SESSION / CUMULATE) is allowed; the rest must be plain column refs.
    std::optional<WindowSpec> window;
    std::vector<std::string> group_keys;
    for (const auto& g : stmt.group_clause) {
        if (std::holds_alternative<std::unique_ptr<ast::FunctionCall>>(g)) {
            const auto& fc = *std::get<std::unique_ptr<ast::FunctionCall>>(g);
            if (fc.name == "tumble" || fc.name == "hop" || fc.name == "session" ||
                fc.name == "cumulate") {
                if (window.has_value()) {
                    bind_error("only one window TVF is allowed per GROUP BY", fc.loc.pos);
                }
                window = decode_window_call(fc, source, alias);
                continue;
            }
            bind_error(
                "function calls in GROUP BY must be TUMBLE / HOP / SESSION / CUMULATE, got: " +
                    fc.name,
                fc.loc.pos);
        }
        if (!std::holds_alternative<ast::ColumnRef>(g)) {
            bind_error("GROUP BY entries must be column references or window TVFs", stmt.loc.pos);
        }
        group_keys.push_back(resolve_value_column_name(std::get<ast::ColumnRef>(g), source, alias));
    }
    // A windowed GROUP BY emits synthetic window_start / window_end columns, so
    // those names are reserved here: a real source column of that name would be
    // overwritten by the synthetic bound at runtime (silent wrong result).
    // Reject the collision at bind time with a clear, actionable error rather
    // than letting the projection resolve ambiguously.
    if (window.has_value()) {
        for (const char* reserved : {"window_start", "window_end"}) {
            for (const auto& sc : source.columns) {
                if (sc.name == reserved) {
                    bind_error(std::string{"column '"} + reserved +
                                   "' collides with the synthetic window bound a windowed GROUP BY "
                                   "emits; rename the source column or use a non-windowed GROUP BY",
                               stmt.loc.pos);
                }
            }
        }
    }

    // Phase 8: GROUP BY without a window TVF is now allowed. The
    // runtime maintains per-group state forever and emits the latest
    // aggregate Row per input record (upsert mode). The output below
    // branches on whether a window was supplied.

    // Validate: every non-aggregate SELECT item must be either a group key or,
    // in a windowed GROUP BY, one of the synthetic window bounds window_start /
    // window_end.
    for (auto& [idx, col] : key_target_indices) {
        // A synthetic window bound (window_start / window_end with no real
        // same-named source column) is valid only with a window TVF present.
        if ((col == "window_start" || col == "window_end") && !source_has_column(source, col)) {
            if (!window.has_value()) {
                bind_error("column " + col +
                               " is only available with a window TVF in GROUP BY "
                               "(TUMBLE / HOP / SESSION / CUMULATE)",
                           stmt.target_list[idx].loc.pos);
            }
            continue;
        }
        bool ok = false;
        for (const auto& gk : group_keys) {
            if (gk == col) {
                ok = true;
                break;
            }
        }
        if (!ok) {
            bind_error("column " + col + " referenced in SELECT must appear in GROUP BY",
                       stmt.target_list[idx].loc.pos);
        }
    }

    // Describe the output columns in target_list order (keys + aggregates
    // interleaved as the user SELECTed them). The schema assembly itself lives
    // in lowering::build_group_output_schema so the programmatic Table API
    // (#59) produces byte-identical output schemas without transcribing this
    // ordering.
    std::vector<lowering::GroupOutputColumn> out_columns;
    out_columns.reserve(stmt.target_list.size());
    for (std::size_t i = 0; i < stmt.target_list.size(); ++i) {
        const auto& item = stmt.target_list[i];
        std::optional<std::size_t> agg_idx;
        for (std::size_t k = 0; k < agg_target_indices.size(); ++k) {
            if (agg_target_indices[k].first == i) {
                agg_idx = k;
                break;
            }
        }
        lowering::GroupOutputColumn c;
        if (agg_idx.has_value()) {
            c.is_aggregate = true;
            c.agg_index = *agg_idx;
        } else {
            const auto& cr = std::get<ast::ColumnRef>(item.expr);
            if (is_synthetic_window_bound(cr, source)) {
                c.is_window_bound = true;
                c.window_is_end = (cr.parts[0] == "window_end");
                c.key_source_column = cr.parts[0];  // literal runtime column name
                c.key_output_name = item.alias.value_or(cr.parts[0]);
            } else {
                c.key_source_column = resolve_value_column_name(cr, source, alias);
                c.key_output_name = item.alias.value_or(c.key_source_column);
            }
        }
        out_columns.push_back(std::move(c));
    }
    // The runtime emits exactly one window_start and one window_end column, so
    // each bound can be projected at most once (a second alias would have no
    // backing value in the emitted Row).
    {
        int n_start = 0, n_end = 0;
        for (const auto& c : out_columns) {
            if (c.is_window_bound)
                (c.window_is_end ? n_end : n_start)++;
        }
        if (n_start > 1 || n_end > 1) {
            bind_error("window_start / window_end may each be projected at most once",
                       stmt.loc.pos);
        }
    }

    std::unique_ptr<LogicalPlan> scan_or_filter = make_table_plan(ref.name, source, ref.loc.pos);
    if (stmt.where_clause.has_value() && !consumed_topn_where_) {
        auto predicate_json = lower_predicate(*stmt.where_clause, source).serialize(0);
        scan_or_filter =
            std::make_unique<LogicalFilter>(std::move(scan_or_filter), std::move(predicate_json));
    }
    auto out_schema = lowering::build_group_output_schema(out_columns, aggregates, source);
    // SQLOPT-3: a SESSION window merges sessions, so a UDAF used there must
    // supply a merge closure. Reject a merge-less UDAF at bind time with a clear
    // error rather than letting it fail mid-merge at runtime. (The runtime keeps
    // a defensive throw too.)
    if (window.has_value() && window->kind == WindowSpec::Kind::Session) {
        for (const auto& a : aggregates) {
            if (is_registered_udaf_name(a.agg_fn)) {
                auto e = AggFunctionRegistry::global().lookup(a.agg_fn);
                if (e && !e->has_merge()) {
                    bind_error("UDAF '" + a.agg_fn +
                                   "' has no merge closure and cannot be used in a SESSION window; "
                                   "register a merge closure or use TUMBLE/HOP/CUMULATE/GROUP BY",
                               ref.loc.pos);
                }
            }
        }
    }
    // Output name per group key (parallel to group_keys): honour a SELECT alias
    // on the key (`GROUP BY user_id` + `SELECT user_id AS uid`), else the raw
    // name. Without this the runtime aggregate emits the key under its raw name
    // even when the SELECT aliased it, so the output column name was wrong.
    std::vector<std::string> key_output_names;
    key_output_names.reserve(group_keys.size());
    for (const auto& gk : group_keys) {
        std::string out_name = gk;
        for (const auto& c : out_columns) {
            if (!c.is_aggregate && !c.is_window_bound && c.key_source_column == gk) {
                out_name = c.key_output_name;
                break;
            }
        }
        key_output_names.push_back(std::move(out_name));
    }
    // Output names for the projected window bounds (empty when not selected), so
    // the runtime op emits each bound under its SELECT alias.
    std::string window_start_output, window_end_output;
    for (const auto& c : out_columns) {
        if (c.is_window_bound) {
            (c.window_is_end ? window_end_output : window_start_output) = c.key_output_name;
        }
    }
    std::unique_ptr<LogicalPlan> agg_plan;
    if (window.has_value()) {
        agg_plan = std::make_unique<LogicalWindowAggregate>(std::move(scan_or_filter),
                                                            std::move(*window),
                                                            group_keys,
                                                            aggregates,
                                                            out_schema,
                                                            key_output_names,
                                                            window_start_output,
                                                            window_end_output);
    } else {
        agg_plan = std::make_unique<LogicalAggregate>(
            std::move(scan_or_filter), group_keys, aggregates, out_schema, key_output_names);
    }
    if (stmt.having_clause.has_value()) {
        // Phase 9: HAVING runs on the aggregate's emitted rows. Build
        // a synthetic source whose columns are (group keys with their
        // source types) + (aggregate alias with declared agg type) so
        // lower_predicate can resolve refs against the post-aggregate
        // schema.
        //
        // Phase 19c: when HAVING references aggregates directly
        // (e.g. `HAVING SUM(amount) > 100` without an alias), rewrite
        // the expression to replace each aggregate FunctionCall with
        // a ColumnRef to the matching aggregate slot's output_name.
        // The matching rule is (fn name, input column): same as how
        // we'd recognise it in SELECT. Unmatched aggregate refs (no
        // matching slot in SELECT) error out.
        auto having = rewrite_aggregates_for_having(*stmt.having_clause, aggregates, source, alias);

        TableDef synthetic;
        synthetic.name = "__having";
        for (const auto& gk : group_keys) {
            for (const auto& c : source.columns) {
                if (c.name == gk) {
                    synthetic.columns.push_back(c);
                    break;
                }
            }
        }
        for (const auto& a : aggregates) {
            synthetic.columns.push_back(ColumnSpec{a.output_name, a.type});
        }
        auto predicate_json = lower_predicate(having, synthetic).serialize(0);
        agg_plan = std::make_unique<LogicalFilter>(std::move(agg_plan), std::move(predicate_json));
    }
    if (stmt.distinct) {
        // SELECT DISTINCT on an aggregate's output. Aggregate rows are
        // already unique per (group, window); DISTINCT only matters
        // when the projected target list drops the differentiating
        // columns. Wrap anyway so the wire shape is honest.
        agg_plan = std::make_unique<LogicalDistinct>(std::move(agg_plan));
    }
    return wrap_top_n_or_limit(std::move(agg_plan), stmt);
}

std::unique_ptr<LogicalPlan> Binder::bind_insert(const ast::InsertStmt& stmt) const {
    BindTimingGuard bind_timer;
    const auto& sink = resolve_table(catalog_, stmt.target, cte_synth_tables_);

    // Phase 28c-frontend: async-lookup lowering. `INSERT INTO out SELECT
    // enrich(*) FROM src`, with `enrich` registered in
    // AsyncFunctionRegistry, lowers to Scan(src) -> AsyncMap(enrich) ->
    // Sink(out). The async function defines the enriched row shape, so
    // the AsyncMap's output schema is the sink's columns and the usual
    // projection / column-list / sink-compat checks below are skipped
    // (they would be tautological against the sink schema here).
    if (auto async = detect_async_lookup(stmt.select)) {
        const auto& src = resolve_table(catalog_, *async->second, cte_synth_tables_);
        auto scan = make_table_plan(async->second->name, src, async->second->loc.pos);
        auto amap = std::make_unique<LogicalAsyncMap>(std::move(scan), async->first, &sink);
        return std::make_unique<LogicalSink>(std::move(amap), &sink);
    }

    auto select_plan = bind_select(stmt.select);

    // Phase 19d: optional column list `INSERT INTO t (a, b)`. Each
    // listed name must be a column of the sink; the SELECT projects
    // in column-list order. We rewrite the plan with a LogicalProject
    // that reorders the SELECT outputs to match the sink's declared
    // column order so the downstream sink doesn't need to care. Phase
    // 19d v1 requires the column list to cover every sink column
    // (just permits reordering); partial inserts with NULL defaults
    // come later.
    if (!stmt.column_list.empty()) {
        const auto& src_schema = *select_plan->schema();
        if (static_cast<int>(stmt.column_list.size()) != src_schema.num_fields()) {
            std::ostringstream msg;
            msg << "INSERT column list has " << stmt.column_list.size()
                << " names but SELECT projects " << src_schema.num_fields();
            bind_error(msg.str(), stmt.loc.pos);
        }
        // Index sink columns by name and the column_list by name->src_idx.
        std::unordered_map<std::string, int> src_idx_by_name;
        for (std::size_t i = 0; i < stmt.column_list.size(); ++i) {
            const auto& n = stmt.column_list[i];
            if (!src_idx_by_name.emplace(n, static_cast<int>(i)).second) {
                bind_error("INSERT column list has duplicate name: " + n, stmt.loc.pos);
            }
        }
        for (const auto& n : stmt.column_list) {
            bool found = false;
            for (const auto& c : sink.columns) {
                if (c.name == n) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                bind_error("INSERT column '" + n + "' is not a column of " + sink.name,
                           stmt.loc.pos);
            }
        }
        if (stmt.column_list.size() != sink.columns.size()) {
            bind_error(
                "INSERT column list must cover every sink column (partial inserts not supported)",
                stmt.loc.pos);
        }
        // Build a reorder Project: for each sink column position, find
        // the corresponding source column index by name.
        std::vector<ProjectOutput> outs;
        outs.reserve(sink.columns.size());
        for (const auto& c : sink.columns) {
            auto it = src_idx_by_name.find(c.name);
            if (it == src_idx_by_name.end()) {
                bind_error("INSERT column list is missing sink column '" + c.name + "'",
                           stmt.loc.pos);
            }
            const auto& src_field = src_schema.field(it->second);
            clink::config::JsonObject ref;
            ref["col"] = clink::config::JsonValue{src_field->name()};
            outs.push_back(ProjectOutput{
                c.name, clink::config::JsonValue{std::move(ref)}.serialize(0), src_field->type()});
        }
        select_plan = std::make_unique<LogicalProject>(std::move(select_plan), std::move(outs));
    }

    check_sink_compatibility(sink, *select_plan->schema(), stmt.loc.pos);

    // Phase 23a: delivery_guarantee='exactly_once' enables a 2PC
    // sink at runtime. Only the connectors that have a 2PC variant
    // are eligible; upsert + 2PC is out of scope for now (upsert
    // sinks aren't barrier-aware yet).
    if (sink.is_exactly_once()) {
        const auto& conn_it = sink.properties.find("connector");
        const std::string connector =
            conn_it != sink.properties.end() ? conn_it->second : std::string{};
        if (connector != "file" && connector != "filesystem" && connector != "kafka" &&
            connector != "parquet") {
            bind_error(
                "delivery_guarantee='exactly_once' is supported only for "
                "connector='file', 'kafka' or 'parquet' (got '" +
                    connector + "')",
                stmt.loc.pos);
        }
        if (sink.is_upsert()) {
            bind_error(
                "delivery_guarantee='exactly_once' is not supported with mode='upsert' "
                "yet (upsert sinks aren't 2PC-aware)",
                stmt.loc.pos);
        }
    } else if (sink.has_commit_group()) {
        // Phase 30a: commit_group is a 2PC concept. Setting it on a
        // non-2PC sink wouldn't do anything useful (no commit phase
        // to coordinate); reject up front so users don't think it's
        // working.
        bind_error(
            "commit_group requires delivery_guarantee='exactly_once' "
            "(commit groups coordinate 2PC commits; at-least-once sinks have no commit phase)",
            stmt.loc.pos);
    }

    // Phase 22a: changelog / append compatibility between SELECT and
    // sink. Plans that include a LogicalTopNPerKey (or any future
    // retracting operator) emit `__row_kind=delete` records; sinks
    // that don't understand them must reject. Upsert sinks must
    // declare a primary key and the SELECT must project every PK
    // column so the runtime can build the upsert lookup.
    const bool produces_changelog = is_changelog_plan(*select_plan);
    if (sink.is_upsert()) {
        if (sink.primary_key.empty()) {
            bind_error("sink " + sink.name +
                           " is mode='upsert' but has no PRIMARY KEY (set primary_key='...')",
                       stmt.loc.pos);
        }
        const auto& schema = *select_plan->schema();
        for (const auto& pk : sink.primary_key) {
            bool found = false;
            for (int i = 0; i < schema.num_fields(); ++i) {
                if (schema.field(i)->name() == pk) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                bind_error("upsert sink " + sink.name + " primary key column '" + pk +
                               "' is not present in the SELECT output",
                           stmt.loc.pos);
            }
        }
    } else if (produces_changelog) {
        // A changelog SELECT also lands in a sink that natively consumes a
        // changelog: the netting sink (connector='changelog', nets +/- by full
        // row) or a discard sink (connector='blackhole', counts and drops). Any
        // other append-only sink rejects.
        auto cit = sink.properties.find("connector");
        const std::string conn = cit != sink.properties.end() ? cit->second : std::string{};
        if (conn != "changelog" && conn != "blackhole") {
            bind_error("sink " + sink.name +
                           " is append-only but the SELECT produces a changelog stream; declare "
                           "mode='upsert' (and primary_key='...'), or use connector='changelog'",
                       stmt.loc.pos);
        }
    }

    return std::make_unique<LogicalSink>(std::move(select_plan), &sink);
}

// #59: public lowering surface for the Table API. Thin forwarders onto the
// file-local helpers above (visible here in the same TU); distinct names avoid
// self-recursion. This is the single-source-of-truth seam so clink::api lowers
// expressions/predicates/types through exactly the SQL frontend's code.
namespace lowering {

clink::config::JsonValue value_expr(const ast::Expression& expr,
                                    const TableDef& source,
                                    const std::string& source_alias) {
    return lower_value_expr(expr, source, source_alias);
}

clink::config::JsonValue predicate(const ast::Expression& expr, const TableDef& source) {
    return lower_predicate(expr, source);
}

std::shared_ptr<arrow::DataType> expr_type(const ast::Expression& expr, const TableDef& source) {
    return infer_expr_type(expr, source);
}

std::vector<ProjectOutput> select_items(const std::vector<ast::SelectItem>& items,
                                        const TableDef& source,
                                        const std::string& source_alias) {
    return resolve_select_items(items, source, source_alias);
}

std::shared_ptr<arrow::DataType> aggregate_type(const std::string& fn,
                                                const TableDef& source,
                                                const std::string& input_column) {
    return aggregate_output_type(fn, source, input_column);
}

void check_sink(const TableDef& sink, const arrow::Schema& source_schema, int loc) {
    check_sink_compatibility(sink, source_schema, loc);
}

std::shared_ptr<arrow::Schema> build_group_output_schema(
    const std::vector<GroupOutputColumn>& columns,
    const std::vector<AggregateOutput>& aggregates,
    const TableDef& source) {
    arrow::FieldVector fields;
    fields.reserve(columns.size());
    for (const auto& c : columns) {
        if (c.is_aggregate) {
            const auto& agg = aggregates.at(c.agg_index);
            fields.push_back(arrow::field(agg.output_name, agg.type));
        } else if (c.is_window_bound) {
            // Synthetic window bounds (window_start / window_end) are ms-since-
            // epoch BIGINT, emitted by the runtime window op under key_output_name.
            fields.push_back(arrow::field(c.key_output_name, arrow::int64()));
        } else {
            std::shared_ptr<arrow::DataType> col_type;
            for (const auto& sc : source.columns) {
                if (sc.name == c.key_source_column) {
                    col_type = sc.type;
                    break;
                }
            }
            fields.push_back(arrow::field(c.key_output_name, col_type));
        }
    }
    return arrow::schema(fields);
}

}  // namespace lowering

}  // namespace clink::sql
