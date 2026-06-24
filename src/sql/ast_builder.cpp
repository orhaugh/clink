#include "clink/sql/ast_builder.hpp"

#include <stdexcept>
#include <string>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/sql/parser.hpp"

// JSON-to-AST translator for libpg_query output.
//
// The PG parse tree encodes node types via a single-key wrapper:
//
//   {"SelectStmt": {...}}
//   {"ColumnRef": {"fields": [{"String": {"sval": "a"}}], "location": 7}}
//
// Each translate_* helper expects the inner body (already unwrapped).
// node_wrapper() does the unwrapping and returns (kind, body). When we
// encounter a kind outside our supported subset, we throw
// TranslationError carrying the source location so callers can point
// users at the line/column.

namespace clink::sql {

using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;

namespace {

ast::Loc loc_from(const JsonValue& node) {
    if (node.is_object() && node.contains("location") && node.at("location").is_number()) {
        return ast::Loc{static_cast<int>(node.at("location").as_number())};
    }
    return {};
}

[[noreturn]] void unsupported(const std::string& what, int pos) {
    throw TranslationError("unsupported SQL construct: " + what, pos);
}

// Unwrap the single-key discriminator wrapper. Returns the (kind, body)
// pair. Throws if the wrapper isn't a single-key object.
std::pair<std::string, const JsonValue*> node_wrapper(const JsonValue& wrapper) {
    if (!wrapper.is_object()) {
        unsupported("expected node wrapper object", 0);
    }
    const auto& obj = wrapper.as_object();
    if (obj.size() != 1) {
        unsupported("node wrapper must have exactly one key", 0);
    }
    return {obj.begin()->first, &obj.begin()->second};
}

// PG encodes string atoms as {"String": {"sval": "..."}}. Helper to
// pull the underlying string out of one of these.
std::string string_atom(const JsonValue& node) {
    if (!node.is_object()) {
        unsupported("expected String atom", 0);
    }
    auto [kind, body] = node_wrapper(node);
    if (kind != "String") {
        unsupported("expected String atom, got " + kind, 0);
    }
    if (!body->contains("sval") || !body->at("sval").is_string()) {
        unsupported("String atom missing sval", 0);
    }
    return body->at("sval").as_string();
}

// --- Expression translation ----------------------------------------

ast::Expression translate_column_ref(const JsonValue& body) {
    ast::ColumnRef ref;
    ref.loc = loc_from(body);
    if (!body.contains("fields") || !body.at("fields").is_array()) {
        unsupported("ColumnRef missing fields", ref.loc.pos);
    }
    for (const auto& field : body.at("fields").as_array()) {
        auto [kind, inner] = node_wrapper(field);
        if (kind == "A_Star") {
            ref.is_star = true;
            continue;
        }
        if (kind == "String" && inner->contains("sval") && inner->at("sval").is_string()) {
            ref.parts.push_back(inner->at("sval").as_string());
            continue;
        }
        unsupported("ColumnRef field kind " + kind, ref.loc.pos);
    }
    return ast::Expression{std::move(ref)};
}

ast::Expression translate_a_const(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    // PG's A_Const carries the literal under a typed wrapper:
    //   {"ival": {"ival": 42}}      integer
    //   {"sval": {"sval": "hi"}}    string
    //   {"fval": {"fval": "1.5"}}   numeric (we reject these in Phase 1)
    //   {"boolval": {"boolval": t}} bool
    //   {"isnull": true}            null
    if (body.contains("isnull") && body.at("isnull").is_bool() && body.at("isnull").as_bool()) {
        return ast::Expression{ast::NullLiteral{loc}};
    }
    if (body.contains("ival") && body.at("ival").is_object()) {
        const auto& inner = body.at("ival");
        if (inner.contains("ival") && inner.at("ival").is_number()) {
            ast::IntLiteral lit{static_cast<std::int64_t>(inner.at("ival").as_number()), loc};
            return ast::Expression{lit};
        }
        // PG protobuf-json encoding omits default-valued scalar
        // fields, so the literal 0 arrives as {"ival": {}}. Treat
        // an empty ival wrapper as the integer zero.
        return ast::Expression{ast::IntLiteral{0, loc}};
    }
    if (body.contains("fval") && body.at("fval").is_object()) {
        // PG carries numeric literals with a fractional part as fval, a
        // decimal string (e.g. "0.5"), to preserve precision through the
        // parser. We use the value as a double.
        const auto& inner = body.at("fval");
        if (inner.contains("fval") && inner.at("fval").is_string()) {
            const std::string& raw = inner.at("fval").as_string();
            try {
                // Keep the raw token (raw) for exact-decimal lowering (#56)
                // alongside the lossy double for legacy numeric callers.
                return ast::Expression{ast::FloatLiteral{std::stod(raw), raw, loc}};
            } catch (...) {
                unsupported("unparseable numeric literal", loc.pos);
            }
        }
    }
    if (body.contains("sval") && body.at("sval").is_object()) {
        const auto& inner = body.at("sval");
        if (inner.contains("sval") && inner.at("sval").is_string()) {
            return ast::Expression{ast::StringLiteral{inner.at("sval").as_string(), loc}};
        }
    }
    if (body.contains("boolval") && body.at("boolval").is_object()) {
        const auto& inner = body.at("boolval");
        if (inner.contains("boolval") && inner.at("boolval").is_bool()) {
            return ast::Expression{ast::BoolLiteral{inner.at("boolval").as_bool(), loc}};
        }
        // PG encodes "true" boolval as {"boolval": {"boolval": true}}
        // and "false" as {"boolval": {}} (the false case drops the
        // key because protobuf json encodes default-valued fields as
        // absent). Treat empty body as false.
        return ast::Expression{ast::BoolLiteral{false, loc}};
    }
    unsupported("A_Const literal kind", loc.pos);
}

ast::Expression translate_expression(const JsonValue& wrapper);
ast::TypeName translate_type_name(const JsonValue& body);
std::optional<std::int64_t> interval_typecast_to_ms(const ast::TypeName& type,
                                                    const JsonValue& arg_wrapper);
ast::FromItem translate_from_item(const JsonValue& wrapper);
ast::SelectStmt translate_select_stmt(const JsonValue& body);
ast::TableRef translate_range_var(const JsonValue& body);

std::optional<ast::BinOp> try_parse_binop(const std::string& name) {
    if (name == "=")
        return ast::BinOp::Eq;
    if (name == "<>" || name == "!=")
        return ast::BinOp::Ne;
    if (name == "<")
        return ast::BinOp::Lt;
    if (name == "<=")
        return ast::BinOp::Le;
    if (name == ">")
        return ast::BinOp::Gt;
    if (name == ">=")
        return ast::BinOp::Ge;
    return std::nullopt;
}

std::optional<ast::ArithKind> try_parse_arithop(const std::string& name) {
    if (name == "+")
        return ast::ArithKind::Plus;
    if (name == "-")
        return ast::ArithKind::Minus;
    if (name == "*")
        return ast::ArithKind::Mul;
    if (name == "/")
        return ast::ArithKind::Div;
    if (name == "%")
        return ast::ArithKind::Mod;
    if (name == "||")
        return ast::ArithKind::Concat;
    return std::nullopt;
}

ast::Expression translate_a_expr(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    if (!body.contains("kind") || !body.at("kind").is_string()) {
        unsupported("A_Expr missing kind", loc.pos);
    }
    const auto& kind_str = body.at("kind").as_string();
    // AEXPR_BETWEEN: x BETWEEN low AND high. PG emits rexpr as a
    // List of two items. Lower into a synthetic 3-arg function call
    // 'between(x, low, high)' so the binder can pattern-match it
    // without inventing a new AST variant.
    if (kind_str == "AEXPR_BETWEEN") {
        auto fc = std::make_unique<ast::FunctionCall>();
        fc->name = "between";
        fc->loc = loc;
        if (!body.contains("lexpr") || !body.contains("rexpr")) {
            unsupported("AEXPR_BETWEEN missing operand", loc.pos);
        }
        fc->args.push_back(translate_expression(body.at("lexpr")));
        // rexpr is a List node carrying [low, high].
        auto [rkind, rbody] = node_wrapper(body.at("rexpr"));
        if (rkind != "List" || !rbody->contains("items") || !rbody->at("items").is_array() ||
            rbody->at("items").as_array().size() != 2) {
            unsupported("AEXPR_BETWEEN rexpr must be a List of two items", loc.pos);
        }
        fc->args.push_back(translate_expression(rbody->at("items").as_array()[0]));
        fc->args.push_back(translate_expression(rbody->at("items").as_array()[1]));
        return ast::Expression{std::move(fc)};
    }
    // AEXPR_LIKE: PG encodes `col LIKE 'pat'` as op-name "~~"
    // and `col NOT LIKE 'pat'` as "!~~". Lower the LIKE itself
    // into a synthetic 2-arg FunctionCall named "like" so the
    // binder's predicate lowering can pick it up; NOT LIKE wraps
    // the same call in a NotOp.
    if (kind_str == "AEXPR_LIKE") {
        if (!body.contains("lexpr") || !body.contains("rexpr") || !body.contains("name") ||
            !body.at("name").is_array() || body.at("name").as_array().empty()) {
            unsupported("AEXPR_LIKE missing operand or name", loc.pos);
        }
        const auto op_name = string_atom(body.at("name").as_array()[0]);
        auto fc = std::make_unique<ast::FunctionCall>();
        fc->name = "like";
        fc->loc = loc;
        fc->args.push_back(translate_expression(body.at("lexpr")));
        fc->args.push_back(translate_expression(body.at("rexpr")));
        if (op_name == "!~~") {
            auto neg = std::make_unique<ast::NotOp>();
            neg->loc = loc;
            neg->arg = ast::Expression{std::move(fc)};
            return ast::Expression{std::move(neg)};
        }
        return ast::Expression{std::move(fc)};
    }
    // Phase 19a: IN literal list. PG emits A_Expr{kind=AEXPR_IN,
    // name=["="]} for IN and name=["<>"] for NOT IN. rexpr is a List
    // of A_Const literals (subquery forms come through as SubLink
    // and are deferred to a later phase). Lower to a FunctionCall
    // 'in' so the binder can route through lower_predicate; NOT IN
    // wraps in NotOp.
    if (kind_str == "AEXPR_IN") {
        if (!body.contains("lexpr") || !body.contains("rexpr") || !body.contains("name") ||
            !body.at("name").is_array() || body.at("name").as_array().empty()) {
            unsupported("AEXPR_IN missing operand or name", loc.pos);
        }
        const auto op_name = string_atom(body.at("name").as_array()[0]);
        auto [rkind, rbody] = node_wrapper(body.at("rexpr"));
        if (rkind != "List" || !rbody->contains("items") || !rbody->at("items").is_array()) {
            unsupported("AEXPR_IN rexpr must be a List of items", loc.pos);
        }
        auto fc = std::make_unique<ast::FunctionCall>();
        fc->name = "in";
        fc->loc = loc;
        fc->args.push_back(translate_expression(body.at("lexpr")));
        for (const auto& item : rbody->at("items").as_array()) {
            fc->args.push_back(translate_expression(item));
        }
        if (op_name == "<>") {
            auto neg = std::make_unique<ast::NotOp>();
            neg->loc = loc;
            neg->arg = ast::Expression{std::move(fc)};
            return ast::Expression{std::move(neg)};
        }
        return ast::Expression{std::move(fc)};
    }
    // Phase 15: NULLIF(a, b) arrives as A_Expr{kind=AEXPR_NULLIF}.
    // Lower to a synthetic 2-arg FunctionCall("nullif") so the binder
    // routes it through lower_value_expr like any other function.
    if (kind_str == "AEXPR_NULLIF") {
        if (!body.contains("lexpr") || !body.contains("rexpr")) {
            unsupported("AEXPR_NULLIF missing operand", loc.pos);
        }
        auto fc = std::make_unique<ast::FunctionCall>();
        fc->name = "nullif";
        fc->loc = loc;
        fc->args.push_back(translate_expression(body.at("lexpr")));
        fc->args.push_back(translate_expression(body.at("rexpr")));
        return ast::Expression{std::move(fc)};
    }
    if (kind_str != "AEXPR_OP") {
        unsupported("A_Expr kind " + kind_str, loc.pos);
    }
    if (!body.contains("name") || !body.at("name").is_array() ||
        body.at("name").as_array().empty()) {
        unsupported("A_Expr missing operator name", loc.pos);
    }
    const auto op_name = string_atom(body.at("name").as_array()[0]);

    if (auto arith = try_parse_arithop(op_name)) {
        auto a = std::make_unique<ast::ArithOp>();
        a->op = *arith;
        a->loc = loc;
        // Unary minus has only rexpr (lexpr missing or null).
        const bool has_left = body.contains("lexpr") && !body.at("lexpr").is_null();
        const bool has_right = body.contains("rexpr") && !body.at("rexpr").is_null();
        if (*arith == ast::ArithKind::Minus && !has_left && has_right) {
            a->op = ast::ArithKind::Neg;
            a->args.push_back(translate_expression(body.at("rexpr")));
            return ast::Expression{std::move(a)};
        }
        if (!has_left || !has_right) {
            unsupported("A_Expr arithmetic missing operand", loc.pos);
        }
        a->args.push_back(translate_expression(body.at("lexpr")));
        a->args.push_back(translate_expression(body.at("rexpr")));
        return ast::Expression{std::move(a)};
    }

    auto bin_kind = try_parse_binop(op_name);
    if (!bin_kind.has_value()) {
        unsupported("unsupported binary operator '" + op_name + "'", loc.pos);
    }
    if (!body.contains("lexpr") || !body.contains("rexpr")) {
        unsupported("A_Expr missing lexpr/rexpr", loc.pos);
    }
    auto bin = std::make_unique<ast::BinaryOp>();
    bin->op = *bin_kind;
    bin->left = translate_expression(body.at("lexpr"));
    bin->right = translate_expression(body.at("rexpr"));
    bin->loc = loc;
    return ast::Expression{std::move(bin)};
}

ast::Expression translate_func_call(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    if (!body.contains("funcname") || !body.at("funcname").is_array() ||
        body.at("funcname").as_array().empty()) {
        unsupported("FuncCall missing funcname", loc.pos);
    }
    auto fc = std::make_unique<ast::FunctionCall>();
    fc->loc = loc;
    // PG normalises function names; take the last component as the
    // unqualified identifier. Schema-qualified function refs
    // (pg_catalog.upper) aren't differentiated in Phase 3.
    const auto& names = body.at("funcname").as_array();
    fc->name = string_atom(names.back());
    for (auto& c : fc->name)
        c = static_cast<char>(std::tolower(c));
    if (body.contains("args") && body.at("args").is_array()) {
        for (const auto& a : body.at("args").as_array()) {
            fc->args.push_back(translate_expression(a));
        }
    }
    // agg(DISTINCT ...): PG sets agg_distinct on the FuncCall node. We
    // carry it so the binder can lower COUNT(DISTINCT x) etc.
    fc->agg_distinct = body.contains("agg_distinct") && body.at("agg_distinct").is_bool() &&
                       body.at("agg_distinct").as_bool();
    // Within-aggregate ORDER BY (e.g. array_agg(x ORDER BY y)) arrives as
    // agg_order. We don't yet implement ordered aggregation, so reject it
    // explicitly rather than silently dropping the sort (which would make
    // the aggregate's element order quietly nondeterministic).
    if (body.contains("agg_order") && body.at("agg_order").is_array() &&
        !body.at("agg_order").as_array().empty()) {
        unsupported("aggregate ORDER BY (e.g. array_agg(x ORDER BY y)) is not supported", loc.pos);
    }
    // Phase 21b: OVER (...) makes this a windowed function call.
    // The WindowDef body is a statically-typed PG field (no node
    // wrapper). frameOptions carries PG's frame-spec encoding which
    // we ignore for ROW_NUMBER (frame is irrelevant for ranking).
    if (body.contains("over") && body.at("over").is_object()) {
        const auto& over = body.at("over");
        auto oc = std::make_unique<ast::OverClause>();
        oc->loc = loc_from(over);
        if (over.contains("partitionClause") && over.at("partitionClause").is_array()) {
            for (const auto& e : over.at("partitionClause").as_array()) {
                oc->partition_by.push_back(translate_expression(e));
            }
        }
        if (over.contains("orderClause") && over.at("orderClause").is_array()) {
            for (const auto& wrap : over.at("orderClause").as_array()) {
                auto [k, sb] = node_wrapper(wrap);
                if (k != "SortBy") {
                    unsupported("OVER orderClause entry kind " + k, oc->loc.pos);
                }
                ast::SortItem item;
                item.loc = loc_from(*sb);
                if (!sb->contains("node")) {
                    unsupported("SortBy missing node", item.loc.pos);
                }
                item.expr = translate_expression(sb->at("node"));
                const auto dir = sb->contains("sortby_dir") && sb->at("sortby_dir").is_string()
                                     ? sb->at("sortby_dir").as_string()
                                     : std::string{"SORTBY_DEFAULT"};
                if (dir == "SORTBY_DESC")
                    item.descending = true;
                else if (dir == "SORTBY_ASC" || dir == "SORTBY_DEFAULT")
                    item.descending = false;
                else
                    unsupported("sortby_dir " + dir, item.loc.pos);
                oc->order_by.push_back(std::move(item));
            }
        }
        // Window frame (Wave 7). PG always resolves a frame; the default
        // (no explicit frame clause) is the running frame
        // (UNBOUNDED PRECEDING ... CURRENT ROW). We additionally accept
        // bounded `<n> PRECEDING ... CURRENT ROW` frames in ROWS and RANGE
        // mode; everything else (FOLLOWING ends, non-CURRENT-ROW ends,
        // GROUPS, EXCLUDE) is rejected. Ranking functions use the default
        // frame and pass through unchanged.
        if (over.contains("frameOptions") && over.at("frameOptions").is_number()) {
            const auto fo = static_cast<std::int64_t>(over.at("frameOptions").as_number());
            // PG FrameOptions bits (parsenodes.h).
            constexpr std::int64_t kRange = 0x00002;
            constexpr std::int64_t kRows = 0x00004;
            constexpr std::int64_t kStartUnboundedPreceding = 0x00020;
            constexpr std::int64_t kEndCurrentRow = 0x00400;
            constexpr std::int64_t kStartOffsetPreceding = 0x00800;
            const bool end_current_row = (fo & kEndCurrentRow) != 0;
            const bool running = (fo & kStartUnboundedPreceding) != 0 && end_current_row;
            const bool bounded_start = (fo & kStartOffsetPreceding) != 0;
            if (running) {
                oc->frame_mode = ast::FrameMode::Running;
            } else if (bounded_start && end_current_row &&
                       ((fo & kRows) != 0 || (fo & kRange) != 0)) {
                oc->frame_mode = (fo & kRows) != 0 ? ast::FrameMode::Rows : ast::FrameMode::Range;
                // Decode the `<n> PRECEDING` start offset: an integer
                // A_Const (row count, or ms for RANGE), or an INTERVAL
                // typecast (ms) for RANGE-over-time.
                if (!over.contains("startOffset")) {
                    unsupported("bounded window frame missing PRECEDING offset", oc->loc.pos);
                }
                auto [ok, ob] = node_wrapper(over.at("startOffset"));
                std::optional<std::int64_t> n;
                if (ok == "A_Const" && ob->contains("ival") && ob->at("ival").is_object()) {
                    const auto& iv = ob->at("ival");
                    n = iv.contains("ival") && iv.at("ival").is_number()
                            ? static_cast<std::int64_t>(iv.at("ival").as_number())
                            : 0;  // PG omits ival for 0
                } else if (ok == "TypeCast" && ob->contains("typeName")) {
                    auto tn = translate_type_name(ob->at("typeName"));
                    if (ob->contains("arg"))
                        n = interval_typecast_to_ms(tn, ob->at("arg"));
                }
                if (!n.has_value() || *n < 0) {
                    unsupported("window frame PRECEDING offset must be a non-negative constant",
                                oc->loc.pos);
                }
                oc->frame_start_preceding = *n;
            } else {
                unsupported(
                    "window frame not supported: only UNBOUNDED PRECEDING or "
                    "<n> PRECEDING (ROWS/RANGE), ending at CURRENT ROW",
                    oc->loc.pos);
            }
        }
        fc->over_clause = std::move(oc);
    }
    return ast::Expression{std::move(fc)};
}

ast::Expression translate_coalesce_expr(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    auto fc = std::make_unique<ast::FunctionCall>();
    fc->name = "coalesce";
    fc->loc = loc;
    if (body.contains("args") && body.at("args").is_array()) {
        for (const auto& a : body.at("args").as_array()) {
            fc->args.push_back(translate_expression(a));
        }
    }
    return ast::Expression{std::move(fc)};
}

// Decode PG's INTERVAL TypeCast (e.g. INTERVAL '5' SECOND) to a raw
// integer millisecond value. PG packs the unit into the typmod's
// low 16 bits as a bitmask (HOUR=1024, MINUTE=2048, SECOND=4096,
// DAY=8); the inner A_Const carries the numeric quantity as a string.
// Returns std::nullopt when the unit isn't one we handle.
std::optional<std::int64_t> interval_typecast_to_ms(const ast::TypeName& type,
                                                    const JsonValue& arg_wrapper) {
    if (type.name != "interval")
        return std::nullopt;
    if (type.typmods.empty())
        return std::nullopt;
    const int mask = type.typmods[0] & 0xFFFF;
    std::int64_t unit_ms = 0;
    switch (mask) {
        case 1 << 12:
            unit_ms = 1000;  // SECOND
            break;
        case 1 << 11:
            unit_ms = 60 * 1000;  // MINUTE
            break;
        case 1 << 10:
            unit_ms = 60 * 60 * 1000;  // HOUR
            break;
        case 1 << 3:
            unit_ms = 24LL * 60 * 60 * 1000;  // DAY
            break;
        default:
            return std::nullopt;
    }
    // The arg under the TypeCast is the A_Const carrying the
    // numeric quantity (PG renders it as a string sval).
    auto [arg_kind, arg_body] = node_wrapper(arg_wrapper);
    if (arg_kind != "A_Const")
        return std::nullopt;
    if (!arg_body->contains("sval") || !arg_body->at("sval").is_object()) {
        return std::nullopt;
    }
    const auto& inner = arg_body->at("sval");
    if (!inner.contains("sval") || !inner.at("sval").is_string())
        return std::nullopt;
    try {
        return std::stoll(inner.at("sval").as_string()) * unit_ms;
    } catch (...) {
        return std::nullopt;
    }
}

ast::Expression translate_type_cast(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    if (!body.contains("arg") || !body.contains("typeName")) {
        unsupported("TypeCast missing arg/typeName", loc.pos);
    }
    auto type_ast = translate_type_name(body.at("typeName"));

    // Special case: INTERVAL '<n>' <unit> -> IntLiteral carrying ms.
    // The binder's window-TVF decoder doesn't need to know about
    // intervals; it just consumes IntLiteral milliseconds.
    if (auto ms = interval_typecast_to_ms(type_ast, body.at("arg"))) {
        ast::IntLiteral lit{*ms, loc};
        return ast::Expression{lit};
    }

    auto c = std::make_unique<ast::CastOp>();
    c->loc = loc;
    c->arg = translate_expression(body.at("arg"));
    // Canonical SQL type string keyed off PG's lowercase name.
    if (type_ast.name == "int8" || type_ast.name == "bigint" || type_ast.name == "int4" ||
        type_ast.name == "integer" || type_ast.name == "int" || type_ast.name == "int2" ||
        type_ast.name == "smallint") {
        c->target_type = "int";
    } else if (type_ast.name == "float8" || type_ast.name == "double" ||
               type_ast.name == "float4" || type_ast.name == "real") {
        c->target_type = "float";
    } else if (type_ast.name == "numeric") {
        // #56: CAST to DECIMAL(p,s) is exact, not the lossy float bucket.
        c->target_type = "decimal";
        c->typmods = type_ast.typmods;  // [precision, scale] (decoded by translate_type_name)
    } else if (type_ast.name == "text" || type_ast.name == "varchar" || type_ast.name == "bpchar" ||
               type_ast.name == "string") {
        c->target_type = "str";
    } else if (type_ast.name == "bool" || type_ast.name == "boolean") {
        c->target_type = "bool";
    } else {
        // Temporal (timestamp/date/time) and bytea casts need a distinct
        // runtime value representation the JSON IR doesn't yet carry
        // (timestamps flow as epoch-millis numbers); reject them precisely
        // rather than silently mis-coercing.
        unsupported("CAST to '" + type_ast.name +
                        "' not supported (supported targets: integer / float / numeric / "
                        "text / boolean)",
                    loc.pos);
    }
    return ast::Expression{std::move(c)};
}

ast::Expression translate_bool_expr(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    if (!body.contains("boolop") || !body.at("boolop").is_string()) {
        unsupported("BoolExpr missing boolop", loc.pos);
    }
    const auto& op = body.at("boolop").as_string();
    if (!body.contains("args") || !body.at("args").is_array()) {
        unsupported("BoolExpr missing args", loc.pos);
    }
    if (op == "NOT_EXPR") {
        if (body.at("args").as_array().size() != 1) {
            unsupported("NOT_EXPR must have exactly one arg", loc.pos);
        }
        auto n = std::make_unique<ast::NotOp>();
        n->arg = translate_expression(body.at("args").as_array()[0]);
        n->loc = loc;
        return ast::Expression{std::move(n)};
    }
    if (op == "AND_EXPR" || op == "OR_EXPR") {
        auto logical = std::make_unique<ast::LogicalOp>();
        logical->op = (op == "AND_EXPR") ? ast::LogicalKind::And : ast::LogicalKind::Or;
        logical->loc = loc;
        for (const auto& a : body.at("args").as_array()) {
            logical->args.push_back(translate_expression(a));
        }
        return ast::Expression{std::move(logical)};
    }
    unsupported("BoolExpr boolop '" + op + "'", loc.pos);
}

ast::Expression translate_null_test(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    if (!body.contains("arg")) {
        unsupported("NullTest missing arg", loc.pos);
    }
    auto n = std::make_unique<ast::IsNullOp>();
    n->arg = translate_expression(body.at("arg"));
    n->loc = loc;
    if (body.contains("nulltesttype") && body.at("nulltesttype").is_string()) {
        n->negated = body.at("nulltesttype").as_string() == "IS_NOT_NULL";
    }
    return ast::Expression{std::move(n)};
}

// Phase 12: CASE WHEN <expr> THEN <expr> [...] [ELSE <expr>] END.
// PG body: { "args": [ {"CaseWhen": {"expr": ..., "result": ...}}, ...],
//            "defresult": <Expression-wrapper or absent> }
// Simple-CASE form (CASE <expr> WHEN <val> THEN ...) sets PG's "arg"
// field on the body; we reject that (users can rewrite with a
// searched CASE since we don't have value-equality lowering for
// arbitrary types yet).
ast::Expression translate_case_expr(const JsonValue& body) {
    auto loc = loc_from(body);
    if (body.contains("arg")) {
        unsupported("simple-form CASE <expr> WHEN ... is not supported; use searched form",
                    loc.pos);
    }
    auto ce = std::make_unique<ast::CaseExpr>();
    ce->loc = loc;
    if (!body.contains("args") || !body.at("args").is_array()) {
        unsupported("CASE expression missing WHEN branches", loc.pos);
    }
    for (const auto& wrap : body.at("args").as_array()) {
        auto [k, b] = node_wrapper(wrap);
        if (k != "CaseWhen") {
            unsupported("CASE branch is not a CaseWhen node: " + k, loc.pos);
        }
        ast::CaseBranch branch;
        branch.loc = loc_from(*b);
        if (!b->contains("expr") || !b->contains("result")) {
            unsupported("CaseWhen missing expr or result", branch.loc.pos);
        }
        branch.when_expr = translate_expression(b->at("expr"));
        branch.then_expr = translate_expression(b->at("result"));
        ce->branches.push_back(std::move(branch));
    }
    if (ce->branches.empty()) {
        unsupported("CASE requires at least one WHEN branch", loc.pos);
    }
    if (body.contains("defresult") && !body.at("defresult").is_null()) {
        ce->else_expr = translate_expression(body.at("defresult"));
    }
    return ce;
}

// Set the IN/NOT IN test on a SubLink. A multi-column LHS arrives as a
// RowExpr `(a, b)` -> its elements go to test_exprs (composite IN); a
// single LHS goes to test_expr.
void set_in_test_(ast::SubLink& sl, const JsonValue& testexpr) {
    auto [kind, b] = node_wrapper(testexpr);
    if (kind == "RowExpr" && b->contains("args") && b->at("args").is_array()) {
        for (const auto& a : b->at("args").as_array()) {
            sl.test_exprs.push_back(translate_expression(a));
        }
        return;
    }
    sl.test_expr = translate_expression(testexpr);
}

// PG SubLink: IN / NOT IN / EXISTS / scalar subqueries. We map the
// supported forms; the binder rewrites them to semi/anti joins or a
// scalar broadcast. `x IN (sub)` is ANY_SUBLINK with operator "=";
// `x NOT IN (sub)` is ALL_SUBLINK with operator "<>".
ast::Expression translate_sublink(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    auto sl = std::make_unique<ast::SubLink>();
    sl->loc = loc;
    const auto sub_type = body.contains("subLinkType") && body.at("subLinkType").is_string()
                              ? body.at("subLinkType").as_string()
                              : std::string{};
    auto op_name = [&]() -> std::string {
        if (body.contains("operName") && body.at("operName").is_array() &&
            !body.at("operName").as_array().empty()) {
            return string_atom(body.at("operName").as_array()[0]);
        }
        return std::string{};
    };
    if (sub_type == "EXISTS_SUBLINK") {
        sl->kind = ast::SubLink::Kind::Exists;
    } else if (sub_type == "EXPR_SUBLINK") {
        sl->kind = ast::SubLink::Kind::Scalar;
    } else if (sub_type == "ANY_SUBLINK") {
        // `x IN (sub)` is `x = ANY (sub)`. PG may leave operName implicit;
        // accept "=" or an empty operator, reject other `op ANY` forms.
        const auto op = op_name();
        if (!op.empty() && op != "=") {
            unsupported("only the `= ANY` (IN) form of ANY-subquery is supported", loc.pos);
        }
        sl->kind = ast::SubLink::Kind::In;
        if (!body.contains("testexpr")) {
            unsupported("IN-subquery missing test expression", loc.pos);
        }
        set_in_test_(*sl, body.at("testexpr"));
    } else if (sub_type == "ALL_SUBLINK") {
        // `x NOT IN (sub)` is `x <> ALL (sub)`.
        const auto op = op_name();
        if (!op.empty() && op != "<>") {
            unsupported("only the `<> ALL` (NOT IN) form of ALL-subquery is supported", loc.pos);
        }
        sl->kind = ast::SubLink::Kind::NotIn;
        if (!body.contains("testexpr")) {
            unsupported("NOT IN-subquery missing test expression", loc.pos);
        }
        set_in_test_(*sl, body.at("testexpr"));
    } else {
        unsupported("subquery form " + sub_type, loc.pos);
    }
    if (!body.contains("subselect")) {
        unsupported("SubLink missing subselect", loc.pos);
    }
    auto [sk, sb] = node_wrapper(body.at("subselect"));
    if (sk != "SelectStmt") {
        unsupported("subquery body must be a SelectStmt: " + sk, loc.pos);
    }
    sl->subselect = std::make_unique<ast::SelectStmt>(translate_select_stmt(*sb));
    return ast::Expression{std::move(sl)};
}

// PG A_ArrayExpr: ARRAY[e1, e2, ...] (Wave 5). The `elements` key is
// absent for an empty `ARRAY[]`. Element expressions recurse.
ast::Expression translate_array_expr(const JsonValue& body) {
    auto arr = std::make_unique<ast::ArrayLiteral>();
    arr->loc = loc_from(body);
    if (body.contains("elements") && body.at("elements").is_array()) {
        for (const auto& e : body.at("elements").as_array()) {
            arr->elements.push_back(translate_expression(e));
        }
    }
    return ast::Expression{std::move(arr)};
}

// PG A_Indirection: array subscript `arg[idx]`, field access `(r).f`,
// and chains/mixes of the two (`(r).a[1]`, `m['k']['j']`) (Wave 5/5c).
// Each `indirection` entry is either an A_Indices (subscript -> Subscript)
// or a String (field name -> FieldAccess); we chain them left-to-right.
// Slices (`a[i:j]`) and field expansion (`(r).*`, an A_Star) are rejected
// with a clear message rather than silently mis-lowered.
ast::Expression translate_indirection(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    if (!body.contains("arg")) {
        unsupported("A_Indirection missing base expression", loc.pos);
    }
    ast::Expression current = translate_expression(body.at("arg"));
    if (!body.contains("indirection") || !body.at("indirection").is_array()) {
        unsupported("A_Indirection missing subscript list", loc.pos);
    }
    for (const auto& item : body.at("indirection").as_array()) {
        auto [ik, ib] = node_wrapper(item);
        if (ik == "A_Indices") {
            if (ib->contains("is_slice") && ib->at("is_slice").is_bool() &&
                ib->at("is_slice").as_bool()) {
                unsupported("array slice access (a[i:j]) is not supported", loc.pos);
            }
            if (!ib->contains("uidx")) {
                unsupported("array subscript missing index expression", loc.pos);
            }
            auto sub = std::make_unique<ast::Subscript>();
            sub->loc = loc;
            sub->base = std::move(current);
            sub->index = translate_expression(ib->at("uidx"));
            current = ast::Expression{std::move(sub)};
            continue;
        }
        if (ik == "String") {
            // (r).f field access. The element is a bare String atom
            // {"String":{"sval":"f"}}; string_atom unwraps it.
            auto fa = std::make_unique<ast::FieldAccess>();
            fa->loc = loc;
            fa->base = std::move(current);
            fa->field = string_atom(item);
            current = ast::Expression{std::move(fa)};
            continue;
        }
        if (ik == "A_Star") {
            unsupported("field expansion (r).* is not supported", loc.pos);
        }
        unsupported("unsupported indirection element: " + ik, loc.pos);
    }
    return current;
}

// PG RowExpr: ROW(e1, e2, ...) row constructor (Wave 5c). Only the
// explicit ROW(...) spelling (row_format=COERCE_EXPLICIT_CALL) builds a
// RowConstructor; the implicit (a, b) form (COERCE_IMPLICIT_CAST) is
// rejected here because it collides with the multi-column IN paren-list
// (`(a,b) IN (...)`), which set_in_test_ intercepts before this point.
// PG attaches optional field names via `colnames` (a String per arg, or
// absent); we carry them and let the binder derive any that are missing.
ast::Expression translate_row_expr(const JsonValue& body) {
    ast::Loc loc = loc_from(body);
    const std::string fmt = body.contains("row_format") && body.at("row_format").is_string()
                                ? body.at("row_format").as_string()
                                : std::string{};
    if (fmt != "COERCE_EXPLICIT_CALL") {
        unsupported(
            "use ROW(...) to construct a row; the implicit (a, b) form is only "
            "allowed as the left side of an IN subquery",
            loc.pos);
    }
    auto rc = std::make_unique<ast::RowConstructor>();
    rc->loc = loc;
    if (body.contains("args") && body.at("args").is_array()) {
        for (const auto& a : body.at("args").as_array()) {
            rc->fields.push_back(translate_expression(a));
        }
    }
    if (body.contains("colnames") && body.at("colnames").is_array()) {
        for (const auto& cn : body.at("colnames").as_array()) {
            if (cn.is_null()) {
                rc->field_names.emplace_back(std::nullopt);
            } else {
                rc->field_names.emplace_back(string_atom(cn));
            }
        }
    }
    rc->field_names.resize(rc->fields.size(), std::nullopt);
    return ast::Expression{std::move(rc)};
}

ast::Expression translate_expression(const JsonValue& wrapper) {
    auto [kind, body] = node_wrapper(wrapper);
    if (kind == "ColumnRef") {
        return translate_column_ref(*body);
    }
    if (kind == "A_ArrayExpr") {
        return translate_array_expr(*body);
    }
    if (kind == "A_Indirection") {
        return translate_indirection(*body);
    }
    if (kind == "RowExpr") {
        return translate_row_expr(*body);
    }
    if (kind == "SubLink") {
        return translate_sublink(*body);
    }
    if (kind == "A_Const") {
        return translate_a_const(*body);
    }
    if (kind == "A_Expr") {
        return translate_a_expr(*body);
    }
    if (kind == "BoolExpr") {
        return translate_bool_expr(*body);
    }
    if (kind == "NullTest") {
        return translate_null_test(*body);
    }
    if (kind == "FuncCall") {
        return translate_func_call(*body);
    }
    if (kind == "CoalesceExpr") {
        return translate_coalesce_expr(*body);
    }
    if (kind == "TypeCast") {
        return translate_type_cast(*body);
    }
    if (kind == "CaseExpr") {
        return translate_case_expr(*body);
    }
    // Phase 15: GREATEST(...) / LEAST(...) arrive as MinMaxExpr with
    // op = IS_GREATEST | IS_LEAST. Lower to a variadic FunctionCall.
    if (kind == "MinMaxExpr") {
        auto fc = std::make_unique<ast::FunctionCall>();
        fc->loc = loc_from(*body);
        const auto op = body->contains("op") && body->at("op").is_string()
                            ? body->at("op").as_string()
                            : std::string{};
        if (op == "IS_LEAST") {
            fc->name = "least";
        } else if (op == "IS_GREATEST" || op.empty()) {
            fc->name = "greatest";
        } else {
            unsupported("MinMaxExpr op " + op, fc->loc.pos);
        }
        if (body->contains("args") && body->at("args").is_array()) {
            for (const auto& a : body->at("args").as_array()) {
                fc->args.push_back(translate_expression(a));
            }
        }
        return ast::Expression{std::move(fc)};
    }
    unsupported("expression kind " + kind, loc_from(*body).pos);
}

// --- Statement-component translation -------------------------------

ast::TableRef translate_range_var(const JsonValue& body) {
    ast::TableRef ref;
    ref.loc = loc_from(body);
    if (!body.contains("relname") || !body.at("relname").is_string()) {
        unsupported("RangeVar missing relname", ref.loc.pos);
    }
    ref.name = body.at("relname").as_string();
    if (body.contains("schemaname") && body.at("schemaname").is_string()) {
        ref.schema = body.at("schemaname").as_string();
    }
    if (body.contains("alias") && body.at("alias").is_object()) {
        // PG omits the {"Alias": ...} wrapper since alias is always
        // an Alias node (statically typed field). Read aliasname
        // straight from the body.
        const auto& alias_body = body.at("alias");
        if (alias_body.contains("aliasname") && alias_body.at("aliasname").is_string()) {
            ref.alias = alias_body.at("aliasname").as_string();
        }
    }
    return ref;
}

// --- FROM-item translation (Phase 5: JOIN support) -----------------

ast::FromItem translate_from_item(const JsonValue& wrapper) {
    auto [kind, body] = node_wrapper(wrapper);
    if (kind == "RangeVar") {
        return translate_range_var(*body);
    }
    if (kind == "JoinExpr") {
        ast::Loc loc = loc_from(*body);
        auto join = std::make_unique<ast::JoinClause>();
        join->loc = loc;
        // jointype: JOIN_INNER / JOIN_LEFT / JOIN_RIGHT / JOIN_FULL
        if (body->contains("jointype") && body->at("jointype").is_string()) {
            const auto& jt = body->at("jointype").as_string();
            if (jt == "JOIN_INNER")
                join->kind = ast::JoinKind::Inner;
            else if (jt == "JOIN_LEFT")
                join->kind = ast::JoinKind::Left;
            else if (jt == "JOIN_RIGHT")
                join->kind = ast::JoinKind::Right;
            else if (jt == "JOIN_FULL")
                join->kind = ast::JoinKind::Full;
            else
                unsupported("join type " + jt, loc.pos);
        }
        if (!body->contains("larg") || !body->contains("rarg")) {
            unsupported("JoinExpr missing larg/rarg", loc.pos);
        }
        join->left = translate_from_item(body->at("larg"));
        join->right = translate_from_item(body->at("rarg"));
        if (body->contains("quals") && !body->at("quals").is_null()) {
            join->on_clause = translate_expression(body->at("quals"));
        }
        return ast::FromItem{std::move(join)};
    }
    if (kind == "RangeSubselect") {
        // Phase 20: FROM (SELECT ...) AS alias. PG requires an alias
        // and we surface a friendly error if it's missing.
        auto loc = loc_from(*body);
        auto sq = std::make_unique<ast::SubqueryItem>();
        sq->loc = loc;
        if (!body->contains("subquery")) {
            unsupported("RangeSubselect missing subquery", loc.pos);
        }
        auto [sk, sb] = node_wrapper(body->at("subquery"));
        if (sk != "SelectStmt") {
            unsupported("derived-table body must be a SelectStmt: " + sk, loc.pos);
        }
        sq->body = std::make_unique<ast::SelectStmt>(translate_select_stmt(*sb));
        if (!body->contains("alias") || !body->at("alias").is_object() ||
            !body->at("alias").contains("aliasname") ||
            !body->at("alias").at("aliasname").is_string()) {
            unsupported("derived table requires an alias (use 'AS sub_name')", loc.pos);
        }
        sq->alias = body->at("alias").at("aliasname").as_string();
        return ast::FromItem{std::move(sq)};
    }
    unsupported("FROM-clause source kind " + kind, 0);
}

ast::TypeName translate_type_name(const JsonValue& body) {
    ast::TypeName type;
    type.loc = loc_from(body);
    if (!body.contains("names") || !body.at("names").is_array()) {
        unsupported("TypeName missing names", type.loc.pos);
    }
    const auto& names = body.at("names").as_array();
    if (names.empty()) {
        unsupported("TypeName has empty names", type.loc.pos);
    }
    // PG encodes types as [schema, name] or just [name]. The last
    // entry is always the type identifier; everything before it is
    // the namespace path. We collapse to (schema, name) since clink
    // doesn't model multi-level type namespaces in Phase 1.
    if (names.size() == 1) {
        type.name = string_atom(names[0]);
    } else if (names.size() == 2) {
        type.schema = string_atom(names[0]);
        type.name = string_atom(names[1]);
    } else {
        unsupported("multi-level type namespace not supported", type.loc.pos);
    }
    if (body.contains("typmods") && body.at("typmods").is_array()) {
        for (const auto& mod : body.at("typmods").as_array()) {
            // PG encodes typmods as A_Const integer wrappers, e.g.
            // TIMESTAMP(3) -> [{"A_Const": {"ival": {"ival": 3}}}].
            auto [kind, mod_body] = node_wrapper(mod);
            if (kind != "A_Const") {
                unsupported("typmod must be A_Const, got " + kind, type.loc.pos);
            }
            if (mod_body->contains("ival") && mod_body->at("ival").is_object()) {
                const auto& inner = mod_body->at("ival");
                if (inner.contains("ival") && inner.at("ival").is_number()) {
                    type.typmods.push_back(static_cast<int>(inner.at("ival").as_number()));
                    continue;
                }
            }
            unsupported("typmod expected integer", type.loc.pos);
        }
    }
    // Array column types (Wave 5): `INT[]` / `INT ARRAY` carry a
    // non-empty `arrayBounds`; its length is the dimensionality. We
    // ignore the bound values (PG uses -1 for unbounded) since clink
    // arrays are variable-length lists.
    if (body.contains("arrayBounds") && body.at("arrayBounds").is_array()) {
        type.array_ndims = static_cast<int>(body.at("arrayBounds").as_array().size());
    }
    return type;
}

ast::ColumnDef translate_column_def(const JsonValue& body) {
    ast::ColumnDef def;
    def.loc = loc_from(body);
    if (!body.contains("colname") || !body.at("colname").is_string()) {
        unsupported("ColumnDef missing colname", def.loc.pos);
    }
    def.name = body.at("colname").as_string();
    if (!body.contains("typeName") || !body.at("typeName").is_object()) {
        unsupported("ColumnDef missing typeName", def.loc.pos);
    }
    // PG omits the {"TypeName": ...} wrapper here because the field
    // is statically typed (typeName ALWAYS holds a TypeName). Apply
    // the body directly. Same convention applies to CreateStmt /
    // InsertStmt .relation (always RangeVar).
    def.type = translate_type_name(body.at("typeName"));
    return def;
}

ast::StorageOption translate_storage_option(const JsonValue& def_elem_body) {
    ast::StorageOption opt;
    opt.loc = loc_from(def_elem_body);
    if (!def_elem_body.contains("defname") || !def_elem_body.at("defname").is_string()) {
        unsupported("DefElem missing defname", opt.loc.pos);
    }
    opt.key = def_elem_body.at("defname").as_string();
    if (!def_elem_body.contains("arg")) {
        unsupported("CREATE TABLE storage option requires a value", opt.loc.pos);
    }
    // Phase 1 scope: only string values are supported (every Kafka /
    // file-sink / Parquet config knob is string-typed at the wire).
    opt.value = string_atom(def_elem_body.at("arg"));
    return opt;
}

ast::SelectItem translate_res_target(const JsonValue& body) {
    ast::SelectItem item;
    item.loc = loc_from(body);
    if (!body.contains("val")) {
        unsupported("ResTarget missing val", item.loc.pos);
    }
    item.expr = translate_expression(body.at("val"));
    if (body.contains("name") && body.at("name").is_string()) {
        item.alias = body.at("name").as_string();
    }
    return item;
}

// --- Statement translation -----------------------------------------

ast::SelectStmt translate_select_stmt(const JsonValue& body) {
    ast::SelectStmt stmt;
    stmt.loc = loc_from(body);

    // Phase 16: WITH clause. PG emits withClause = {recursive, ctes}.
    // RECURSIVE is rejected; each CTE has ctename (string) and
    // ctequery (a SelectStmt). The CTE bodies are translated as full
    // SelectStmts up-front so the binder can pre-bind them.
    if (body.contains("withClause") && body.at("withClause").is_object()) {
        const auto& wc = body.at("withClause");
        if (wc.contains("recursive") && wc.at("recursive").is_bool() &&
            wc.at("recursive").as_bool()) {
            unsupported("WITH RECURSIVE is not supported", stmt.loc.pos);
        }
        if (wc.contains("ctes") && wc.at("ctes").is_array()) {
            for (const auto& wrap : wc.at("ctes").as_array()) {
                auto [k, cbody] = node_wrapper(wrap);
                if (k != "CommonTableExpr") {
                    unsupported("withClause entry kind " + k, stmt.loc.pos);
                }
                ast::CommonTableExpr cte;
                cte.loc = loc_from(*cbody);
                if (!cbody->contains("ctename") || !cbody->at("ctename").is_string()) {
                    unsupported("CommonTableExpr missing ctename", cte.loc.pos);
                }
                cte.name = cbody->at("ctename").as_string();
                if (!cbody->contains("ctequery")) {
                    unsupported("CommonTableExpr missing ctequery", cte.loc.pos);
                }
                // ctequery wrapper is a {"SelectStmt": {...}} node.
                auto [qk, qbody] = node_wrapper(cbody->at("ctequery"));
                if (qk != "SelectStmt") {
                    unsupported("CTE body must be a SelectStmt: " + qk, cte.loc.pos);
                }
                cte.body = std::make_unique<ast::SelectStmt>(translate_select_stmt(*qbody));
                stmt.with_clause.push_back(std::move(cte));
            }
        }
    }

    // Phase 13: set-op SELECT (UNION / INTERSECT / EXCEPT). PG marks
    // the outer SelectStmt with op != "SETOP_NONE" and emits the
    // branches under bare larg/rarg fields (no node wrapper because
    // they're statically-typed SelectStmt fields).
    if (body.contains("op") && body.at("op").is_string()) {
        const auto& op_str = body.at("op").as_string();
        const bool all =
            body.contains("all") && body.at("all").is_bool() && body.at("all").as_bool();
        auto set_branches = [&](const char* name) {
            if (!body.contains("larg") || !body.contains("rarg")) {
                unsupported(std::string{name} + " missing larg/rarg in parse tree", stmt.loc.pos);
            }
            stmt.larg = std::make_unique<ast::SelectStmt>(translate_select_stmt(body.at("larg")));
            stmt.rarg = std::make_unique<ast::SelectStmt>(translate_select_stmt(body.at("rarg")));
        };
        if (op_str == "SETOP_UNION") {
            stmt.set_op = all ? ast::SelectSetOp::UnionAll : ast::SelectSetOp::UnionDistinct;
            set_branches("UNION");
            return stmt;
        }
        if (op_str == "SETOP_INTERSECT") {
            stmt.set_op = all ? ast::SelectSetOp::IntersectAll : ast::SelectSetOp::Intersect;
            set_branches("INTERSECT");
            return stmt;
        }
        if (op_str == "SETOP_EXCEPT") {
            stmt.set_op = all ? ast::SelectSetOp::ExceptAll : ast::SelectSetOp::Except;
            set_branches("EXCEPT");
            return stmt;
        }
    }

    if (body.contains("targetList") && body.at("targetList").is_array()) {
        for (const auto& wrapper : body.at("targetList").as_array()) {
            auto [kind, inner] = node_wrapper(wrapper);
            if (kind != "ResTarget") {
                unsupported("targetList entry kind " + kind, stmt.loc.pos);
            }
            stmt.target_list.push_back(translate_res_target(*inner));
        }
    }
    if (body.contains("fromClause") && body.at("fromClause").is_array()) {
        for (const auto& wrapper : body.at("fromClause").as_array()) {
            stmt.from_items.push_back(translate_from_item(wrapper));
            // Also populate from_clause for the simple single-table
            // path that the binder still uses when from_items has
            // no joins. Mirrors existing Phase 1-4 access patterns.
            if (std::holds_alternative<ast::TableRef>(stmt.from_items.back())) {
                stmt.from_clause.push_back(std::get<ast::TableRef>(stmt.from_items.back()));
            }
        }
    }
    if (body.contains("whereClause") && !body.at("whereClause").is_null()) {
        stmt.where_clause = translate_expression(body.at("whereClause"));
    }
    if (body.contains("groupClause") && body.at("groupClause").is_array()) {
        for (const auto& g : body.at("groupClause").as_array()) {
            stmt.group_clause.push_back(translate_expression(g));
        }
    }
    if (body.contains("havingClause") && !body.at("havingClause").is_null()) {
        stmt.having_clause = translate_expression(body.at("havingClause"));
    }
    // Phase 17: ORDER BY. Each entry is a SortBy node carrying a
    // sort expression and direction. sortby_dir is one of
    // SORTBY_ASC / SORTBY_DESC / SORTBY_DEFAULT (treated as ASC).
    // The binder validates that the sort expr is a column ref.
    if (body.contains("sortClause") && body.at("sortClause").is_array()) {
        for (const auto& wrap : body.at("sortClause").as_array()) {
            auto [k, sb] = node_wrapper(wrap);
            if (k != "SortBy") {
                unsupported("sortClause entry kind " + k, stmt.loc.pos);
            }
            ast::SortItem item;
            item.loc = loc_from(*sb);
            if (!sb->contains("node")) {
                unsupported("SortBy missing node", item.loc.pos);
            }
            item.expr = translate_expression(sb->at("node"));
            const auto dir = sb->contains("sortby_dir") && sb->at("sortby_dir").is_string()
                                 ? sb->at("sortby_dir").as_string()
                                 : std::string{"SORTBY_DEFAULT"};
            if (dir == "SORTBY_DESC")
                item.descending = true;
            else if (dir == "SORTBY_ASC" || dir == "SORTBY_DEFAULT")
                item.descending = false;
            else
                unsupported("sortby_dir " + dir, item.loc.pos);
            stmt.sort_clause.push_back(std::move(item));
        }
    }

    // Phase 11: LIMIT n. PG sends limitCount as a wrapped A_Const
    // (or ParamRef). We support integer literals only - bind expr
    // LIMITs and parameterised LIMITs aren't streamable today.
    if (body.contains("limitCount") && !body.at("limitCount").is_null()) {
        const auto& lc = body.at("limitCount");
        std::int64_t value = 0;
        bool ok = false;
        if (lc.is_object() && lc.as_object().size() == 1) {
            const auto& [k, v] = *lc.as_object().begin();
            if (k == "A_Const" && v.is_object()) {
                const auto& av = v.contains("val") ? v.at("val") : v;
                if (av.is_object() && av.contains("ival") && av.at("ival").is_object() &&
                    av.at("ival").contains("ival")) {
                    value = static_cast<std::int64_t>(av.at("ival").at("ival").as_number());
                    ok = true;
                } else if (av.is_object() && av.contains("Integer") &&
                           av.at("Integer").is_object() && av.at("Integer").contains("ival")) {
                    value = static_cast<std::int64_t>(av.at("Integer").at("ival").as_number());
                    ok = true;
                }
            }
        }
        if (!ok) {
            unsupported("LIMIT must be a non-negative integer literal", stmt.loc.pos);
        }
        if (value < 0) {
            unsupported("LIMIT must be a non-negative integer literal", stmt.loc.pos);
        }
        stmt.limit_count = value;
    }
    // Phase 19b: OFFSET n. Same A_Const integer-literal shape as
    // LIMIT. Expression / ParamRef offsets are rejected for the same
    // streaming-safety reason.
    if (body.contains("limitOffset") && !body.at("limitOffset").is_null()) {
        const auto& lo = body.at("limitOffset");
        std::int64_t value = 0;
        bool ok = false;
        if (lo.is_object() && lo.as_object().size() == 1) {
            const auto& [k, v] = *lo.as_object().begin();
            if (k == "A_Const" && v.is_object()) {
                const auto& av = v.contains("val") ? v.at("val") : v;
                if (av.is_object() && av.contains("ival") && av.at("ival").is_object() &&
                    av.at("ival").contains("ival")) {
                    value = static_cast<std::int64_t>(av.at("ival").at("ival").as_number());
                    ok = true;
                } else if (av.is_object() && av.contains("ival") && av.at("ival").is_object() &&
                           av.at("ival").as_object().empty()) {
                    value = 0;
                    ok = true;
                }
            }
        }
        if (!ok || value < 0) {
            unsupported("OFFSET must be a non-negative integer literal", stmt.loc.pos);
        }
        stmt.offset_count = value;
    }
    // Phase 10: PG emits a non-empty distinctClause array for any
    // DISTINCT form. SELECT DISTINCT ON (...) sends column refs in
    // that array; we only support plain SELECT DISTINCT for now.
    if (body.contains("distinctClause") && body.at("distinctClause").is_array() &&
        !body.at("distinctClause").as_array().empty()) {
        const auto& arr = body.at("distinctClause").as_array();
        for (const auto& entry : arr) {
            if (entry.is_object() && !entry.as_object().empty()) {
                unsupported("SELECT DISTINCT ON (...) is not supported yet", stmt.loc.pos);
            }
        }
        stmt.distinct = true;
    }
    return stmt;
}

ast::CreateTableStmt translate_create_stmt(const JsonValue& body) {
    ast::CreateTableStmt stmt;
    stmt.loc = loc_from(body);
    if (!body.contains("relation") || !body.at("relation").is_object()) {
        unsupported("CreateStmt missing relation", stmt.loc.pos);
    }
    // relation is a statically-typed RangeVar field; no node wrapper.
    auto table_ref = translate_range_var(body.at("relation"));
    stmt.table_name = std::move(table_ref.name);
    stmt.schema = std::move(table_ref.schema);
    if (body.contains("if_not_exists") && body.at("if_not_exists").is_bool()) {
        stmt.if_not_exists = body.at("if_not_exists").as_bool();
    }
    if (body.contains("tableElts") && body.at("tableElts").is_array()) {
        for (const auto& wrapper : body.at("tableElts").as_array()) {
            auto [kind, inner] = node_wrapper(wrapper);
            if (kind != "ColumnDef") {
                // Constraints / indexes etc. come in later phases.
                unsupported("table element kind " + kind, stmt.loc.pos);
            }
            stmt.columns.push_back(translate_column_def(*inner));
        }
    }
    if (body.contains("options") && body.at("options").is_array()) {
        for (const auto& wrapper : body.at("options").as_array()) {
            auto [kind, inner] = node_wrapper(wrapper);
            if (kind != "DefElem") {
                unsupported("CREATE TABLE options entry kind " + kind, stmt.loc.pos);
            }
            stmt.options.push_back(translate_storage_option(*inner));
        }
    }
    return stmt;
}

ast::InsertStmt translate_insert_stmt(const JsonValue& body) {
    ast::InsertStmt stmt;
    stmt.loc = loc_from(body);
    if (!body.contains("relation") || !body.at("relation").is_object()) {
        unsupported("InsertStmt missing relation", stmt.loc.pos);
    }
    // relation is a statically-typed RangeVar field; no node wrapper.
    stmt.target = translate_range_var(body.at("relation"));
    if (!body.contains("selectStmt") || !body.at("selectStmt").is_object()) {
        // INSERT VALUES (without SELECT) is rejected in Phase 1; the
        // streaming case is INSERT INTO sink SELECT FROM source.
        unsupported("INSERT requires a SELECT source", stmt.loc.pos);
    }
    auto [sel_kind, sel_body] = node_wrapper(body.at("selectStmt"));
    if (sel_kind != "SelectStmt") {
        unsupported("INSERT selectStmt kind " + sel_kind, stmt.loc.pos);
    }
    stmt.select = translate_select_stmt(*sel_body);
    // Phase 19d: optional column list. PG emits cols as an array of
    // ResTarget nodes whose `name` field carries the target column
    // name. (PG also allows multi-part names for nested fields; we
    // accept simple identifiers only.)
    if (body.contains("cols") && body.at("cols").is_array()) {
        for (const auto& wrap : body.at("cols").as_array()) {
            auto [k, rt] = node_wrapper(wrap);
            if (k != "ResTarget") {
                unsupported("INSERT cols entry kind " + k, stmt.loc.pos);
            }
            if (!rt->contains("name") || !rt->at("name").is_string()) {
                unsupported("INSERT column-list entry missing name", stmt.loc.pos);
            }
            stmt.column_list.push_back(rt->at("name").as_string());
        }
    }
    return stmt;
}

ast::DropTableStmt translate_drop_stmt(const JsonValue& body) {
    ast::DropTableStmt stmt;
    stmt.loc = loc_from(body);
    // PG's DropStmt covers many object kinds (TABLE, INDEX, etc).
    // We support TABLE only.
    if (!body.contains("removeType") || !body.at("removeType").is_string()) {
        unsupported("DropStmt missing removeType", stmt.loc.pos);
    }
    if (body.at("removeType").as_string() != "OBJECT_TABLE") {
        unsupported(
            "only DROP TABLE is supported in Phase 2; got " + body.at("removeType").as_string(),
            stmt.loc.pos);
    }
    if (body.contains("missing_ok") && body.at("missing_ok").is_bool()) {
        stmt.if_exists = body.at("missing_ok").as_bool();
    }
    if (!body.contains("objects") || !body.at("objects").is_array() ||
        body.at("objects").as_array().empty()) {
        unsupported("DropStmt missing objects", stmt.loc.pos);
    }
    // DROP TABLE a, b, c: each object is a List naming one (possibly
    // schema-qualified) table; collect them in order.
    for (const auto& object : body.at("objects").as_array()) {
        auto [list_kind, list_body] = node_wrapper(object);
        if (list_kind != "List") {
            unsupported("DropStmt object must be a List, got " + list_kind, stmt.loc.pos);
        }
        if (!list_body->contains("items") || !list_body->at("items").is_array() ||
            list_body->at("items").as_array().empty()) {
            unsupported("DropStmt object list is empty", stmt.loc.pos);
        }
        const auto& items = list_body->at("items").as_array();
        if (items.size() == 1) {
            stmt.table_names.push_back(string_atom(items[0]));
        } else if (items.size() == 2) {
            // schema.table - we ignore the schema for now; the catalog
            // is a single flat namespace.
            stmt.table_names.push_back(string_atom(items[1]));
        } else {
            unsupported("DROP TABLE: unsupported qualified-name length", stmt.loc.pos);
        }
    }
    return stmt;
}

ast::ShowTablesStmt translate_show_stmt(const JsonValue& body) {
    ast::ShowTablesStmt stmt;
    stmt.loc = loc_from(body);
    // VariableShowStmt is PG's catchall for SHOW <name>. We accept
    // SHOW TABLES (matches MySQL-style listing) and reject other
    // SHOW variants which would otherwise silently return nothing.
    if (!body.contains("name") || !body.at("name").is_string()) {
        unsupported("VariableShowStmt missing name", stmt.loc.pos);
    }
    if (body.at("name").as_string() != "tables") {
        unsupported("only SHOW TABLES is supported; got SHOW " + body.at("name").as_string(),
                    stmt.loc.pos);
    }
    return stmt;
}

// MATTBL: CREATE MATERIALIZED VIEW <name> WITH (...) AS <SELECT>. libpg_query
// emits this as a CreateTableAsStmt carrying objtype=OBJECT_MATVIEW, the WITH
// options under into.options (DefElem list, identical shape to CREATE TABLE
// WITH), the view name under into.rel (a RangeVar), and the defining query
// under the top-level `query` (a SelectStmt wrapper). The non-matview siblings
// of CreateTableAsStmt (plain CREATE TABLE AS SELECT, SELECT INTO) are rejected
// so they do not silently fall into the matview path.
ast::CreateMaterializedViewStmt translate_matview_stmt(const JsonValue& body) {
    ast::CreateMaterializedViewStmt stmt;
    stmt.loc = loc_from(body);
    const std::string objtype = body.contains("objtype") && body.at("objtype").is_string()
                                    ? body.at("objtype").as_string()
                                    : std::string{};
    if (objtype != "OBJECT_MATVIEW") {
        unsupported("CREATE TABLE AS / SELECT INTO (only CREATE MATERIALIZED VIEW is supported)",
                    stmt.loc.pos);
    }
    if (!body.contains("into") || !body.at("into").is_object()) {
        unsupported("CreateTableAsStmt missing into clause", stmt.loc.pos);
    }
    const auto& into = body.at("into");
    if (!into.contains("rel") || !into.at("rel").is_object()) {
        unsupported("materialized view missing target relation", stmt.loc.pos);
    }
    auto rel = translate_range_var(into.at("rel"));
    stmt.view_name = std::move(rel.name);
    stmt.schema = std::move(rel.schema);
    if (into.contains("options") && into.at("options").is_array()) {
        for (const auto& wrapper : into.at("options").as_array()) {
            auto [okind, inner] = node_wrapper(wrapper);
            if (okind != "DefElem") {
                unsupported("materialized view options entry kind " + okind, stmt.loc.pos);
            }
            stmt.options.push_back(translate_storage_option(*inner));
        }
    }
    if (!body.contains("query") || !body.at("query").is_object()) {
        unsupported("materialized view missing defining query", stmt.loc.pos);
    }
    auto [qkind, qbody] = node_wrapper(body.at("query"));
    if (qkind != "SelectStmt") {
        unsupported("materialized view defining query kind " + qkind, stmt.loc.pos);
    }
    stmt.query = translate_select_stmt(*qbody);
    return stmt;
}

// ANALYZE [<col>...] <table> parses as a PG VacuumStmt. We support the ANALYZE
// form on a single table; VACUUM has no clink meaning and is rejected. The
// optional `TABLE` keyword in the Flink-style spelling is stripped by the
// pre-parser before this point.
ast::AnalyzeStmt translate_vacuum_stmt(const JsonValue& body) {
    ast::AnalyzeStmt stmt;
    stmt.loc = loc_from(body);
    if (body.contains("is_vacuumcmd") && body.at("is_vacuumcmd").is_bool() &&
        body.at("is_vacuumcmd").as_bool()) {
        unsupported("VACUUM is not supported; use ANALYZE <table>", stmt.loc.pos);
    }
    if (!body.contains("rels") || !body.at("rels").is_array() ||
        body.at("rels").as_array().empty()) {
        unsupported("ANALYZE requires a table name", stmt.loc.pos);
    }
    const auto& rels = body.at("rels").as_array();
    if (rels.size() != 1) {
        unsupported("ANALYZE supports a single table in v1", stmt.loc.pos);
    }
    auto [vk, vbody] = node_wrapper(rels[0]);
    if (vk != "VacuumRelation" || vbody == nullptr) {
        unsupported("ANALYZE: expected a VacuumRelation, got " + vk, stmt.loc.pos);
    }
    if (!vbody->contains("relation") || !vbody->at("relation").is_object()) {
        unsupported("ANALYZE: missing relation", stmt.loc.pos);
    }
    const auto& rel = vbody->at("relation");
    if (!rel.contains("relname") || !rel.at("relname").is_string()) {
        unsupported("ANALYZE: relation missing relname", stmt.loc.pos);
    }
    stmt.table = rel.at("relname").as_string();
    if (vbody->contains("va_cols") && vbody->at("va_cols").is_array()) {
        for (const auto& c : vbody->at("va_cols").as_array()) {
            stmt.columns.push_back(string_atom(c));
        }
    }
    return stmt;
}

ast::Statement translate_statement(const JsonValue& outer_stmt) {
    if (!outer_stmt.is_object() || !outer_stmt.contains("stmt")) {
        unsupported("missing stmt wrapper", 0);
    }
    auto [kind, body] = node_wrapper(outer_stmt.at("stmt"));
    if (kind == "SelectStmt") {
        return ast::Statement{translate_select_stmt(*body)};
    }
    if (kind == "CreateStmt") {
        return ast::Statement{translate_create_stmt(*body)};
    }
    if (kind == "CreateTableAsStmt") {
        return ast::Statement{translate_matview_stmt(*body)};
    }
    if (kind == "InsertStmt") {
        return ast::Statement{translate_insert_stmt(*body)};
    }
    if (kind == "DropStmt") {
        return ast::Statement{translate_drop_stmt(*body)};
    }
    if (kind == "VariableShowStmt") {
        return ast::Statement{translate_show_stmt(*body)};
    }
    if (kind == "VacuumStmt") {
        return ast::Statement{translate_vacuum_stmt(*body)};
    }
    if (kind == "ExplainStmt") {
        if (!body->contains("query")) {
            unsupported("ExplainStmt missing inner query", loc_from(*body).pos);
        }
        // Inner query is itself a Statement node wrapped under a kind
        // discriminator (SelectStmt / InsertStmt / etc.). Build a
        // synthetic outer-stmt wrapper around it so we can reuse the
        // translate_statement dispatch.
        JsonObject outer;
        outer["stmt"] = body->at("query");
        ast::Statement inner = translate_statement(clink::config::JsonValue{std::move(outer)});
        auto exp = std::make_unique<ast::ExplainStmt>();
        exp->loc = loc_from(*body);
        exp->query = std::move(inner);
        return ast::Statement{std::move(exp)};
    }
    unsupported("statement kind " + kind, loc_from(*body).pos);
}

}  // namespace

ast::Script translate_to_ast(std::string_view pg_json) {
    auto json = clink::config::parse(pg_json);
    if (!json.is_object()) {
        throw TranslationError("libpg_query JSON root must be an object", 0);
    }
    if (!json.contains("stmts") || !json.at("stmts").is_array()) {
        throw TranslationError("libpg_query JSON missing stmts array", 0);
    }
    ast::Script script;
    for (const auto& outer : json.at("stmts").as_array()) {
        script.statements.push_back(translate_statement(outer));
    }
    return script;
}

}  // namespace clink::sql
