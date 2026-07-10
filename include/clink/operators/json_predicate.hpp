#pragma once

#include <concepts>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"

// JSON predicate evaluator. Used by filter_string_predicate
// (single-TEXT column) and filter_row_predicate (multi-column Row
// records).
//
// Evaluation is COMPILED: CompiledPredicate::compile() walks the
// predicate JSON once and builds a small node tree (op names resolved
// to node kinds, literals and patterns extracted), so the per-record
// eval is virtual dispatch over that tree with no JSON probing and no
// op-name string comparisons. Build an operator's predicate once and
// evaluate it per record. The template evaluate_json_predicate[_tri]
// entry points below keep the original one-shot signature (they
// compile then evaluate) for tests and non-hot callers.
//
// Malformed predicate shapes (missing keys, unknown ops) compile to a
// node that throws WHEN EVALUATED, not at compile time. This
// preserves the interpreter's lazy-error semantics exactly: a
// malformed sub-predicate behind an AND/OR short-circuit only throws
// if a record actually reaches it.
//
// SQL-standard three-valued logic. Comparisons with NULL on either
// side yield Unknown (not False); AND / OR / NOT follow the
// three-valued truth tables. WHERE filters keep a row only when the
// predicate evaluates to True - both False and Unknown drop the row,
// identical to standard SQL.
//
// Predicate JSON shape:
//
//   {"op": "eq"|"ne"|"lt"|"le"|"gt"|"ge", "col": "<name>",
//    "literal": <JsonValue>}             literal carries its type
//   {"op": "eq"|"ne"|"lt"|"le"|"gt"|"ge", "col": "<name>",
//    "rhs_col": "<name>"}                compare two columns of the same row
//   {"op": "like", "col": "<name>", "pattern": "<sql-like>"}
//   {"op": "is_null"|"is_not_null", "col": "<name>"}      always 2-val
//   {"op": "and"|"or", "args": [<pred>, ...]}
//   {"op": "not", "arg": <pred>}
//
// The resolver returns a JsonValue (use {nullptr} for SQL NULL).
// Type-aware comparison rules:
//   * Both sides numeric: numeric comparison.
//   * Both sides string: lexicographic comparison.
//   * Mixed numeric / string: coerce string to number when it parses,
//     else the comparison yields Unknown.
//
// LIKE patterns: '%' matches any run of characters, '_' matches one;
// other characters match literally. NULL LIKE anything -> Unknown.

namespace clink::operators {

template <typename Resolver>
concept ColumnResolver = requires(Resolver& r, const std::string& name) {
    { r(name) } -> std::convertible_to<clink::config::JsonValue>;
};

namespace detail {

inline bool like_match(std::string_view pattern, std::string_view input) {
    std::size_t pi = 0;
    std::size_t ii = 0;
    std::size_t star_pi = std::string::npos;
    std::size_t star_ii = 0;
    while (ii < input.size()) {
        if (pi < pattern.size() && pattern[pi] == '%') {
            star_pi = pi++;
            star_ii = ii;
        } else if (pi < pattern.size() && (pattern[pi] == '_' || pattern[pi] == input[ii])) {
            ++pi;
            ++ii;
        } else if (star_pi != std::string::npos) {
            pi = star_pi + 1;
            ii = ++star_ii;
        } else {
            return false;
        }
    }
    while (pi < pattern.size() && pattern[pi] == '%')
        ++pi;
    return pi == pattern.size();
}

inline std::optional<double> try_to_number(const clink::config::JsonValue& v) {
    if (v.is_number())
        return v.as_number();
    if (clink::config::is_dec_string(v)) {  // #56: a dec-string is numeric (lossy as double)
        if (auto d = clink::config::dec_parse(v.as_string()))
            return clink::config::dec_to_double(*d);
        return std::nullopt;
    }
    if (v.is_string()) {
        try {
            std::size_t end = 0;
            double d = std::stod(v.as_string(), &end);
            if (end == v.as_string().size())
                return d;
        } catch (...) {
        }
    }
    if (v.is_bool())
        return v.as_bool() ? 1.0 : 0.0;
    return std::nullopt;
}

inline std::string to_text(const clink::config::JsonValue& v) {
    if (clink::config::is_dec_string(v))
        return v.as_string().substr(1);  // #56: canonical decimal, sentinel stripped
    if (v.is_string())
        return v.as_string();
    if (v.is_null())
        return {};
    if (v.is_bool())
        return v.as_bool() ? "true" : "false";
    if (v.is_number()) {
        double d = v.as_number();
        if (d == static_cast<double>(static_cast<std::int64_t>(d))) {
            return std::to_string(static_cast<std::int64_t>(d));
        }
        return std::to_string(d);
    }
    return v.serialize(0);
}

// Compare two values for ordering. Returns -1 / 0 / 1 if comparable,
// std::nullopt when the operands aren't ordered against each other
// (e.g. one is a non-numeric string and the other is a number that
// can't be coerced).
inline std::optional<int> compare(const clink::config::JsonValue& a,
                                  const clink::config::JsonValue& b) {
    if (a.is_null() || b.is_null())
        return std::nullopt;
    // #56: exact decimal comparison when either side is a dec-string. Both
    // decimal-or-integer -> exact (scale-insensitive: 1.10 == 1.1); decimal
    // vs a non-integral double -> compare via double (lossy, rare); decimal
    // vs a non-numeric string -> UNKNOWN.
    if (clink::config::is_dec_string(a) || clink::config::is_dec_string(b)) {
        auto da = clink::config::as_decimal(a);
        auto db = clink::config::as_decimal(b);
        if (da && db)
            return clink::config::dec_compare(*da, *db);
        auto fa = try_to_number(a);
        auto fb = try_to_number(b);
        if (fa && fb)
            return (*fa < *fb) ? -1 : (*fa > *fb ? 1 : 0);
        return std::nullopt;
    }
    // Numeric path: at least one side is numeric. Coerce the other if
    // it parses; otherwise the comparison collapses.
    if (a.is_number() || b.is_number()) {
        auto da = try_to_number(a);
        auto db = try_to_number(b);
        if (!da || !db)
            return std::nullopt;
        if (*da < *db)
            return -1;
        if (*da > *db)
            return 1;
        return 0;
    }
    // String path.
    if (a.is_string() && b.is_string()) {
        const auto& sa = a.as_string();
        const auto& sb = b.as_string();
        if (sa < sb)
            return -1;
        if (sa > sb)
            return 1;
        return 0;
    }
    // Bool path: only meaningful for eq/ne. fall back to numeric.
    if (a.is_bool() && b.is_bool()) {
        int ia = a.as_bool() ? 1 : 0;
        int ib = b.as_bool() ? 1 : 0;
        return ia == ib ? 0 : (ia < ib ? -1 : 1);
    }
    return std::nullopt;
}

}  // namespace detail

// Three-valued logic outcome.
enum class TriBool { False, True, Unknown };

namespace detail {

constexpr TriBool from_bool(bool b) {
    return b ? TriBool::True : TriBool::False;
}

constexpr TriBool not_tri(TriBool t) {
    if (t == TriBool::True)
        return TriBool::False;
    if (t == TriBool::False)
        return TriBool::True;
    return TriBool::Unknown;
}

constexpr TriBool and_tri(TriBool a, TriBool b) {
    if (a == TriBool::False || b == TriBool::False)
        return TriBool::False;
    if (a == TriBool::Unknown || b == TriBool::Unknown)
        return TriBool::Unknown;
    return TriBool::True;
}

constexpr TriBool or_tri(TriBool a, TriBool b) {
    if (a == TriBool::True || b == TriBool::True)
        return TriBool::True;
    if (a == TriBool::Unknown || b == TriBool::Unknown)
        return TriBool::Unknown;
    return TriBool::False;
}

}  // namespace detail

// Non-owning reference to a column resolver: any callable mapping a
// column name to its JsonValue for the record under evaluation.
// Compiled programs take this instead of a template Resolver so the
// node tree is a concrete (non-template) type; the cost is one
// function-pointer call per column reference. The referenced resolver
// must outlive the ColumnLookup (call sites construct it per record
// or per batch around a local lambda).
class ColumnLookup {
public:
    template <ColumnResolver R>
    ColumnLookup(R& r)
        : obj_(&r), fn_([](void* o, const std::string& n) -> clink::config::JsonValue {
              return (*static_cast<R*>(o))(n);
          }) {}

    clink::config::JsonValue operator()(const std::string& name) const { return fn_(obj_, name); }

private:
    void* obj_;
    clink::config::JsonValue (*fn_)(void*, const std::string&);
};

namespace pred_detail {

class PredNode {
public:
    PredNode() = default;
    PredNode(const PredNode&) = delete;
    PredNode& operator=(const PredNode&) = delete;
    virtual ~PredNode() = default;
    virtual TriBool eval(const ColumnLookup& resolve) const = 0;
};

using PredNodePtr = std::unique_ptr<const PredNode>;

// Malformed shape: the error is deferred to evaluation so a bad
// sub-predicate behind a short-circuit keeps the interpreter's
// never-reached-never-thrown behaviour.
class PredThrowNode final : public PredNode {
public:
    explicit PredThrowNode(std::string msg) : msg_(std::move(msg)) {}
    [[noreturn]] TriBool eval(const ColumnLookup&) const override {
        throw std::runtime_error(msg_);
    }

private:
    std::string msg_;
};

class AndNode final : public PredNode {
public:
    explicit AndNode(std::vector<PredNodePtr> children) : children_(std::move(children)) {}
    TriBool eval(const ColumnLookup& resolve) const override {
        TriBool acc = TriBool::True;
        for (const auto& c : children_) {
            acc = detail::and_tri(acc, c->eval(resolve));
            if (acc == TriBool::False)
                return acc;
        }
        return acc;
    }

private:
    std::vector<PredNodePtr> children_;
};

class OrNode final : public PredNode {
public:
    explicit OrNode(std::vector<PredNodePtr> children) : children_(std::move(children)) {}
    TriBool eval(const ColumnLookup& resolve) const override {
        TriBool acc = TriBool::False;
        for (const auto& c : children_) {
            acc = detail::or_tri(acc, c->eval(resolve));
            if (acc == TriBool::True)
                return acc;
        }
        return acc;
    }

private:
    std::vector<PredNodePtr> children_;
};

class NotNode final : public PredNode {
public:
    explicit NotNode(PredNodePtr child) : child_(std::move(child)) {}
    TriBool eval(const ColumnLookup& resolve) const override {
        return detail::not_tri(child_->eval(resolve));
    }

private:
    PredNodePtr child_;
};

// IS NULL / IS NOT NULL are always two-valued (their purpose is to
// test for NULL deterministically).
class IsNullNode final : public PredNode {
public:
    IsNullNode(std::string col, bool negate) : col_(std::move(col)), negate_(negate) {}
    TriBool eval(const ColumnLookup& resolve) const override {
        const bool is_null = resolve(col_).is_null();
        return detail::from_bool(negate_ ? !is_null : is_null);
    }

private:
    std::string col_;
    bool negate_;
};

class LikeNode final : public PredNode {
public:
    LikeNode(std::string col, std::string pattern)
        : col_(std::move(col)), pattern_(std::move(pattern)) {}
    TriBool eval(const ColumnLookup& resolve) const override {
        auto val = resolve(col_);
        if (val.is_null())
            return TriBool::Unknown;
        return detail::from_bool(detail::like_match(pattern_, detail::to_text(val)));
    }

private:
    std::string col_;
    std::string pattern_;
};

// x IN (v1, v2, ...). Three-valued semantics: NULL on the column side
// is Unknown; if no value matches but at least one value is NULL, the
// result is Unknown (matches SQL's `x IN (1, NULL)` returning Unknown
// when x != 1). NULL list entries are stripped at compile time into
// the has_null flag.
class InNode final : public PredNode {
public:
    InNode(std::string col, std::vector<clink::config::JsonValue> values, bool has_null)
        : col_(std::move(col)), values_(std::move(values)), has_null_(has_null) {}
    TriBool eval(const ColumnLookup& resolve) const override {
        auto val = resolve(col_);
        if (val.is_null())
            return TriBool::Unknown;
        for (const auto& v : values_) {
            auto cmp = detail::compare(val, v);
            if (cmp.has_value() && *cmp == 0)
                return TriBool::True;
        }
        return has_null_ ? TriBool::Unknown : TriBool::False;
    }

private:
    std::string col_;
    std::vector<clink::config::JsonValue> values_;
    bool has_null_;
};

// Comparison: typed-aware. The RHS is a literal value or another
// column of the same row (column-vs-column, e.g. a post-join residual
// a.x >= b.y). NULL on either side -> Unknown.
class CmpNode final : public PredNode {
public:
    enum class Op { Eq, Ne, Lt, Le, Gt, Ge };
    CmpNode(Op op,
            std::string col,
            std::optional<std::string> rhs_col,
            clink::config::JsonValue literal)
        : op_(op),
          col_(std::move(col)),
          rhs_col_(std::move(rhs_col)),
          literal_(std::move(literal)) {}
    TriBool eval(const ColumnLookup& resolve) const override {
        auto col_val = resolve(col_);
        clink::config::JsonValue rhs_resolved;
        const clink::config::JsonValue* rhs = &literal_;
        if (rhs_col_) {
            rhs_resolved = resolve(*rhs_col_);
            rhs = &rhs_resolved;
        }
        if (col_val.is_null() || rhs->is_null())
            return TriBool::Unknown;
        auto cmp = detail::compare(col_val, *rhs);
        if (!cmp.has_value())
            return TriBool::Unknown;
        switch (op_) {
            case Op::Eq:
                return detail::from_bool(*cmp == 0);
            case Op::Ne:
                return detail::from_bool(*cmp != 0);
            case Op::Lt:
                return detail::from_bool(*cmp < 0);
            case Op::Le:
                return detail::from_bool(*cmp <= 0);
            case Op::Gt:
                return detail::from_bool(*cmp > 0);
            case Op::Ge:
                return detail::from_bool(*cmp >= 0);
        }
        return TriBool::Unknown;  // unreachable
    }

private:
    Op op_;
    std::string col_;
    std::optional<std::string> rhs_col_;
    clink::config::JsonValue literal_;
};

inline PredNodePtr compile_pred(const clink::config::JsonValue& pred) {
    using clink::config::JsonValue;
    auto malformed = [](std::string msg) -> PredNodePtr {
        return std::make_unique<PredThrowNode>(std::move(msg));
    };
    if (!pred.is_object() || !pred.contains("op")) {
        return malformed("json_predicate: missing 'op'");
    }
    const JsonValue& opv = pred.at("op");
    if (!opv.is_string()) {
        return malformed("json_predicate: 'op' must be a string");
    }
    const std::string& op = opv.as_string();

    if (op == "and" || op == "or") {
        if (!pred.contains("args") || !pred.at("args").is_array()) {
            return malformed("json_predicate: '" + op + "' needs an 'args' array");
        }
        std::vector<PredNodePtr> children;
        children.reserve(pred.at("args").as_array().size());
        for (const auto& sub : pred.at("args").as_array()) {
            children.push_back(compile_pred(sub));
        }
        if (op == "and") {
            return std::make_unique<AndNode>(std::move(children));
        }
        return std::make_unique<OrNode>(std::move(children));
    }
    if (op == "not") {
        if (!pred.contains("arg")) {
            return malformed("json_predicate: 'not' needs 'arg'");
        }
        return std::make_unique<NotNode>(compile_pred(pred.at("arg")));
    }

    if (!pred.contains("col") || !pred.at("col").is_string()) {
        return malformed("json_predicate: '" + op + "' needs a 'col' string");
    }
    const std::string& col = pred.at("col").as_string();

    if (op == "is_null" || op == "is_not_null") {
        return std::make_unique<IsNullNode>(col, op == "is_not_null");
    }
    if (op == "like") {
        if (!pred.contains("pattern") || !pred.at("pattern").is_string()) {
            return malformed("json_predicate: 'like' needs a 'pattern' string");
        }
        return std::make_unique<LikeNode>(col, pred.at("pattern").as_string());
    }
    if (op == "in") {
        if (!pred.contains("values") || !pred.at("values").is_array()) {
            return malformed("json_predicate: 'in' needs a 'values' array");
        }
        std::vector<JsonValue> values;
        bool has_null = false;
        for (const auto& v : pred.at("values").as_array()) {
            if (v.is_null()) {
                has_null = true;
            } else {
                values.push_back(v);
            }
        }
        return std::make_unique<InNode>(col, std::move(values), has_null);
    }

    CmpNode::Op cmp_op;
    if (op == "eq") {
        cmp_op = CmpNode::Op::Eq;
    } else if (op == "ne") {
        cmp_op = CmpNode::Op::Ne;
    } else if (op == "lt") {
        cmp_op = CmpNode::Op::Lt;
    } else if (op == "le") {
        cmp_op = CmpNode::Op::Le;
    } else if (op == "gt") {
        cmp_op = CmpNode::Op::Gt;
    } else if (op == "ge") {
        cmp_op = CmpNode::Op::Ge;
    } else {
        return malformed("json_predicate: unknown op '" + op + "'");
    }
    if (pred.contains("rhs_col")) {
        if (!pred.at("rhs_col").is_string()) {
            return malformed("json_predicate: 'rhs_col' must be a string");
        }
        return std::make_unique<CmpNode>(
            cmp_op, col, pred.at("rhs_col").as_string(), JsonValue{nullptr});
    }
    if (pred.contains("literal")) {
        return std::make_unique<CmpNode>(cmp_op, col, std::nullopt, pred.at("literal"));
    }
    return malformed("json_predicate: '" + op + "' needs 'literal' or 'rhs_col'");
}

}  // namespace pred_detail

// A predicate compiled to a node tree. Compile once at operator build,
// evaluate per record. Copyable (the immutable tree is shared), so it
// can be captured in std::function-based operator closures.
class CompiledPredicate {
public:
    static CompiledPredicate compile(const clink::config::JsonValue& pred) {
        return CompiledPredicate{pred_detail::compile_pred(pred)};
    }

    TriBool evaluate_tri(const ColumnLookup& resolve) const { return root_->eval(resolve); }
    bool evaluate(const ColumnLookup& resolve) const {
        return root_->eval(resolve) == TriBool::True;
    }

private:
    explicit CompiledPredicate(pred_detail::PredNodePtr root) : root_(std::move(root)) {}
    std::shared_ptr<const pred_detail::PredNode> root_;
};

// One-shot convenience entry points (compile then evaluate). Per-record
// callers should compile once and reuse the CompiledPredicate instead.
template <ColumnResolver Resolver>
TriBool evaluate_json_predicate_tri(const clink::config::JsonValue& pred, Resolver& resolve) {
    const ColumnLookup lookup{resolve};
    return pred_detail::compile_pred(pred)->eval(lookup);
}

// Two-valued wrapper used by WHERE filters: keep the row only when
// the predicate evaluates to True. False and Unknown both drop -
// matches SQL's WHERE semantics where "WHERE x = NULL" silently
// excludes every row (NULL = anything is Unknown, not True).
template <ColumnResolver Resolver>
bool evaluate_json_predicate(const clink::config::JsonValue& pred, Resolver& resolve) {
    return evaluate_json_predicate_tri(pred, resolve) == TriBool::True;
}

}  // namespace clink::operators
