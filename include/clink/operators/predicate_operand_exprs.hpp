#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/operators/json_predicate.hpp"
#include "clink/operators/json_value_expr.hpp"

// Comparison operands that are expressions rather than bare column references.
//
// The predicate format in json_predicate.hpp compares a NAMED COLUMN against a
// literal or against another column of the same row. That covers every
// predicate the columnar program can accelerate, but not every predicate SQL
// can write: `WHERE MOD(auction, 123) = 0` and `WHERE 0.908 * price > 1000000`
// each compare an EXPRESSION, and there is no column to name.
//
// The predicate evaluator cannot simply learn to evaluate expressions, because
// the dependency already runs the other way - a value expression can contain a
// predicate (CASE WHEN), which is why json_value_expr.hpp includes
// json_predicate.hpp and not the reverse. So instead of deepening either
// evaluator, the binder emits such an operand as `col_expr` / `rhs_expr`
// carrying a value expression, and this helper lowers it back into the shape
// the evaluator already understands: each expression is compiled once and
// bound to a synthetic name, and the operand becomes an ordinary reference to
// that name.
//
// A resolver wrapped in synth_resolver() answers the synthetic names by
// evaluating the compiled expression against the record in hand, and passes
// every other name through to the caller's own resolver. The predicate
// evaluator, the typed columnar predicate program, and every existing resolver
// are therefore untouched: a predicate with an expression operand names a
// column the columnar program cannot find in the batch schema, so that program
// declines to compile and the row interpreter - which the synthetic resolver
// does answer - handles it. Un-accelerated, never wrong.

namespace clink::operators {

// Rewritten predicate plus the compiled expressions its synthetic names refer
// to. Build once per operator; evaluate per record.
class PredicateOperandExprs {
public:
    // Synthetic operand names. A leading '$' cannot appear in an unquoted SQL
    // identifier, so these never shadow a declared column.
    static constexpr std::string_view kPrefix = "$expr";

    // Walks a copy of `predicate`, replacing every `col_expr` with
    // `col: "$exprN"` and every `rhs_expr` with `rhs_col: "$exprN"`, and
    // compiling each extracted expression. A predicate with no expression
    // operand comes back unchanged and empty().
    static PredicateOperandExprs build(const clink::config::JsonValue& predicate) {
        PredicateOperandExprs out;
        out.predicate_ = predicate;
        out.rewrite_(out.predicate_);
        return out;
    }

    [[nodiscard]] const clink::config::JsonValue& predicate() const noexcept { return predicate_; }
    [[nodiscard]] bool empty() const noexcept { return compiled_.empty(); }

    // Index behind a synthetic name, or nullopt for an ordinary column.
    [[nodiscard]] static std::optional<std::size_t> synthetic_index(
        std::string_view name) noexcept {
        if (name.size() <= kPrefix.size() || !name.starts_with(kPrefix)) {
            return std::nullopt;
        }
        std::size_t idx = 0;
        for (const char c : name.substr(kPrefix.size())) {
            if (c < '0' || c > '9') {
                return std::nullopt;
            }
            idx = (idx * 10) + static_cast<std::size_t>(c - '0');
        }
        return idx;
    }

    [[nodiscard]] clink::config::JsonValue evaluate(std::size_t idx,
                                                    const ColumnLookup& base) const {
        if (idx >= compiled_.size()) {
            return clink::config::JsonValue{nullptr};
        }
        return compiled_[idx].evaluate(base);
    }

private:
    void rewrite_(clink::config::JsonValue& node) {
        if (!node.is_object()) {
            return;
        }
        auto& obj = node.as_object();
        // Operand positions, in the order the binder emits them.
        for (const auto& [expr_key, ref_key] :
             {std::pair<const char*, const char*>{"col_expr", "col"},
              std::pair<const char*, const char*>{"rhs_expr", "rhs_col"}}) {
            const auto it = obj.find(expr_key);
            if (it == obj.end()) {
                continue;
            }
            compiled_.push_back(CompiledValueExpr::compile(it->second));
            obj.erase(expr_key);
            obj[ref_key] = clink::config::JsonValue{std::string{kPrefix} +
                                                    std::to_string(compiled_.size() - 1)};
        }
        // and / or carry `args`; not carries `arg`.
        if (const auto args = obj.find("args"); args != obj.end() && args->second.is_array()) {
            for (auto& a : args->second.as_array()) {
                rewrite_(a);
            }
        }
        if (const auto arg = obj.find("arg"); arg != obj.end()) {
            rewrite_(arg->second);
        }
    }

    clink::config::JsonValue predicate_{nullptr};
    std::vector<CompiledValueExpr> compiled_;
};

// Wraps a caller's per-record resolver so synthetic names resolve to their
// compiled expression evaluated against that same record. Holds references:
// construct per record (or per batch) alongside the resolver it wraps.
template <ColumnResolver Base>
class SynthOperandResolver {
public:
    SynthOperandResolver(const PredicateOperandExprs& exprs, Base& base)
        : exprs_(&exprs), base_(&base), base_lookup_(base) {}

    clink::config::JsonValue operator()(const std::string& name) const {
        if (const auto idx = PredicateOperandExprs::synthetic_index(name); idx.has_value()) {
            return exprs_->evaluate(*idx, base_lookup_);
        }
        return (*base_)(name);
    }

private:
    const PredicateOperandExprs* exprs_;
    Base* base_;
    ColumnLookup base_lookup_;
};

}  // namespace clink::operators
