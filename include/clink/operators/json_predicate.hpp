#pragma once

#include <concepts>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"

// JSON predicate evaluator. Used by filter_string_predicate
// (single-TEXT column) and filter_row_predicate (multi-column Row
// records).
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

template <ColumnResolver Resolver>
TriBool evaluate_json_predicate_tri(const clink::config::JsonValue& pred, Resolver& resolve) {
    if (!pred.is_object() || !pred.contains("op")) {
        throw std::runtime_error("json_predicate: missing 'op'");
    }
    const auto& op = pred.at("op").as_string();

    if (op == "and") {
        TriBool acc = TriBool::True;
        for (const auto& sub : pred.at("args").as_array()) {
            acc = detail::and_tri(acc, evaluate_json_predicate_tri(sub, resolve));
            if (acc == TriBool::False)
                return acc;
        }
        return acc;
    }
    if (op == "or") {
        TriBool acc = TriBool::False;
        for (const auto& sub : pred.at("args").as_array()) {
            acc = detail::or_tri(acc, evaluate_json_predicate_tri(sub, resolve));
            if (acc == TriBool::True)
                return acc;
        }
        return acc;
    }
    if (op == "not") {
        return detail::not_tri(evaluate_json_predicate_tri(pred.at("arg"), resolve));
    }
    // IS NULL / IS NOT NULL are always two-valued (their purpose is
    // to test for NULL deterministically).
    if (op == "is_null") {
        return detail::from_bool(resolve(pred.at("col").as_string()).is_null());
    }
    if (op == "is_not_null") {
        return detail::from_bool(!resolve(pred.at("col").as_string()).is_null());
    }
    if (op == "like") {
        auto val = resolve(pred.at("col").as_string());
        if (val.is_null())
            return TriBool::Unknown;
        const auto& pattern = pred.at("pattern").as_string();
        return detail::from_bool(detail::like_match(pattern, detail::to_text(val)));
    }
    // x IN (v1, v2, ...). Three-valued semantics: NULL on
    // the column side is Unknown; if no value matches but at least one
    // value is NULL, the result is Unknown (matches SQL's `x IN (1,
    // NULL)` returning Unknown when x != 1).
    if (op == "in") {
        auto val = resolve(pred.at("col").as_string());
        if (val.is_null())
            return TriBool::Unknown;
        const auto& values = pred.at("values").as_array();
        bool saw_null = false;
        for (const auto& v : values) {
            if (v.is_null()) {
                saw_null = true;
                continue;
            }
            auto cmp = detail::compare(val, v);
            if (cmp.has_value() && *cmp == 0)
                return TriBool::True;
        }
        return saw_null ? TriBool::Unknown : TriBool::False;
    }
    // Comparison: typed-aware. The RHS is a literal value or another column of
    // the same row (column-vs-column, e.g. a post-join residual a.x >= b.y).
    // NULL on either side -> Unknown.
    auto col_val = resolve(pred.at("col").as_string());
    clink::config::JsonValue rhs_val;
    if (pred.contains("rhs_col")) {
        rhs_val = resolve(pred.at("rhs_col").as_string());
    } else if (pred.contains("literal")) {
        rhs_val = pred.at("literal");
    } else {
        throw std::runtime_error("json_predicate: '" + op + "' needs 'literal' or 'rhs_col'");
    }
    if (col_val.is_null() || rhs_val.is_null())
        return TriBool::Unknown;
    auto cmp = detail::compare(col_val, rhs_val);
    if (!cmp.has_value())
        return TriBool::Unknown;
    if (op == "eq")
        return detail::from_bool(*cmp == 0);
    if (op == "ne")
        return detail::from_bool(*cmp != 0);
    if (op == "lt")
        return detail::from_bool(*cmp < 0);
    if (op == "le")
        return detail::from_bool(*cmp <= 0);
    if (op == "gt")
        return detail::from_bool(*cmp > 0);
    if (op == "ge")
        return detail::from_bool(*cmp >= 0);
    throw std::runtime_error("json_predicate: unknown op '" + op + "'");
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
