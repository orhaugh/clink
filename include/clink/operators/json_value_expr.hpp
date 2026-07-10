#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <memory>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/json_predicate.hpp"
#include "clink/operators/scalar_function_registry.hpp"

// JSON value-expression evaluator. Sibling of json_predicate.hpp:
// json_predicate returns bool; json_value_expr returns a JsonValue.
// Used by the project_row operator factory to compute expression-
// based outputs (arithmetic, string concat, function calls).
//
// Evaluation is COMPILED, mirroring CompiledPredicate: compile() walks
// the expression JSON once into a node tree (op names resolved to an
// enum, literals and op-object parameters extracted), so the per-record
// eval is virtual dispatch with no JSON probing and no op-name string
// chain. Malformed shapes compile to a node that throws when
// evaluated, and an op name that is not a built-in resolves against
// the ScalarFunctionRegistry at evaluation time (not compile time), so
// the interpreter's lazy-error and late-UDF-registration semantics are
// preserved exactly. The template evaluate_json_value_expr entry point
// keeps the one-shot signature (compile then evaluate) for tests and
// non-hot callers.
//
// Expression grammar:
//
//   {"col": "<name>"}              column reference, looked up via
//                                  the caller's resolver
//   {"lit": <JsonValue>}           literal (number / string / bool /
//                                  null)
//   {"op": "<name>", "args": [...]} operation. Reserved op names:
//                                    add, sub, mul, div, mod, neg,
//                                    concat
//                                    upper, lower, length, coalesce,
//                                    cast_int, cast_float, cast_str
//
// Null propagation: any non-numeric / non-string operand collapses
// the expression to null, and any arithmetic with a null operand
// yields null.

namespace clink::operators {

template <typename Resolver>
concept ValueColumnResolver = requires(Resolver& r, const std::string& name) {
    { r(name) } -> std::convertible_to<clink::config::JsonValue>;
};

namespace value_expr_detail {

inline bool is_numeric(const clink::config::JsonValue& v) {
    return v.is_number();
}
inline bool is_textual(const clink::config::JsonValue& v) {
    return v.is_string();
}

inline clink::config::JsonValue null_value() {
    return clink::config::JsonValue{nullptr};
}

// A numeric value usable as a double: a JSON number, or a #56 dec-string
// (lossy, for the demote-to-double paths and float-returning functions).
inline std::optional<double> numeric_as_double(const clink::config::JsonValue& v) {
    if (v.is_number())
        return v.as_number();
    if (clink::config::is_dec_string(v)) {
        if (auto d = clink::config::dec_parse(v.as_string()))
            return clink::config::dec_to_double(*d);
    }
    return std::nullopt;
}

inline std::string to_text(const clink::config::JsonValue& v) {
    if (clink::config::is_dec_string(v))
        return v.as_string().substr(1);  // #56: render canonical decimal, sentinel stripped
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

// Canonical key for a MAP value. A JSON object has string keys, so a SQL
// MAP key is rendered to its canonical wire form: a string verbatim, every
// other scalar through the single JsonValue serializer (NOT to_text's
// divergent std::to_string(double), so the key is deterministic and
// matches the wire rendering of the value). Consequence (documented, v1):
// keys are compared by this string form, so the INT key 1 and the VARCHAR
// key '1' are the same key. Key-type distinctness is not kept.
inline std::string to_map_key(const clink::config::JsonValue& v) {
    if (clink::config::is_dec_string(v))
        return v.as_string().substr(1);  // #56: canonical decimal key, sentinel stripped
    if (v.is_string())
        return v.as_string();
    return v.serialize(0);
}

// --- Temporal helpers ----------------------------------------------
//
// clink represents timestamps at runtime as epoch milliseconds (the
// same int64 it uses for event time). The civil-date conversions below
// are Howard Hinnant's public-domain algorithms: pure integer math, no
// timezone database or locale, so they are fully deterministic and
// portable (UTC). DATE_FORMAT / TO_TIMESTAMP share a small token set
// (yyyy MM dd HH mm ss) so they round-trip.

// Floor division (rounds toward negative infinity) for pre-1970 epochs.
inline std::int64_t floordiv(std::int64_t a, std::int64_t b) {
    std::int64_t q = a / b;
    std::int64_t r = a % b;
    if (r != 0 && ((r < 0) != (b < 0)))
        --q;
    return q;
}

// Days since 1970-01-01 for civil date y-m-d (m in [1,12], d in [1,31]).
inline std::int64_t days_from_civil(std::int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const auto yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

struct CivilDate {
    std::int64_t y;
    unsigned m;
    unsigned d;
};

// Civil date from days since 1970-01-01 (inverse of days_from_civil).
inline CivilDate civil_from_days(std::int64_t z) {
    z += 719468;
    const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const auto doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const std::int64_t y = static_cast<std::int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    const unsigned d = doy - (153 * mp + 2) / 5 + 1;
    const unsigned m = mp + (mp < 10 ? 3 : -9);
    return CivilDate{y + (m <= 2 ? 1 : 0), m, d};
}

// Day of week [0,6], 0 = Sunday, for days since 1970-01-01 (a Thursday).
inline unsigned weekday_from_days(std::int64_t z) {
    return static_cast<unsigned>((((z % 7) + 7) % 7 + 4) % 7);
}

inline std::string pad_int(std::int64_t v, int width) {
    std::string s = std::to_string(v);
    while (static_cast<int>(s.size()) < width)
        s = "0" + s;
    return s;
}

// Format epoch-millis (UTC) with the yyyy/MM/dd/HH/mm/ss token subset;
// any other character is copied through literally.
inline std::string format_timestamp(std::int64_t ms, const std::string& fmt) {
    constexpr std::int64_t kDayMs = 86400000;
    const std::int64_t days = floordiv(ms, kDayMs);
    const std::int64_t tod = ms - days * kDayMs;
    const CivilDate cd = civil_from_days(days);
    const std::int64_t hour = tod / 3600000;
    const std::int64_t minute = (tod / 60000) % 60;
    const std::int64_t second = (tod / 1000) % 60;
    std::string out;
    std::size_t i = 0;
    while (i < fmt.size()) {
        if (fmt.compare(i, 4, "yyyy") == 0) {
            out += pad_int(cd.y, 4);
            i += 4;
        } else if (fmt.compare(i, 2, "MM") == 0) {
            out += pad_int(cd.m, 2);
            i += 2;
        } else if (fmt.compare(i, 2, "dd") == 0) {
            out += pad_int(cd.d, 2);
            i += 2;
        } else if (fmt.compare(i, 2, "HH") == 0) {
            out += pad_int(hour, 2);
            i += 2;
        } else if (fmt.compare(i, 2, "mm") == 0) {
            out += pad_int(minute, 2);
            i += 2;
        } else if (fmt.compare(i, 2, "ss") == 0) {
            out += pad_int(second, 2);
            i += 2;
        } else {
            out += fmt[i++];
        }
    }
    return out;
}

// Parse a string into epoch-millis (UTC) with the same token subset.
// Returns nullopt when the input does not match the format.
inline std::optional<std::int64_t> parse_timestamp(const std::string& s, const std::string& fmt) {
    std::int64_t y = 1970;
    long mo = 1, d = 1, hh = 0, mi = 0, ss = 0;
    std::size_t si = 0, fi = 0;
    auto take_int = [&](int width, long& dst) -> bool {
        if (si + static_cast<std::size_t>(width) > s.size())
            return false;
        for (int k = 0; k < width; ++k) {
            if (std::isdigit(static_cast<unsigned char>(s[si + static_cast<std::size_t>(k)])) == 0)
                return false;
        }
        dst = std::stol(s.substr(si, static_cast<std::size_t>(width)));
        si += static_cast<std::size_t>(width);
        return true;
    };
    while (fi < fmt.size()) {
        long tmp = 0;
        if (fmt.compare(fi, 4, "yyyy") == 0) {
            if (!take_int(4, tmp))
                return std::nullopt;
            y = tmp;
            fi += 4;
        } else if (fmt.compare(fi, 2, "MM") == 0) {
            if (!take_int(2, mo))
                return std::nullopt;
            fi += 2;
        } else if (fmt.compare(fi, 2, "dd") == 0) {
            if (!take_int(2, d))
                return std::nullopt;
            fi += 2;
        } else if (fmt.compare(fi, 2, "HH") == 0) {
            if (!take_int(2, hh))
                return std::nullopt;
            fi += 2;
        } else if (fmt.compare(fi, 2, "mm") == 0) {
            if (!take_int(2, mi))
                return std::nullopt;
            fi += 2;
        } else if (fmt.compare(fi, 2, "ss") == 0) {
            if (!take_int(2, ss))
                return std::nullopt;
            fi += 2;
        } else {
            if (si >= s.size() || s[si] != fmt[fi])
                return std::nullopt;
            ++si;
            ++fi;
        }
    }
    const std::int64_t days =
        days_from_civil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d));
    return days * 86400000 + hh * 3600000 + mi * 60000 + ss * 1000;
}

// --- JSON-path helpers ---------------------------------------------
//
// A column may arrive either as JSON text (a string) or, when the
// source already parsed a nested field, as a structured JsonValue.
// coerce_json normalizes: parse a string as JSON, or use the value
// directly; a string that is not valid JSON is treated as a scalar.
inline clink::config::JsonValue coerce_json(const clink::config::JsonValue& v) {
    if (v.is_string()) {
        try {
            return clink::config::parse(v.as_string());
        } catch (...) {
            return v;  // not JSON: treat the string as a scalar
        }
    }
    return v;
}

struct JsonPathStep {
    bool is_index = false;
    std::string key;
    std::size_t index = 0;
};

// Parse a small JSONPath subset: '$' root, '.key', '[index]'. Returns
// nullopt on a malformed path. Bracketed string keys and wildcards are
// not supported.
inline std::optional<std::vector<JsonPathStep>> parse_json_path(const std::string& p) {
    if (p.empty() || p[0] != '$')
        return std::nullopt;
    std::vector<JsonPathStep> steps;
    std::size_t i = 1;
    while (i < p.size()) {
        if (p[i] == '.') {
            ++i;
            std::string key;
            while (i < p.size() &&
                   (std::isalnum(static_cast<unsigned char>(p[i])) != 0 || p[i] == '_')) {
                key += p[i++];
            }
            if (key.empty())
                return std::nullopt;
            steps.push_back(JsonPathStep{false, std::move(key), 0});
        } else if (p[i] == '[') {
            ++i;
            std::string num;
            while (i < p.size() && std::isdigit(static_cast<unsigned char>(p[i])) != 0)
                num += p[i++];
            if (i >= p.size() || p[i] != ']' || num.empty())
                return std::nullopt;
            ++i;
            steps.push_back(JsonPathStep{true, "", static_cast<std::size_t>(std::stoull(num))});
        } else {
            return std::nullopt;
        }
    }
    return steps;
}

// Navigate steps from a root node; nullopt if any step does not resolve.
inline std::optional<clink::config::JsonValue> navigate_json(
    clink::config::JsonValue node, const std::vector<JsonPathStep>& steps) {
    for (const auto& s : steps) {
        // Copy the descended value into a local before reassigning
        // node: the array/object reference points into node's own
        // storage, so assigning node directly from it would read freed
        // memory mid-assignment.
        if (s.is_index) {
            if (!node.is_array())
                return std::nullopt;
            const auto& arr = node.as_array();
            if (s.index >= arr.size())
                return std::nullopt;
            clink::config::JsonValue next = arr[s.index];
            node = std::move(next);
        } else {
            if (!node.is_object())
                return std::nullopt;
            const auto& obj = node.as_object();
            auto it = obj.find(s.key);
            if (it == obj.end())
                return std::nullopt;
            clink::config::JsonValue next = it->second;
            node = std::move(next);
        }
    }
    return node;
}

// The op set, resolved from the op-name string once at compile time.
enum class ValueOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Neg,
    Concat,
    Upper,
    Lower,
    Length,
    Coalesce,
    CastInt,
    CastFloat,
    CastStr,
    CastBool,
    CastDecimal,
    MakeArray,
    ElementAt,
    MakeRow,
    FieldAt,
    MapMake,
    MapGet,
    Substring,
    Position,
    Replace,
    Btrim,
    Ltrim,
    Rtrim,
    RegexpExtract,
    SplitIndex,
    Abs,
    Floor,
    Ceil,
    Round,
    NullIf,
    Greatest,
    Least,
    CharLength,
    Ascii,
    Chr,
    Reverse,
    Repeat,
    Initcap,
    Left,
    Right,
    Lpad,
    Rpad,
    SplitPart,
    StartsWith,
    Sqrt,
    Exp,
    Ln,
    Log10,
    Sign,
    Power,
    Trunc,
    Extract,
    DateTrunc,
    DateFormat,
    ToTimestamp,
    JsonValueFn,
    JsonQuery,
    JsonExists,
    JsonObjectFn,
    Udf,  // not a built-in: resolved via ScalarFunctionRegistry at eval time
};

inline std::optional<ValueOp> lookup_value_op(std::string_view name) {
    static const std::unordered_map<std::string_view, ValueOp> kOps = {
        {"add", ValueOp::Add},
        {"sub", ValueOp::Sub},
        {"mul", ValueOp::Mul},
        {"div", ValueOp::Div},
        {"mod", ValueOp::Mod},
        {"neg", ValueOp::Neg},
        {"concat", ValueOp::Concat},
        {"upper", ValueOp::Upper},
        {"lower", ValueOp::Lower},
        {"length", ValueOp::Length},
        {"coalesce", ValueOp::Coalesce},
        {"cast_int", ValueOp::CastInt},
        {"cast_float", ValueOp::CastFloat},
        {"cast_str", ValueOp::CastStr},
        {"cast_bool", ValueOp::CastBool},
        {"cast_decimal", ValueOp::CastDecimal},
        {"make_array", ValueOp::MakeArray},
        {"element_at", ValueOp::ElementAt},
        {"make_row", ValueOp::MakeRow},
        {"field_at", ValueOp::FieldAt},
        {"map", ValueOp::MapMake},
        {"map_get", ValueOp::MapGet},
        {"substring", ValueOp::Substring},
        {"position", ValueOp::Position},
        {"replace", ValueOp::Replace},
        {"btrim", ValueOp::Btrim},
        {"ltrim", ValueOp::Ltrim},
        {"rtrim", ValueOp::Rtrim},
        {"regexp_extract", ValueOp::RegexpExtract},
        {"split_index", ValueOp::SplitIndex},
        {"abs", ValueOp::Abs},
        {"floor", ValueOp::Floor},
        {"ceil", ValueOp::Ceil},
        {"ceiling", ValueOp::Ceil},
        {"round", ValueOp::Round},
        {"nullif", ValueOp::NullIf},
        {"greatest", ValueOp::Greatest},
        {"least", ValueOp::Least},
        {"char_length", ValueOp::CharLength},
        {"character_length", ValueOp::CharLength},
        {"ascii", ValueOp::Ascii},
        {"chr", ValueOp::Chr},
        {"reverse", ValueOp::Reverse},
        {"repeat", ValueOp::Repeat},
        {"initcap", ValueOp::Initcap},
        {"left", ValueOp::Left},
        {"right", ValueOp::Right},
        {"lpad", ValueOp::Lpad},
        {"rpad", ValueOp::Rpad},
        {"split_part", ValueOp::SplitPart},
        {"starts_with", ValueOp::StartsWith},
        {"sqrt", ValueOp::Sqrt},
        {"exp", ValueOp::Exp},
        {"ln", ValueOp::Ln},
        {"log10", ValueOp::Log10},
        {"sign", ValueOp::Sign},
        {"power", ValueOp::Power},
        {"pow", ValueOp::Power},
        {"trunc", ValueOp::Trunc},
        {"extract", ValueOp::Extract},
        {"date_trunc", ValueOp::DateTrunc},
        {"date_format", ValueOp::DateFormat},
        {"to_timestamp", ValueOp::ToTimestamp},
        {"json_value", ValueOp::JsonValueFn},
        {"json_query", ValueOp::JsonQuery},
        {"json_exists", ValueOp::JsonExists},
        {"json_object", ValueOp::JsonObjectFn},
    };
    auto it = kOps.find(name);
    if (it == kOps.end()) {
        return std::nullopt;
    }
    return it->second;
}

// Parameters some ops carry on the op object itself (not in 'args'),
// extracted once at compile time.
struct OpExtras {
    int scale = 9;       // cast_decimal
    int precision = 38;  // cast_decimal
    // make_row: (arg index, field name) for each string entry of 'fields'.
    std::vector<std::pair<std::size_t, std::string>> row_fields;
    std::optional<std::string> field;  // field_at
};

class ValueNode {
public:
    ValueNode() = default;
    ValueNode(const ValueNode&) = delete;
    ValueNode& operator=(const ValueNode&) = delete;
    virtual ~ValueNode() = default;
    virtual clink::config::JsonValue eval(const ColumnLookup& resolve) const = 0;
};

using ValueNodePtr = std::unique_ptr<const ValueNode>;

// Malformed shape: the error is deferred to evaluation (a CASE branch
// that is never taken must never throw, matching the interpreter).
class ValueThrowNode final : public ValueNode {
public:
    explicit ValueThrowNode(std::string msg) : msg_(std::move(msg)) {}
    [[noreturn]] clink::config::JsonValue eval(const ColumnLookup&) const override {
        throw std::runtime_error(msg_);
    }

private:
    std::string msg_;
};

class ColNode final : public ValueNode {
public:
    explicit ColNode(std::string name) : name_(std::move(name)) {}
    clink::config::JsonValue eval(const ColumnLookup& resolve) const override {
        return resolve(name_);
    }

private:
    std::string name_;
};

class LitNode final : public ValueNode {
public:
    explicit LitNode(clink::config::JsonValue value) : value_(std::move(value)) {}
    clink::config::JsonValue eval(const ColumnLookup&) const override { return value_; }

private:
    clink::config::JsonValue value_;
};

// CASE WHEN <pred> THEN <val> [...] [ELSE <val>] END.
// Branches use lower_predicate-shaped JSON for `when` and the
// value-expression shape for `then` / `else`. Lazy evaluation:
// only the first matching then-branch is evaluated; if no when
// is true and there's no else, the result is NULL. A when whose
// three-valued evaluation is Unknown counts as not-matched.
class CaseNode final : public ValueNode {
public:
    struct Branch {
        pred_detail::PredNodePtr when;
        ValueNodePtr then;
    };
    CaseNode(std::vector<Branch> branches, ValueNodePtr else_expr)
        : branches_(std::move(branches)), else_(std::move(else_expr)) {}
    clink::config::JsonValue eval(const ColumnLookup& resolve) const override {
        for (const auto& b : branches_) {
            if (b.when->eval(resolve) == TriBool::True) {
                return b.then->eval(resolve);
            }
        }
        if (else_) {
            return else_->eval(resolve);
        }
        return null_value();
    }

private:
    std::vector<Branch> branches_;
    ValueNodePtr else_;
};

// The per-op evaluation bodies, dispatched on the compile-time enum.
// `name` is the original op-name string (error messages, UDF lookup);
// `args` holds the already-evaluated child values.
clink::config::JsonValue eval_value_op(ValueOp op,
                                       const std::string& name,
                                       std::vector<clink::config::JsonValue>& args,
                                       const OpExtras& ex);

class OpNode final : public ValueNode {
public:
    OpNode(ValueOp op, std::string name, std::vector<ValueNodePtr> children, OpExtras extras)
        : op_(op),
          name_(std::move(name)),
          children_(std::move(children)),
          extras_(std::move(extras)) {}
    clink::config::JsonValue eval(const ColumnLookup& resolve) const override {
        std::vector<clink::config::JsonValue> args;
        args.reserve(children_.size());
        for (const auto& c : children_) {
            args.push_back(c->eval(resolve));
        }
        return eval_value_op(op_, name_, args, extras_);
    }

private:
    ValueOp op_;
    std::string name_;
    std::vector<ValueNodePtr> children_;
    OpExtras extras_;
};

inline clink::config::JsonValue eval_value_op(ValueOp op,
                                              const std::string& name,
                                              std::vector<clink::config::JsonValue>& args,
                                              const OpExtras& ex) {
    using clink::config::JsonValue;

    // --- Arithmetic (exact DECIMAL path #56) ---
    // #56: when an operand is a dec-string, compute EXACTLY in fixed point.
    // Returns nullopt to fall through to the double path (no decimal operand,
    // or a decimal mixed with a non-integral double -> documented demote).
    auto decimal_binop = [&]() -> std::optional<JsonValue> {
        if (args.size() != 2)
            return std::nullopt;
        if (!clink::config::is_dec_string(args[0]) && !clink::config::is_dec_string(args[1]))
            return std::nullopt;
        if (args[0].is_null() || args[1].is_null())
            return value_expr_detail::null_value();
        auto a = clink::config::as_decimal(args[0]);
        auto b = clink::config::as_decimal(args[1]);
        if (!a || !b)
            return std::nullopt;  // decimal + non-integral double -> double path (lossy)
        std::optional<clink::config::Decimal> r;
        if (op == ValueOp::Add)
            r = clink::config::dec_add(*a, *b);
        else if (op == ValueOp::Sub)
            r = clink::config::dec_sub(*a, *b);
        else if (op == ValueOp::Mul)
            r = clink::config::dec_mul(*a, *b);
        else if (op == ValueOp::Div)
            r = clink::config::dec_div(*a, *b, std::max({a->scale, b->scale, 6}));
        else if (op == ValueOp::Mod)
            r = clink::config::dec_mod(*a, *b);
        else
            return std::nullopt;
        if (!r)
            return value_expr_detail::null_value();  // overflow / div-by-zero -> NULL
        return clink::config::make_dec_value(*r);
    };
    auto numeric_binop = [&](auto fn) -> JsonValue {
        if (args.size() != 2) {
            throw std::runtime_error("json_value_expr: '" + name + "' takes two args");
        }
        if (args[0].is_null() || args[1].is_null())
            return value_expr_detail::null_value();
        auto a = value_expr_detail::numeric_as_double(args[0]);
        auto b = value_expr_detail::numeric_as_double(args[1]);
        if (!a || !b) {
            return value_expr_detail::null_value();
        }
        return JsonValue{fn(*a, *b)};
    };
    if (op == ValueOp::Add || op == ValueOp::Sub || op == ValueOp::Mul || op == ValueOp::Div ||
        op == ValueOp::Mod) {
        if (auto d = decimal_binop())
            return *d;
    }
    if (op == ValueOp::Add)
        return numeric_binop([](double a, double b) { return a + b; });
    if (op == ValueOp::Sub)
        return numeric_binop([](double a, double b) { return a - b; });
    if (op == ValueOp::Mul)
        return numeric_binop([](double a, double b) { return a * b; });
    if (op == ValueOp::Div)
        return numeric_binop([](double a, double b) -> double {
            if (b == 0.0)
                return std::nan("");
            return a / b;
        });
    if (op == ValueOp::Mod)
        return numeric_binop([](double a, double b) -> double {
            if (b == 0.0)
                return std::nan("");
            return std::fmod(a, b);
        });
    if (op == ValueOp::Neg) {
        if (args.size() != 1)
            throw std::runtime_error("json_value_expr: 'neg' takes one arg");
        if (args[0].is_null())
            return value_expr_detail::null_value();
        if (clink::config::is_dec_string(args[0])) {
            if (auto d = clink::config::as_decimal(args[0]))
                return clink::config::make_dec_value(clink::config::dec_negate(*d));
            return value_expr_detail::null_value();
        }
        auto n = value_expr_detail::numeric_as_double(args[0]);
        if (!n)
            return value_expr_detail::null_value();
        return JsonValue{-*n};
    }
    if (op == ValueOp::Concat) {
        // SQL || is null-strict (any null collapses).
        std::string out;
        for (const auto& a : args) {
            if (a.is_null())
                return value_expr_detail::null_value();
            out += value_expr_detail::to_text(a);
        }
        return JsonValue{out};
    }

    // --- Functions ---
    if (op == ValueOp::Upper || op == ValueOp::Lower) {
        if (args.size() != 1) {
            throw std::runtime_error("json_value_expr: '" + name + "' takes one arg");
        }
        if (args[0].is_null())
            return value_expr_detail::null_value();
        std::string s = value_expr_detail::to_text(args[0]);
        for (char& c : s)
            c = op == ValueOp::Upper ? static_cast<char>(std::toupper(c))
                                     : static_cast<char>(std::tolower(c));
        return JsonValue{s};
    }
    if (op == ValueOp::Length) {
        if (args.size() != 1)
            throw std::runtime_error("json_value_expr: 'length' takes one arg");
        if (args[0].is_null())
            return value_expr_detail::null_value();
        return JsonValue{static_cast<std::int64_t>(value_expr_detail::to_text(args[0]).size())};
    }
    if (op == ValueOp::Coalesce) {
        for (const auto& a : args) {
            if (!a.is_null())
                return a;
        }
        return value_expr_detail::null_value();
    }
    if (op == ValueOp::CastInt) {
        if (args.size() != 1) {
            throw std::runtime_error("json_value_expr: 'cast_int' takes one arg");
        }
        if (args[0].is_null())
            return value_expr_detail::null_value();
        if (clink::config::is_dec_string(args[0])) {
            // #56: CAST(decimal AS INT) truncates toward zero (as the double
            // path does), but exactly (no IEEE round-off for large values).
            auto d = clink::config::as_decimal(args[0]);
            if (!d)
                return value_expr_detail::null_value();
            arrow::Decimal128 trunc =
                d->scale > 0 ? arrow::Decimal128(d->unscaled.ReduceScaleBy(d->scale, false))
                             : d->unscaled;
            try {
                return JsonValue{static_cast<std::int64_t>(std::stoll(trunc.ToString(0)))};
            } catch (...) {
                return value_expr_detail::null_value();
            }
        }
        if (args[0].is_number()) {
            return JsonValue{static_cast<std::int64_t>(args[0].as_number())};
        }
        if (args[0].is_string()) {
            try {
                return JsonValue{static_cast<std::int64_t>(std::stoll(args[0].as_string()))};
            } catch (...) {
                return value_expr_detail::null_value();
            }
        }
        if (args[0].is_bool())
            return JsonValue{static_cast<std::int64_t>(args[0].as_bool() ? 1 : 0)};
        return value_expr_detail::null_value();
    }
    if (op == ValueOp::CastFloat) {
        if (args.size() != 1) {
            throw std::runtime_error("json_value_expr: 'cast_float' takes one arg");
        }
        if (args[0].is_null())
            return value_expr_detail::null_value();
        if (clink::config::is_dec_string(args[0])) {
            if (auto d = clink::config::as_decimal(args[0]))
                return JsonValue{clink::config::dec_to_double(*d)};  // #56: lossy, intended
            return value_expr_detail::null_value();
        }
        if (args[0].is_number())
            return args[0];
        if (args[0].is_string()) {
            try {
                return JsonValue{std::stod(args[0].as_string())};
            } catch (...) {
                return value_expr_detail::null_value();
            }
        }
        if (args[0].is_bool())
            return JsonValue{args[0].as_bool() ? 1.0 : 0.0};
        return value_expr_detail::null_value();
    }
    if (op == ValueOp::CastStr) {
        if (args.size() != 1) {
            throw std::runtime_error("json_value_expr: 'cast_str' takes one arg");
        }
        if (args[0].is_null())
            return value_expr_detail::null_value();
        return JsonValue{value_expr_detail::to_text(args[0])};
    }
    if (op == ValueOp::CastBool) {
        if (args.size() != 1) {
            throw std::runtime_error("json_value_expr: 'cast_bool' takes one arg");
        }
        if (args[0].is_null())
            return value_expr_detail::null_value();
        if (args[0].is_bool())
            return args[0];
        // SQL boolean coercion: numbers -> false iff zero; the canonical
        // PG text literals -> bool, anything else -> null (UNKNOWN).
        if (args[0].is_number())
            return JsonValue{args[0].as_number() != 0.0};
        if (args[0].is_string()) {
            std::string s = args[0].as_string();
            for (auto& ch : s)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (s == "true" || s == "t" || s == "yes" || s == "y" || s == "on" || s == "1")
                return JsonValue{true};
            if (s == "false" || s == "f" || s == "no" || s == "n" || s == "off" || s == "0")
                return JsonValue{false};
            return value_expr_detail::null_value();
        }
        return value_expr_detail::null_value();
    }
    if (op == ValueOp::CastDecimal) {
        // #56: CAST(x AS DECIMAL(p,s)). Re-quantise to scale s (HALF_UP);
        // NULL on overflow past p digits. From a dec-string: exact. From a
        // number/string: parse its text (a double's text repr is lossy,
        // documented). The binder supplies precision/scale on the op object.
        if (args.size() != 1) {
            throw std::runtime_error("json_value_expr: 'cast_decimal' takes one arg");
        }
        if (args[0].is_null())
            return value_expr_detail::null_value();
        const int scale = ex.scale;
        const int precision = ex.precision;
        std::optional<clink::config::Decimal> d;
        if (clink::config::is_dec_string(args[0])) {
            d = clink::config::as_decimal(args[0]);
        } else if (args[0].is_number() || args[0].is_string()) {
            d = clink::config::dec_parse(value_expr_detail::to_text(args[0]));
        }
        if (!d)
            return value_expr_detail::null_value();
        auto rescaled = clink::config::dec_rescale(*d, scale);
        if (!rescaled || !rescaled->unscaled.FitsInPrecision(precision))
            return value_expr_detail::null_value();  // overflow past declared precision
        return clink::config::make_dec_value(*rescaled);
    }

    // ARRAY[e1, e2, ...] constructor -> a JSON array of the (already
    // evaluated) element values. NULL elements are kept (SQL arrays may
    // contain NULL).
    if (op == ValueOp::MakeArray) {
        clink::config::JsonArray arr;
        arr.reserve(args.size());
        for (auto& a : args) {
            arr.push_back(std::move(a));
        }
        return JsonValue{std::move(arr)};
    }
    // a[i] element access. SQL subscripts are 1-based. A non-array base, a
    // non-numeric index, a non-finite index (NaN/inf), or an out-of-range
    // index yields NULL (SQL-standard: no error). A finite fractional index
    // is truncated toward zero (cast to integer). The range test runs in
    // double space *before* the integer cast so NaN/inf/huge values can
    // never make `static_cast<...>(idx)` undefined behavior.
    if (op == ValueOp::ElementAt) {
        if (args.size() != 2) {
            throw std::runtime_error("json_value_expr: 'element_at' takes two args");
        }
        auto idx_opt = value_expr_detail::numeric_as_double(args[1]);  // #56: decimal index ok
        if (!args[0].is_array() || !idx_opt) {
            return value_expr_detail::null_value();
        }
        const auto& arr = args[0].as_array();
        const double idx_d = *idx_opt;
        if (!std::isfinite(idx_d) || idx_d < 1.0 ||
            idx_d >= static_cast<double>(arr.size()) + 1.0) {
            return value_expr_detail::null_value();
        }
        const auto idx = static_cast<std::size_t>(idx_d);
        return arr[idx - 1];
    }

    // ROW(...) constructor (Wave 5c) -> a JSON object keyed by field name.
    // "fields" holds the parallel field-name strings (lowered metadata, not
    // evaluated); "args" the already-evaluated field values. NULL field
    // values are kept. Keys are a std::map so the object is canonical
    // (name-sorted), making ROW(b,a) and ROW(a,b) compare equal under
    // changelog retraction when their named values match.
    if (op == ValueOp::MakeRow) {
        clink::config::JsonObject obj;
        for (const auto& [idx, fname] : ex.row_fields) {
            if (idx < args.size()) {
                obj[fname] = std::move(args[idx]);
            }
        }
        return JsonValue{std::move(obj)};
    }
    // (r).f field access (Wave 5c). A non-row (non-object) base or a
    // missing field yields NULL (SQL-standard: no error). An explicit-NULL
    // field and an absent field are indistinguishable (both NULL); clink
    // carries no field-nullability metadata to tell them apart.
    if (op == ValueOp::FieldAt) {
        if (args.size() != 1 || !args[0].is_object() || !ex.field.has_value()) {
            return value_expr_detail::null_value();
        }
        const auto& obj = args[0].as_object();
        auto it = obj.find(*ex.field);
        if (it == obj.end()) {
            return value_expr_detail::null_value();
        }
        return it->second;
    }
    // MAP(k1,v1,k2,v2,...) constructor (Wave 5b) -> a JSON object keyed by
    // each key's canonical string form. An odd argument count is a query
    // error (throw). A NULL key fails soft to a NULL map (SQL forbids null
    // keys; clink does not abort the stream on one bad row, matching the
    // NULL-on-bad-input convention of element_at/field_at). NULL values are
    // kept; a duplicate key is last-wins (std::map assignment).
    if (op == ValueOp::MapMake) {
        if (args.size() % 2 != 0) {
            throw std::runtime_error(
                "json_value_expr: 'map' takes an even number of args (key/value pairs)");
        }
        clink::config::JsonObject obj;
        for (std::size_t i = 0; i + 1 < args.size(); i += 2) {
            if (args[i].is_null()) {
                return value_expr_detail::null_value();
            }
            obj[value_expr_detail::to_map_key(args[i])] = std::move(args[i + 1]);
        }
        return JsonValue{std::move(obj)};
    }
    // m['k'] map element access (Wave 5b). A non-map (non-object) base, a
    // NULL key, or an absent key yields NULL (SQL-standard: no error). The
    // key is matched by its canonical string form (see to_map_key). Bound
    // only when the base type is statically MAP; an array base keeps
    // element_at (see the binder's Subscript dispatch).
    if (op == ValueOp::MapGet) {
        if (args.size() != 2 || !args[0].is_object() || args[1].is_null()) {
            return value_expr_detail::null_value();
        }
        const auto& obj = args[0].as_object();
        auto it = obj.find(value_expr_detail::to_map_key(args[1]));
        if (it == obj.end()) {
            return value_expr_detail::null_value();
        }
        return it->second;
    }

    // --- Extended scalar built-ins ---

    // substring(s, start, len?) - SQL: 1-based start, optional length;
    // out-of-range bounds clamp to the string. Null in any arg -> null.
    if (op == ValueOp::Substring) {
        if (args.size() < 2 || args.size() > 3) {
            throw std::runtime_error("json_value_expr: 'substring' takes 2 or 3 args");
        }
        for (const auto& a : args)
            if (a.is_null())
                return value_expr_detail::null_value();
        if (!args[1].is_number())
            return value_expr_detail::null_value();
        std::string s = value_expr_detail::to_text(args[0]);
        std::int64_t start = static_cast<std::int64_t>(args[1].as_number());
        // SQL substring is 1-based: start=1 means index 0 in C++.
        std::int64_t zero_start = start - 1;
        if (zero_start < 0)
            zero_start = 0;
        if (zero_start >= static_cast<std::int64_t>(s.size()))
            return JsonValue{std::string{}};
        std::size_t pos = static_cast<std::size_t>(zero_start);
        if (args.size() == 2)
            return JsonValue{s.substr(pos)};
        if (!args[2].is_number())
            return value_expr_detail::null_value();
        std::int64_t len = static_cast<std::int64_t>(args[2].as_number());
        if (len < 0)
            return value_expr_detail::null_value();
        return JsonValue{s.substr(pos, static_cast<std::size_t>(len))};
    }
    // position(haystack, needle) - PG's POSITION(needle IN haystack)
    // is reordered at parse time so args are [haystack, needle]; we
    // return the 1-based index, or 0 if absent.
    if (op == ValueOp::Position) {
        if (args.size() != 2)
            throw std::runtime_error("json_value_expr: 'position' takes 2 args");
        if (args[0].is_null() || args[1].is_null())
            return value_expr_detail::null_value();
        const std::string hay = value_expr_detail::to_text(args[0]);
        const std::string needle = value_expr_detail::to_text(args[1]);
        if (needle.empty())
            return JsonValue{static_cast<std::int64_t>(0)};
        auto idx = hay.find(needle);
        return JsonValue{idx == std::string::npos ? static_cast<std::int64_t>(0)
                                                  : static_cast<std::int64_t>(idx + 1)};
    }
    if (op == ValueOp::Replace) {
        if (args.size() != 3)
            throw std::runtime_error("json_value_expr: 'replace' takes 3 args");
        for (const auto& a : args)
            if (a.is_null())
                return value_expr_detail::null_value();
        std::string s = value_expr_detail::to_text(args[0]);
        const std::string from = value_expr_detail::to_text(args[1]);
        const std::string to = value_expr_detail::to_text(args[2]);
        if (from.empty())
            return JsonValue{s};
        std::string out;
        out.reserve(s.size());
        std::size_t i = 0;
        while (i < s.size()) {
            if (s.compare(i, from.size(), from) == 0) {
                out += to;
                i += from.size();
            } else {
                out += s[i++];
            }
        }
        return JsonValue{out};
    }
    // btrim / ltrim / rtrim. PG TRIM(LEADING/TRAILING/BOTH chars FROM
    // s) lowers to one of these. Without a chars arg, default to ' '.
    if (op == ValueOp::Btrim || op == ValueOp::Ltrim || op == ValueOp::Rtrim) {
        if (args.empty() || args.size() > 2)
            throw std::runtime_error("json_value_expr: '" + name + "' takes 1 or 2 args");
        if (args[0].is_null())
            return value_expr_detail::null_value();
        if (args.size() == 2 && args[1].is_null())
            return value_expr_detail::null_value();
        std::string s = value_expr_detail::to_text(args[0]);
        std::string chars =
            args.size() == 2 ? value_expr_detail::to_text(args[1]) : std::string{" \t\n\r"};
        if (op == ValueOp::Ltrim || op == ValueOp::Btrim) {
            std::size_t begin = 0;
            while (begin < s.size() && chars.find(s[begin]) != std::string::npos)
                ++begin;
            s.erase(0, begin);
        }
        if (op == ValueOp::Rtrim || op == ValueOp::Btrim) {
            std::size_t end = s.size();
            while (end > 0 && chars.find(s[end - 1]) != std::string::npos)
                --end;
            s.resize(end);
        }
        return JsonValue{s};
    }
    // regexp_extract(s, pattern, group?) - the `group`-th capture (default 0 =
    // the whole match) of the FIRST match of `pattern` in `s`. Returns null on no
    // match, an out-of-range group, a bad pattern, or any null arg.
    //
    // LIMITATION: this uses std::regex (ECMAScript), a backtracking engine, so a
    // pathological pattern (nested quantifiers like (a+)+ against a long
    // non-matching input) can backtrack super-linearly - i.e. it is open to
    // catastrophic-backtracking ReDoS if the PATTERN is attacker-controlled. In
    // SQL the pattern is a query literal written by the job author, not stream
    // data, so this is a query-authoring concern, not a data-path one. A
    // linear-time guarantee would need a non-backtracking engine (RE2), which is
    // a deliberate non-dependency here. Keep patterns simple and avoid nested
    // quantifiers over untrusted-length inputs.
    if (op == ValueOp::RegexpExtract) {
        if (args.size() < 2 || args.size() > 3) {
            throw std::runtime_error("json_value_expr: 'regexp_extract' takes 2 or 3 args");
        }
        for (const auto& a : args)
            if (a.is_null())
                return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        const std::string pat = value_expr_detail::to_text(args[1]);
        std::size_t group = 0;
        if (args.size() == 3) {
            if (!args[2].is_number())
                return value_expr_detail::null_value();
            const std::int64_t g = static_cast<std::int64_t>(args[2].as_number());
            if (g < 0)
                return value_expr_detail::null_value();
            group = static_cast<std::size_t>(g);
        }
        try {
            std::regex re(pat);
            std::smatch m;
            if (std::regex_search(s, m, re) && group < m.size()) {
                return JsonValue{m[group].str()};
            }
        } catch (const std::regex_error&) {
            return value_expr_detail::null_value();
        }
        return value_expr_detail::null_value();
    }
    // split_index(s, delim, idx) - the idx-th (0-based, Flink semantics) field of
    // `s` split by the string `delim`. Returns null on an out-of-range index, an
    // empty delimiter, or any null arg.
    if (op == ValueOp::SplitIndex) {
        if (args.size() != 3) {
            throw std::runtime_error("json_value_expr: 'split_index' takes 3 args");
        }
        for (const auto& a : args)
            if (a.is_null())
                return value_expr_detail::null_value();
        if (!args[2].is_number())
            return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        const std::string delim = value_expr_detail::to_text(args[1]);
        const std::int64_t idx = static_cast<std::int64_t>(args[2].as_number());
        if (idx < 0 || delim.empty())
            return value_expr_detail::null_value();
        std::size_t pos = 0;
        std::int64_t cur = 0;
        while (true) {
            const std::size_t next = s.find(delim, pos);
            if (cur == idx) {
                return JsonValue{next == std::string::npos ? s.substr(pos)
                                                           : s.substr(pos, next - pos)};
            }
            if (next == std::string::npos)
                return value_expr_detail::null_value();  // idx past the last field
            pos = next + delim.size();
            ++cur;
        }
    }
    // abs/floor/ceil/round accept a #56 dec-string operand via the double
    // value (the binder types these float64, so the result is a double).
    if (op == ValueOp::Abs) {
        if (args.size() != 1)
            throw std::runtime_error("json_value_expr: 'abs' takes one arg");
        auto n = value_expr_detail::numeric_as_double(args[0]);
        if (!n)
            return value_expr_detail::null_value();
        return JsonValue{std::fabs(*n)};
    }
    if (op == ValueOp::Floor) {
        if (args.size() != 1)
            throw std::runtime_error("json_value_expr: 'floor' takes one arg");
        auto n = value_expr_detail::numeric_as_double(args[0]);
        if (!n)
            return value_expr_detail::null_value();
        return JsonValue{std::floor(*n)};
    }
    if (op == ValueOp::Ceil) {
        if (args.size() != 1)
            throw std::runtime_error("json_value_expr: 'ceil' takes one arg");
        auto n = value_expr_detail::numeric_as_double(args[0]);
        if (!n)
            return value_expr_detail::null_value();
        return JsonValue{std::ceil(*n)};
    }
    if (op == ValueOp::Round) {
        if (args.empty() || args.size() > 2)
            throw std::runtime_error("json_value_expr: 'round' takes 1 or 2 args");
        auto n = value_expr_detail::numeric_as_double(args[0]);
        if (!n)
            return value_expr_detail::null_value();
        if (args.size() == 1)
            return JsonValue{std::round(*n)};
        auto digits = value_expr_detail::numeric_as_double(args[1]);
        if (!digits)
            return value_expr_detail::null_value();
        const double factor = std::pow(10.0, *digits);
        return JsonValue{std::round(*n * factor) / factor};
    }
    if (op == ValueOp::NullIf) {
        if (args.size() != 2)
            throw std::runtime_error("json_value_expr: 'nullif' takes 2 args");
        if (args[0].is_null())
            return value_expr_detail::null_value();
        // Compare by serialised value: matches both 1==1 and "a"=="a"
        // without type coercion. PG semantics are stricter (same type
        // required) but this is enough for the SQL frontend's needs.
        if (!args[1].is_null() && args[0].serialize(0) == args[1].serialize(0))
            return value_expr_detail::null_value();
        return args[0];
    }
    if (op == ValueOp::Greatest || op == ValueOp::Least) {
        if (args.empty())
            return value_expr_detail::null_value();
        const bool want_max = (op == ValueOp::Greatest);
        // Nulls are ignored in PG semantics; return null only if every
        // arg is null.
        std::optional<JsonValue> best;
        for (const auto& a : args) {
            if (a.is_null())
                continue;
            if (!best.has_value()) {
                best = a;
                continue;
            }
            const auto& b = *best;
            bool a_better = false;
            // #56: detail::compare is decimal-aware (and handles number/string/
            // bool); only fall back to a serialized compare for incomparable
            // mixed types.
            if (auto cmp = clink::operators::detail::compare(a, b)) {
                a_better = want_max ? (*cmp > 0) : (*cmp < 0);
            } else {
                a_better = want_max ? (a.serialize(0) > b.serialize(0))
                                    : (a.serialize(0) < b.serialize(0));
            }
            if (a_better)
                best = a;
        }
        return best.value_or(value_expr_detail::null_value());
    }

    // --- Parity wave: extended string builtins ---
    auto require_args = [&](std::size_t n) {
        if (args.size() != n) {
            throw std::runtime_error("json_value_expr: '" + name + "' takes " + std::to_string(n) +
                                     " args");
        }
    };
    auto any_null = [&]() {
        for (const auto& a : args)
            if (a.is_null())
                return true;
        return false;
    };
    if (op == ValueOp::CharLength) {
        require_args(1);
        if (args[0].is_null())
            return value_expr_detail::null_value();
        return JsonValue{static_cast<std::int64_t>(value_expr_detail::to_text(args[0]).size())};
    }
    if (op == ValueOp::Ascii) {
        require_args(1);
        if (args[0].is_null())
            return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        return JsonValue{
            static_cast<std::int64_t>(s.empty() ? 0 : static_cast<unsigned char>(s[0]))};
    }
    if (op == ValueOp::Chr) {
        require_args(1);
        if (args[0].is_null() || !args[0].is_number())
            return value_expr_detail::null_value();
        auto code = static_cast<int>(args[0].as_number());
        if (code < 0 || code > 255)
            return value_expr_detail::null_value();
        return JsonValue{std::string(1, static_cast<char>(code))};
    }
    if (op == ValueOp::Reverse) {
        require_args(1);
        if (args[0].is_null())
            return value_expr_detail::null_value();
        std::string s = value_expr_detail::to_text(args[0]);
        std::reverse(s.begin(), s.end());
        return JsonValue{s};
    }
    if (op == ValueOp::Repeat) {
        require_args(2);
        if (any_null() || !args[1].is_number())
            return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        auto n = static_cast<std::int64_t>(args[1].as_number());
        std::string out;
        for (std::int64_t i = 0; i < n; ++i)
            out += s;
        return JsonValue{out};
    }
    if (op == ValueOp::Initcap) {
        require_args(1);
        if (args[0].is_null())
            return value_expr_detail::null_value();
        std::string s = value_expr_detail::to_text(args[0]);
        bool start = true;
        for (char& c : s) {
            if (std::isalnum(static_cast<unsigned char>(c))) {
                c = start ? static_cast<char>(std::toupper(c)) : static_cast<char>(std::tolower(c));
                start = false;
            } else {
                start = true;
            }
        }
        return JsonValue{s};
    }
    if (op == ValueOp::Left || op == ValueOp::Right) {
        require_args(2);
        if (any_null() || !args[1].is_number())
            return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        auto n = static_cast<std::int64_t>(args[1].as_number());
        if (n <= 0)
            return JsonValue{std::string{}};
        auto count = std::min<std::size_t>(static_cast<std::size_t>(n), s.size());
        return JsonValue{op == ValueOp::Left ? s.substr(0, count) : s.substr(s.size() - count)};
    }
    if (op == ValueOp::Lpad || op == ValueOp::Rpad) {
        if (args.size() < 2 || args.size() > 3)
            throw std::runtime_error("json_value_expr: '" + name + "' takes 2 or 3 args");
        if (args[0].is_null() || args[1].is_null() || !args[1].is_number())
            return value_expr_detail::null_value();
        std::string s = value_expr_detail::to_text(args[0]);
        auto target = static_cast<std::int64_t>(args[1].as_number());
        std::string pad = args.size() == 3 && !args[2].is_null()
                              ? value_expr_detail::to_text(args[2])
                              : std::string{" "};
        if (target < 0)
            target = 0;
        auto tlen = static_cast<std::size_t>(target);
        if (s.size() >= tlen)
            return JsonValue{s.substr(0, tlen)};
        if (pad.empty())
            return JsonValue{s};
        std::string fill;
        while (fill.size() < tlen - s.size())
            fill += pad;
        fill.resize(tlen - s.size());
        return JsonValue{op == ValueOp::Lpad ? fill + s : s + fill};
    }
    if (op == ValueOp::SplitPart) {
        require_args(3);
        if (any_null() || !args[2].is_number())
            return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        const std::string delim = value_expr_detail::to_text(args[1]);
        auto idx = static_cast<std::int64_t>(args[2].as_number());  // 1-based
        if (idx < 1 || delim.empty())
            return JsonValue{std::string{}};
        std::size_t start = 0;
        std::int64_t part = 1;
        while (part < idx) {
            auto pos = s.find(delim, start);
            if (pos == std::string::npos)
                return JsonValue{std::string{}};
            start = pos + delim.size();
            ++part;
        }
        auto pos = s.find(delim, start);
        return JsonValue{
            s.substr(start, pos == std::string::npos ? std::string::npos : pos - start)};
    }
    if (op == ValueOp::StartsWith) {
        require_args(2);
        if (any_null())
            return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        const std::string prefix = value_expr_detail::to_text(args[1]);
        return JsonValue{s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0};
    }

    // --- Parity wave: extended math builtins (accept #56 decimals as double) ---
    auto unary_double = [&](auto fn) -> JsonValue {
        require_args(1);
        auto x = value_expr_detail::numeric_as_double(args[0]);
        if (!x)
            return value_expr_detail::null_value();
        return JsonValue{fn(*x)};
    };
    if (op == ValueOp::Sqrt)
        return unary_double([](double x) { return std::sqrt(x); });
    if (op == ValueOp::Exp)
        return unary_double([](double x) { return std::exp(x); });
    if (op == ValueOp::Ln)
        return unary_double([](double x) { return std::log(x); });
    if (op == ValueOp::Log10)
        return unary_double([](double x) { return std::log10(x); });
    if (op == ValueOp::Sign)
        return unary_double([](double x) { return (x > 0) - (x < 0); });
    if (op == ValueOp::Power) {
        require_args(2);
        auto base = value_expr_detail::numeric_as_double(args[0]);
        auto exp_v = value_expr_detail::numeric_as_double(args[1]);
        if (!base || !exp_v)
            return value_expr_detail::null_value();
        return JsonValue{std::pow(*base, *exp_v)};
    }
    if (op == ValueOp::Trunc) {
        if (args.empty() || args.size() > 2)
            throw std::runtime_error("json_value_expr: 'trunc' takes 1 or 2 args");
        auto x = value_expr_detail::numeric_as_double(args[0]);
        if (!x)
            return value_expr_detail::null_value();
        if (args.size() == 1)
            return JsonValue{std::trunc(*x)};
        auto digits = value_expr_detail::numeric_as_double(args[1]);
        if (!digits)
            return value_expr_detail::null_value();
        const double factor = std::pow(10.0, *digits);
        return JsonValue{std::trunc(*x * factor) / factor};
    }

    // --- Date / time (epoch-millis int64, UTC) ---
    // EXTRACT(field FROM ts) parses as extract('field', ts); the field
    // is a lowercase string literal.
    if (op == ValueOp::Extract) {
        if (args.size() != 2)
            throw std::runtime_error("json_value_expr: 'extract' takes 2 args");
        if (args[0].is_null() || args[1].is_null() || !args[1].is_number())
            return value_expr_detail::null_value();
        std::string field = value_expr_detail::to_text(args[0]);
        std::transform(field.begin(), field.end(), field.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        const auto ms = static_cast<std::int64_t>(args[1].as_number());
        constexpr std::int64_t kDayMs = 86400000;
        const std::int64_t days = value_expr_detail::floordiv(ms, kDayMs);
        const std::int64_t tod = ms - days * kDayMs;
        const auto cd = value_expr_detail::civil_from_days(days);
        std::int64_t result = 0;
        if (field == "year")
            result = cd.y;
        else if (field == "month")
            result = cd.m;
        else if (field == "day")
            result = cd.d;
        else if (field == "hour")
            result = tod / 3600000;
        else if (field == "minute")
            result = (tod / 60000) % 60;
        else if (field == "second")
            result = (tod / 1000) % 60;
        else if (field == "dow")
            result = value_expr_detail::weekday_from_days(days);
        else if (field == "doy")
            result = days - value_expr_detail::days_from_civil(cd.y, 1, 1) + 1;
        else if (field == "quarter")
            result = (cd.m - 1) / 3 + 1;
        else if (field == "epoch")
            result = value_expr_detail::floordiv(ms, 1000);
        else
            throw std::runtime_error("json_value_expr: extract: unsupported field '" + field + "'");
        return JsonValue{result};
    }
    if (op == ValueOp::DateTrunc) {
        if (args.size() != 2)
            throw std::runtime_error("json_value_expr: 'date_trunc' takes 2 args");
        if (args[0].is_null() || args[1].is_null() || !args[1].is_number())
            return value_expr_detail::null_value();
        std::string unit = value_expr_detail::to_text(args[0]);
        std::transform(unit.begin(), unit.end(), unit.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        const auto ms = static_cast<std::int64_t>(args[1].as_number());
        constexpr std::int64_t kDayMs = 86400000;
        auto floor_to = [&](std::int64_t step) {
            return value_expr_detail::floordiv(ms, step) * step;
        };
        if (unit == "second")
            return JsonValue{floor_to(1000)};
        if (unit == "minute")
            return JsonValue{floor_to(60000)};
        if (unit == "hour")
            return JsonValue{floor_to(3600000)};
        if (unit == "day")
            return JsonValue{floor_to(kDayMs)};
        const std::int64_t days = value_expr_detail::floordiv(ms, kDayMs);
        const auto cd = value_expr_detail::civil_from_days(days);
        if (unit == "week") {  // ISO: truncate to Monday.
            const std::int64_t offset = (value_expr_detail::weekday_from_days(days) + 6) % 7;
            return JsonValue{(days - offset) * kDayMs};
        }
        if (unit == "month")
            return JsonValue{value_expr_detail::days_from_civil(cd.y, cd.m, 1) * kDayMs};
        if (unit == "quarter") {
            const unsigned qm = (cd.m - 1) / 3 * 3 + 1;
            return JsonValue{value_expr_detail::days_from_civil(cd.y, qm, 1) * kDayMs};
        }
        if (unit == "year")
            return JsonValue{value_expr_detail::days_from_civil(cd.y, 1, 1) * kDayMs};
        throw std::runtime_error("json_value_expr: date_trunc: unsupported unit '" + unit + "'");
    }
    if (op == ValueOp::DateFormat) {
        if (args.size() != 2)
            throw std::runtime_error("json_value_expr: 'date_format' takes 2 args");
        if (args[0].is_null() || args[1].is_null() || !args[0].is_number())
            return value_expr_detail::null_value();
        return JsonValue{value_expr_detail::format_timestamp(
            static_cast<std::int64_t>(args[0].as_number()), value_expr_detail::to_text(args[1]))};
    }
    if (op == ValueOp::ToTimestamp) {
        if (args.empty() || args.size() > 2)
            throw std::runtime_error("json_value_expr: 'to_timestamp' takes 1 or 2 args");
        if (args[0].is_null() || (args.size() == 2 && args[1].is_null()))
            return value_expr_detail::null_value();
        const std::string s = value_expr_detail::to_text(args[0]);
        const std::string fmt =
            args.size() == 2 ? value_expr_detail::to_text(args[1]) : "yyyy-MM-dd HH:mm:ss";
        auto ms = value_expr_detail::parse_timestamp(s, fmt);
        if (!ms.has_value())
            return value_expr_detail::null_value();
        return JsonValue{*ms};
    }
    // --- JSON path / construction ---
    // JSON_VALUE(json, path) -> scalar as text (null for object/array
    // or a missing path). JSON_QUERY(json, path) -> object/array as
    // JSON text (null for a scalar). JSON_EXISTS(json, path) -> bool.
    // The json arg may be JSON text or an already-structured value.
    if (op == ValueOp::JsonValueFn || op == ValueOp::JsonQuery || op == ValueOp::JsonExists) {
        if (args.size() != 2)
            throw std::runtime_error("json_value_expr: '" + name + "' takes 2 args");
        if (args[0].is_null() || args[1].is_null() || !args[1].is_string()) {
            return value_expr_detail::null_value();
        }
        const auto steps = value_expr_detail::parse_json_path(args[1].as_string());
        if (!steps.has_value()) {
            if (op == ValueOp::JsonExists)
                return JsonValue{false};
            return value_expr_detail::null_value();
        }
        auto node =
            value_expr_detail::navigate_json(value_expr_detail::coerce_json(args[0]), *steps);
        if (op == ValueOp::JsonExists)
            return JsonValue{node.has_value()};
        if (!node.has_value())
            return value_expr_detail::null_value();
        const JsonValue& n = *node;
        if (op == ValueOp::JsonValueFn) {
            if (n.is_null() || n.is_object() || n.is_array())
                return value_expr_detail::null_value();
            if (n.is_string())
                return JsonValue{n.as_string()};
            return JsonValue{value_expr_detail::to_text(n)};
        }
        // json_query: only objects / arrays, serialized back to text.
        if (n.is_object() || n.is_array())
            return JsonValue{n.serialize(0)};
        return value_expr_detail::null_value();
    }
    // JSON_OBJECT('k1', v1, 'k2', v2, ...) -> JSON object text. (The
    // SQL KEY 'k' VALUE v spelling is not modeled; use the positional
    // form.) Keys sorted for deterministic output.
    if (op == ValueOp::JsonObjectFn) {
        if (args.size() % 2 != 0) {
            throw std::runtime_error(
                "json_value_expr: 'json_object' takes an even number of args (key, value, ...)");
        }
        clink::config::JsonObject obj;
        for (std::size_t i = 0; i + 1 < args.size(); i += 2) {
            obj[value_expr_detail::to_text(args[i])] = args[i + 1];
        }
        return JsonValue{JsonValue{std::move(obj)}.serialize(0)};
    }
    // SQLOPT-3: not a built-in (ValueOp::Udf) - try a registered scalar UDF,
    // invoked with the already-evaluated argument values. Resolved at
    // EVALUATION time, not compile time, so late-registered catalog UDFs and
    // DROP FUNCTION behave exactly as before. Absent => genuinely unknown op.
    if (auto udf = clink::ScalarFunctionRegistry::global().lookup(name); udf.has_value()) {
        return udf->fn(args);
    }
    throw std::runtime_error("json_value_expr: unknown op '" + name + "'");
}

// Compile the value-expression JSON into a node tree. Never throws for
// a malformed shape: the error is packaged as a ValueThrowNode so it
// surfaces on evaluation, exactly where the interpreter surfaced it.
inline ValueNodePtr compile_value_expr(const clink::config::JsonValue& expr) {
    using clink::config::JsonValue;
    auto malformed = [](std::string msg) -> ValueNodePtr {
        return std::make_unique<ValueThrowNode>(std::move(msg));
    };
    if (!expr.is_object()) {
        return malformed("json_value_expr: expression must be an object");
    }
    if (expr.contains("col")) {
        const JsonValue& c = expr.at("col");
        if (!c.is_string()) {
            return malformed("json_value_expr: 'col' must be a string");
        }
        return std::make_unique<ColNode>(c.as_string());
    }
    if (expr.contains("lit")) {
        return std::make_unique<LitNode>(expr.at("lit"));
    }
    if (!expr.contains("op")) {
        return malformed("json_value_expr: missing 'op'/'col'/'lit'");
    }
    const JsonValue& opv = expr.at("op");
    if (!opv.is_string()) {
        return malformed("json_value_expr: 'op' must be a string");
    }
    const std::string& op = opv.as_string();

    if (op == "case") {
        if (!expr.contains("branches") || !expr.at("branches").is_array()) {
            return malformed("json_value_expr: 'case' needs a 'branches' array");
        }
        std::vector<CaseNode::Branch> branches;
        branches.reserve(expr.at("branches").as_array().size());
        for (const auto& b : expr.at("branches").as_array()) {
            CaseNode::Branch br;
            if (b.is_object() && b.contains("when")) {
                br.when = pred_detail::compile_pred(b.at("when"));
            } else {
                br.when = std::make_unique<pred_detail::PredThrowNode>(
                    "json_value_expr: case branch needs 'when'");
            }
            if (b.is_object() && b.contains("then")) {
                br.then = compile_value_expr(b.at("then"));
            } else {
                br.then = malformed("json_value_expr: case branch needs 'then'");
            }
            branches.push_back(std::move(br));
        }
        ValueNodePtr else_node;  // absent ELSE -> NULL
        if (expr.contains("else")) {
            else_node = compile_value_expr(expr.at("else"));
        }
        return std::make_unique<CaseNode>(std::move(branches), std::move(else_node));
    }

    // Ops evaluate their 'args' children eagerly (CASE above is the only
    // lazy construct). A missing or non-array 'args' means zero args,
    // matching the interpreter.
    std::vector<ValueNodePtr> children;
    if (expr.contains("args") && expr.at("args").is_array()) {
        children.reserve(expr.at("args").as_array().size());
        for (const auto& a : expr.at("args").as_array()) {
            children.push_back(compile_value_expr(a));
        }
    }

    OpExtras ex;
    const auto vo = lookup_value_op(op);
    if (!vo.has_value()) {
        return std::make_unique<OpNode>(ValueOp::Udf, op, std::move(children), std::move(ex));
    }
    if (*vo == ValueOp::CastDecimal) {
        if (expr.contains("scale")) {
            const JsonValue& v = expr.at("scale");
            if (!v.is_number()) {
                return malformed("json_value_expr: 'cast_decimal' scale must be a number");
            }
            ex.scale = static_cast<int>(v.as_number());
        }
        if (expr.contains("precision")) {
            const JsonValue& v = expr.at("precision");
            if (!v.is_number()) {
                return malformed("json_value_expr: 'cast_decimal' precision must be a number");
            }
            ex.precision = static_cast<int>(v.as_number());
        }
    } else if (*vo == ValueOp::MakeRow) {
        if (expr.contains("fields") && expr.at("fields").is_array()) {
            const auto& name_arr = expr.at("fields").as_array();
            for (std::size_t i = 0; i < name_arr.size() && i < children.size(); ++i) {
                if (name_arr[i].is_string()) {
                    ex.row_fields.emplace_back(i, name_arr[i].as_string());
                }
            }
        }
    } else if (*vo == ValueOp::FieldAt) {
        if (expr.contains("field") && expr.at("field").is_string()) {
            ex.field = expr.at("field").as_string();
        }
    }
    return std::make_unique<OpNode>(*vo, op, std::move(children), std::move(ex));
}

}  // namespace value_expr_detail

// A value expression compiled to a node tree. Compile once at operator
// build, evaluate per record. Copyable (the immutable tree is shared),
// so it can be captured in std::function-based operator closures.
class CompiledValueExpr {
public:
    static CompiledValueExpr compile(const clink::config::JsonValue& expr) {
        return CompiledValueExpr{value_expr_detail::compile_value_expr(expr)};
    }

    clink::config::JsonValue evaluate(const ColumnLookup& resolve) const {
        return root_->eval(resolve);
    }

private:
    explicit CompiledValueExpr(value_expr_detail::ValueNodePtr root) : root_(std::move(root)) {}
    std::shared_ptr<const value_expr_detail::ValueNode> root_;
};

// One-shot convenience entry point (compile then evaluate). Per-record
// callers should compile once and reuse the CompiledValueExpr instead.
template <ValueColumnResolver Resolver>
clink::config::JsonValue evaluate_json_value_expr(const clink::config::JsonValue& expr,
                                                  Resolver& resolve) {
    const ColumnLookup lookup{resolve};
    return value_expr_detail::compile_value_expr(expr)->eval(lookup);
}

}  // namespace clink::operators
