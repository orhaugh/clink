#pragma once

// Typed columnar expression programs (WS1).
//
// A compiled, column-index-resolved program that evaluates a SQL predicate
// over a whole Arrow RecordBatch column-at-a-time, replacing the per-row
// json_predicate walk (string-ladder op dispatch + per-cell read_cell ->
// JsonValue box + std::map probe) on the columnar hot path.
//
// Parity is the contract. ColumnarPredicateProgram::compile() only accepts the
// type-matched comparison buckets where a typed kernel is PROVABLY identical to
// detail::compare + the three-valued logic in evaluate_json_predicate_tri:
//   * numeric column (int64/int32/double/float) vs a plain numeric literal,
//   * decimal128 column vs a sentinel-tagged dec-string literal (exact),
//   * string column vs a plain string literal (lexicographic),
//   * bool column vs a bool literal,
//   * is_null / is_not_null on a present column,
//   * and / or / not over the above (SQL three-valued logic).
// For ANYTHING else (mixed numeric/string coercion, LIKE, IN, rhs_col, a
// missing column, a null/dec-string/string literal against a numeric column,
// etc.) compile() returns nullopt and the caller falls back to the row
// interpreter. So an unsupported shape is never wrong, only un-accelerated.
//
// Increment 1 ships the predicate program (WHERE). The value program (SELECT
// projections) is the next increment and will live alongside it here.

#ifdef CLINK_HAS_ARROW

#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"

namespace clink::operators {

namespace expr_detail {

// Physical bucket of a compiled comparison leaf - mirrors how read_cell types a
// column so the kernel reproduces detail::compare's branch exactly.
enum class CmpType { Numeric, Decimal, Str, Bool };
enum class CmpOp { Eq, Ne, Lt, Le, Gt, Ge };

// Map a comparison sign (-1/0/1) to the boolean for an op. Byte-identical to
// the eq/ne/lt/le/gt/ge tail of evaluate_json_predicate_tri.
inline bool apply_cmp(CmpOp op, int c) noexcept {
    switch (op) {
        case CmpOp::Eq:
            return c == 0;
        case CmpOp::Ne:
            return c != 0;
        case CmpOp::Lt:
            return c < 0;
        case CmpOp::Le:
            return c <= 0;
        case CmpOp::Gt:
            return c > 0;
        case CmpOp::Ge:
            return c >= 0;
    }
    return false;
}

// Ordering sign for two doubles, identical to detail::compare's numeric path
// (NaN compares as equal there too: neither < nor >, so 0).
inline int cmp3_d(double a, double b) noexcept {
    return (a < b) ? -1 : (a > b ? 1 : 0);
}

// Resolve a Row VALUE column by name the way the row path (Row.values) does:
// EXCLUDING the position-0 event-time column that every columnar Row batch
// carries. schema.GetFieldIndex would return 0 for a name colliding with the
// engine's "event_time" field, which process() never exposes in Row.values, so
// a columnar path using GetFieldIndex would read the timestamp while the row
// path resolves NULL. Searching from index 1 keeps the columnar and row paths
// byte-identical (a genuine value column still resolves at index >= 1).
inline int value_field_index(const arrow::Schema& schema, const std::string& name) {
    for (int i = 1; i < schema.num_fields(); ++i) {
        if (schema.field(i)->name() == name) {
            return i;
        }
    }
    return -1;
}

}  // namespace expr_detail

class ColumnarPredicateProgram {
public:
    struct Result {
        std::shared_ptr<arrow::BooleanArray> mask;  // true == keep (predicate True)
        std::int64_t kept{0};
    };

    // Compile `pred` against `schema`, or nullopt if any node is outside the
    // supported, parity-proven set (caller must then run the row predicate).
    static std::optional<ColumnarPredicateProgram> compile(const clink::config::JsonValue& pred,
                                                           const arrow::Schema& schema) {
        ColumnarPredicateProgram prog;
        const int root = prog.compile_node_(pred, schema);
        if (root < 0) {
            return std::nullopt;
        }
        prog.root_ = root;
        return prog;
    }

    // Evaluate over `rb` (compiled against an equal schema). Returns the keep
    // mask + kept count, or nullopt on an unexpected Arrow build error so the
    // caller can fall back without having emitted anything.
    [[nodiscard]] std::optional<Result> evaluate(const arrow::RecordBatch& rb) const {
        const std::int64_t n = rb.num_rows();
        // A string column can carry a dec-string (0x01-sentinel) cell, which the
        // row interpreter routes through detail::compare's decimal/coercion
        // branch rather than a lexicographic compare. The Str kernel cannot
        // reproduce that, so it sets bail and we defer the whole batch to the
        // row interpreter (correct, just un-accelerated). The common no-sentinel
        // case never trips it.
        bool bail = false;
        const TriVec r = eval_node_(root_, rb, bail);
        if (bail) {
            return std::nullopt;
        }
        arrow::BooleanBuilder b;
        if (!b.Reserve(n).ok()) {
            return std::nullopt;
        }
        std::int64_t kept = 0;
        for (std::int64_t i = 0; i < n; ++i) {
            const bool keep = (r.known[static_cast<std::size_t>(i)] != 0) &&
                              (r.val[static_cast<std::size_t>(i)] != 0);
            b.UnsafeAppend(keep);
            kept += keep ? 1 : 0;
        }
        std::shared_ptr<arrow::Array> mask;
        if (!b.Finish(&mask).ok()) {
            return std::nullopt;
        }
        return Result{std::static_pointer_cast<arrow::BooleanArray>(mask), kept};
    }

private:
    enum class NOp { Cmp, IsNull, IsNotNull, And, Or, Not };

    struct Node {
        NOp op{};
        // Cmp leaf.
        int col_idx{-1};
        arrow::Type::type arrow_type{};  // INT64/INT32/DOUBLE/FLOAT/STRING/BOOL/DECIMAL128
        expr_detail::CmpType ctype{};
        expr_detail::CmpOp cmp{};
        double lit_num{0.0};                            // Numeric
        std::optional<clink::config::Decimal> lit_dec;  // Decimal
        std::string lit_str;                            // Str
        bool lit_bool{false};                           // Bool
        int dec_scale{0};                               // column decimal scale
        // And/Or/Not.
        std::vector<int> children;
    };

    // Per-row three-valued result: val (the boolean), known (false == Unknown).
    struct TriVec {
        std::vector<std::uint8_t> val;
        std::vector<std::uint8_t> known;
    };

    int add_(Node n) {
        nodes_.push_back(std::move(n));
        return static_cast<int>(nodes_.size()) - 1;
    }

    // Returns the node index, or -1 if this subtree is not representable.
    int compile_node_(const clink::config::JsonValue& pred, const arrow::Schema& schema) {
        if (!pred.is_object() || !pred.contains("op")) {
            return -1;
        }
        const auto& op = pred.at("op").as_string();

        if (op == "and" || op == "or") {
            if (!pred.contains("args") || !pred.at("args").is_array()) {
                return -1;
            }
            Node n;
            n.op = (op == "and") ? NOp::And : NOp::Or;
            for (const auto& sub : pred.at("args").as_array()) {
                const int c = compile_node_(sub, schema);
                if (c < 0) {
                    return -1;
                }
                n.children.push_back(c);
            }
            if (n.children.empty()) {
                return -1;
            }
            return add_(std::move(n));
        }
        if (op == "not") {
            if (!pred.contains("arg")) {
                return -1;
            }
            const int c = compile_node_(pred.at("arg"), schema);
            if (c < 0) {
                return -1;
            }
            Node n;
            n.op = NOp::Not;
            n.children.push_back(c);
            return add_(std::move(n));
        }
        if (op == "is_null" || op == "is_not_null") {
            const int idx = field_index_(pred, schema);
            if (idx < 0) {
                return -1;
            }
            Node n;
            n.op = (op == "is_null") ? NOp::IsNull : NOp::IsNotNull;
            n.col_idx = idx;
            return add_(std::move(n));
        }
        // Comparison ops only beyond here. A column-vs-column form (rhs_col),
        // LIKE, IN and any unknown op are not compiled.
        if (op != "eq" && op != "ne" && op != "lt" && op != "le" && op != "gt" && op != "ge") {
            return -1;
        }
        if (pred.contains("rhs_col") || !pred.contains("literal")) {
            return -1;  // column-vs-column or malformed -> fall back
        }
        const int idx = field_index_(pred, schema);
        if (idx < 0) {
            return -1;
        }
        const auto& lit = pred.at("literal");
        if (lit.is_null()) {
            return -1;  // NULL literal -> Unknown for every row; let the row path handle it
        }

        Node n;
        n.op = NOp::Cmp;
        n.col_idx = idx;
        n.cmp = map_cmp_(op);
        n.arrow_type = schema.field(idx)->type()->id();

        switch (n.arrow_type) {
            case arrow::Type::INT64:
            case arrow::Type::INT32:
            case arrow::Type::DOUBLE:
            case arrow::Type::FLOAT:
                // Numeric column: only a plain numeric literal stays on the
                // numeric branch of detail::compare (a dec-string literal would
                // trip the decimal branch; a string would need per-row coercion).
                if (!lit.is_number() || clink::config::is_dec_string(lit)) {
                    return -1;
                }
                n.ctype = expr_detail::CmpType::Numeric;
                n.lit_num = lit.as_number();
                break;
            case arrow::Type::DECIMAL128: {
                if (!clink::config::is_dec_string(lit)) {
                    return -1;
                }
                auto d = clink::config::as_decimal(lit);
                if (!d) {
                    return -1;
                }
                n.ctype = expr_detail::CmpType::Decimal;
                n.lit_dec = *d;
                n.dec_scale =
                    static_cast<const arrow::Decimal128Type&>(*schema.field(idx)->type()).scale();
                break;
            }
            case arrow::Type::BOOL:
                if (!lit.is_bool()) {
                    return -1;
                }
                n.ctype = expr_detail::CmpType::Bool;
                n.lit_bool = lit.as_bool();
                break;
            case arrow::Type::STRING:
                // Plain string literal only. A dec-string sentinel would trip
                // the decimal branch; a numeric literal would need coercion.
                if (!lit.is_string() || clink::config::is_dec_string(lit)) {
                    return -1;
                }
                n.ctype = expr_detail::CmpType::Str;
                n.lit_str = lit.as_string();
                break;
            default:
                return -1;  // unsupported column physical type -> fall back
        }
        return add_(std::move(n));
    }

    static expr_detail::CmpOp map_cmp_(const std::string& op) {
        if (op == "eq") {
            return expr_detail::CmpOp::Eq;
        }
        if (op == "ne") {
            return expr_detail::CmpOp::Ne;
        }
        if (op == "lt") {
            return expr_detail::CmpOp::Lt;
        }
        if (op == "le") {
            return expr_detail::CmpOp::Le;
        }
        if (op == "gt") {
            return expr_detail::CmpOp::Gt;
        }
        return expr_detail::CmpOp::Ge;
    }

    static int field_index_(const clink::config::JsonValue& pred, const arrow::Schema& schema) {
        if (!pred.contains("col")) {
            return -1;
        }
        return expr_detail::value_field_index(schema, pred.at("col").as_string());
    }

    TriVec eval_node_(int idx, const arrow::RecordBatch& rb, bool& bail) const {
        const Node& node = nodes_[static_cast<std::size_t>(idx)];
        const auto n = static_cast<std::size_t>(rb.num_rows());
        TriVec r{std::vector<std::uint8_t>(n, 0), std::vector<std::uint8_t>(n, 0)};

        switch (node.op) {
            case NOp::IsNull:
            case NOp::IsNotNull: {
                const auto& arr = *rb.column(node.col_idx);
                const bool want_null = (node.op == NOp::IsNull);
                for (std::size_t i = 0; i < n; ++i) {
                    r.known[i] = 1;  // IS [NOT] NULL is always two-valued
                    r.val[i] = (arr.IsNull(static_cast<std::int64_t>(i)) == want_null) ? 1 : 0;
                }
                return r;
            }
            case NOp::Not: {
                r = eval_node_(node.children[0], rb, bail);
                for (std::size_t i = 0; i < n; ++i) {
                    if (r.known[i] != 0) {
                        r.val[i] = r.val[i] != 0 ? 0 : 1;  // NOT Unknown stays Unknown
                    }
                }
                return r;
            }
            case NOp::And: {
                // Identity: True.
                for (std::size_t i = 0; i < n; ++i) {
                    r.val[i] = 1;
                    r.known[i] = 1;
                }
                for (const int c : node.children) {
                    const TriVec cv = eval_node_(c, rb, bail);
                    for (std::size_t i = 0; i < n; ++i) {
                        const bool a_false = (r.known[i] != 0) && (r.val[i] == 0);
                        const bool b_false = (cv.known[i] != 0) && (cv.val[i] == 0);
                        if (a_false || b_false) {
                            r.known[i] = 1;
                            r.val[i] = 0;
                        } else if (r.known[i] == 0 || cv.known[i] == 0) {
                            r.known[i] = 0;
                            r.val[i] = 0;
                        } else {
                            r.known[i] = 1;
                            r.val[i] = 1;
                        }
                    }
                }
                return r;
            }
            case NOp::Or: {
                // Identity: False.
                for (std::size_t i = 0; i < n; ++i) {
                    r.val[i] = 0;
                    r.known[i] = 1;
                }
                for (const int c : node.children) {
                    const TriVec cv = eval_node_(c, rb, bail);
                    for (std::size_t i = 0; i < n; ++i) {
                        const bool a_true = (r.known[i] != 0) && (r.val[i] != 0);
                        const bool b_true = (cv.known[i] != 0) && (cv.val[i] != 0);
                        if (a_true || b_true) {
                            r.known[i] = 1;
                            r.val[i] = 1;
                        } else if (r.known[i] == 0 || cv.known[i] == 0) {
                            r.known[i] = 0;
                            r.val[i] = 0;
                        } else {
                            r.known[i] = 1;
                            r.val[i] = 0;
                        }
                    }
                }
                return r;
            }
            case NOp::Cmp:
                eval_cmp_(node, rb, r, bail);
                return r;
        }
        return r;
    }

    void eval_cmp_(const Node& node, const arrow::RecordBatch& rb, TriVec& r, bool& bail) const {
        const auto& arr = *rb.column(node.col_idx);
        const auto n = static_cast<std::size_t>(rb.num_rows());
        const expr_detail::CmpOp op = node.cmp;

        switch (node.ctype) {
            case expr_detail::CmpType::Numeric: {
                const double lit = node.lit_num;
                auto run = [&](auto&& read) {
                    for (std::size_t i = 0; i < n; ++i) {
                        const auto ii = static_cast<std::int64_t>(i);
                        if (arr.IsNull(ii)) {
                            continue;  // NULL -> Unknown (known stays 0)
                        }
                        r.known[i] = 1;
                        r.val[i] =
                            expr_detail::apply_cmp(op, expr_detail::cmp3_d(read(ii), lit)) ? 1 : 0;
                    }
                };
                if (node.arrow_type == arrow::Type::INT64) {
                    const auto& a = static_cast<const arrow::Int64Array&>(arr);
                    run([&](std::int64_t ii) { return static_cast<double>(a.Value(ii)); });
                } else if (node.arrow_type == arrow::Type::INT32) {
                    const auto& a = static_cast<const arrow::Int32Array&>(arr);
                    run([&](std::int64_t ii) { return static_cast<double>(a.Value(ii)); });
                } else if (node.arrow_type == arrow::Type::DOUBLE) {
                    const auto& a = static_cast<const arrow::DoubleArray&>(arr);
                    run([&](std::int64_t ii) { return a.Value(ii); });
                } else {  // FLOAT
                    const auto& a = static_cast<const arrow::FloatArray&>(arr);
                    run([&](std::int64_t ii) { return static_cast<double>(a.Value(ii)); });
                }
                return;
            }
            case expr_detail::CmpType::Decimal: {
                const auto& a = static_cast<const arrow::Decimal128Array&>(arr);
                for (std::size_t i = 0; i < n; ++i) {
                    const auto ii = static_cast<std::int64_t>(i);
                    if (arr.IsNull(ii)) {
                        continue;
                    }
                    const clink::config::Decimal cell{arrow::Decimal128(a.GetValue(ii)),
                                                      node.dec_scale};
                    r.known[i] = 1;
                    r.val[i] =
                        expr_detail::apply_cmp(op, clink::config::dec_compare(cell, *node.lit_dec))
                            ? 1
                            : 0;
                }
                return;
            }
            case expr_detail::CmpType::Str: {
                const auto& a = static_cast<const arrow::StringArray&>(arr);
                for (std::size_t i = 0; i < n; ++i) {
                    const auto ii = static_cast<std::int64_t>(i);
                    if (arr.IsNull(ii)) {
                        continue;
                    }
                    const std::string_view sv = a.GetView(ii);
                    // A dec-string (0x01-sentinel) cell is read by the row path
                    // as a decimal (detail::compare's decimal/coercion branch),
                    // not a lexicographic string. The Str kernel cannot match
                    // that, so defer the whole batch to the row interpreter.
                    if (!sv.empty() && sv.front() == clink::config::kDecimalSentinel) {
                        bail = true;
                        return;
                    }
                    const int c = sv.compare(node.lit_str);
                    r.known[i] = 1;
                    r.val[i] = expr_detail::apply_cmp(op, (c < 0) ? -1 : (c > 0 ? 1 : 0)) ? 1 : 0;
                }
                return;
            }
            case expr_detail::CmpType::Bool: {
                const auto& a = static_cast<const arrow::BooleanArray&>(arr);
                const int ib = node.lit_bool ? 1 : 0;
                for (std::size_t i = 0; i < n; ++i) {
                    const auto ii = static_cast<std::int64_t>(i);
                    if (arr.IsNull(ii)) {
                        continue;
                    }
                    const int ia = a.Value(ii) ? 1 : 0;
                    r.known[i] = 1;
                    r.val[i] = expr_detail::apply_cmp(op, expr_detail::cmp3_d(ia, ib)) ? 1 : 0;
                }
                return;
            }
        }
    }

    std::vector<Node> nodes_;
    int root_{-1};
};

// Typed columnar VALUE program (WS1 inc2): compiles a numeric SQL scalar
// expression - arithmetic add/sub/mul/div/mod/neg over numeric columns
// (int64/int32/double/float) and plain numeric literals - into a program that
// produces a float64 output column byte-identical to evaluate_json_value_expr
// (numeric_binop): a double result, NULL when any operand is null, NaN on
// div/mod by zero. The binder already declares arithmetic outputs as float64,
// so the produced column type matches the row path's downstream type too.
// compile() returns nullopt for anything else (a decimal/string/bool operand, a
// dec-string literal, casts, concat, CASE, functions, a bare column or literal
// top-level), so the projection operator defers that output to the row path.
class ColumnarValueProgram {
public:
    static std::optional<ColumnarValueProgram> compile(const clink::config::JsonValue& expr,
                                                       const arrow::Schema& schema) {
        ColumnarValueProgram p;
        const int root = p.compile_node_(expr, schema);
        if (root < 0) {
            return std::nullopt;
        }
        p.root_ = root;
        return p;
    }

    // A float64 array of length rb.num_rows(), NULL where any operand is null.
    // nullptr on an unexpected Arrow build error (caller falls back).
    [[nodiscard]] std::shared_ptr<arrow::Array> evaluate(const arrow::RecordBatch& rb) const {
        const DVec r = eval_node_(root_, rb);
        const auto n = static_cast<std::size_t>(rb.num_rows());
        arrow::DoubleBuilder b;
        if (!b.Reserve(rb.num_rows()).ok()) {
            return nullptr;
        }
        for (std::size_t i = 0; i < n; ++i) {
            if (r.valid[i] != 0) {
                b.UnsafeAppend(r.val[i]);
            } else {
                b.UnsafeAppendNull();
            }
        }
        std::shared_ptr<arrow::Array> out;
        if (!b.Finish(&out).ok()) {
            return nullptr;
        }
        return out;
    }

private:
    enum class VOp { Col, Lit, Add, Sub, Mul, Div, Mod, Neg };

    struct Node {
        VOp op{};
        int col_idx{-1};
        arrow::Type::type arrow_type{};
        double lit{0.0};
        std::vector<int> children;
    };

    // Per-row double value + validity (false == any contributing operand null).
    struct DVec {
        std::vector<double> val;
        std::vector<std::uint8_t> valid;
    };

    int add_(Node n) {
        nodes_.push_back(std::move(n));
        return static_cast<int>(nodes_.size()) - 1;
    }

    int compile_node_(const clink::config::JsonValue& e, const arrow::Schema& schema) {
        if (!e.is_object()) {
            return -1;
        }
        if (e.contains("col")) {
            const int idx = expr_detail::value_field_index(schema, e.at("col").as_string());
            if (idx < 0) {
                return -1;
            }
            const auto t = schema.field(idx)->type()->id();
            if (t != arrow::Type::INT64 && t != arrow::Type::INT32 && t != arrow::Type::DOUBLE &&
                t != arrow::Type::FLOAT) {
                return -1;  // decimal/string/bool operand -> row fallback
            }
            Node n;
            n.op = VOp::Col;
            n.col_idx = idx;
            n.arrow_type = t;
            return add_(std::move(n));
        }
        if (e.contains("lit")) {
            const auto& lit = e.at("lit");
            if (!lit.is_number() || clink::config::is_dec_string(lit)) {
                return -1;
            }
            Node n;
            n.op = VOp::Lit;
            n.lit = lit.as_number();
            return add_(std::move(n));
        }
        if (!e.contains("op")) {
            return -1;
        }
        const auto& op = e.at("op").as_string();
        const auto& args = e.contains("args") ? e.at("args") : clink::config::JsonValue{};
        if (op == "neg") {
            if (!args.is_array() || args.as_array().size() != 1) {
                return -1;
            }
            const int c = compile_node_(args.as_array()[0], schema);
            if (c < 0) {
                return -1;
            }
            Node n;
            n.op = VOp::Neg;
            n.children.push_back(c);
            return add_(std::move(n));
        }
        VOp vop{};
        if (op == "add") {
            vop = VOp::Add;
        } else if (op == "sub") {
            vop = VOp::Sub;
        } else if (op == "mul") {
            vop = VOp::Mul;
        } else if (op == "div") {
            vop = VOp::Div;
        } else if (op == "mod") {
            vop = VOp::Mod;
        } else {
            return -1;
        }
        if (!args.is_array() || args.as_array().size() != 2) {
            return -1;
        }
        const int a = compile_node_(args.as_array()[0], schema);
        const int b = compile_node_(args.as_array()[1], schema);
        if (a < 0 || b < 0) {
            return -1;
        }
        Node n;
        n.op = vop;
        n.children.push_back(a);
        n.children.push_back(b);
        return add_(std::move(n));
    }

    DVec eval_node_(int idx, const arrow::RecordBatch& rb) const {
        const Node& node = nodes_[static_cast<std::size_t>(idx)];
        const auto n = static_cast<std::size_t>(rb.num_rows());
        DVec r{std::vector<double>(n, 0.0), std::vector<std::uint8_t>(n, 1)};

        if (node.op == VOp::Lit) {
            for (std::size_t i = 0; i < n; ++i) {
                r.val[i] = node.lit;
            }
            return r;
        }
        if (node.op == VOp::Col) {
            const auto& arr = *rb.column(node.col_idx);
            auto run = [&](auto&& read) {
                for (std::size_t i = 0; i < n; ++i) {
                    const auto ii = static_cast<std::int64_t>(i);
                    if (arr.IsNull(ii)) {
                        r.valid[i] = 0;
                    } else {
                        r.val[i] = read(ii);
                    }
                }
            };
            if (node.arrow_type == arrow::Type::INT64) {
                const auto& a = static_cast<const arrow::Int64Array&>(arr);
                run([&](std::int64_t ii) { return static_cast<double>(a.Value(ii)); });
            } else if (node.arrow_type == arrow::Type::INT32) {
                const auto& a = static_cast<const arrow::Int32Array&>(arr);
                run([&](std::int64_t ii) { return static_cast<double>(a.Value(ii)); });
            } else if (node.arrow_type == arrow::Type::DOUBLE) {
                const auto& a = static_cast<const arrow::DoubleArray&>(arr);
                run([&](std::int64_t ii) { return a.Value(ii); });
            } else {  // FLOAT
                const auto& a = static_cast<const arrow::FloatArray&>(arr);
                run([&](std::int64_t ii) { return static_cast<double>(a.Value(ii)); });
            }
            return r;
        }
        if (node.op == VOp::Neg) {
            r = eval_node_(node.children[0], rb);
            for (std::size_t i = 0; i < n; ++i) {
                if (r.valid[i] != 0) {
                    r.val[i] = -r.val[i];
                }
            }
            return r;
        }
        // Binary arithmetic: null if either operand null; NaN on div/mod by 0
        // (byte-identical to numeric_binop's lambdas).
        const DVec a = eval_node_(node.children[0], rb);
        const DVec b = eval_node_(node.children[1], rb);
        for (std::size_t i = 0; i < n; ++i) {
            if (a.valid[i] == 0 || b.valid[i] == 0) {
                r.valid[i] = 0;
                continue;
            }
            const double x = a.val[i];
            const double y = b.val[i];
            switch (node.op) {
                case VOp::Add:
                    r.val[i] = x + y;
                    break;
                case VOp::Sub:
                    r.val[i] = x - y;
                    break;
                case VOp::Mul:
                    r.val[i] = x * y;
                    break;
                case VOp::Div:
                    r.val[i] = (y == 0.0) ? std::nan("") : x / y;
                    break;
                case VOp::Mod:
                    r.val[i] = (y == 0.0) ? std::nan("") : std::fmod(x, y);
                    break;
                default:
                    break;
            }
        }
        return r;
    }

    std::vector<Node> nodes_;
    int root_{-1};
};

}  // namespace clink::operators

#endif  // CLINK_HAS_ARROW
