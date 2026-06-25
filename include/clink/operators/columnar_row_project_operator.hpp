#pragma once

// ColumnarRowProjectOperator - columnar-native SELECT/projection on the SQL
// Row channel.
//
// When the input Batch<Row> is columnar AND every projection output is a bare
// column reference ({"col": name}), the output RecordBatch is assembled by
// selecting (and renaming) the referenced input columns plus the event-time
// column - zero-copy column reuse, no Row materialization, no JSON evaluation -
// so the chain downstream keeps riding the fast path. This covers the common
// column-pruning / rename case (SELECT a, b, c / SELECT a AS x). Any literal or
// computed output, or row-only input, falls back to the identical row path
// (evaluate_json_value_expr per output) so results are byte-identical to the
// pre-columnar project_row.
//
// A __row_kind changelog marker present as an input column is carried through
// unchanged - the columnar analogue of copy_row_kind - so retraction semantics
// survive the fast path.

#include <memory>
#include <string>
#include <utility>
#include <vector>

#ifdef CLINK_HAS_ARROW

#include <arrow/api.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/columnar_expr_program.hpp"
#include "clink/operators/json_value_expr.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/row_kind.hpp"

namespace clink {

class ColumnarRowProjectOperator final : public Operator<sql::Row, sql::Row> {
public:
    // One projected output: its declared name and its value expression
    // (json_value_expr IR). A bare {"col": name} expr is eligible for the
    // columnar fast path; anything else forces the row fallback.
    using Output = std::pair<std::string, clink::config::JsonValue>;

    explicit ColumnarRowProjectOperator(std::vector<Output> outputs,
                                        std::string name = "project_row")
        : outputs_(std::move(outputs)), name_(std::move(name)) {}

    [[nodiscard]] bool supports_columnar() const noexcept override { return true; }

    bool process_columnar(const StreamElement<sql::Row>& element, Emitter<sql::Row>& out) override {
        if (!element.is_data() || !element.as_data().is_columnar()) {
            return false;
        }
        const auto& rb = element.as_data().arrow();
        if (!rb || rb->num_columns() < 1) {
            return false;
        }
        const std::int64_t n = rb->num_rows();
        if (n == 0) {
            return true;  // suppress empty emission, matching the row path
        }

        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        fields.reserve(outputs_.size() + 2);
        arrays.reserve(outputs_.size() + 2);

        // Event-time column rides through at position 0 (the layout every
        // columnar Row producer uses).
        fields.push_back(rb->schema()->field(0));
        arrays.push_back(rb->column(0));

        for (const auto& [out_name, expr] : outputs_) {
            // Bare column reference: zero-copy reuse (and rename), preserving the
            // source column type.
            if (expr.is_object() && expr.contains("col")) {
                const int idx = rb->schema()->GetFieldIndex(expr.at("col").as_string());
                if (idx < 0) {
                    return false;  // referenced column absent: defer to the row path
                }
                fields.push_back(
                    arrow::field(out_name, rb->schema()->field(idx)->type(), /*nullable=*/true));
                arrays.push_back(rb->column(idx));
                continue;
            }
            // Computed numeric expression: a typed value program produces a
            // float64 column byte-identical to evaluate_json_value_expr
            // (numeric_binop), matching the binder's declared arithmetic type.
            // compile() declines anything it cannot model (decimal/string/bool
            // operands, casts, concat, CASE, functions, a bare literal), so the
            // WHOLE projection defers to the row path - we have not emitted yet.
            if (expr.is_object() && expr.contains("op")) {
                auto vp = clink::operators::ColumnarValueProgram::compile(expr, *rb->schema());
                if (!vp) {
                    return false;
                }
                auto col = vp->evaluate(*rb);
                if (!col) {
                    return false;
                }
                fields.push_back(arrow::field(out_name, arrow::float64(), /*nullable=*/true));
                arrays.push_back(std::move(col));
                continue;
            }
            // A bare literal or anything else: defer to the row path.
            return false;
        }

        // Carry a changelog marker column through unchanged (columnar
        // copy_row_kind). Projection is 1:1, so length/order are preserved.
        if (const int rk = rb->schema()->GetFieldIndex(std::string{sql::kRowKindField}); rk >= 0) {
            fields.push_back(rb->schema()->field(rk));
            arrays.push_back(rb->column(rk));
        }

        auto out_rb = arrow::RecordBatch::Make(arrow::schema(fields), n, arrays);
        out.emit_data(Batch<sql::Row>{
            std::move(out_rb), static_cast<std::size_t>(n), sql::row_materialize_fn()});
        return true;
    }

    void process(const StreamElement<sql::Row>& element, Emitter<sql::Row>& out) override {
        if (element.is_data()) {
            const Batch<sql::Row>& in_batch = element.as_data();
            Batch<sql::Row> out_batch;
            out_batch.reserve(in_batch.size());
            for (const auto& record : in_batch) {
                const sql::Row& r = record.value();
                auto resolve = [&](const std::string& name) -> clink::config::JsonValue {
                    auto it = r.values.find(name);
                    if (it == r.values.end()) {
                        return clink::config::JsonValue{nullptr};
                    }
                    return it->second;
                };
                sql::Row projected;
                for (const auto& [name, expr] : outputs_) {
                    projected.values[name] =
                        clink::operators::evaluate_json_value_expr(expr, resolve);
                }
                sql::copy_row_kind(r, projected);
                Record<sql::Row> rec(std::move(projected));
                if (const auto ts = record.event_time(); ts.has_value()) {
                    rec.set_event_time(*ts);
                }
                if (const auto p = record.pane(); p.has_value()) {
                    rec.set_pane(*p);
                }
                out_batch.push(std::move(rec));
            }
            if (!out_batch.empty()) {
                out.emit_data(std::move(out_batch));
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return name_; }

private:
    std::vector<Output> outputs_;
    std::string name_;
};

}  // namespace clink

#endif  // CLINK_HAS_ARROW
