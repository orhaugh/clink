#pragma once

#include <cmath>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/type.h>

#include "clink/config/json.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::sql {

// Columnar variant of json_string_to_row (Wave 2, increment 1).
//
// Decodes a batch of NDJSON payload strings into Rows EXACTLY as
// json_string_to_row does (same TextFormat, same value_or(Row{}) on a decode
// miss, same event-time / source-partition preservation), then attaches a
// typed Arrow sidecar built by make_row_columnar_arrow_batcher so the emitted
// Batch<Row> is_columnar(). That lets the downstream columnar fast paths
// (filter / project / aggregate / window) light up on the Kafka path, where
// today they stay dormant because the source decodes to row form.
//
// This is the "lazy decode-then-build" seam: it still pays the per-record JSON
// parse and Row build (a true zero-intermediate JSON->column decoder is a later
// increment). Its job here is to prove the wiring, the gating, and the
// fallback, and to BE the permanent row fallback for payloads the columnar
// codec cannot represent.
//
// Correctness contract - byte-equivalence with json_string_to_row:
//   build_column does NOT reject a value that does not fit its declared
//   column; it silently nulls or coerces it (a string in an int column ->
//   null; a non-integer in an int column -> truncated; a number in a string
//   column -> stringified; an undeclared JSON field -> dropped; an absent
//   declared field -> a present-null cell). Any of those would diverge
//   silently from the schema-loose row decode. So this operator emits a
//   COLUMNAR batch only when EVERY record round-trips exactly (see
//   row_faithful_); otherwise it emits the plain row-form batch, which is
//   identical to what json_string_to_row would have produced. FLOAT (lossy
//   double<->float) and DECIMAL128 (exact-or-fails coercion) columns are
//   excluded at the schema level and always take the row path here; a later
//   increment adds a direct typed decoder for them.
//
//   A PARTITIONED record (Record::source_partition set - every record from a
//   Kafka topic) also forces the row fallback: the Arrow sidecar carries no
//   partition column, so a columnar batch would lose source_partition and the
//   downstream partition-aware watermark assigner would collapse per-partition
//   watermarking to a single global watermark (wrong windowed results). This
//   means that on a real (partitioned) Kafka source the columnar path currently
//   falls back to row form; carrying partition through the columnar watermark
//   path is a later increment, after which windowed/keyed Kafka queries go
//   columnar end-to-end. The columnar emit here is exercised by non-partitioned
//   inputs (and proves the seam + gating + fallback).
class JsonStringToRowColumnarOperator final : public Operator<std::string, Row> {
public:
    explicit JsonStringToRowColumnarOperator(std::vector<RowColumn> columns,
                                             std::string name = "json_string_to_row_columnar")
        : fmt_(row_json_text_format()),
          batcher_(make_row_columnar_arrow_batcher(columns)),
          name_(std::move(name)) {
        resolved_.reserve(columns.size());
        for (const auto& c : columns) {
            auto eff = row_columnar_detail::effective_type(c.type);
            if (!columnar_capable_type_(eff->id())) {
                schema_capable_ = false;
            }
            resolved_.push_back({c.name, std::move(eff)});
        }
    }

    void process(const StreamElement<std::string>& element, Emitter<Row>& out) override {
        if (element.is_data()) {
            const Batch<std::string>& in = element.as_data();

            // (1) Decode to a row-form Batch<Row>, byte-identical to
            //     json_string_to_row. A decode miss yields an empty Row (it is
            //     not faithfully columnar, so it forces the row fallback).
            Batch<Row> rows;
            rows.reserve(in.size());
            bool faithful = schema_capable_;
            for (const auto& rec : in) {
                auto decoded = fmt_.decode(rec.value());
                if (!decoded.has_value()) {
                    faithful = false;
                }
                Row row = decoded.value_or(Row{});
                if (faithful && !row_faithful_(row)) {
                    faithful = false;
                }
                Record<Row> out_rec = rec.event_time().has_value()
                                          ? Record<Row>(std::move(row), *rec.event_time())
                                          : Record<Row>(std::move(row));
                if (auto p = rec.source_partition(); p.has_value()) {
                    out_rec.set_source_partition(*p);
                    // A partitioned record (every record from a Kafka topic) is
                    // NOT columnar-representable: the Arrow sidecar carries no
                    // source_partition column, so the downstream partition-aware
                    // watermark assigner's columnar fast path would feed the
                    // strategy partition-unset and collapse per-partition
                    // watermarking to a single global watermark (racing to the
                    // fastest partition -> slow-partition records marked late ->
                    // wrong windowed results). Force the row fallback, which
                    // preserves source_partition (set above) and keeps
                    // per-partition watermarking correct. Carrying partition
                    // through the columnar path is a later increment.
                    faithful = false;
                }
                rows.push(std::move(out_rec));
            }

            // (2) Go columnar only when every record round-trips exactly.
            //     Otherwise emit the row-form batch (the permanent fallback,
            //     identical to json_string_to_row).
            if (faithful) {
                if (auto rb = batcher_.build(rows); rb != nullptr) {
                    if (auto columnar = batcher_.parse(*rb); columnar.has_value()) {
                        out.emit_data(std::move(*columnar));
                        return;
                    }
                }
            }
            out.emit_data(std::move(rows));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    std::string name() const override { return name_; }

private:
    struct Resolved {
        std::string name;
        std::shared_ptr<arrow::DataType> eff;
    };

    // Types whose build_column / read_cell round-trip is an exact identity for
    // a conforming value. FLOAT is lossy (double->float->double) and DECIMAL128
    // coerces exact-or-null, so both take the row path in this increment.
    static bool columnar_capable_type_(arrow::Type::type id) {
        switch (id) {
            case arrow::Type::INT64:
            case arrow::Type::INT32:
            case arrow::Type::DOUBLE:
            case arrow::Type::BOOL:
            case arrow::Type::STRING:
                return true;
            default:
                return false;
        }
    }

    // True iff `row` carries EXACTLY the declared columns and every value is
    // representable in its effective Arrow type with an identity round-trip
    // through build_column / read_cell. This is what makes the emitted columnar
    // batch byte-equivalent to the row decode.
    bool row_faithful_(const Row& row) const {
        if (row.values.size() != resolved_.size()) {
            return false;  // an undeclared field, or a missing declared column
        }
        for (const auto& c : resolved_) {
            auto it = row.values.find(c.name);
            if (it == row.values.end()) {
                return false;  // declared column absent (would reconstruct as present-null)
            }
            const auto& v = it->second;
            if (v.is_null()) {
                continue;  // null -> null cell -> null, faithful
            }
            switch (c.eff->id()) {
                case arrow::Type::INT64: {
                    if (!v.is_number()) {
                        return false;  // wrong type: build would null it
                    }
                    const double d = v.as_number();
                    if (!std::isfinite(d) || d != std::floor(d)) {
                        return false;  // non-integer: build would truncate it
                    }
                    // Must be exactly representable as int64; outside [-2^63, 2^63)
                    // the static_cast in build_column is lossy / undefined.
                    if (d < -9223372036854775808.0 || d >= 9223372036854775808.0) {
                        return false;
                    }
                    break;
                }
                case arrow::Type::INT32: {
                    if (!v.is_number()) {
                        return false;
                    }
                    const double d = v.as_number();
                    if (!std::isfinite(d) || d != std::floor(d)) {
                        return false;
                    }
                    if (d < -2147483648.0 || d > 2147483647.0) {
                        return false;
                    }
                    break;
                }
                case arrow::Type::DOUBLE:
                    if (!v.is_number()) {
                        return false;
                    }
                    break;
                case arrow::Type::BOOL:
                    if (!v.is_bool()) {
                        return false;
                    }
                    break;
                case arrow::Type::STRING:
                    // A number/bool in a string column would be stringified by
                    // to_utf8; a dec-string would have its sentinel stripped.
                    if (!v.is_string() || clink::config::is_dec_string(v)) {
                        return false;
                    }
                    break;
                default:
                    return false;  // unreachable: schema_capable_ excludes these
            }
        }
        return true;
    }

    clink::TextFormat<Row> fmt_;
    ArrowBatcher<Row> batcher_;
    std::string name_;
    std::vector<Resolved> resolved_;
    bool schema_capable_{true};
};

}  // namespace clink::sql
