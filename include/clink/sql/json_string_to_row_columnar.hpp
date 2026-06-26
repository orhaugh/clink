#pragma once

#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "clink/config/json.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::sql {

// kSourcePartitionColumn (the engine-only partition sidecar column) is defined
// in row_columnar_batcher.hpp so the self-describing row reader there can drop
// it on materialisation. The assigner reads it columnar via
// with_columnar_partitions; this operator's materialize restores it onto the
// row records, so a row consumer (or the assigner's row fallback) is unchanged.

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
//   Kafka topic) is carried through the columnar path via the engine-only
//   kSourcePartitionColumn sidecar column, which the downstream partition-aware
//   watermark assigner reads (with_columnar_partitions) to keep per-partition
//   watermarking without materialising. The lazy materialize closure restores
//   source_partition onto the row records too, so the assigner's row fallback
//   (or any row consumer) stays byte- and metadata-equivalent to
//   json_string_to_row. (Before this was added, going columnar dropped
//   source_partition and collapsed per-partition watermarking to a single
//   global watermark - wrong windowed results.)
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
            // A declared column in the engine-reserved "__" namespace would
            // collide with the partition sidecar column this operator appends
            // (a duplicate name makes GetFieldIndex ambiguous -> the partition
            // reader silently yields nothing -> per-partition watermarking
            // collapses to a global watermark). Refuse the columnar path for
            // such a schema; the row fallback is always correct.
            if (c.name.rfind("__", 0) == 0) {
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
            //     source_partition is carried alongside (parts) for the sidecar
            //     and preserved on every record for the row fallback.
            Batch<Row> rows;
            rows.reserve(in.size());
            std::vector<std::optional<std::int32_t>> parts;
            parts.reserve(in.size());
            bool faithful = schema_capable_;
            bool any_partition = false;
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
                auto p = rec.source_partition();
                if (p.has_value()) {
                    out_rec.set_source_partition(*p);
                    any_partition = true;
                }
                parts.push_back(p);
                rows.push(std::move(out_rec));
            }

            // (2) Go columnar only when every record round-trips exactly.
            //     Otherwise emit the row-form batch (the permanent fallback,
            //     identical to json_string_to_row - source_partition preserved).
            if (faithful) {
                if (auto columnar = build_columnar_(rows, parts, any_partition);
                    columnar.has_value()) {
                    out.emit_data(std::move(*columnar));
                    return;
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

    // Build the columnar Batch<Row>: the shared typed sidecar (event_time +
    // declared columns) from batcher_.build, plus an engine-only
    // kSourcePartitionColumn int32 column when any record is partitioned. The
    // lazy materialize restores the declared columns (byte-identical to
    // make_row_columnar_arrow_batcher's own materialize) AND source_partition,
    // so a row consumer or the assigner's row fallback sees the same records as
    // json_string_to_row. Returns nullopt if the sidecar cannot be built (then
    // the caller emits the row form).
    std::optional<Batch<Row>> build_columnar_(const Batch<Row>& rows,
                                              const std::vector<std::optional<std::int32_t>>& parts,
                                              bool any_partition) const {
        auto rb = batcher_.build(rows);
        if (rb == nullptr) {
            return std::nullopt;
        }
        if (any_partition) {
            arrow::Int32Builder pb;
            if (!pb.Reserve(static_cast<std::int64_t>(parts.size())).ok()) {
                return std::nullopt;
            }
            for (const auto& p : parts) {
                if (p.has_value()) {
                    if (!pb.Append(*p).ok()) {
                        return std::nullopt;
                    }
                } else if (!pb.AppendNull().ok()) {
                    return std::nullopt;
                }
            }
            std::shared_ptr<arrow::Array> p_arr;
            if (!pb.Finish(&p_arr).ok()) {
                return std::nullopt;
            }
            auto added = rb->AddColumn(
                rb->num_columns(), arrow::field(kSourcePartitionColumn, arrow::int32()), p_arr);
            if (!added.ok()) {
                return std::nullopt;
            }
            rb = *added;
        }

        const auto n = static_cast<std::size_t>(rb->num_rows());
        auto resolved = resolved_;  // capture by value for lifetime safety
        auto materialize = [resolved](const arrow::RecordBatch& b) -> std::vector<Record<Row>> {
            const auto* t_arr = dynamic_cast<const arrow::Int64Array*>(b.column(0).get());
            const int part_idx = b.schema()->GetFieldIndex(kSourcePartitionColumn);
            const arrow::Int32Array* p_arr =
                part_idx >= 0 ? dynamic_cast<const arrow::Int32Array*>(b.column(part_idx).get())
                              : nullptr;
            const auto rn = b.num_rows();
            std::vector<Row> decoded(static_cast<std::size_t>(rn));
            for (std::size_t ci = 0; ci < resolved.size(); ++ci) {
                const auto& c = resolved[ci];
                const auto& col = *b.column(static_cast<int>(ci) + 1);
                for (std::int64_t i = 0; i < rn; ++i) {
                    decoded[static_cast<std::size_t>(i)].values[c.name] =
                        row_columnar_detail::read_cell(c.eff, col, i);
                }
            }
            std::vector<Record<Row>> recs;
            recs.reserve(static_cast<std::size_t>(rn));
            for (std::int64_t i = 0; i < rn; ++i) {
                std::optional<EventTime> ts;
                if (t_arr != nullptr) {
                    ts = clink::detail::read_event_time(*t_arr, i);
                }
                Record<Row> rec =
                    ts.has_value()
                        ? Record<Row>(std::move(decoded[static_cast<std::size_t>(i)]), *ts)
                        : Record<Row>(std::move(decoded[static_cast<std::size_t>(i)]));
                if (p_arr != nullptr && !p_arr->IsNull(i)) {
                    rec.set_source_partition(p_arr->Value(i));
                }
                recs.push_back(std::move(rec));
            }
            return recs;
        };
        return Batch<Row>{std::move(rb), n, std::move(materialize)};
    }

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
