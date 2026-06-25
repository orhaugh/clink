#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

#include <arrow/api.h>

#include "clink/core/hash_map.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// The aggregation a ColumnarKeyedAggregateOperator computes per key.
//   * Sum   - sum of the values.
//   * Count - number of records (the value column is ignored).
//   * Min   - smallest value.
//   * Max   - largest value.
enum class AggKind : std::uint8_t { Sum, Count, Min, Max };

// Columnar-native keyed aggregation (increments 2 + 6). Groups (key, value)
// pairs by key, folds them with the chosen AggKind, then emits one (key, agg)
// row per group at end-of-stream (flush). When the input batch carries a
// 3-column Arrow sidecar {event_time, key, value} it folds the whole key+value
// columns into the accumulator in a single dense pass straight off the Arrow
// buffers - ZERO row materialization, no per-record Record<pair> construction.
// When the input is row-only (or the schema is unexpected) it falls back to an
// identical row loop (the SAME fold_one_), so the columnar and row paths are
// exactly equivalent; only the row decode is skipped.
//
// Why a hand-rolled scan and not an Arrow hash-aggregate kernel: arrow::compute
// ::Initialize() is not an exported symbol in this Arrow package, so hash_sum /
// group_by are not registered (same constraint ColumnarFilterOperator hit for
// the comparison kernels). The win here is skipping the row decode + the
// std::vector<Record<pair>> allocation the row arm pays, not a SIMD aggregate.
//
// Scope: int64 key + int64 value, GLOBAL grouping (no windowing - emits final
// per-key results at end-of-stream). Windowed columnar aggregation lives in the
// window operators (which fold an arbitrary user combiner columnar). State is
// NOT checkpointed (a perf primitive; the durable aggregation path is the
// window-operator family).
class ColumnarKeyedAggregateOperator final
    : public Operator<std::pair<std::int64_t, std::int64_t>,
                      std::pair<std::int64_t, std::int64_t>> {
public:
    using KV = std::pair<std::int64_t, std::int64_t>;

    // enable_columnar=false forces the row path even for a columnar upstream.
    // Used by the bench to compare the columnar fast path against the row
    // baseline with the SAME operator + SAME columnar source, isolating ingest
    // as the only variable.
    explicit ColumnarKeyedAggregateOperator(AggKind kind = AggKind::Sum,
                                            std::string name = "columnar_keyed_agg",
                                            bool enable_columnar = true)
        : kind_(kind), name_(std::move(name)), enable_columnar_(enable_columnar) {}

    void open() override { acc_.clear(); }

    [[nodiscard]] bool supports_columnar() const noexcept override { return enable_columnar_; }

    // Vectorized fast path. Returns false (fall back to process()) on any
    // schema surprise - BEFORE touching the accumulator, so a false return
    // never double-counts when the runner re-runs process().
    bool process_columnar(const StreamElement<KV>& element, Emitter<KV>& out) override {
        (void)out;  // aggregation emits at flush(), not per batch
        if (!element.is_data() || !element.as_data().is_columnar()) {
            return false;
        }
        const auto& rb = element.as_data().arrow();
        if (!rb || rb->num_columns() < 3) {
            return false;
        }
        // Schema is {event_time(0), key(1), value(2)}; key+value int64.
        const auto* key = dynamic_cast<const arrow::Int64Array*>(rb->column(1).get());
        const auto* val = dynamic_cast<const arrow::Int64Array*>(rb->column(2).get());
        if (key == nullptr || val == nullptr) {
            return false;
        }
        // Dense single pass straight off the Arrow buffers via the SAME
        // fold_one_ the row path uses. No Record<pair> objects, no row vector -
        // the columnar win.
        const std::int64_t n = rb->num_rows();
        for (std::int64_t i = 0; i < n; ++i) {
            fold_one_(key->Value(i), val->Value(i));
        }
        return true;
    }

    // Row path: fallback for a row-only upstream / when columnar is disabled,
    // and the watermark/barrier forwarder. Identical aggregation to the
    // columnar path (same fold_one_).
    void process(const StreamElement<KV>& element, Emitter<KV>& out) override {
        if (element.is_data()) {
            for (const auto& record : element.as_data()) {
                fold_one_(record.value().first, record.value().second);
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    // Emit the final per-key results at end-of-stream.
    void flush(Emitter<KV>& out) override {
        if (acc_.empty()) {
            return;
        }
        Batch<KV> out_batch;
        out_batch.reserve(acc_.size());
        for (const auto& [k, agg] : acc_) {
            out_batch.emplace(KV{k, agg});
        }
        out.emit_data(std::move(out_batch));
        acc_.clear();
    }

    std::string name() const override { return name_; }

private:
    // Fold one (key, value) into the accumulator per the configured kind. The
    // branch is on a constant member (one kind per operator instance) so it
    // predicts perfectly; the value is unguarded int64, same as any int64 reduce
    // in the engine. Sum/Count seed from the map's default 0; Min/Max seed from
    // the first value via try_emplace so the sentinel is never folded in.
    void fold_one_(std::int64_t k, std::int64_t v) {
        switch (kind_) {
            case AggKind::Sum:
                acc_[k] += v;
                break;
            case AggKind::Count:
                acc_[k] += 1;  // value ignored
                break;
            case AggKind::Min: {
                auto [it, inserted] = acc_.try_emplace(k, v);
                if (!inserted) {
                    it->second = std::min(it->second, v);
                }
                break;
            }
            case AggKind::Max: {
                auto [it, inserted] = acc_.try_emplace(k, v);
                if (!inserted) {
                    it->second = std::max(it->second, v);
                }
                break;
            }
        }
    }

    AggKind kind_;
    std::string name_;
    bool enable_columnar_;
    clink::FlatMap<std::int64_t, std::int64_t> acc_;
};

}  // namespace clink
