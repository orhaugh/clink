#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <arrow/api.h>

#include "clink/core/hash_map.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/columnar_keyed_aggregate_operator.hpp"  // AggKind
#include "clink/operators/operator_base.hpp"

namespace clink {

// Columnar-native STRING-keyed aggregation (increment 7b). Groups
// (string key, int64 value) pairs by key, folds them with the chosen AggKind,
// and emits one (key, agg) row per group at flush. The columnar fast path scans
// the Arrow {event_time, key:utf8, value:int64} sidecar and folds straight off
// the buffers; it looks the key up in the accumulator via std::string_view
// (heterogeneous lookup) and allocates a std::string only when a NEW key is
// inserted - not per record. The row path materializes a std::string per record
// first, then folds via the SAME fold_one_, so the two paths are exactly
// equivalent; the columnar win is dodging the per-record key allocation.
//
// Both paths call fold_one_(string_view, value), so Sum/Count/Min/Max behave
// identically across them. Scope: utf8 key + int64 value, GLOBAL grouping, not
// checkpointed (a perf primitive).
class ColumnarKeyedStringAggregateOperator final
    : public Operator<std::pair<std::string, std::int64_t>, std::pair<std::string, std::int64_t>> {
public:
    using KV = std::pair<std::string, std::int64_t>;

    explicit ColumnarKeyedStringAggregateOperator(AggKind kind = AggKind::Sum,
                                                  std::string name = "columnar_keyed_string_agg",
                                                  bool enable_columnar = true)
        : kind_(kind), name_(std::move(name)), enable_columnar_(enable_columnar) {}

    void open() override { acc_.clear(); }

    [[nodiscard]] bool supports_columnar() const noexcept override { return enable_columnar_; }

    bool process_columnar(const StreamElement<KV>& element, Emitter<KV>& out) override {
        (void)out;  // aggregation emits at flush()
        if (!element.is_data() || !element.as_data().is_columnar()) {
            return false;
        }
        const auto& rb = element.as_data().arrow();
        if (!rb || rb->num_columns() < 3) {
            return false;
        }
        // Schema {event_time(0), key(1):utf8, value(2):int64}.
        const auto* key = dynamic_cast<const arrow::StringArray*>(rb->column(1).get());
        const auto* val = dynamic_cast<const arrow::Int64Array*>(rb->column(2).get());
        if (key == nullptr || val == nullptr) {
            return false;
        }
        // Guards passed before any fold. Scan via string_view - no per-record
        // std::string; the only allocations are new-key inserts in fold_one_.
        const std::int64_t n = rb->num_rows();
        for (std::int64_t i = 0; i < n; ++i) {
            fold_one_(key->GetView(i), val->Value(i));
        }
        return true;
    }

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
    // Transparent hash so the unordered_map supports heterogeneous lookup by
    // string_view (find(sv) builds no std::string). std::equal_to<> (void) is
    // already transparent.
    struct TransparentStringHash {
        using is_transparent = void;
        std::size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };

    // Fold one (key, value) per the configured kind. find() is heterogeneous
    // (no alloc); a std::string is constructed only on the new-key insert.
    void fold_one_(std::string_view k, std::int64_t v) {
        auto it = acc_.find(k);
        if (it == acc_.end()) {
            acc_.emplace(std::string(k), kind_ == AggKind::Count ? std::int64_t{1} : v);
            return;
        }
        switch (kind_) {
            case AggKind::Sum:
                it->second += v;
                break;
            case AggKind::Count:
                it->second += 1;  // value ignored
                break;
            case AggKind::Min:
                it->second = std::min(it->second, v);
                break;
            case AggKind::Max:
                it->second = std::max(it->second, v);
                break;
        }
    }

    AggKind kind_;
    std::string name_;
    bool enable_columnar_;
    clink::FlatMap<std::string, std::int64_t, TransparentStringHash, std::equal_to<>> acc_;
};

}  // namespace clink
