#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "clink/core/pane_info.hpp"
#include "clink/time/event_time.hpp"

// Forward declaration only - keep the heavyweight Arrow headers out of this
// hot core header. Batch<T> carries an arrow::RecordBatch sidecar by
// shared_ptr and by const-ref in the materialize closure, neither of which
// needs the complete type here.
namespace arrow {
class RecordBatch;
}

namespace clink {

namespace detail {
// Process-wide count of Batch row-materializations: each increment is one
// lazy decode of a columnar Arrow sidecar into Record rows. A pure columnar
// fast path (columnar producer -> columnar operator -> columnar sink) never
// increments it; a row consumer of a columnar batch does, once per batch.
// Benchmarks and tests read it to prove the columnar path did zero row
// decode. Incremented only on an actual decode (rare), so it is not a
// hot-path cost.
inline std::atomic<std::uint64_t>& batch_materialize_counter() noexcept {
    static std::atomic<std::uint64_t> counter{0};
    return counter;
}
}  // namespace detail

// A Record is the smallest unit of data flowing through the engine.
//
// We attach an optional event-time per record. A record without event time is
// processed against processing-time semantics for any time-aware operator.
//
// Window operators additionally attach an optional PaneInfo describing the
// pane (early/on-time/late, pane index, first/last flags) the emission
// belongs to. The field is engine-only metadata: serialization codecs do
// not encode it, so PaneInfo is local to a single operator chain.
template <typename T>
class Record {
public:
    Record() = default;

    explicit Record(T value) : value_(std::move(value)) {}

    Record(T value, EventTime ts) : value_(std::move(value)), event_time_(ts) {}

    const T& value() const& noexcept { return value_; }
    T& value() & noexcept { return value_; }
    T&& value() && noexcept { return std::move(value_); }

    std::optional<EventTime> event_time() const noexcept { return event_time_; }
    void set_event_time(EventTime ts) noexcept { event_time_ = ts; }

    std::optional<PaneInfo> pane() const noexcept { return pane_; }
    void set_pane(PaneInfo p) noexcept { pane_ = p; }

private:
    T value_{};
    std::optional<EventTime> event_time_{};
    std::optional<PaneInfo> pane_{};
};

// A Batch is the operator-boundary unit. We pass batches rather than singletons
// so the engine has a natural place to land Arrow RecordBatch.
//
// Batch<T> is a row vector of Record<T> AND, optionally, a columnar
// arrow::RecordBatch sidecar. A columnar producer (an Arrow source, a
// columnar operator) sets the sidecar and leaves the row vector empty; a
// columnar-aware consumer reads arrow() directly and pays no row decode -
// that is the columnar-native execution fast path. A row consumer that
// touches any row accessor (begin/operator[]/records) triggers a one-shot
// lazy materialization of the sidecar into the row vector, so the entire
// existing row API and every row operator keep working unchanged. A
// pure-row batch (no sidecar) behaves exactly as before.
template <typename T>
class Batch {
public:
    using value_type = Record<T>;
    using iterator = typename std::vector<value_type>::iterator;
    using const_iterator = typename std::vector<value_type>::const_iterator;

    // Decodes the columnar sidecar to rows on first row-API access. Returns
    // the row vector. Supplied by the columnar producer (it just wraps
    // ArrowBatcher<T>::parse). Takes arrow::RecordBatch by const-ref so this
    // header needs only the forward declaration above.
    using MaterializeFn = std::function<std::vector<value_type>(const arrow::RecordBatch&)>;

    Batch() = default;
    explicit Batch(std::vector<value_type> records) : records_(std::move(records)) {}

    // Columnar batch. Rows are NOT built until a row accessor is touched, at
    // which point `materialize` lazily decodes the sidecar. `rows` is the row
    // count so size()/empty() answer without materializing.
    Batch(std::shared_ptr<arrow::RecordBatch> arrow, std::size_t rows, MaterializeFn materialize)
        : arrow_(std::move(arrow)), arrow_rows_(rows), materialize_(std::move(materialize)) {}

    void push(value_type r) { records_.push_back(std::move(r)); }
    void emplace(T v) { records_.emplace_back(std::move(v)); }
    void emplace(T v, EventTime ts) { records_.emplace_back(std::move(v), ts); }
    void reserve(std::size_t n) { records_.reserve(n); }

    // Columnar sidecar surface. is_columnar() lets a columnar-aware operator
    // opt into the fast path; arrow() hands it the RecordBatch.
    [[nodiscard]] bool is_columnar() const noexcept { return arrow_ != nullptr; }
    [[nodiscard]] const std::shared_ptr<arrow::RecordBatch>& arrow() const noexcept {
        return arrow_;
    }

    // size()/empty() answer from the sidecar WITHOUT decoding rows.
    std::size_t size() const noexcept { return arrow_ ? arrow_rows_ : records_.size(); }
    bool empty() const noexcept { return size() == 0; }

    value_type& operator[](std::size_t i) {
        materialize_rows_();
        return records_[i];
    }
    const value_type& operator[](std::size_t i) const {
        materialize_rows_();
        return records_[i];
    }

    iterator begin() {
        materialize_rows_();
        return records_.begin();
    }
    iterator end() {
        materialize_rows_();
        return records_.end();
    }
    const_iterator begin() const {
        materialize_rows_();
        return records_.begin();
    }
    const_iterator end() const {
        materialize_rows_();
        return records_.end();
    }

    const std::vector<value_type>& records() const {
        materialize_rows_();
        return records_;
    }

    // Move the row vector out. Used by a columnar producer's materialize
    // closure to reuse ArrowBatcher<T>::parse without an extra copy.
    std::vector<value_type> take_records() { return std::move(records_); }

    // Create a sibling columnar batch over a DIFFERENT RecordBatch (e.g. a
    // partition gather or a filter/project result) reusing THIS batch's lazy
    // row-materialization closure. Lets the columnar shuffle split a columnar
    // batch into per-subtask columnar sub-batches, and a columnar operator
    // re-emit a derived batch, without knowing the row type's batcher. The
    // closure is schema-self-describing for the Row channel, so it decodes the
    // new (same-schema) RecordBatch correctly.
    [[nodiscard]] Batch with_arrow(std::shared_ptr<arrow::RecordBatch> rb, std::size_t rows) const {
        return Batch{std::move(rb), rows, materialize_};
    }

private:
    // Lazily decode the columnar sidecar into rows the first time a row
    // accessor is touched. A pure-row batch (arrow_ == null) is a no-op. The
    // mutable cache is safe because a batch is owned by exactly one operator
    // thread at a time in the runtime.
    void materialize_rows_() const {
        if (arrow_ && records_.empty() && materialize_) {
            records_ = materialize_(*arrow_);
            detail::batch_materialize_counter().fetch_add(1, std::memory_order_relaxed);
        }
    }

    mutable std::vector<value_type> records_{};
    std::shared_ptr<arrow::RecordBatch> arrow_{};
    std::size_t arrow_rows_{0};
    MaterializeFn materialize_{};
};

}  // namespace clink
