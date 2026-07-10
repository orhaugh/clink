#pragma once

// clink::test::OutputCapture - the typed record of everything an
// operator (or a bare collector-based function) emitted, in emission
// order. It is BOTH the stateless-function collector and the operator
// harness's output model: it exposes a real Emitter<T>, so anything
// that emits through the engine's own emitter type can be captured
// without a runtime.
//
//   clink::test::OutputCapture<int> cap;
//   my_flatmap.process(element, cap.emitter());
//   EXPECT_EQ(cap.values(), (std::vector<int>{1, 2, 3}));
//
// Events are stored as the engine's own StreamElement<T> (data batches,
// watermarks, barriers), so nothing is lost in translation; the value/
// record/watermark views are flattened projections over that log.
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::test {

template <typename T>
class OutputCapture {
public:
    using Event = StreamElement<T>;

    // One data record with its (optional) event time - the timestamped view.
    struct TimestampedValue {
        T value;
        std::optional<std::int64_t> event_time_ms;

        friend bool operator==(const TimestampedValue& a, const TimestampedValue& b) {
            return a.value == b.value && a.event_time_ms == b.event_time_ms;
        }
    };

    OutputCapture()
        : events_(std::make_unique<std::vector<Event>>()),
          emitter_(std::make_unique<Emitter<T>>(make_forward_(events_.get()))) {}

    // Movable (the log and emitter live behind stable pointers); not copyable.
    OutputCapture(OutputCapture&&) noexcept = default;
    OutputCapture& operator=(OutputCapture&&) noexcept = default;
    OutputCapture(const OutputCapture&) = delete;
    OutputCapture& operator=(const OutputCapture&) = delete;

    // The engine-native emitter appending to this capture. Hand it to
    // anything that takes an Emitter<T>&.
    Emitter<T>& emitter() noexcept { return *emitter_; }

    // ---- Views over the log (emission order preserved) ----

    const std::vector<Event>& events() const noexcept { return *events_; }

    // Every data value, flattened across batches.
    std::vector<T> values() const {
        std::vector<T> out;
        for (const auto& e : *events_) {
            if (!e.is_data()) {
                continue;
            }
            for (const auto& rec : e.as_data()) {
                out.push_back(rec.value());
            }
        }
        return out;
    }

    // Every data value with its event time.
    std::vector<TimestampedValue> records() const {
        std::vector<TimestampedValue> out;
        for (const auto& e : *events_) {
            if (!e.is_data()) {
                continue;
            }
            for (const auto& rec : e.as_data()) {
                const auto ts = rec.event_time();
                out.push_back(TimestampedValue{
                    rec.value(), ts ? std::optional<std::int64_t>{ts->millis()} : std::nullopt});
            }
        }
        return out;
    }

    std::vector<Watermark> watermarks() const {
        std::vector<Watermark> out;
        for (const auto& e : *events_) {
            if (e.is_watermark()) {
                out.push_back(e.as_watermark());
            }
        }
        return out;
    }

    std::vector<CheckpointBarrier> barriers() const {
        std::vector<CheckpointBarrier> out;
        for (const auto& e : *events_) {
            if (e.is_barrier()) {
                out.push_back(e.as_barrier());
            }
        }
        return out;
    }

    std::size_t value_count() const {
        std::size_t n = 0;
        for (const auto& e : *events_) {
            if (e.is_data()) {
                n += e.as_data().size();
            }
        }
        return n;
    }

    bool empty() const noexcept { return events_->empty(); }

    // Predicate queries over data values.
    template <typename Pred>
    bool any_value(Pred&& pred) const {
        for (const auto& e : *events_) {
            if (!e.is_data()) {
                continue;
            }
            for (const auto& rec : e.as_data()) {
                if (pred(rec.value())) {
                    return true;
                }
            }
        }
        return false;
    }

    template <typename Pred>
    std::size_t count_values(Pred&& pred) const {
        std::size_t n = 0;
        for (const auto& e : *events_) {
            if (!e.is_data()) {
                continue;
            }
            for (const auto& rec : e.as_data()) {
                if (pred(rec.value())) {
                    ++n;
                }
            }
        }
        return n;
    }

    // ---- Mutation ----

    void clear() noexcept { events_->clear(); }

    // Drain the log (subsequent emissions start a fresh one). Useful for
    // phase-by-phase assertions.
    std::vector<Event> take_events() {
        std::vector<Event> out;
        out.swap(*events_);
        return out;
    }

private:
    static typename Emitter<T>::Forward make_forward_(std::vector<Event>* log) {
        return [log](Event e) {
            log->push_back(std::move(e));
            return true;
        };
    }

    // The log and the emitter live behind unique_ptrs so a move of the
    // capture never invalidates the emitter's captured pointer.
    std::unique_ptr<std::vector<Event>> events_;
    std::unique_ptr<Emitter<T>> emitter_;
};

}  // namespace clink::test
