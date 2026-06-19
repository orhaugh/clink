#pragma once

#include <cstdint>
#include <utility>
#include <variant>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/record.hpp"
#include "clink/time/watermark.hpp"

namespace clink {

// Phase 29b: drain marker for adaptive rescaling.
//
// Carried in-band on the operator wire (StreamElement::Drain). When
// an operator's run() loop is asked to rescale out (29c/d will wire
// the dispatch), it emits a DrainMarker downstream announcing that
// THIS upstream subtask is winding down and routing for its
// key-groups is moving to peer subtasks (at target_parallelism).
// Downstream operators consume the marker as a signal to expect a
// fresh stream from the new subtask set; the old upstream's
// subsequent records (if any, before its shutdown) still flow as
// the last of its tail.
//
// Why in-band: a drain signal must respect ordering with respect to
// data and checkpoints. An out-of-band rescale-coordination message
// could arrive at a downstream operator before the upstream's
// tail records; the downstream's aligner / state could then double-
// count or drop. Sharing the same channel makes ordering trivial.
struct DrainMarker {
    // Source subtask that's being drained. Operator runners receive
    // this on the relevant input; multi-input aligners may use it
    // to know "this specific upstream is going away."
    std::uint32_t subtask_idx{0};
    // Target parallelism the rescale is moving towards. Informational
    // for downstream metrics / logs; the actual routing change is
    // owned by the JM's RescaleCoordinator (29c/d).
    std::uint32_t target_parallelism{0};

    constexpr bool operator==(const DrainMarker&) const noexcept = default;
};

// StreamElement is the in-band envelope flowing on every channel.
//
// Four kinds:
//   - Data      : a Batch<T> of user records
//   - Watermark : the time-progress signal
//   - Barrier   : a checkpoint barrier
//   - Drain     : Phase 29b - this upstream subtask is winding down;
//                 downstream should expect a fresh stream from the
//                 rescaled set
//
// All four share the same channel because correct stream semantics require
// time, checkpoints, and rescaling to flow in-band with data. Sending
// watermarks out-of-band breaks alignment guarantees; sending checkpoint
// barriers out-of-band breaks exactly-once; sending drain out-of-band
// breaks the ordering between an upstream's tail records and its
// retirement.
template <typename T>
class StreamElement {
public:
    enum class Kind { Data, Watermark, Barrier, Drain };

    static StreamElement<T> data(Batch<T> b) { return StreamElement<T>{std::move(b)}; }
    static StreamElement<T> watermark(Watermark w) { return StreamElement<T>{w}; }
    static StreamElement<T> barrier(CheckpointBarrier b) { return StreamElement<T>{b}; }
    static StreamElement<T> drain(DrainMarker d) { return StreamElement<T>{d}; }

    Kind kind() const noexcept { return static_cast<Kind>(payload_.index()); }

    bool is_data() const noexcept { return kind() == Kind::Data; }
    bool is_watermark() const noexcept { return kind() == Kind::Watermark; }
    bool is_barrier() const noexcept { return kind() == Kind::Barrier; }
    bool is_drain() const noexcept { return kind() == Kind::Drain; }

    const Batch<T>& as_data() const { return std::get<Batch<T>>(payload_); }
    Batch<T>& as_data() { return std::get<Batch<T>>(payload_); }
    const Watermark& as_watermark() const { return std::get<Watermark>(payload_); }
    const CheckpointBarrier& as_barrier() const { return std::get<CheckpointBarrier>(payload_); }
    const DrainMarker& as_drain() const { return std::get<DrainMarker>(payload_); }

    // Visit pattern for operator dispatch. The visitor must accept Batch<T>,
    // Watermark, CheckpointBarrier, and DrainMarker overloads.
    template <typename Visitor>
    decltype(auto) visit(Visitor&& v) const {
        return std::visit(std::forward<Visitor>(v), payload_);
    }

    template <typename Visitor>
    decltype(auto) visit(Visitor&& v) {
        return std::visit(std::forward<Visitor>(v), payload_);
    }

private:
    using Payload = std::variant<Batch<T>, Watermark, CheckpointBarrier, DrainMarker>;

    explicit StreamElement(Batch<T> b) : payload_(std::move(b)) {}
    explicit StreamElement(Watermark w) : payload_(w) {}
    explicit StreamElement(CheckpointBarrier b) : payload_(b) {}
    explicit StreamElement(DrainMarker d) : payload_(d) {}

    Payload payload_;
};

}  // namespace clink
