#pragma once

#include <cstdint>

#include "clink/core/types.hpp"

namespace clink {

// A CheckpointBarrier travels in-band with the data stream. When an operator
// receives barriers on all of its input channels for the same checkpoint id,
// it snapshots its state, forwards the barrier downstream, and acknowledges to
// the CheckpointCoordinator. This is the Chandy-Lamport-style alignment that
//  uses.
//
// For the MVP we only support single-input alignment (i.e. forward immediately
// on receipt). The interface is designed so multi-input alignment can be added
// without changing call sites.
//
// Phase 26a: each barrier carries its own alignment Mode. The
// CheckpointCoordinator stamps the mode when issuing a barrier; downstream
// operators honour the stamped mode rather than capturing a job-global
// config flag at startup. This is what lets one checkpoint go aligned and
// the next go unaligned over the same wire, and is the substrate for the
// adaptive-per-operator policy in 26c.
class CheckpointBarrier {
public:
    // Alignment policy stamped on the barrier by the coordinator.
    // Aligned (default): multi-input operators pause inputs that
    //   already saw the barrier and wait for every alive input
    //   before forwarding; in-flight records are NOT persisted.
    // Unaligned: forward the barrier on the first input's delivery
    //   and snapshot the in-flight records of the other inputs into
    //   state. Recovers faster under backpressure; pays state size.
    enum class Mode : std::uint8_t {
        Aligned = 0,
        Unaligned = 1,
    };

    constexpr CheckpointBarrier() = default;
    constexpr explicit CheckpointBarrier(CheckpointId id) noexcept : id_(id) {}
    constexpr CheckpointBarrier(CheckpointId id, bool terminal) noexcept
        : id_(id), terminal_(terminal) {}
    constexpr CheckpointBarrier(CheckpointId id, bool terminal, Mode mode) noexcept
        : id_(id), terminal_(terminal), mode_(mode) {}
    constexpr CheckpointBarrier(CheckpointId id, Mode mode) noexcept : id_(id), mode_(mode) {}

    constexpr CheckpointId id() const noexcept { return id_; }
    // Terminal barriers are emitted by a source after it returns false
    // from produce(). They flow downstream like any other barrier, but
    // sinks treat them as both pre-commit AND commit (no JM round-trip
    // - there's no recovery scenario after end-of-stream). This is how
    // 2PC sinks commit the tail of bounded streams without periodic
    // checkpointing reaching every record.
    constexpr bool is_terminal() const noexcept { return terminal_; }
    constexpr Mode mode() const noexcept { return mode_; }

    constexpr bool operator==(const CheckpointBarrier&) const = default;

private:
    CheckpointId id_{0};
    bool terminal_{false};
    Mode mode_{Mode::Aligned};
};

}  // namespace clink
