#pragma once

// ReplayDriver - type-erased offline replay of one captured operator epoch,
// for operators whose channels are NOT the SQL Row type.
//
// `clink replay` re-executes an operator over its captured input epoch and
// compares emissions (--verify: two runs byte-identical; --emit-test:
// against a frozen golden). The SQL path is row->row, so one type covers the
// capture-read and the emission-render. A plugin operator is In->Out with
// arbitrary custom types, so the drive loop must be instantiated where BOTH
// types are known - i.e. at register_operator<In, Out>. That call site builds
// a ReplayDriver (see detail::make_replay_driver) capturing In's codec (to
// read the capture), the operator factory, and Out's codec (to serialise
// emissions deterministically - the bytes ARE the comparable form, no human
// renderer needed), and hangs it on the op's OperatorFactory. The driver
// therefore crosses the dlopen boundary for free on the OperatorFactory the
// registry already carries, and `clink replay` finds it by the same
// find_operator lookup it already does.

#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "clink/runtime/record_capture.hpp"

namespace clink::cluster {

// Drive one captured epoch through a freshly-built operator and return its
// emissions as deterministic byte-strings (one per emitted record), in
// emission order. Inputs:
//   op_id         - the operator's OperatorId (state slots are keyed on it,
//                   so a restore must use the capture-time id)
//   capture_bytes - the epoch's .cap file contents (read via In's codec)
//   spec          - the op sidecar (op_type / channels / uid / params)
//   snapshot      - restore state from this snapshot (nullopt = fresh state)
//   state_id      - the checkpoint id the snapshot belongs to (0 = fresh)
//   flush         - call flush() at end-of-epoch
using ReplayDriver =
    std::function<std::vector<std::string>(std::uint64_t /*op_id*/,
                                           std::span<const std::byte> /*capture_bytes*/,
                                           const capture::OpSpecSidecar& /*spec*/,
                                           std::optional<std::span<const std::byte>> /*snapshot*/,
                                           std::uint64_t /*state_id*/,
                                           bool /*flush*/)>;

}  // namespace clink::cluster
