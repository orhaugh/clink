#pragma once

// canonicalise_state_snapshot - render ANY state snapshot blob to the
// canonical 3-column Arrow stream (docs/internals/state-snapshot-format.md):
//
//   * a canonical stream passes through unchanged;
//   * a changelog stream (the 4-column variant with a leading
//     row_kind: uint8) is REPLAYED - materialisation first, then the
//     log - through a ChangelogStateBackend over an in-memory inner,
//     and the inner's contents are re-exported canonically. A
//     row_kind=3 external-materialisation handle resolves through
//     `store`; without one the replay throws the backend's clear
//     configuration error.
//
// This is the seam that makes every state tool (state-cat, state-diff,
// state-export, state-query, check-savepoint) read changelog-backed
// jobs' checkpoints - including changelog+rocksdb's external
// materialisation store - not just the canonical-form backends.

#include <cstddef>
#include <memory>
#include <vector>

#include "clink/state/external_materialization_store.hpp"

namespace clink {

std::vector<std::byte> canonicalise_state_snapshot(
    std::vector<std::byte> bytes, std::shared_ptr<ExternalMaterializationStore> store = nullptr);

}  // namespace clink
