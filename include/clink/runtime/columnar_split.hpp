#pragma once

// Columnar keyed-split helpers for hash-partitioned (shuffle) edges.
//
// A keyed edge routes each record to the downstream subtask that owns its
// key group. For a columnar Batch<T> (Arrow sidecar set, rows undecoded)
// the split must not materialise rows just to route them - that would
// de-columnarise the stream at every shuffle hop and silence the columnar
// operator fast paths downstream. Instead:
//
//   1. A per-batch COLUMNAR KEY EXTRACTOR (KeyExtractorRegistry::
//      find_columnar) reads the same int64 partition keys the row extractor
//      would produce, straight from the sidecar (for the SQL Row channel:
//      the __key column row_compute_key appended for exactly this purpose).
//   2. make_keyed_columnar_split composes it with the key-group routing
//      (key_group_for_key -> subtask_for_key_group, byte-identical to the
//      row selector) and gathers one columnar sub-batch per target with
//      gather_columnar_by_target.
//
// The result plugs into Dag::add_split's columnar_split hook. Any failure
// (no sidecar, key column absent, Arrow error) returns nullopt and the
// split falls back to the row path - decided before any push, so no
// double-emit. Routing parity between the two carriers is a correctness
// requirement, not an optimisation: a stream can mix columnar and row-form
// batches (per-batch decode fallback upstream), and both must agree on
// key -> subtask ownership.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "clink/core/record.hpp"
#include "clink/runtime/key_groups.hpp"

#ifdef CLINK_HAS_ARROW
#include <arrow/api.h>
#include <arrow/compute/api.h>
#endif

namespace clink {

#ifdef CLINK_HAS_ARROW

// Gather a columnar batch into one columnar sub-batch per target index.
// targets[i] names the destination of row i; a negative target drops the
// row (mirroring add_split's drop semantics). Each sub-batch keeps the
// parent's materialize closure, so a row consumer downstream of the split
// is unaffected. Returns nullopt on any Arrow failure so the caller can
// fall back to the row split.
template <typename T>
std::optional<std::vector<Batch<T>>> gather_columnar_by_target(const Batch<T>& batch,
                                                               const std::vector<int>& targets,
                                                               std::size_t n_out) {
    const auto& rb = batch.arrow();
    if (!rb || targets.size() != static_cast<std::size_t>(rb->num_rows())) {
        return std::nullopt;
    }
    const std::int64_t n = rb->num_rows();

    // Per-target INDEX lists built in one pass, then one Take per target.
    //
    // The previous shape was a boolean mask per target followed by a Filter per target,
    // which is O(rows x targets) twice over: it appended n_out booleans for every row
    // (only one of them true), and then each Filter walked the whole batch again to
    // select its subset. At parallelism 4 that scanned the batch four times to move each
    // row once. Indices are O(rows) to build - one push_back per row, into the one list
    // that wants it - and Take gathers only the rows it is given, so the total gathered
    // across all targets is n rather than n per target.
    //
    // Row ORDER within a target is preserved either way: indices are appended in row
    // order and Take emits in index order, which is what Filter did.
    std::vector<arrow::Int32Builder> idx(n_out);
    // Even split is the expectation, so reserve for it; a skewed key distribution just
    // grows some of them.
    const std::int64_t per_target_hint = n_out > 0 ? (n / static_cast<std::int64_t>(n_out)) + 1 : n;
    for (auto& b : idx) {
        if (!b.Reserve(per_target_hint).ok()) {
            return std::nullopt;
        }
    }
    for (std::int64_t i = 0; i < n; ++i) {
        const int target = targets[static_cast<std::size_t>(i)];
        if (target < 0 || static_cast<std::size_t>(target) >= n_out) {
            continue;  // out-of-range target drops the row, mirroring add_split
        }
        // Reserve above is a hint, not a bound (skew can exceed it), so this is the
        // checked Append rather than UnsafeAppend.
        if (!idx[static_cast<std::size_t>(target)].Append(static_cast<std::int32_t>(i)).ok()) {
            return std::nullopt;
        }
    }

    std::vector<Batch<T>> out(n_out);
    for (std::size_t t = 0; t < n_out; ++t) {
        if (idx[t].length() == 0) {
            continue;  // no rows for this target; leave the empty batch in place
        }
        std::shared_ptr<arrow::Array> indices;
        if (!idx[t].Finish(&indices).ok()) {
            return std::nullopt;
        }
        auto taken = arrow::compute::Take(arrow::Datum(rb), arrow::Datum(indices));
        if (!taken.ok() || taken->kind() != arrow::Datum::RECORD_BATCH) {
            return std::nullopt;
        }
        auto sub_rb = taken->record_batch();
        if (sub_rb->num_rows() > 0) {
            out[t] = batch.with_arrow(sub_rb, static_cast<std::size_t>(sub_rb->num_rows()));
        }
    }
    return out;
}

// Compose a columnar key extractor with the key-group routing into an
// add_split columnar_split hook: keys -> key_group_for_key ->
// subtask_for_key_group(n) -> per-target gather. The key bytes fed to
// key_group_for_key are the int64's object representation, exactly as the
// row selector hashes the row extractor's return value, so the two
// carriers route identically.
template <typename T>
std::function<std::optional<std::vector<Batch<T>>>(const Batch<T>&)> make_keyed_columnar_split(
    std::function<std::optional<std::vector<std::int64_t>>(const Batch<T>&)> columnar_keys,
    std::size_t n_out) {
    return [columnar_keys = std::move(columnar_keys),
            n_out](const Batch<T>& batch) -> std::optional<std::vector<Batch<T>>> {
        auto keys = columnar_keys(batch);
        if (!keys.has_value()) {
            return std::nullopt;
        }
        std::vector<int> targets;
        targets.reserve(keys->size());
        for (const std::int64_t k : *keys) {
            const auto k_bytes =
                std::span<const std::byte>{reinterpret_cast<const std::byte*>(&k), sizeof(k)};
            const auto group_id = key_group_for_key(k_bytes);
            targets.push_back(static_cast<int>(
                subtask_for_key_group(group_id, static_cast<std::uint32_t>(n_out))));
        }
        return gather_columnar_by_target(batch, targets, n_out);
    };
}

#endif  // CLINK_HAS_ARROW

}  // namespace clink
