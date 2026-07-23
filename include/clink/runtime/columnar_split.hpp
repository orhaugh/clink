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
    std::vector<arrow::BooleanBuilder> masks(n_out);
    for (auto& m : masks) {
        if (!m.Reserve(n).ok()) {
            return std::nullopt;
        }
    }
    for (std::int64_t i = 0; i < n; ++i) {
        const int target = targets[static_cast<std::size_t>(i)];
        for (std::size_t t = 0; t < n_out; ++t) {
            masks[t].UnsafeAppend(static_cast<int>(t) == target);
        }
    }
    std::vector<Batch<T>> out(n_out);
    for (std::size_t t = 0; t < n_out; ++t) {
        std::shared_ptr<arrow::Array> mask;
        if (!masks[t].Finish(&mask).ok()) {
            return std::nullopt;
        }
        auto filtered = arrow::compute::Filter(arrow::Datum(rb), arrow::Datum(mask));
        if (!filtered.ok() || filtered->kind() != arrow::Datum::RECORD_BATCH) {
            return std::nullopt;
        }
        auto sub_rb = filtered->record_batch();
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
