#pragma once

// Key-group-aware partitioner for SubtaskEmitter.
//
// SubtaskEmitter routes each record through a Partitioner -> subtask index.
// The historical in-process partitioner is `std::hash(key) % parallelism`,
// which is NOT aligned with how clink partitions keyed STATE: state, restore
// and rescale all route by key group (fnv1a_64(key) % kNumKeyGroups, then
// subtask = key_group * parallelism / kNumKeyGroups, see key_groups.hpp).
//
// make_key_group_partitioner closes that gap: it routes records to the SAME
// subtask that owns their key group's state. This is the routing half of
// shard-per-core - a record reaches the thread that owns its key group, so
// that thread's state shard is the only one it ever touches.
//
// CONTRACT: build the partitioner with `parallelism == output_count` of the
// emitter it feeds. subtask_for_key_group already returns an index in
// [0, parallelism), so the emitter's `% outputs_.size()` is then a no-op. A
// MISMATCH silently mis-routes: with parallelism > output_count the high
// indices fold onto the wrong shard; with parallelism < output_count the top
// shards starve. Either way a record can land on a subtask that does NOT own
// its key group's state, breaking the routing/ownership agreement this helper
// exists to guarantee. The mismatch is not checkable here (the emitter is not
// in scope), so the call site must keep the two counts equal.

#include <cstddef>
#include <functional>
#include <span>
#include <vector>

#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/subtask_emitter.hpp"

namespace clink {

// Extracts the partitioning key of a record as raw bytes. The bytes must be
// produced the same way the keyed-state encoder hashes a key, so routing and
// state ownership agree. Typically wraps the operator's key selector + the
// Codec used to serialize that key.
template <typename T>
using KeyBytesOf = std::function<std::vector<std::byte>(const T&)>;

// Build a SubtaskEmitter<T>::Partitioner that routes by key group. Same key ->
// same key group -> same subtask, deterministically and consistently with
// state/rescale routing.
template <typename T>
typename SubtaskEmitter<T>::Partitioner make_key_group_partitioner(KeyBytesOf<T> key_bytes_of,
                                                                   std::uint32_t parallelism) {
    return [key_bytes_of = std::move(key_bytes_of), parallelism](const T& value) -> std::size_t {
        const std::vector<std::byte> bytes = key_bytes_of(value);
        const KeyGroup kg = key_group_for_key(std::span<const std::byte>(bytes));
        return subtask_for_key_group(kg, parallelism);
    };
}

}  // namespace clink
