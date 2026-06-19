#pragma once

// Key groups: the partitioning primitive that lets clink rescale a
// keyed pipeline without moving records between subtasks one by one.
//
// At record time, every keyed event is bucketed into one of
// NUM_KEY_GROUPS = 128 groups by a stable hash of its key. The router
// then sends records to the subtask that owns that group:
//
//   key_group  = fnv1a_64(key_bytes) mod NUM_KEY_GROUPS
//   subtask    = key_group * parallelism / NUM_KEY_GROUPS
//
// As long as parallelism doesn't change, this is identical to the
// classic `subtask_idx = hash(key) mod parallelism` partitioning - the
// SAME key lands on the SAME subtask. The advantage shows up when the
// JM rescales a running operator: each new subtask just inherits a
// new range of key_groups, and on restore reads exactly the state
// files (or filters within them) that hold those groups.
//
// 128 is a middle ground: small enough that each
// group's serialized state is cheap to seek, large enough that
// rescaling from N to 2N gives every new subtask a clean range. Tune
// only if a workload genuinely benefits - anything from 16 to 1024
// works without changing the protocol.

#include <cstdint>
#include <span>
#include <utility>

namespace clink {

inline constexpr std::uint16_t kNumKeyGroups = 128;

using KeyGroup = std::uint16_t;

// Reserved leading byte that marks an OPERATOR-state key (source offsets,
// broadcast slots - state with no key and therefore no key group). It is
// deliberately >= kNumKeyGroups so it can never collide with a real key
// group: the rescale restore filter narrows a row only when its first byte
// is a valid key group (< kNumKeyGroups), so a key carrying this prefix is
// exempt and every subtask restores it whole (broadcast/union semantics).
// Keyed-state keys always begin with their key group in [0, kNumKeyGroups),
// so 0xFF can never be mistaken for one.
inline constexpr std::uint8_t kOperatorStateKeyPrefix = 0xFF;

// FNV-1a 64-bit hash over the byte representation of a serialized key.
// Stable across runs and architectures; not cryptographic - collisions
// just affect partition uniformity, never correctness.
inline std::uint64_t fnv1a_64_key(std::span<const std::byte> bytes) noexcept {
    constexpr std::uint64_t kOffset = 0xcbf29ce484222325ULL;
    constexpr std::uint64_t kPrime = 0x100000001b3ULL;
    std::uint64_t h = kOffset;
    for (auto b : bytes) {
        h ^= static_cast<std::uint8_t>(b);
        h *= kPrime;
    }
    return h;
}

inline KeyGroup key_group_for_key(std::span<const std::byte> key_bytes) noexcept {
    return static_cast<KeyGroup>(fnv1a_64_key(key_bytes) % kNumKeyGroups);
}

// Subtask responsible for a given group at this parallelism.
//
// Choice of formula matters for rescale efficiency. The -standard
// formula `key_group * parallelism / NUM_KEY_GROUPS` gives contiguous
// ranges per subtask, so each subtask owns one contiguous slice of
// groups. Rescaling redistributes those slices; many groups stay
// where they are (specifically, groups in the overlap of old and new
// ranges).
inline std::uint32_t subtask_for_key_group(KeyGroup group, std::uint32_t parallelism) noexcept {
    if (parallelism == 0)
        return 0;
    const auto idx = (static_cast<std::uint64_t>(group) * parallelism) / kNumKeyGroups;
    return static_cast<std::uint32_t>(idx);
}

// Inverse of subtask_for_key_group: the [first, last) half-open range
// of groups a subtask owns. Uses ceiling-division formula so
// subtask_for_key_group(g) is the consistent inverse:
//
//   start_inclusive(i) = ceil(i * N / p)
//   end_inclusive(i)   = ceil((i+1) * N / p) - 1
//
// Used by snapshot/restore to filter on read.
inline std::pair<KeyGroup, KeyGroup> key_group_range_for_subtask(
    std::uint32_t subtask_idx, std::uint32_t parallelism) noexcept {
    if (parallelism == 0)
        return {KeyGroup{0}, KeyGroup{0}};
    const auto p = static_cast<std::uint64_t>(parallelism);
    const auto N = static_cast<std::uint64_t>(kNumKeyGroups);
    const auto start = (static_cast<std::uint64_t>(subtask_idx) * N + p - 1) / p;
    const auto end_inclusive = ((static_cast<std::uint64_t>(subtask_idx + 1) * N + p - 1) / p) - 1;
    return {static_cast<KeyGroup>(start), static_cast<KeyGroup>(end_inclusive + 1)};
}

}  // namespace clink
