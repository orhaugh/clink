#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>

// Open-addressing hash-map seam (WS4).
//
// clink::FlatMap / clink::FlatSet are the single seam through which the hot
// keyed-state and operator working-set maps are declared. With
// CLINK_USE_FLAT_HASH_MAP OFF (the default) they alias std::unordered_map /
// std::unordered_set, a behaviour-preserving no-op. With it ON they route
// through ankerl::unordered_dense, an open-addressing container with
// metadata-byte (SwissTable-style) probing, with NO call-site changes.
//
// The default Hash is std::hash<Key> in BOTH branches, NOT ankerl's avalanching
// hash, so the existing std::hash specialisations (OperatorId, std::string) and
// the custom hashes the call sites pass (detail::TransparentStringHash,
// detail::PairKeyHash) keep working unchanged. ankerl detects a non-avalanching
// hash and applies its own mixing on top, and it honours is_transparent for the
// zero-alloc string_view probe the state backend relies on.
//
// The flag is a WHOLE-BUILD toggle: it is defined globally (see CMakeLists) so
// every TU agrees on the container type, because FlatMap appears in public
// headers (InMemoryStateBackend::State). Snapshot/restore is unaffected by the
// choice: the snapshot format encodes logical (op, key, value) content, not the
// container, and restore is iteration-order independent. (Iteration order does
// differ between the two containers, so any operator that emits by iterating a
// FlatMap emits in a different - but still unordered, already non-deterministic
// under std - order; tests assert on set/sorted views, not positional order.)
//
// Intentionally NOT routed through this seam (order is load-bearing, must stay
// std::map / std::unordered_map): clink::config::JsonObject (Row.values, sorted
// for canonical-JSON equality + DISTINCT), the window_map_codec inner
// std::map<int64,...>, and the coalescer's transient pending_ staging buffer.

#if defined(CLINK_USE_FLAT_HASH_MAP) && CLINK_USE_FLAT_HASH_MAP

#include <ankerl/unordered_dense.h>

namespace clink {

template <class Key, class T, class Hash = std::hash<Key>, class Eq = std::equal_to<Key>>
using FlatMap = ankerl::unordered_dense::map<Key, T, Hash, Eq>;

template <class Key, class Hash = std::hash<Key>, class Eq = std::equal_to<Key>>
using FlatSet = ankerl::unordered_dense::set<Key, Hash, Eq>;

}  // namespace clink

#else

namespace clink {

template <class Key, class T, class Hash = std::hash<Key>, class Eq = std::equal_to<Key>>
using FlatMap = std::unordered_map<Key, T, Hash, Eq>;

template <class Key, class Hash = std::hash<Key>, class Eq = std::equal_to<Key>>
using FlatSet = std::unordered_set<Key, Hash, Eq>;

}  // namespace clink

#endif
