#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>

// Open-addressing hash-map seam (WS4, Phase 0).
//
// clink::FlatMap / clink::FlatSet are the single seam through which the hot
// keyed-state and operator working-set maps are declared. Today they alias
// std::unordered_map / std::unordered_set, so routing a call site through them
// is a behaviour-preserving no-op (same iteration semantics, same snapshot
// bytes, same heterogeneous-lookup contract when a transparent Hash/Eq pair is
// supplied).
//
// Phase 1 flips CLINK_USE_FLAT_HASH_MAP to route these through a header-only
// open-addressing container (ankerl::unordered_dense), inheriting metadata-byte
// (SwissTable-style) probing with NO call-site changes. That flip must reconcile
// three things the std alias hides and the ankerl branch will need to handle:
//   1. default hash: ankerl wants an avalanching hash (ankerl::unordered_dense::
//      hash) rather than std::hash; custom key hashes (e.g. PairKeyHash) must be
//      marked is_avalanching or wrapped.
//   2. heterogeneous lookup: the string-keyed sites probe with a string_view via
//      detail::TransparentStringHash + std::equal_to<>; verify the chosen
//      container preserves the zero-alloc probe, else measure the regression.
//   3. rehash relocation: open-addressing tables move elements on growth, unlike
//      node-based std::unordered_map. Audit every site that holds a reference or
//      iterator across an insert that may rehash (notably the SQL join states).
//
// Intentionally NOT routed through this seam (order is load-bearing, must stay
// std::map / std::unordered_map): clink::config::JsonObject (Row.values, sorted
// for canonical-JSON equality + DISTINCT), the window_map_codec inner
// std::map<int64,...>, and the coalescer's transient pending_ staging buffer.

#if defined(CLINK_USE_FLAT_HASH_MAP) && CLINK_USE_FLAT_HASH_MAP
#error "CLINK_USE_FLAT_HASH_MAP is reserved for Phase 1 (ankerl wiring not landed)"
#endif

namespace clink {

template <class Key, class T, class Hash = std::hash<Key>, class Eq = std::equal_to<Key>>
using FlatMap = std::unordered_map<Key, T, Hash, Eq>;

template <class Key, class Hash = std::hash<Key>, class Eq = std::equal_to<Key>>
using FlatSet = std::unordered_set<Key, Hash, Eq>;

}  // namespace clink
