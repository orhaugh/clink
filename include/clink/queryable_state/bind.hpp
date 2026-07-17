#pragma once

// Bind helpers - connect a typed KeyedState<K, V> to a Registry slot.
//
// Usage from an operator's open():
//
//   void open(RuntimeContext& ctx) override {
//     state_ = std::make_unique<KeyedState<K, V>>(
//         ctx.keyed_state<K, V>("counter", kc, vc));
//     clink::queryable_state::bind_keyed_state(
//         registry_, "counter", *state_, kc, vc);
//   }
//
// The closure captures references to the KeyedState (which captures
// the backend). Same lifetime contract documented on registry.hpp:
// unregister in close() if the backend goes away while registry
// outlives the operator.

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink::queryable_state {

// Bind a typed KeyedState<K, V> as a queryable slot. The closure
// stored in the registry decodes the inbound key bytes via `kc`,
// hits the state slot via the operator's encode-key path (which
// includes the kg byte + slot prefix), and encodes the value back
// via `vc`. Caller-supplied codecs must match the ones the operator
// itself uses for the slot.
template <typename K, typename V>
inline void bind_keyed_state(Registry& registry,
                             const std::string& slot,
                             std::shared_ptr<KeyedState<K, V>> state,
                             Codec<K> kc,
                             Codec<V> vc) {
    registry.register_slot(slot,
                           [state, kc, vc](std::span<const std::byte> key_bytes)
                               -> std::optional<std::vector<std::byte>> {
                               auto k = kc.decode(key_bytes);
                               if (!k.has_value()) {
                                   return std::nullopt;
                               }
                               auto v = state->get(*k);
                               if (!v.has_value()) {
                                   return std::nullopt;
                               }
                               return vc.encode(*v);
                           });
}

// Bind a typed KeyedState<K, V> under a subtask-scoped slot. Same
// shape as bind_keyed_state but namespaces the slot by (role,
// subtask_idx) so multiple subtasks of the same op on the same worker
// don't collide. Look up via Client::get(role, subtask_idx, slot,
// key) or the corresponding HTTP route. The non-scoped variant
// remains available for single-subtask jobs and back-compat.
template <typename K, typename V>
inline void bind_keyed_state_for_subtask(Registry& registry,
                                         const std::string& role,
                                         std::uint32_t subtask_idx,
                                         const std::string& slot,
                                         std::shared_ptr<KeyedState<K, V>> state,
                                         Codec<K> kc,
                                         Codec<V> vc) {
    bind_keyed_state(registry,
                     compose_subtask_slot(role, subtask_idx, slot),
                     std::move(state),
                     std::move(kc),
                     std::move(vc));
}

}  // namespace clink::queryable_state
