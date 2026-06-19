#pragma once

#include <cstring>
#include <optional>
#include <string>
#include <utility>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

// BroadcastState<V> is the operator-side handle for state that is NOT
// per-key - a single value scoped to (operator, slot).
//
// Use cases:
//   - feature flag / config updates that arrive on a separate stream
//   - rules tables consumed by an event-routing operator
//   - lookup maps that occasionally rebuild themselves
//
// In a future distributed runtime, broadcast state is the same value across
// every parallel instance of the operator (the documented semantics). In the
// in-process MVP that distinction collapses, but the storage shape stays -
// which means jobs that use it today will keep working when distribution
// lands without API churn.
//
// Implementation: backed by StateBackend with a single fixed key
// `<slot_name>|`. Slot-name prefixing means an operator can host multiple
// independent broadcast slots without collision.
template <typename V>
class BroadcastState {
public:
    BroadcastState(StateBackend& backend,
                   OperatorId op,
                   std::string slot_name,
                   Codec<V> value_codec)
        : backend_(&backend),
          op_(op),
          slot_name_(std::move(slot_name)),
          value_codec_(std::move(value_codec)) {}

    void put(const V& v) {
        const auto bytes = value_codec_.encode(v);
        const std::string_view view(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        // Broadcast state is operator-state: it must reach every subtask on
        // restore, so it goes through the operator-state path (exempt from
        // the rescale key-group filter) rather than plain keyed put().
        backend_->put_operator_state(op_, encoded_key(), view);
    }

    std::optional<V> get() const {
        auto v = backend_->get_operator_state(op_, encoded_key());
        if (!v.has_value()) {
            return std::nullopt;
        }
        return value_codec_.decode(std::span<const std::byte>{v->data(), v->size()});
    }

    void erase() { backend_->erase_operator_state(op_, encoded_key()); }

    OperatorId operator_id() const noexcept { return op_; }
    const std::string& slot_name() const noexcept { return slot_name_; }

private:
    std::string encoded_key() const { return slot_name_ + "|"; }

    StateBackend* backend_;
    OperatorId op_;
    std::string slot_name_;
    Codec<V> value_codec_;
};

}  // namespace clink
