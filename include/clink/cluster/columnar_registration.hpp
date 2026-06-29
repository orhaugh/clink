#pragma once

// Explicit-intent registration for types that carry a CLINK_ARROW_FIELDS
// description: register the type with its codec AND the generated columnar
// ArrowBatcher<T> in one call.
//
//     struct Trade { std::int64_t id; std::string symbol; double px; };
//     CLINK_ARROW_FIELDS(Trade, id, symbol, px)
//
//     // TypeRegistry (in-process / cluster role):
//     clink::cluster::register_columnar_typed<Trade>(reg, "Trade", trade_codec());
//
//     // PluginRegistry (compiled job .so):
//     clink::plugin::register_columnar_type<Trade>(reg, "Trade", trade_codec());
//
// NOTE: the plain codec-only register_typed / register_type now AUTO-SELECT
// the generated columnar batcher for any CLINK_ARROW_FIELDS type (and the
// binary fallback otherwise), so these helpers are no longer required to get
// typed columns - the plain register does it. They remain as an explicit
// statement of intent and, via the HasArrowFields constraint, as a compile
// error if the type was never described (where the plain register would
// silently use the binary fallback).
//
// The Codec<T> is still required: it backs state serialization
// (keyed/operator state) and the network bridges capture it alongside
// the batcher. The batcher only governs the on-wire / Parquet columnar
// layout.

#include <string>
#include <utility>

#include "clink/core/codec.hpp"
#include "clink/core/columnar_batcher.hpp"

#ifdef CLINK_HAS_ARROW

#include "clink/cluster/type_registry.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::cluster {

// Register T on `reg` with its codec and the generated columnar batcher.
template <clink::HasArrowFields T>
inline void register_columnar_typed(TypeRegistry& reg,
                                    const std::string& name,
                                    clink::Codec<T> codec) {
    reg.register_typed<T>(name, std::move(codec), clink::make_columnar_arrow_batcher<T>());
}

}  // namespace clink::cluster

namespace clink::plugin {

// PluginRegistry equivalent, for compiled-job .so registration.
template <clink::HasArrowFields T>
inline void register_columnar_type(PluginRegistry& reg,
                                   const std::string& name,
                                   clink::Codec<T> codec) {
    reg.register_type<T>(name, std::move(codec), clink::make_columnar_arrow_batcher<T>());
}

}  // namespace clink::plugin

#endif  // CLINK_HAS_ARROW
