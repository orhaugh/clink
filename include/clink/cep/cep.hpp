#pragma once

// CEP entry point. Mirrors `CEP.pattern(stream, pattern)`:
//
//   auto matched = clink::cep::pattern(stream, p, clink::trivial_codec<Event>())
//                      .select<Alert>([](const auto& match) { ... });
//
// Two overloads - keyed and non-keyed - pick the right per-key
// routing and key extractor automatically. The non-keyed form wires
// a constant-0 extractor and lands all events on sentinel key 0 so
// the same CepOperator implementation backs both paths.
//
// Pattern semantics, the deferred surface, and the runtime contract
// are documented in pattern.hpp and cep_operator.hpp.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/cep/cep_operator.hpp"
#include "clink/cep/pattern.hpp"
#include "clink/cep/pattern_stream.hpp"
#include "clink/core/codec.hpp"

namespace clink::cep {

// Keyed CEP. The upstream KeyedDataStream<T> already carries the
// extractor name registered via .key_by(...); we look it up to get
// the typed int64 extractor for the CepOperator's per-key state
// access. The op's OperatorSpec carries key_by so the planner uses
// Hash routing on the incoming edge.
template <typename T>
PatternStream<T> pattern(const api::KeyedDataStream<T>& stream, Pattern<T> p, Codec<T> codec) {
    auto* env = stream.env();
    auto& reg = env->registry();
    auto* key_reg = &reg.key_extractor_registry();
    const auto& channel = stream.channel_type();
    const auto& key_name = stream.key_by();
    auto extractor = key_reg->template find<T>(channel, key_name);
    if (!extractor) {
        throw std::runtime_error("cep::pattern: key extractor '" + key_name + "' for channel '" +
                                 channel + "' is not registered (call .key_by(...) first)");
    }
    return PatternStream<T>(env,
                            stream.id(),
                            stream.channel_type(),
                            stream.key_by(),
                            std::move(p),
                            std::move(codec),
                            [extractor](const T& v) -> std::int64_t { return extractor(v); });
}

// Non-keyed CEP. All events route through sentinel key 0; the
// emitted op has key_by unset, so the planner leaves routing as
// Forward / Rebalance (whichever the input edge inherits). Mostly
// useful for small in-process CEP - partition first for production.
template <typename T>
PatternStream<T> pattern(const api::DataStream<T>& stream, Pattern<T> p, Codec<T> codec) {
    return PatternStream<T>(stream.env(),
                            stream.id(),
                            stream.channel_type(),
                            /*key_by=*/std::string{},
                            std::move(p),
                            std::move(codec),
                            [](const T&) -> std::int64_t { return 0; });
}

}  // namespace clink::cep
