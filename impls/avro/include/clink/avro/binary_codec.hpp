// clink::avro::binary_codec<T>() - wraps the Avro binary encoder/decoder
// in a `clink::Codec<T>`. T must be an avrogencpp-generated struct (i.e.
// `avro::codec_traits<T>` is specialised, which avrogencpp emits for
// every Avro record / union it generates).
//
// Wire shape: the raw Avro binary bytes for T, no framing. Users that
// need length-prefix framing (e.g. multiple Avro records concatenated
// in one state slot) should compose with `clink::vector_codec<T>` or
// `clink::pair_codec<...>` from core.

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>

#include "clink/core/codec.hpp"
#include "clink/metrics/connector_metrics.hpp"

namespace clink::avro {

template <typename T>
inline clink::Codec<T> binary_codec() {
    return clink::Codec<T>{.encode =
                               [](const T& v) {
                                   auto stream = ::avro::memoryOutputStream();
                                   auto enc = ::avro::binaryEncoder();
                                   enc->init(*stream);
                                   ::avro::encode(*enc, v);
                                   enc->flush();
                                   auto snap = ::avro::snapshot(*stream);
                                   typename clink::Codec<T>::Bytes out;
                                   out.reserve(snap->size());
                                   for (auto b : *snap) {
                                       out.push_back(static_cast<std::byte>(b));
                                   }
                                   return out;
                               },
                           .decode = [](typename clink::Codec<T>::BytesView b) -> std::optional<T> {
                               std::vector<std::uint8_t> bytes;
                               bytes.reserve(b.size());
                               for (auto byte : b) {
                                   bytes.push_back(static_cast<std::uint8_t>(byte));
                               }
                               auto stream = ::avro::memoryInputStream(bytes.data(), bytes.size());
                               auto dec = ::avro::binaryDecoder();
                               dec->init(*stream);
                               T out;
                               try {
                                   ::avro::decode(*dec, out);
                               } catch (...) {
                                   clink::metrics::connector::error_inc("avro", "source");
                                   return std::nullopt;
                               }
                               return out;
                           }};
}

}  // namespace clink::avro
