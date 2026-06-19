// clink::avro::json_codec<T>(schema_path) - Codec<T> backed by Avro's
// JSON-text wire format. Each encoded value is a single self-describing
// JSON object (no surrounding length prefix); decoding parses one JSON
// value from the buffer.
//
// The schema file is read once at codec construction. T must be an
// avrogencpp-generated struct matching the schema; mismatches surface
// as decode failures (nullopt) at runtime.
//
// This is the wire format Java's `DecoderFactory.jsonDecoder(schema,
// json)` produces, so user codebases bridging Java/ → C++ readers
// can use this for cross-language interchange.

#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>

#include "clink/core/codec.hpp"
#include "clink/metrics/connector_metrics.hpp"

namespace clink::avro {

template <typename T>
inline clink::Codec<T> json_codec(std::string schema_path) {
    auto schema = std::make_shared<::avro::ValidSchema>(
        ::avro::compileJsonSchemaFromFile(schema_path.c_str()));
    return clink::Codec<T>{
        .encode =
            [schema](const T& v) {
                auto stream = ::avro::memoryOutputStream();
                auto enc = ::avro::jsonEncoder(*schema);
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
        .decode = [schema](typename clink::Codec<T>::BytesView b) -> std::optional<T> {
            std::vector<std::uint8_t> bytes;
            bytes.reserve(b.size());
            for (auto byte : b) {
                bytes.push_back(static_cast<std::uint8_t>(byte));
            }
            auto stream = ::avro::memoryInputStream(bytes.data(), bytes.size());
            auto dec = ::avro::jsonDecoder(*schema);
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
