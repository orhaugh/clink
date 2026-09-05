#pragma once
//
// Per-format constructors behind make_decoder / make_encoder. Each format's
// translation unit is compiled only when the build has its dependency; the
// dispatcher (formats.cpp) refuses the others by name.

#include <memory>

#include "clink/schema_registry/formats.hpp"

namespace clink::schema_registry::detail {

std::unique_ptr<ValueDecoder> make_json_schema_decoder(const FormatOptions& opts,
                                                       std::shared_ptr<Client> client);
std::unique_ptr<ValueEncoder> make_json_schema_encoder(const FormatOptions& opts,
                                                       std::shared_ptr<Client> client);

#ifdef CLINK_SCHEMA_REGISTRY_HAS_AVRO
std::unique_ptr<ValueDecoder> make_avro_decoder(const FormatOptions& opts,
                                                std::shared_ptr<Client> client);
std::unique_ptr<ValueEncoder> make_avro_encoder(const FormatOptions& opts,
                                                std::shared_ptr<Client> client);
#endif

#ifdef CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF
std::unique_ptr<ValueDecoder> make_protobuf_decoder(const FormatOptions& opts,
                                                    std::shared_ptr<Client> client);
std::unique_ptr<ValueEncoder> make_protobuf_encoder(const FormatOptions& opts,
                                                    std::shared_ptr<Client> client);
#endif

// Splits the planner's "name:code;..." column text. Shared by the
// derivations and by the encoders that filter or type top-level columns.
struct ColumnSpec {
    std::string name;
    std::string code;  // i64, i32, f64, f32, bool, str, dec_<p>_<s>, list_f32
};
std::vector<ColumnSpec> parse_columns(const std::string& columns);
// dec_<p>_<s> -> (p, s); nullopt for any other code.
std::optional<std::pair<int, int>> decimal_precision_scale(const std::string& code);

}  // namespace clink::schema_registry::detail
