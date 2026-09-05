#pragma once
//
// The registry-framed value formats a broker connector can speak on its
// string channel: each turns a registry-framed message into one JSON object
// text (decode side) or one JSON object text into a registry-framed message
// (encode side). The connector keeps its existing JSON path either side; the
// SQL planner's json_string_to_row / row_to_json_string bridges do not change.
//
//   avro         Avro binary against the writer schema the frame's id names.
//   protobuf     Protobuf binary against the .proto the id names, message
//                chosen by the frame's index array.
//   json-schema  JSON text; the header is stripped (decode) or added (encode).
//
// Which formats a build has is decided at compile time (Avro needs avro-cpp,
// Protobuf needs libprotobuf + libprotoc); supported_format_names() says so,
// and make_decoder / make_encoder refuse a missing one by name.
//
// JSON mapping (decode; the encode side accepts the same shapes back):
//   Avro record -> object keyed by field name; union -> its branch's value;
//   enum -> symbol; array -> array; map -> object; bytes / fixed -> base64
//   string; decimal (bytes or fixed) -> decimal string with the schema's
//   scale; date -> "YYYY-MM-DD"; time-millis / time-micros -> "HH:MM:SS.fff";
//   every timestamp logical type -> integer epoch MILLISECONDS (micros and
//   nanos are scaled down); float / double NaN or infinity -> null.
//   Protobuf message -> object keyed by proto field name (not the camelCase
//   JSON name); int64 / uint64 -> JSON integers, not strings; bytes ->
//   base64; enum -> name; map -> object; google.protobuf.Timestamp ->
//   integer epoch milliseconds; a field with presence that is unset is
//   omitted, a proto3 scalar without presence is always written.
//   A non-record top-level Avro value is wrapped as {"value": ...}.

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/schema_registry/client.hpp"

namespace clink::schema_registry {

enum class Format { Avro, Protobuf, JsonSchema };
const char* format_name(Format f) noexcept;  // "avro", "protobuf", "json-schema"
// Accepts the names above; also "json_schema" and "jsonschema" for the third.
std::optional<Format> parse_format(std::string_view text);
bool format_compiled_in(Format f) noexcept;
// The formats this build compiled in, in a stable order (avro, protobuf, json-schema).
std::vector<std::string> supported_format_names();
SchemaType schema_type_for(Format f) noexcept;

struct FormatOptions {
    Format format{Format::Avro};
    ClientOptions client;
    // Encode side. The subject the encoder writes under (a Kafka sink defaults
    // it to "<topic>-value"). With auto_register the encoder derives a schema
    // from `columns` and registers it (the registry hands back the existing id
    // when it is already there); without it the subject's latest version is
    // the schema, and the encoder fails at construction if there is none.
    std::string subject;
    bool auto_register{true};
    // serialize_row_schema() text ("name:code;...") of the declared columns.
    std::string columns;
    // Names for a derived schema: the Avro record / Protobuf message name
    // (default: derived from the subject) and the Avro namespace.
    std::string record_name;
    std::string record_namespace{"clink"};
    // Protobuf encode side: which message of the (non-derived) schema to write;
    // default the first top-level message.
    std::string message_name;
};

class ValueDecoder {
public:
    virtual ~ValueDecoder() = default;
    // Registry-framed bytes -> one JSON object text. Throws std::runtime_error
    // for a malformed frame, an id the registry does not know, a schema of the
    // wrong type for this format, or a payload the schema cannot decode.
    virtual std::string to_json(std::string_view framed) = 0;
    [[nodiscard]] virtual Format format() const noexcept = 0;
};

class ValueEncoder {
public:
    virtual ~ValueEncoder() = default;
    // One JSON object text -> registry-framed bytes. Keys the schema does not
    // name are dropped; a missing key encodes as null where the schema allows
    // it and is an error where it does not. Throws std::runtime_error.
    virtual std::string from_json(std::string_view json) = 0;
    [[nodiscard]] virtual std::int32_t schema_id() const noexcept = 0;
    [[nodiscard]] virtual Format format() const noexcept = 0;
};

// Construct a decoder / encoder. The encoder talks to the registry in its
// constructor (register or fetch the subject's schema), so a misconfigured
// sink fails at build time, not on its first record. Both throw
// std::runtime_error, naming the format, when the build lacks it.
std::unique_ptr<ValueDecoder> make_decoder(const FormatOptions& opts,
                                           std::shared_ptr<Client> client);
std::unique_ptr<ValueEncoder> make_encoder(const FormatOptions& opts,
                                           std::shared_ptr<Client> client);

// Connector option parsing shared by every connector offering these formats.
// `param(key, fallback)` is the connector's option lookup (BuildContext::
// param_or, which resolves env:// secrets). Reads:
//   format                            avro | protobuf | json-schema (anything else: not ours)
//   schema_registry_url               required for the formats above
//   schema_registry_auth              "user:password" (basic auth)
//   schema_registry_token             bearer token
//   schema_registry_verify_tls        default true
//   schema_registry_subject           default "<topic>-value"
//   schema_registry_auto_register     default true (sinks)
//   schema_registry_record_name       Avro record / Protobuf message name for a derived schema
//   schema_registry_namespace         Avro namespace for a derived schema (default "clink")
//   schema_registry_message           Protobuf message to write from a registry-held schema
//   schema_registry_timeout_ms        connect and read timeout (default 30000)
//   schema_columns                    the planner's serialised column list (sinks)
// Returns no options and an empty error for a `format` that is not a registry
// format (json, text, unset); an error message for a registry format the
// build lacks or that is missing its URL.
using ParamLookup = std::function<std::string(const std::string& key, const std::string& fallback)>;
struct ParsedFormatOptions {
    std::optional<FormatOptions> options;
    std::string error;
};
ParsedFormatOptions parse_format_options(const ParamLookup& param,
                                         const std::string& topic,
                                         const char* connector_name);

}  // namespace clink::schema_registry
