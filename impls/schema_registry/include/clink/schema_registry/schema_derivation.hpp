#pragma once
//
// Schemas derived from a declared SQL column list (the planner's
// serialize_row_schema() text, "name:code;..."), for the encode side's
// auto-registration. One derivation per format; the rules are in the
// connector page. Every derived field is nullable (Avro: ["null", T] with a
// null default; Protobuf: proto3 scalars, so an absent value is the type's
// default; JSON Schema: a type array including "null").

#include <string>

namespace clink::schema_registry {

// Column type codes (row_columnar_batcher.hpp): i64, i32, f64, f32, bool,
// str, dec_<p>_<s>, list_f32.

// An Avro record schema (JSON text). `record_name` must be a valid Avro
// name; sanitise_name() makes one from a subject or topic.
std::string derive_avro_schema(const std::string& columns,
                               const std::string& record_name,
                               const std::string& record_namespace);

// A proto3 file with one message (`.proto` text). Decimals become string
// fields; list_f32 becomes `repeated float`.
std::string derive_protobuf_schema(const std::string& columns, const std::string& message_name);

// A draft-07 JSON Schema object (JSON text) with one property per column.
std::string derive_json_schema(const std::string& columns, const std::string& title);

// Turn a subject or topic into a valid Avro / Protobuf identifier: every
// character outside [A-Za-z0-9_] becomes '_', a leading digit gets a '_'
// prefix, and a trailing "-value" / "-key" subject suffix is dropped first.
std::string sanitise_name(const std::string& raw);

}  // namespace clink::schema_registry
