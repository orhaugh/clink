#pragma once

// Delta Lake transaction-log layer (the metadata half of a Delta sink). PURE: it
// maps an Arrow schema to the Delta schema string and builds the _delta_log action
// JSON lines, with NO object-store I/O - so the protocol correctness is unit-tested
// without writing a table. The sink (delta_row_sink.hpp) writes the Parquet data
// files and commits the lines this produces.
//
// A Delta commit is one newline-delimited-JSON file _delta_log/<version>.json. Each
// line is one action object. For an append commit we emit (on version 0)
// protocol + metaData, then commitInfo + one `add` per data file; later versions
// emit commitInfo + the adds. The presence-or-absence of <version>.json is the
// atomic unit of the commit (see delta_row_sink for the create-if-absent / listing
// version logic). We target the minimal append-only feature set: protocol
// (1,2), no deletion vectors / column mapping / table features.
//
// Spec: https://github.com/delta-io/delta/blob/master/PROTOCOL.md

#include <cstdint>
#include <stdexcept>
#include <string>

#ifdef CLINK_HAS_ARROW
#include <arrow/type.h>
#endif

#include "clink/config/json.hpp"

namespace clink::delta {

// The 20-digit zero-padded log file name for a version, e.g. version 1 ->
// "00000000000000000001.json".
inline std::string version_filename(std::int64_t version) {
    std::string n = std::to_string(version);
    if (n.size() < 20) {
        n = std::string(20 - n.size(), '0') + n;
    }
    return n + ".json";
}

#ifdef CLINK_HAS_ARROW

// Map an Arrow type to its Delta primitive type name. Throws on a type Delta has no
// primitive for (the caller surfaces it rather than writing an unreadable table).
inline std::string arrow_type_to_delta(const arrow::DataType& t) {
    switch (t.id()) {
        case arrow::Type::INT64:
            return "long";
        case arrow::Type::INT32:
            return "integer";
        case arrow::Type::INT16:
            return "short";
        case arrow::Type::INT8:
            return "byte";
        case arrow::Type::DOUBLE:
            return "double";
        case arrow::Type::FLOAT:
            return "float";
        case arrow::Type::BOOL:
            return "boolean";
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            return "string";
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
            return "binary";
        case arrow::Type::DATE32:
            return "date";
        case arrow::Type::TIMESTAMP:
            return "timestamp";
        case arrow::Type::DECIMAL128: {
            const auto& d = static_cast<const arrow::Decimal128Type&>(t);
            return "decimal(" + std::to_string(d.precision()) + "," + std::to_string(d.scale()) +
                   ")";
        }
        default:
            throw std::runtime_error("delta: no Delta primitive type for Arrow type " +
                                     t.ToString());
    }
}

// The Delta schemaString: a JSON struct describing the columns. Stored (by the
// metaData action) as a STRING value, so this returns the serialized JSON text.
inline std::string arrow_schema_to_delta_schema_json(const arrow::Schema& schema) {
    clink::config::JsonArray fields;
    for (const auto& f : schema.fields()) {
        clink::config::JsonObject field;
        field["name"] = clink::config::JsonValue{f->name()};
        field["type"] = clink::config::JsonValue{arrow_type_to_delta(*f->type())};
        field["nullable"] = clink::config::JsonValue{f->nullable()};
        field["metadata"] = clink::config::JsonValue{clink::config::JsonObject{}};
        fields.push_back(clink::config::JsonValue{std::move(field)});
    }
    clink::config::JsonObject root;
    root["type"] = clink::config::JsonValue{std::string("struct")};
    root["fields"] = clink::config::JsonValue{std::move(fields)};
    return clink::config::JsonValue{std::move(root)}.serialize(0);
}

#endif  // CLINK_HAS_ARROW

// One add action (a data file added in this commit). `path` is relative to the
// table root; size in bytes; modification_time epoch-millis; num_records for the
// (minimal) stats blob.
inline std::string add_action_json(const std::string& path,
                                   std::int64_t size,
                                   std::int64_t modification_time,
                                   std::int64_t num_records) {
    clink::config::JsonObject stats;
    stats["numRecords"] = clink::config::JsonValue{static_cast<std::int64_t>(num_records)};

    clink::config::JsonObject add;
    add["path"] = clink::config::JsonValue{path};
    add["partitionValues"] = clink::config::JsonValue{clink::config::JsonObject{}};
    add["size"] = clink::config::JsonValue{size};
    add["modificationTime"] = clink::config::JsonValue{modification_time};
    add["dataChange"] = clink::config::JsonValue{true};
    add["stats"] =
        clink::config::JsonValue{clink::config::JsonValue{std::move(stats)}.serialize(0)};

    clink::config::JsonObject root;
    root["add"] = clink::config::JsonValue{std::move(add)};
    return clink::config::JsonValue{std::move(root)}.serialize(0);
}

// The protocol action (minimal reader/writer versions for an append-only table).
inline std::string protocol_action_json(int min_reader = 1, int min_writer = 2) {
    clink::config::JsonObject p;
    p["minReaderVersion"] = clink::config::JsonValue{static_cast<std::int64_t>(min_reader)};
    p["minWriterVersion"] = clink::config::JsonValue{static_cast<std::int64_t>(min_writer)};
    clink::config::JsonObject root;
    root["protocol"] = clink::config::JsonValue{std::move(p)};
    return clink::config::JsonValue{std::move(root)}.serialize(0);
}

// The metaData action. `schema_string` is the serialized Delta schema JSON
// (arrow_schema_to_delta_schema_json). `table_id` is a stable UUID-ish id.
inline std::string metadata_action_json(const std::string& table_id,
                                        const std::string& schema_string,
                                        std::int64_t created_time) {
    clink::config::JsonObject fmt;
    fmt["provider"] = clink::config::JsonValue{std::string("parquet")};
    fmt["options"] = clink::config::JsonValue{clink::config::JsonObject{}};

    clink::config::JsonObject md;
    md["id"] = clink::config::JsonValue{table_id};
    md["format"] = clink::config::JsonValue{std::move(fmt)};
    md["schemaString"] = clink::config::JsonValue{schema_string};
    md["partitionColumns"] = clink::config::JsonValue{clink::config::JsonArray{}};
    md["configuration"] = clink::config::JsonValue{clink::config::JsonObject{}};
    md["createdTime"] = clink::config::JsonValue{created_time};

    clink::config::JsonObject root;
    root["metaData"] = clink::config::JsonValue{std::move(md)};
    return clink::config::JsonValue{std::move(root)}.serialize(0);
}

// The commitInfo action (informational provenance for one commit).
inline std::string commit_info_action_json(std::int64_t timestamp, const std::string& operation) {
    clink::config::JsonObject ci;
    ci["timestamp"] = clink::config::JsonValue{timestamp};
    ci["operation"] = clink::config::JsonValue{operation};
    ci["operationParameters"] = clink::config::JsonValue{clink::config::JsonObject{}};
    ci["engineInfo"] = clink::config::JsonValue{std::string("clink")};
    clink::config::JsonObject root;
    root["commitInfo"] = clink::config::JsonValue{std::move(ci)};
    return clink::config::JsonValue{std::move(root)}.serialize(0);
}

}  // namespace clink::delta
