#include "clink/schema_registry/schema_derivation.hpp"

#include <cctype>

#include "clink/config/json.hpp"

#include "format_impls.hpp"

namespace clink::schema_registry {

using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;

std::string sanitise_name(const std::string& raw) {
    std::string s = raw;
    for (const char* suffix : {"-value", "-key"}) {
        const std::string suf(suffix);
        if (s.size() >= suf.size() && s.compare(s.size() - suf.size(), suf.size(), suf) == 0) {
            s.erase(s.size() - suf.size());
            break;
        }
    }
    std::string out;
    out.reserve(s.size() + 1);
    for (const unsigned char c : s) {
        out.push_back((std::isalnum(c) != 0 || c == '_') ? static_cast<char>(c) : '_');
    }
    if (out.empty()) {
        out = "record";
    }
    if (std::isdigit(static_cast<unsigned char>(out[0])) != 0) {
        out.insert(0, "_");
    }
    return out;
}

namespace {

JsonValue avro_type_for(const detail::ColumnSpec& c) {
    const auto& code = c.code;
    if (code == "i64") {
        return JsonValue{"long"};
    }
    if (code == "i32") {
        return JsonValue{"int"};
    }
    if (code == "f64") {
        return JsonValue{"double"};
    }
    if (code == "f32") {
        return JsonValue{"float"};
    }
    if (code == "bool") {
        return JsonValue{"boolean"};
    }
    if (code == "list_f32") {
        JsonObject arr;
        arr.emplace("type", JsonValue{"array"});
        arr.emplace("items", JsonValue{"float"});
        return JsonValue{std::move(arr)};
    }
    if (const auto ps = detail::decimal_precision_scale(code)) {
        JsonObject dec;
        dec.emplace("type", JsonValue{"bytes"});
        dec.emplace("logicalType", JsonValue{"decimal"});
        dec.emplace("precision", JsonValue{static_cast<std::int64_t>(ps->first)});
        dec.emplace("scale", JsonValue{static_cast<std::int64_t>(ps->second)});
        return JsonValue{std::move(dec)};
    }
    return JsonValue{"string"};
}

}  // namespace

std::string derive_avro_schema(const std::string& columns,
                               const std::string& record_name,
                               const std::string& record_namespace) {
    JsonObject rec;
    rec.emplace("type", JsonValue{"record"});
    rec.emplace("name", JsonValue{record_name});
    if (!record_namespace.empty()) {
        rec.emplace("namespace", JsonValue{record_namespace});
    }
    JsonArray fields;
    for (const auto& c : detail::parse_columns(columns)) {
        JsonObject f;
        f.emplace("name", JsonValue{c.name});
        JsonArray union_type;
        union_type.emplace_back("null");
        union_type.emplace_back(avro_type_for(c));
        f.emplace("type", JsonValue{std::move(union_type)});
        f.emplace("default", JsonValue{});
        fields.emplace_back(std::move(f));
    }
    rec.emplace("fields", JsonValue{std::move(fields)});
    return JsonValue{std::move(rec)}.serialize();
}

std::string derive_protobuf_schema(const std::string& columns, const std::string& message_name) {
    std::string out = "syntax = \"proto3\";\n\nmessage " + message_name + " {\n";
    int number = 1;
    for (const auto& c : detail::parse_columns(columns)) {
        std::string type = "string";
        if (c.code == "i64") {
            type = "int64";
        } else if (c.code == "i32") {
            type = "int32";
        } else if (c.code == "f64") {
            type = "double";
        } else if (c.code == "f32") {
            type = "float";
        } else if (c.code == "bool") {
            type = "bool";
        } else if (c.code == "list_f32") {
            type = "repeated float";
        }
        // Decimals and everything else travel as strings: proto3 has no exact
        // decimal scalar, and a string keeps every digit.
        out += "  " + type + " " + c.name + " = " + std::to_string(number++) + ";\n";
    }
    out += "}\n";
    return out;
}

std::string derive_json_schema(const std::string& columns, const std::string& title) {
    JsonObject schema;
    schema.emplace("$schema", JsonValue{"http://json-schema.org/draft-07/schema#"});
    schema.emplace("title", JsonValue{title});
    schema.emplace("type", JsonValue{"object"});
    JsonObject props;
    for (const auto& c : detail::parse_columns(columns)) {
        JsonObject p;
        JsonArray types;
        if (c.code == "i64" || c.code == "i32") {
            types.emplace_back("integer");
        } else if (c.code == "f64" || c.code == "f32" || c.code.rfind("dec_", 0) == 0) {
            types.emplace_back("number");
        } else if (c.code == "bool") {
            types.emplace_back("boolean");
        } else if (c.code == "list_f32") {
            types.emplace_back("array");
            JsonObject items;
            items.emplace("type", JsonValue{"number"});
            p.emplace("items", JsonValue{std::move(items)});
        } else {
            types.emplace_back("string");
        }
        types.emplace_back("null");
        p.emplace("type", JsonValue{std::move(types)});
        props.emplace(c.name, JsonValue{std::move(p)});
    }
    schema.emplace("properties", JsonValue{std::move(props)});
    return JsonValue{std::move(schema)}.serialize();
}

}  // namespace clink::schema_registry
