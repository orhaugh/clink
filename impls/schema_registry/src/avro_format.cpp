// The Avro format: registry-framed Avro binary <-> JSON object text, through
// avro-cpp's generic (schema-driven) datum. The writer schema is the one the
// frame's id names, fetched once per id. Logical types are mapped as the
// formats.hpp header documents.

#include <cmath>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Exception.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/LogicalType.hh>
#include <avro/Node.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>

#include "clink/config/json.hpp"
#include "clink/core/base64.hpp"
#include "clink/schema_registry/schema_derivation.hpp"
#include "clink/schema_registry/wire_format.hpp"

#include "format_impls.hpp"
#include "value_conversions.hpp"

namespace clink::schema_registry::detail {
namespace {

using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;

std::shared_ptr<avro::ValidSchema> compile(const std::string& text, const std::string& what) {
    try {
        return std::make_shared<avro::ValidSchema>(avro::compileJsonSchemaFromString(text));
    } catch (const std::exception& e) {
        throw std::runtime_error("avro: " + what + ": schema does not compile: " + e.what());
    }
}

// ---- datum -> JSON -------------------------------------------------------

JsonValue base64_of(const std::vector<std::uint8_t>& bytes) {
    return JsonValue{clink::base64_encode(
        std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()))};
}

JsonValue decimal_of(const std::vector<std::uint8_t>& bytes, int scale) {
    const auto d = decimal_from_bytes(bytes.data(), bytes.size());
    if (!d.has_value()) {
        throw std::runtime_error("avro decode: decimal wider than 16 bytes is not supported");
    }
    return JsonValue{decimal_string(*d, scale)};
}

JsonValue datum_to_json(const avro::GenericDatum& d) {
    const auto lt = d.logicalType().type();
    switch (d.type()) {
        case avro::AVRO_NULL:
            return JsonValue{};
        case avro::AVRO_BOOL:
            return JsonValue{d.value<bool>()};
        case avro::AVRO_INT: {
            const auto v = d.value<std::int32_t>();
            if (lt == avro::LogicalType::DATE) {
                return JsonValue{date_string(v)};
            }
            if (lt == avro::LogicalType::TIME_MILLIS) {
                return JsonValue{time_string(v, 3)};
            }
            return JsonValue{static_cast<std::int64_t>(v)};
        }
        case avro::AVRO_LONG: {
            const auto v = d.value<std::int64_t>();
            switch (lt) {
                case avro::LogicalType::TIME_MICROS:
                    return JsonValue{time_string(v, 6)};
                case avro::LogicalType::TIMESTAMP_MICROS:
                case avro::LogicalType::LOCAL_TIMESTAMP_MICROS:
                    return JsonValue{floor_div(v, 1000)};
                case avro::LogicalType::TIMESTAMP_NANOS:
                case avro::LogicalType::LOCAL_TIMESTAMP_NANOS:
                    return JsonValue{floor_div(v, 1000000)};
                default:
                    return JsonValue{v};
            }
        }
        case avro::AVRO_FLOAT: {
            const double v = d.value<float>();
            return std::isfinite(v) ? JsonValue{v} : JsonValue{};
        }
        case avro::AVRO_DOUBLE: {
            const double v = d.value<double>();
            return std::isfinite(v) ? JsonValue{v} : JsonValue{};
        }
        case avro::AVRO_STRING:
            return JsonValue{d.value<std::string>()};
        case avro::AVRO_BYTES: {
            const auto& b = d.value<std::vector<std::uint8_t>>();
            if (lt == avro::LogicalType::DECIMAL) {
                return decimal_of(b, d.logicalType().scale());
            }
            return base64_of(b);
        }
        case avro::AVRO_FIXED: {
            const auto& f = d.value<avro::GenericFixed>();
            if (lt == avro::LogicalType::DECIMAL) {
                return decimal_of(f.value(), d.logicalType().scale());
            }
            return base64_of(f.value());
        }
        case avro::AVRO_ENUM:
            return JsonValue{d.value<avro::GenericEnum>().symbol()};
        case avro::AVRO_ARRAY: {
            JsonArray arr;
            for (const auto& e : d.value<avro::GenericArray>().value()) {
                arr.push_back(datum_to_json(e));
            }
            return JsonValue{std::move(arr)};
        }
        case avro::AVRO_MAP: {
            JsonObject obj;
            for (const auto& [k, v] : d.value<avro::GenericMap>().value()) {
                obj.emplace(k, datum_to_json(v));
            }
            return JsonValue{std::move(obj)};
        }
        case avro::AVRO_RECORD: {
            const auto& r = d.value<avro::GenericRecord>();
            const auto& node = r.schema();
            JsonObject obj;
            for (std::size_t i = 0; i < r.fieldCount(); ++i) {
                obj.emplace(node->nameAt(i), datum_to_json(r.fieldAt(i)));
            }
            return JsonValue{std::move(obj)};
        }
        case avro::AVRO_UNION:
        default:
            throw std::runtime_error("avro decode: unexpected datum type");
    }
}

// ---- JSON -> datum -------------------------------------------------------

struct EncodeContext {
    // Exact numeral text of the row's top-level decimal fields (see
    // raw_number_tokens): looked up by field name at depth 0 only.
    const std::map<std::string, std::string>* raw_tokens{nullptr};
};

[[noreturn]] void bad(const std::string& path, const std::string& what) {
    throw std::runtime_error("avro encode: field '" + path + "': " + what);
}

std::int64_t integer_of(const JsonValue& v, const std::string& path) {
    if (v.is_integral_number()) {
        return v.as_int();
    }
    if (v.is_number()) {
        const double d = v.as_number();
        if (std::floor(d) != d || !std::isfinite(d) || std::fabs(d) > 9.2e18) {
            bad(path, "expected an integer, got " + v.serialize());
        }
        return static_cast<std::int64_t>(d);
    }
    if (v.is_string()) {
        const auto& s = v.as_string();
        std::int64_t out = 0;
        const auto* end = s.data() + s.size();
        const auto r = std::from_chars(s.data(), end, out);
        if (r.ec == std::errc{} && r.ptr == end) {
            return out;
        }
    }
    bad(path, "expected an integer, got " + v.serialize());
}

double number_of(const JsonValue& v, const std::string& path) {
    if (v.is_number()) {
        return v.as_number();
    }
    if (v.is_string()) {
        try {
            return std::stod(v.as_string());
        } catch (const std::exception&) {
        }
    }
    bad(path, "expected a number, got " + v.serialize());
}

bool accepts(const avro::NodePtr& node, const JsonValue& v) {
    const auto t = node->type();
    const auto lt = node->logicalType().type();
    if (v.is_null()) {
        return t == avro::AVRO_NULL;
    }
    if (v.is_bool()) {
        return t == avro::AVRO_BOOL;
    }
    if (v.is_number()) {
        if (t == avro::AVRO_LONG || t == avro::AVRO_DOUBLE || t == avro::AVRO_FLOAT) {
            return true;
        }
        if (t == avro::AVRO_INT) {
            return is_integral_json(v) && lt == avro::LogicalType::NONE;
        }
        return (t == avro::AVRO_BYTES || t == avro::AVRO_FIXED) && lt == avro::LogicalType::DECIMAL;
    }
    if (v.is_string()) {
        switch (t) {
            case avro::AVRO_STRING:
                return true;
            case avro::AVRO_ENUM: {
                std::size_t idx = 0;
                return node->nameIndex(v.as_string(), idx);
            }
            case avro::AVRO_INT:
                return lt == avro::LogicalType::DATE || lt == avro::LogicalType::TIME_MILLIS;
            case avro::AVRO_LONG:
                return lt != avro::LogicalType::NONE;
            case avro::AVRO_BYTES:
            case avro::AVRO_FIXED:
                return true;
            default:
                return false;
        }
    }
    if (v.is_object()) {
        return t == avro::AVRO_RECORD || t == avro::AVRO_MAP;
    }
    if (v.is_array()) {
        return t == avro::AVRO_ARRAY;
    }
    return false;
}

// Branch preference when several accept the value: exact-type matches first
// (a JSON integer prefers long over double, a string prefers string over
// bytes), so the ordering above is the tie-break.
std::size_t choose_branch(const avro::NodePtr& union_node,
                          const JsonValue* v,
                          const std::string& path) {
    const std::size_t n = union_node->leaves();
    if (v == nullptr || v->is_null()) {
        for (std::size_t i = 0; i < n; ++i) {
            if (union_node->leafAt(i)->type() == avro::AVRO_NULL) {
                return i;
            }
        }
        bad(path,
            v == nullptr ? "missing and the schema does not allow null"
                         : "null but the schema does not allow it");
    }
    // Pass 1: the natural carrier for the JSON type.
    for (std::size_t i = 0; i < n; ++i) {
        const auto& leaf = union_node->leafAt(i);
        const auto t = leaf->type();
        const bool natural =
            (v->is_bool() && t == avro::AVRO_BOOL) ||
            (is_integral_json(*v) && (t == avro::AVRO_LONG || t == avro::AVRO_INT)) ||
            (v->is_number() && !is_integral_json(*v) &&
             (t == avro::AVRO_DOUBLE || t == avro::AVRO_FLOAT)) ||
            (v->is_string() && (t == avro::AVRO_STRING || t == avro::AVRO_ENUM)) ||
            (v->is_object() && t == avro::AVRO_RECORD) || (v->is_array() && t == avro::AVRO_ARRAY);
        if (natural && accepts(leaf, *v)) {
            return i;
        }
    }
    for (std::size_t i = 0; i < n; ++i) {
        if (accepts(union_node->leafAt(i), *v)) {
            return i;
        }
    }
    bad(path, "no union branch accepts " + v->serialize());
}

void fill(avro::GenericDatum& d,
          const avro::NodePtr& node,
          const JsonValue* v,
          const std::string& path,
          const EncodeContext& ctx,
          int depth);

void fill_bytes(std::vector<std::uint8_t>& out,
                const avro::NodePtr& node,
                const JsonValue& v,
                const std::string& path,
                const EncodeContext& ctx,
                int depth,
                std::optional<std::size_t> fixed_size) {
    const auto lt = node->logicalType();
    if (lt.type() == avro::LogicalType::DECIMAL) {
        const std::string* raw = nullptr;
        if (depth == 1 && ctx.raw_tokens != nullptr) {
            if (const auto it = ctx.raw_tokens->find(path); it != ctx.raw_tokens->end()) {
                raw = &it->second;
            }
        }
        const auto dec = decimal_at_scale(v, raw, lt.scale());
        if (!dec.has_value()) {
            bad(path,
                "cannot represent " + v.serialize() + " as decimal(" +
                    std::to_string(lt.precision()) + "," + std::to_string(lt.scale()) + ")");
        }
        out = decimal_to_bytes(*dec, fixed_size);
        return;
    }
    if (!v.is_string()) {
        bad(path, "expected a base64 string for bytes, got " + v.serialize());
    }
    const auto decoded = clink::base64_decode(v.as_string());
    if (!decoded.has_value()) {
        bad(path, "bytes value is not valid base64");
    }
    if (fixed_size.has_value() && decoded->size() != *fixed_size) {
        bad(path,
            "fixed(" + std::to_string(*fixed_size) + ") needs exactly that many bytes, got " +
                std::to_string(decoded->size()));
    }
    out.assign(decoded->begin(), decoded->end());
}

void fill(avro::GenericDatum& d,
          const avro::NodePtr& node,
          const JsonValue* v,
          const std::string& path,
          const EncodeContext& ctx,
          int depth) {
    const auto t = node->type();
    if (t == avro::AVRO_UNION) {
        const auto branch = choose_branch(node, v, path);
        d.selectBranch(branch);
        fill(d, node->leafAt(branch), v, path, ctx, depth);
        return;
    }
    if (v == nullptr) {
        if (t == avro::AVRO_NULL) {
            return;
        }
        bad(path, "missing and the schema does not allow null");
    }
    const auto lt = node->logicalType().type();
    switch (t) {
        case avro::AVRO_NULL:
            if (!v->is_null()) {
                bad(path, "schema says null, got " + v->serialize());
            }
            return;
        case avro::AVRO_BOOL:
            if (!v->is_bool()) {
                bad(path, "expected a boolean, got " + v->serialize());
            }
            d.value<bool>() = v->as_bool();
            return;
        case avro::AVRO_INT: {
            std::int64_t x = 0;
            if (v->is_string() && lt == avro::LogicalType::DATE) {
                const auto days = parse_date(v->as_string());
                if (!days.has_value()) {
                    bad(path, "expected a YYYY-MM-DD date, got " + v->serialize());
                }
                x = *days;
            } else if (v->is_string() && lt == avro::LogicalType::TIME_MILLIS) {
                const auto us = parse_time_micros(v->as_string());
                if (!us.has_value()) {
                    bad(path, "expected an HH:MM:SS.fff time, got " + v->serialize());
                }
                x = *us / 1000;
            } else {
                x = integer_of(*v, path);
            }
            if (x < std::numeric_limits<std::int32_t>::min() ||
                x > std::numeric_limits<std::int32_t>::max()) {
                bad(path, "value out of int range");
            }
            d.value<std::int32_t>() = static_cast<std::int32_t>(x);
            return;
        }
        case avro::AVRO_LONG: {
            std::int64_t x = 0;
            const bool timestamp = lt == avro::LogicalType::TIMESTAMP_MILLIS ||
                                   lt == avro::LogicalType::TIMESTAMP_MICROS ||
                                   lt == avro::LogicalType::TIMESTAMP_NANOS ||
                                   lt == avro::LogicalType::LOCAL_TIMESTAMP_MILLIS ||
                                   lt == avro::LogicalType::LOCAL_TIMESTAMP_MICROS ||
                                   lt == avro::LogicalType::LOCAL_TIMESTAMP_NANOS;
            if (v->is_string() && timestamp) {
                const auto us = parse_timestamp_micros(v->as_string());
                if (!us.has_value()) {
                    bad(path, "expected an ISO-8601 timestamp, got " + v->serialize());
                }
                if (lt == avro::LogicalType::TIMESTAMP_MICROS ||
                    lt == avro::LogicalType::LOCAL_TIMESTAMP_MICROS) {
                    x = *us;
                } else if (lt == avro::LogicalType::TIMESTAMP_NANOS ||
                           lt == avro::LogicalType::LOCAL_TIMESTAMP_NANOS) {
                    x = *us * 1000;
                } else {
                    x = floor_div(*us, 1000);
                }
            } else if (v->is_string() && lt == avro::LogicalType::TIME_MICROS) {
                const auto us = parse_time_micros(v->as_string());
                if (!us.has_value()) {
                    bad(path, "expected an HH:MM:SS.ffffff time, got " + v->serialize());
                }
                x = *us;
            } else {
                x = integer_of(*v, path);
                // A JSON integer for a timestamp is epoch milliseconds (the
                // decode side's convention); scale to the schema's unit.
                if (lt == avro::LogicalType::TIMESTAMP_MICROS ||
                    lt == avro::LogicalType::LOCAL_TIMESTAMP_MICROS) {
                    x *= 1000;
                } else if (lt == avro::LogicalType::TIMESTAMP_NANOS ||
                           lt == avro::LogicalType::LOCAL_TIMESTAMP_NANOS) {
                    x *= 1000000;
                }
            }
            d.value<std::int64_t>() = x;
            return;
        }
        case avro::AVRO_FLOAT:
            d.value<float>() = static_cast<float>(number_of(*v, path));
            return;
        case avro::AVRO_DOUBLE:
            d.value<double>() = number_of(*v, path);
            return;
        case avro::AVRO_STRING:
            if (v->is_string()) {
                d.value<std::string>() = v->as_string();
            } else if (v->is_number() || v->is_bool()) {
                d.value<std::string>() = v->serialize();
            } else {
                bad(path, "expected a string, got " + v->serialize());
            }
            return;
        case avro::AVRO_BYTES:
            fill_bytes(
                d.value<std::vector<std::uint8_t>>(), node, *v, path, ctx, depth, std::nullopt);
            return;
        case avro::AVRO_FIXED: {
            std::vector<std::uint8_t> bytes;
            fill_bytes(bytes, node, *v, path, ctx, depth, node->fixedSize());
            d.value<avro::GenericFixed>().value() = std::move(bytes);
            return;
        }
        case avro::AVRO_ENUM: {
            if (!v->is_string()) {
                bad(path, "expected an enum symbol string, got " + v->serialize());
            }
            std::size_t idx = 0;
            if (!node->nameIndex(v->as_string(), idx)) {
                bad(path,
                    "'" + v->as_string() + "' is not a symbol of enum " + node->name().fullname());
            }
            d.value<avro::GenericEnum>().set(v->as_string());
            return;
        }
        case avro::AVRO_ARRAY: {
            if (!v->is_array()) {
                bad(path, "expected an array, got " + v->serialize());
            }
            auto& items = d.value<avro::GenericArray>().value();
            items.clear();
            const auto& item_node = node->leafAt(0);
            std::size_t i = 0;
            for (const auto& e : v->as_array()) {
                items.emplace_back(item_node);
                fill(items.back(),
                     item_node,
                     &e,
                     path + "[" + std::to_string(i++) + "]",
                     ctx,
                     depth + 1);
            }
            return;
        }
        case avro::AVRO_MAP: {
            if (!v->is_object()) {
                bad(path, "expected an object for a map, got " + v->serialize());
            }
            auto& entries = d.value<avro::GenericMap>().value();
            entries.clear();
            const auto& value_node = node->leafAt(1);
            for (const auto& [k, val] : v->as_object()) {
                entries.emplace_back(std::string(k), avro::GenericDatum(value_node));
                fill(entries.back().second,
                     value_node,
                     &val,
                     path + "." + std::string(k),
                     ctx,
                     depth + 1);
            }
            return;
        }
        case avro::AVRO_RECORD: {
            if (!v->is_object()) {
                bad(path, "expected an object for a record, got " + v->serialize());
            }
            auto& rec = d.value<avro::GenericRecord>();
            const auto& obj = v->as_object();
            for (std::size_t i = 0; i < node->leaves(); ++i) {
                const auto& name = node->nameAt(i);
                const auto it = obj.find(name);
                fill(rec.fieldAt(i),
                     node->leafAt(i),
                     it == obj.end() ? nullptr : &it->second,
                     path.empty() ? name : path + "." + name,
                     ctx,
                     depth + 1);
            }
            return;
        }
        default:
            bad(path, "unsupported schema type");
    }
}

// The row's top-level decimal fields (bytes/fixed with the decimal logical
// type, directly or as a union branch), name -> scale, for exact-digit
// ingestion via raw_number_tokens.
std::map<std::string, int> top_level_decimals(const avro::NodePtr& root) {
    std::map<std::string, int> out;
    if (root->type() != avro::AVRO_RECORD) {
        return out;
    }
    for (std::size_t i = 0; i < root->leaves(); ++i) {
        const auto& leaf = root->leafAt(i);
        auto check = [&](const avro::NodePtr& n) {
            if ((n->type() == avro::AVRO_BYTES || n->type() == avro::AVRO_FIXED) &&
                n->logicalType().type() == avro::LogicalType::DECIMAL) {
                out[root->nameAt(i)] = n->logicalType().scale();
            }
        };
        if (leaf->type() == avro::AVRO_UNION) {
            for (std::size_t b = 0; b < leaf->leaves(); ++b) {
                check(leaf->leafAt(b));
            }
        } else {
            check(leaf);
        }
    }
    return out;
}

class AvroDecoder final : public ValueDecoder {
public:
    explicit AvroDecoder(std::shared_ptr<Client> client) : client_(std::move(client)) {}

    std::string to_json(std::string_view framed) override {
        std::string err;
        const auto f = parse_frame(framed, /*with_message_indexes=*/false, &err);
        if (!f.has_value()) {
            throw std::runtime_error("avro decode: " + err);
        }
        const auto schema = schema_for_(f->schema_id);
        avro::GenericDatum datum(*schema);
        try {
            auto in = avro::memoryInputStream(
                reinterpret_cast<const std::uint8_t*>(f->payload.data()), f->payload.size());
            auto dec = avro::binaryDecoder();
            dec->init(*in);
            avro::decode(*dec, datum);
        } catch (const std::exception& e) {
            throw std::runtime_error("avro decode: schema id " + std::to_string(f->schema_id) +
                                     " cannot decode the payload: " + e.what());
        }
        JsonValue v = datum_to_json(datum);
        if (!v.is_object()) {
            JsonObject wrapped;
            wrapped.emplace("value", std::move(v));
            v = JsonValue{std::move(wrapped)};
        }
        return v.serialize();
    }
    [[nodiscard]] Format format() const noexcept override { return Format::Avro; }

private:
    std::shared_ptr<avro::ValidSchema> schema_for_(std::int32_t id) {
        {
            std::lock_guard lk(mu_);
            if (const auto it = schemas_.find(id); it != schemas_.end()) {
                return it->second;
            }
        }
        const auto s = client_->schema_by_id(id);
        if (s.type != SchemaType::Avro) {
            throw std::runtime_error(
                "avro decode: schema id " + std::to_string(id) + " is a " +
                schema_type_name(s.type) + " schema, not Avro; declare format='" +
                (s.type == SchemaType::Protobuf ? "protobuf" : "json-schema") + "'");
        }
        auto compiled = compile(s.schema, "schema id " + std::to_string(id));
        std::lock_guard lk(mu_);
        schemas_.emplace(id, compiled);
        return compiled;
    }

    std::shared_ptr<Client> client_;
    std::mutex mu_;
    std::unordered_map<std::int32_t, std::shared_ptr<avro::ValidSchema>> schemas_;
};

class AvroEncoder final : public ValueEncoder {
public:
    AvroEncoder(const FormatOptions& opts, std::shared_ptr<Client> client)
        : client_(std::move(client)) {
        if (opts.auto_register) {
            if (opts.columns.empty()) {
                throw std::runtime_error(
                    "avro sink: no declared columns to derive a schema from (schema_columns); set "
                    "schema_registry_auto_register='false' to use the subject's registered schema");
            }
            const std::string name =
                opts.record_name.empty() ? sanitise_name(opts.subject) : opts.record_name;
            const auto text = derive_avro_schema(opts.columns, name, opts.record_namespace);
            schema_ = compile(text, "derived schema for subject '" + opts.subject + "'");
            id_ = client_->register_schema(opts.subject, SchemaType::Avro, text);
        } else {
            const auto latest = client_->latest(opts.subject);
            if (latest.type != SchemaType::Avro) {
                throw std::runtime_error("avro sink: subject '" + opts.subject + "' holds a " +
                                         schema_type_name(latest.type) + " schema, not Avro");
            }
            schema_ =
                compile(latest.schema,
                        "subject '" + opts.subject + "' version " + std::to_string(latest.version));
            id_ = latest.id;
        }
        decimals_ = top_level_decimals(schema_->root());
    }

    std::string from_json(std::string_view json) override {
        JsonValue v;
        try {
            v = clink::config::parse(json);
        } catch (const std::exception& e) {
            throw std::runtime_error(std::string{"avro encode: row is not JSON: "} + e.what());
        }
        const auto raw = decimals_.empty() ? std::map<std::string, std::string>{}
                                           : clink::config::raw_number_tokens(json, decimals_);
        EncodeContext ctx;
        ctx.raw_tokens = &raw;
        avro::GenericDatum datum(*schema_);
        const auto& root = schema_->root();
        if (root->type() == avro::AVRO_RECORD || root->type() == avro::AVRO_UNION) {
            fill(datum, root, &v, "", ctx, 0);
        } else {
            // A non-record top-level schema takes the row's "value" field.
            const JsonValue* inner = &v;
            if (v.is_object()) {
                const auto it = v.as_object().find("value");
                inner = it == v.as_object().end() ? nullptr : &it->second;
            }
            fill(datum, root, inner, "value", ctx, 1);
        }
        std::string bytes;
        try {
            auto out = avro::memoryOutputStream();
            auto enc = avro::binaryEncoder();
            enc->init(*out);
            avro::encode(*enc, datum);
            enc->flush();
            const auto snap = avro::snapshot(*out);
            bytes.assign(snap->begin(), snap->end());
        } catch (const std::exception& e) {
            throw std::runtime_error(std::string{"avro encode: "} + e.what());
        }
        return frame(id_, bytes);
    }
    [[nodiscard]] std::int32_t schema_id() const noexcept override { return id_; }
    [[nodiscard]] Format format() const noexcept override { return Format::Avro; }

private:
    std::shared_ptr<Client> client_;
    std::shared_ptr<avro::ValidSchema> schema_;
    std::int32_t id_{-1};
    std::map<std::string, int> decimals_;
};

}  // namespace

std::unique_ptr<ValueDecoder> make_avro_decoder(const FormatOptions& /*opts*/,
                                                std::shared_ptr<Client> client) {
    return std::make_unique<AvroDecoder>(std::move(client));
}

std::unique_ptr<ValueEncoder> make_avro_encoder(const FormatOptions& opts,
                                                std::shared_ptr<Client> client) {
    return std::make_unique<AvroEncoder>(opts, std::move(client));
}

}  // namespace clink::schema_registry::detail
