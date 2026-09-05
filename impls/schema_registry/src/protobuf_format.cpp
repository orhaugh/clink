// The Protobuf format: registry-framed Protobuf binary <-> JSON object text.
// The .proto text the frame's id names is parsed with the protobuf compiler
// library into a descriptor pool (references resolved by fetching each
// referenced subject version), the frame's message indexes select the
// message, and a dynamic message is walked through the reflection API in
// both directions. Reflection rather than the JSON utility so int64 stays a
// JSON integer, field names stay as declared, and the mapping is the same on
// every libprotobuf version the pinned toolchains carry (3.21 in the image,
// the current release on the host).

#include <cmath>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <unordered_map>

#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>

#include "clink/config/json.hpp"
#include "clink/core/base64.hpp"
#include "clink/schema_registry/schema_derivation.hpp"
#include "clink/schema_registry/wire_format.hpp"

#include "format_impls.hpp"
#include "value_conversions.hpp"

#if GOOGLE_PROTOBUF_VERSION >= 4022000
#include <absl/strings/string_view.h>
#endif

namespace clink::schema_registry::detail {
namespace {

namespace pb = google::protobuf;
using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;

class ParseErrors final : public pb::io::ErrorCollector {
public:
#if GOOGLE_PROTOBUF_VERSION >= 4022000
    void RecordError(int line, pb::io::ColumnNumber column, absl::string_view message) override {
        add_(line, column, std::string(message));
    }
#else
    void AddError(int line, pb::io::ColumnNumber column, const std::string& message) override {
        add_(line, column, message);
    }
#endif
    [[nodiscard]] const std::string& text() const noexcept { return text_; }
    [[nodiscard]] bool failed() const noexcept { return !text_.empty(); }

private:
    void add_(int line, int column, const std::string& message) {
        if (!text_.empty()) {
            text_ += "; ";
        }
        text_ +=
            "line " + std::to_string(line + 1) + ":" + std::to_string(column + 1) + ": " + message;
    }
    std::string text_;
};

// One registered Protobuf schema, compiled: its own pool (layered over the
// well-known types in the generated pool) and a factory for its messages.
struct CompiledSchema {
    std::unique_ptr<pb::DescriptorPool> pool;
    std::unique_ptr<pb::DynamicMessageFactory> factory;
    const pb::FileDescriptor* file{nullptr};
};

pb::FileDescriptorProto parse_proto(const std::string& text, const std::string& file_name) {
    pb::io::ArrayInputStream input(text.data(), static_cast<int>(text.size()));
    ParseErrors errors;
    pb::io::Tokenizer tokenizer(&input, &errors);
    pb::compiler::Parser parser;
    parser.RecordErrorsTo(&errors);
    pb::FileDescriptorProto fdp;
    if (!parser.Parse(&tokenizer, &fdp) || errors.failed()) {
        throw std::runtime_error("protobuf: schema '" + file_name +
                                 "' does not parse: " + errors.text());
    }
    fdp.set_name(file_name);
    return fdp;
}

// Build `schema` (and, first, every reference it imports) into `pool`.
const pb::FileDescriptor* build_into(pb::DescriptorPool& pool,
                                     Client& client,
                                     const RegisteredSchema& schema,
                                     const std::string& file_name,
                                     std::set<std::string>& building) {
    if (const auto* existing = pool.FindFileByName(file_name); existing != nullptr) {
        return existing;
    }
    if (!building.insert(file_name).second) {
        throw std::runtime_error("protobuf: schema references form a cycle at '" + file_name + "'");
    }
    auto fdp = parse_proto(schema.schema, file_name);
    for (int i = 0; i < fdp.dependency_size(); ++i) {
        const std::string dep = fdp.dependency(i);
        if (pool.FindFileByName(dep) != nullptr) {
            continue;  // a well-known type, or already built
        }
        const SchemaReference* ref = nullptr;
        for (const auto& r : schema.references) {
            if (r.name == dep) {
                ref = &r;
                break;
            }
        }
        if (ref == nullptr) {
            throw std::runtime_error(
                "protobuf: schema '" + file_name + "' imports '" + dep +
                "', which is neither a well-known type nor one of its registry references");
        }
        const auto referenced = ref->version > 0 ? client.version(ref->subject, ref->version)
                                                 : client.latest(ref->subject);
        if (referenced.type != SchemaType::Protobuf) {
            throw std::runtime_error("protobuf: reference '" + dep + "' (subject '" + ref->subject +
                                     "') is not a Protobuf schema");
        }
        build_into(pool, client, referenced, dep, building);
    }
    const auto* built = pool.BuildFile(fdp);
    if (built == nullptr) {
        throw std::runtime_error("protobuf: schema '" + file_name +
                                 "' does not build (an unresolved type or a duplicate name)");
    }
    building.erase(file_name);
    return built;
}

std::shared_ptr<CompiledSchema> compile(Client& client,
                                        const RegisteredSchema& schema,
                                        const std::string& file_name) {
    auto compiled = std::make_shared<CompiledSchema>();
    compiled->pool = std::make_unique<pb::DescriptorPool>(pb::DescriptorPool::generated_pool());
    std::set<std::string> building;
    compiled->file = build_into(*compiled->pool, client, schema, file_name, building);
    compiled->factory = std::make_unique<pb::DynamicMessageFactory>(compiled->pool.get());
    return compiled;
}

const pb::Descriptor* message_at(const pb::FileDescriptor& file,
                                 const std::vector<std::int32_t>& indexes) {
    if (indexes.empty() || indexes[0] < 0 || indexes[0] >= file.message_type_count()) {
        throw std::runtime_error("protobuf decode: message index out of range for schema '" +
                                 std::string(file.name()) + "'");
    }
    const pb::Descriptor* d = file.message_type(indexes[0]);
    for (std::size_t i = 1; i < indexes.size(); ++i) {
        if (indexes[i] < 0 || indexes[i] >= d->nested_type_count()) {
            throw std::runtime_error("protobuf decode: nested message index out of range in '" +
                                     std::string(d->full_name()) + "'");
        }
        d = d->nested_type(indexes[i]);
    }
    return d;
}

// "Outer.Inner" -> the index path the frame carries.
std::vector<std::int32_t> indexes_of(const pb::FileDescriptor& file, const std::string& dotted) {
    std::vector<std::int32_t> out;
    std::size_t pos = 0;
    const pb::Descriptor* current = nullptr;
    while (pos <= dotted.size()) {
        auto dot = dotted.find('.', pos);
        if (dot == std::string::npos) {
            dot = dotted.size();
        }
        const std::string part = dotted.substr(pos, dot - pos);
        bool found = false;
        if (current == nullptr) {
            for (int i = 0; i < file.message_type_count(); ++i) {
                if (std::string(file.message_type(i)->name()) == part) {
                    out.push_back(i);
                    current = file.message_type(i);
                    found = true;
                    break;
                }
            }
        } else {
            for (int i = 0; i < current->nested_type_count(); ++i) {
                if (std::string(current->nested_type(i)->name()) == part) {
                    out.push_back(i);
                    current = current->nested_type(i);
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            throw std::runtime_error("protobuf sink: message '" + dotted +
                                     "' is not defined in the schema");
        }
        pos = dot + 1;
    }
    return out;
}

bool is_timestamp(const pb::Descriptor& d) {
    return std::string(d.full_name()) == "google.protobuf.Timestamp";
}

// ---- message -> JSON -----------------------------------------------------

JsonValue to_json(const pb::Message& msg);

JsonValue timestamp_millis(const pb::Message& ts) {
    const auto* r = ts.GetReflection();
    const auto* d = ts.GetDescriptor();
    const auto secs = r->GetInt64(ts, d->FindFieldByName("seconds"));
    const auto nanos = r->GetInt32(ts, d->FindFieldByName("nanos"));
    return JsonValue{secs * 1000 + nanos / 1000000};
}

JsonValue scalar_to_json(const pb::Message& msg,
                         const pb::Reflection& r,
                         const pb::FieldDescriptor& f,
                         int index) {
    const bool rep = f.is_repeated();
    switch (f.cpp_type()) {
        case pb::FieldDescriptor::CPPTYPE_INT32:
            return JsonValue{static_cast<std::int64_t>(rep ? r.GetRepeatedInt32(msg, &f, index)
                                                           : r.GetInt32(msg, &f))};
        case pb::FieldDescriptor::CPPTYPE_INT64:
            return JsonValue{rep ? r.GetRepeatedInt64(msg, &f, index) : r.GetInt64(msg, &f)};
        case pb::FieldDescriptor::CPPTYPE_UINT32:
            return JsonValue{static_cast<std::int64_t>(rep ? r.GetRepeatedUInt32(msg, &f, index)
                                                           : r.GetUInt32(msg, &f))};
        case pb::FieldDescriptor::CPPTYPE_UINT64: {
            const auto u = rep ? r.GetRepeatedUInt64(msg, &f, index) : r.GetUInt64(msg, &f);
            if (u > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
                return JsonValue{
                    std::to_string(u)};  // beyond JSON's integer range here: keep the digits
            }
            return JsonValue{static_cast<std::int64_t>(u)};
        }
        case pb::FieldDescriptor::CPPTYPE_DOUBLE: {
            const double v = rep ? r.GetRepeatedDouble(msg, &f, index) : r.GetDouble(msg, &f);
            return std::isfinite(v) ? JsonValue{v} : JsonValue{};
        }
        case pb::FieldDescriptor::CPPTYPE_FLOAT: {
            const double v = rep ? r.GetRepeatedFloat(msg, &f, index) : r.GetFloat(msg, &f);
            return std::isfinite(v) ? JsonValue{v} : JsonValue{};
        }
        case pb::FieldDescriptor::CPPTYPE_BOOL:
            return JsonValue{rep ? r.GetRepeatedBool(msg, &f, index) : r.GetBool(msg, &f)};
        case pb::FieldDescriptor::CPPTYPE_ENUM: {
            const auto* ev = rep ? r.GetRepeatedEnum(msg, &f, index) : r.GetEnum(msg, &f);
            return ev != nullptr ? JsonValue{std::string(ev->name())} : JsonValue{};
        }
        case pb::FieldDescriptor::CPPTYPE_STRING: {
            std::string s = rep ? r.GetRepeatedString(msg, &f, index) : r.GetString(msg, &f);
            if (f.type() == pb::FieldDescriptor::TYPE_BYTES) {
                return JsonValue{clink::base64_encode(s)};
            }
            return JsonValue{std::move(s)};
        }
        case pb::FieldDescriptor::CPPTYPE_MESSAGE: {
            const auto& sub = rep ? r.GetRepeatedMessage(msg, &f, index) : r.GetMessage(msg, &f);
            if (is_timestamp(*sub.GetDescriptor())) {
                return timestamp_millis(sub);
            }
            return to_json(sub);
        }
    }
    return JsonValue{};
}

JsonValue to_json(const pb::Message& msg) {
    const auto* d = msg.GetDescriptor();
    const auto* r = msg.GetReflection();
    JsonObject obj;
    for (int i = 0; i < d->field_count(); ++i) {
        const auto* f = d->field(i);
        const std::string name(f->name());
        if (f->is_map()) {
            JsonObject m;
            const auto* entry = f->message_type();
            const auto* kf = entry->map_key();
            const auto* vf = entry->map_value();
            const int n = r->FieldSize(msg, f);
            for (int k = 0; k < n; ++k) {
                const auto& e = r->GetRepeatedMessage(msg, f, k);
                const auto key = scalar_to_json(e, *e.GetReflection(), *kf, 0);
                const std::string key_text = key.is_string() ? key.as_string() : key.serialize();
                m.emplace(key_text, scalar_to_json(e, *e.GetReflection(), *vf, 0));
            }
            obj.emplace(name, JsonValue{std::move(m)});
            continue;
        }
        if (f->is_repeated()) {
            JsonArray arr;
            const int n = r->FieldSize(msg, f);
            for (int k = 0; k < n; ++k) {
                arr.push_back(scalar_to_json(msg, *r, *f, k));
            }
            obj.emplace(name, JsonValue{std::move(arr)});
            continue;
        }
        if (f->has_presence() && !r->HasField(msg, f)) {
            continue;  // unset optional / message / oneof member: omitted
        }
        obj.emplace(name, scalar_to_json(msg, *r, *f, 0));
    }
    return JsonValue{std::move(obj)};
}

// ---- JSON -> message -----------------------------------------------------

[[noreturn]] void bad(const std::string& path, const std::string& what) {
    throw std::runtime_error("protobuf encode: field '" + path + "': " + what);
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
        try {
            std::size_t used = 0;
            const auto out = std::stoll(v.as_string(), &used);
            if (used == v.as_string().size()) {
                return out;
            }
        } catch (const std::exception&) {
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

// `raw_tokens`: exact numeral text of the row's top-level fields (see
// raw_number_tokens), consulted for a number landing in a string field so a
// decimal column keeps every digit; only at the top level (path == field name).
void from_json(pb::Message& msg,
               const JsonValue& v,
               const std::string& path,
               const std::map<std::string, std::string>* raw_tokens);

void set_timestamp(pb::Message& ts, const JsonValue& v, const std::string& path) {
    std::int64_t micros = 0;
    if (v.is_string()) {
        const auto us = parse_timestamp_micros(v.as_string());
        if (!us.has_value()) {
            bad(path, "expected an ISO-8601 timestamp or epoch milliseconds, got " + v.serialize());
        }
        micros = *us;
    } else if (v.is_object()) {
        from_json(ts, v, path, nullptr);  // {"seconds":..,"nanos":..} as written
        return;
    } else {
        micros = integer_of(v, path) * 1000;  // epoch milliseconds, the decode side's convention
    }
    const auto* r = ts.GetReflection();
    const auto* d = ts.GetDescriptor();
    const std::int64_t secs = floor_div(micros, 1000000);
    r->SetInt64(&ts, d->FindFieldByName("seconds"), secs);
    r->SetInt32(&ts,
                d->FindFieldByName("nanos"),
                static_cast<std::int32_t>((micros - secs * 1000000) * 1000));
}

// Assign one scalar (or message) value; `add` selects the repeated adder.
void set_scalar(pb::Message& msg,
                const pb::Reflection& r,
                const pb::FieldDescriptor& f,
                const JsonValue& v,
                const std::string& path,
                bool add,
                const std::string* raw_token = nullptr) {
    switch (f.cpp_type()) {
        case pb::FieldDescriptor::CPPTYPE_INT32: {
            const auto x = integer_of(v, path);
            if (x < std::numeric_limits<std::int32_t>::min() ||
                x > std::numeric_limits<std::int32_t>::max()) {
                bad(path, "out of int32 range");
            }
            add ? r.AddInt32(&msg, &f, static_cast<std::int32_t>(x))
                : r.SetInt32(&msg, &f, static_cast<std::int32_t>(x));
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_INT64: {
            const auto x = integer_of(v, path);
            add ? r.AddInt64(&msg, &f, x) : r.SetInt64(&msg, &f, x);
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_UINT32: {
            const auto x = integer_of(v, path);
            if (x < 0 || x > std::numeric_limits<std::uint32_t>::max()) {
                bad(path, "out of uint32 range");
            }
            add ? r.AddUInt32(&msg, &f, static_cast<std::uint32_t>(x))
                : r.SetUInt32(&msg, &f, static_cast<std::uint32_t>(x));
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_UINT64: {
            std::uint64_t x = 0;
            if (v.is_string()) {
                try {
                    x = std::stoull(v.as_string());
                } catch (const std::exception&) {
                    bad(path, "expected an unsigned integer, got " + v.serialize());
                }
            } else {
                const auto s = integer_of(v, path);
                if (s < 0) {
                    bad(path, "negative value for uint64");
                }
                x = static_cast<std::uint64_t>(s);
            }
            add ? r.AddUInt64(&msg, &f, x) : r.SetUInt64(&msg, &f, x);
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_DOUBLE: {
            const double x = number_of(v, path);
            add ? r.AddDouble(&msg, &f, x) : r.SetDouble(&msg, &f, x);
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_FLOAT: {
            const auto x = static_cast<float>(number_of(v, path));
            add ? r.AddFloat(&msg, &f, x) : r.SetFloat(&msg, &f, x);
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_BOOL: {
            if (!v.is_bool()) {
                bad(path, "expected a boolean, got " + v.serialize());
            }
            add ? r.AddBool(&msg, &f, v.as_bool()) : r.SetBool(&msg, &f, v.as_bool());
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_ENUM: {
            const pb::EnumValueDescriptor* ev = nullptr;
            if (v.is_string()) {
                ev = f.enum_type()->FindValueByName(v.as_string());
            } else if (v.is_number()) {
                ev = f.enum_type()->FindValueByNumber(static_cast<int>(integer_of(v, path)));
            }
            if (ev == nullptr) {
                bad(path,
                    v.serialize() + " is not a value of enum " +
                        std::string(f.enum_type()->full_name()));
            }
            add ? r.AddEnum(&msg, &f, ev) : r.SetEnum(&msg, &f, ev);
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_STRING: {
            std::string s;
            if (f.type() == pb::FieldDescriptor::TYPE_BYTES) {
                if (!v.is_string()) {
                    bad(path, "expected a base64 string for bytes, got " + v.serialize());
                }
                const auto decoded = clink::base64_decode(v.as_string());
                if (!decoded.has_value()) {
                    bad(path, "bytes value is not valid base64");
                }
                s = *decoded;
            } else if (v.is_string()) {
                s = v.as_string();
            } else if (v.is_number() && raw_token != nullptr) {
                s = *raw_token;  // a decimal column into a string field: the row's exact digits
            } else if (v.is_number() || v.is_bool()) {
                s = v.serialize();
            } else {
                bad(path, "expected a string, got " + v.serialize());
            }
            add ? r.AddString(&msg, &f, s) : r.SetString(&msg, &f, s);
            return;
        }
        case pb::FieldDescriptor::CPPTYPE_MESSAGE: {
            pb::Message* sub = add ? r.AddMessage(&msg, &f) : r.MutableMessage(&msg, &f);
            if (is_timestamp(*sub->GetDescriptor())) {
                set_timestamp(*sub, v, path);
            } else {
                from_json(*sub, v, path, nullptr);
            }
            return;
        }
    }
}

void from_json(pb::Message& msg,
               const JsonValue& v,
               const std::string& path,
               const std::map<std::string, std::string>* raw_tokens) {
    if (!v.is_object()) {
        bad(path.empty() ? "<root>" : path, "expected an object, got " + v.serialize());
    }
    const auto* d = msg.GetDescriptor();
    const auto* r = msg.GetReflection();
    const auto& obj = v.as_object();
    for (int i = 0; i < d->field_count(); ++i) {
        const auto* f = d->field(i);
        const std::string name(f->name());
        auto it = obj.find(name);
        if (it == obj.end()) {
            it = obj.find(std::string(f->json_name()));
        }
        if (it == obj.end() || it->second.is_null()) {
            continue;  // absent or null: the field's default (proto3) / unset (presence)
        }
        const std::string fpath = path.empty() ? name : path + "." + name;
        const auto& val = it->second;
        if (f->is_map()) {
            if (!val.is_object()) {
                bad(fpath, "expected an object for a map, got " + val.serialize());
            }
            const auto* entry = f->message_type();
            for (const auto& [k, mv] : val.as_object()) {
                pb::Message* e = r->AddMessage(&msg, f);
                set_scalar(*e,
                           *e->GetReflection(),
                           *entry->map_key(),
                           JsonValue{std::string(k)},
                           fpath + "." + std::string(k),
                           false);
                set_scalar(*e,
                           *e->GetReflection(),
                           *entry->map_value(),
                           mv,
                           fpath + "." + std::string(k),
                           false);
            }
            continue;
        }
        if (f->is_repeated()) {
            if (!val.is_array()) {
                bad(fpath, "expected an array, got " + val.serialize());
            }
            std::size_t idx = 0;
            for (const auto& e : val.as_array()) {
                set_scalar(msg, *r, *f, e, fpath + "[" + std::to_string(idx++) + "]", true);
            }
            continue;
        }
        const std::string* raw = nullptr;
        if (raw_tokens != nullptr && path.empty()) {
            if (const auto rt = raw_tokens->find(name); rt != raw_tokens->end()) {
                raw = &rt->second;
            }
        }
        set_scalar(msg, *r, *f, val, fpath, false, raw);
    }
}

class ProtobufDecoder final : public ValueDecoder {
public:
    explicit ProtobufDecoder(std::shared_ptr<Client> client) : client_(std::move(client)) {}

    std::string to_json(std::string_view framed) override {
        std::string err;
        const auto f = parse_frame(framed, /*with_message_indexes=*/true, &err);
        if (!f.has_value()) {
            throw std::runtime_error("protobuf decode: " + err);
        }
        const auto compiled = schema_for_(f->schema_id);
        const auto* desc = message_at(*compiled->file, f->message_indexes);
        std::unique_ptr<pb::Message> msg(compiled->factory->GetPrototype(desc)->New());
        if (!msg->ParseFromArray(f->payload.data(), static_cast<int>(f->payload.size()))) {
            throw std::runtime_error("protobuf decode: payload is not a valid " +
                                     std::string(desc->full_name()) + " (schema id " +
                                     std::to_string(f->schema_id) + ")");
        }
        return detail::to_json(*msg).serialize();
    }
    [[nodiscard]] Format format() const noexcept override { return Format::Protobuf; }

private:
    std::shared_ptr<CompiledSchema> schema_for_(std::int32_t id) {
        {
            std::lock_guard lk(mu_);
            if (const auto it = schemas_.find(id); it != schemas_.end()) {
                return it->second;
            }
        }
        const auto s = client_->schema_by_id(id);
        if (s.type != SchemaType::Protobuf) {
            throw std::runtime_error("protobuf decode: schema id " + std::to_string(id) + " is a " +
                                     schema_type_name(s.type) +
                                     " schema, not Protobuf; declare format='" +
                                     (s.type == SchemaType::Avro ? "avro" : "json-schema") + "'");
        }
        auto compiled = compile(*client_, s, "schema-" + std::to_string(id) + ".proto");
        std::lock_guard lk(mu_);
        schemas_.emplace(id, compiled);
        return compiled;
    }

    std::shared_ptr<Client> client_;
    std::mutex mu_;
    std::unordered_map<std::int32_t, std::shared_ptr<CompiledSchema>> schemas_;
};

class ProtobufEncoder final : public ValueEncoder {
public:
    ProtobufEncoder(const FormatOptions& opts, std::shared_ptr<Client> client)
        : client_(std::move(client)) {
        RegisteredSchema schema;
        if (opts.auto_register) {
            if (opts.columns.empty()) {
                throw std::runtime_error(
                    "protobuf sink: no declared columns to derive a schema from (schema_columns); "
                    "set "
                    "schema_registry_auto_register='false' to use the subject's registered schema");
            }
            const std::string name =
                opts.record_name.empty() ? sanitise_name(opts.subject) : opts.record_name;
            schema.type = SchemaType::Protobuf;
            schema.schema = derive_protobuf_schema(opts.columns, name);
            schema.id = client_->register_schema(opts.subject, SchemaType::Protobuf, schema.schema);
        } else {
            schema = client_->latest(opts.subject);
            if (schema.type != SchemaType::Protobuf) {
                throw std::runtime_error("protobuf sink: subject '" + opts.subject + "' holds a " +
                                         schema_type_name(schema.type) + " schema, not Protobuf");
            }
        }
        id_ = schema.id;
        compiled_ = compile(*client_, schema, sanitise_name(opts.subject) + ".proto");
        if (compiled_->file->message_type_count() == 0) {
            throw std::runtime_error("protobuf sink: subject '" + opts.subject +
                                     "' defines no message");
        }
        indexes_ = opts.message_name.empty() ? std::vector<std::int32_t>{0}
                                             : indexes_of(*compiled_->file, opts.message_name);
        descriptor_ = message_at(*compiled_->file, indexes_);
        for (int i = 0; i < descriptor_->field_count(); ++i) {
            const auto* f = descriptor_->field(i);
            if (!f->is_repeated() && f->cpp_type() == pb::FieldDescriptor::CPPTYPE_STRING &&
                f->type() != pb::FieldDescriptor::TYPE_BYTES) {
                string_fields_[std::string(f->name())] = 0;
            }
        }
    }

    std::string from_json(std::string_view json) override {
        JsonValue v;
        try {
            v = clink::config::parse(json);
        } catch (const std::exception& e) {
            throw std::runtime_error(std::string{"protobuf encode: row is not JSON: "} + e.what());
        }
        std::unique_ptr<pb::Message> msg(compiled_->factory->GetPrototype(descriptor_)->New());
        const auto raw = string_fields_.empty()
                             ? std::map<std::string, std::string>{}
                             : clink::config::raw_number_tokens(json, string_fields_);
        detail::from_json(*msg, v, "", &raw);
        std::string bytes;
        if (!msg->SerializeToString(&bytes)) {
            throw std::runtime_error("protobuf encode: serialisation failed");
        }
        return frame(id_, indexes_, bytes);
    }
    [[nodiscard]] std::int32_t schema_id() const noexcept override { return id_; }
    [[nodiscard]] Format format() const noexcept override { return Format::Protobuf; }

private:
    std::shared_ptr<Client> client_;
    std::shared_ptr<CompiledSchema> compiled_;
    const pb::Descriptor* descriptor_{nullptr};
    std::vector<std::int32_t> indexes_;
    std::int32_t id_{-1};
    std::map<std::string, int> string_fields_;  // top-level string fields, for raw_number_tokens
};

}  // namespace

std::unique_ptr<ValueDecoder> make_protobuf_decoder(const FormatOptions& /*opts*/,
                                                    std::shared_ptr<Client> client) {
    return std::make_unique<ProtobufDecoder>(std::move(client));
}

std::unique_ptr<ValueEncoder> make_protobuf_encoder(const FormatOptions& opts,
                                                    std::shared_ptr<Client> client) {
    return std::make_unique<ProtobufEncoder>(opts, std::move(client));
}

}  // namespace clink::schema_registry::detail
