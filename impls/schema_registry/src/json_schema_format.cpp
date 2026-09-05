// The JSON Schema format: the payload is JSON text already, so decoding is
// the header strip (after checking the id really names a JSON schema) and
// encoding is the header prefix. The encode side registers a schema derived
// from the declared columns and, when it did, re-serialises each row with only
// those columns so what reaches the topic is what the schema describes.

#include <mutex>
#include <stdexcept>
#include <unordered_set>

#include "clink/config/json.hpp"
#include "clink/schema_registry/schema_derivation.hpp"
#include "clink/schema_registry/wire_format.hpp"

#include "format_impls.hpp"

namespace clink::schema_registry::detail {
namespace {

class JsonSchemaDecoder final : public ValueDecoder {
public:
    explicit JsonSchemaDecoder(std::shared_ptr<Client> client) : client_(std::move(client)) {}

    std::string to_json(std::string_view framed) override {
        std::string err;
        const auto f = parse_frame(framed, /*with_message_indexes=*/false, &err);
        if (!f.has_value()) {
            throw std::runtime_error("json-schema decode: " + err);
        }
        check_type_(f->schema_id);
        return std::string(f->payload);
    }
    [[nodiscard]] Format format() const noexcept override { return Format::JsonSchema; }

private:
    // One registry round trip per distinct id: the payload needs no schema to
    // decode, but an id of the wrong type means the topic is not what the
    // table says it is, and that deserves a clear refusal.
    void check_type_(std::int32_t id) {
        {
            std::lock_guard lk(mu_);
            if (seen_.count(id) != 0) {
                return;
            }
        }
        const auto s = client_->schema_by_id(id);
        if (s.type != SchemaType::Json) {
            throw std::runtime_error("json-schema decode: schema id " + std::to_string(id) +
                                     " is a " + schema_type_name(s.type) +
                                     " schema, not JSON; declare format='" +
                                     (s.type == SchemaType::Avro ? "avro" : "protobuf") + "'");
        }
        std::lock_guard lk(mu_);
        seen_.insert(id);
    }

    std::shared_ptr<Client> client_;
    std::mutex mu_;
    std::unordered_set<std::int32_t> seen_;
};

class JsonSchemaEncoder final : public ValueEncoder {
public:
    JsonSchemaEncoder(const FormatOptions& opts, std::shared_ptr<Client> client)
        : client_(std::move(client)) {
        if (opts.auto_register) {
            if (opts.columns.empty()) {
                throw std::runtime_error(
                    "json-schema sink: no declared columns to derive a schema from "
                    "(schema_columns); set schema_registry_auto_register='false' to use the "
                    "subject's registered schema instead");
            }
            const std::string title =
                opts.record_name.empty() ? sanitise_name(opts.subject) : opts.record_name;
            const auto schema = derive_json_schema(opts.columns, title);
            id_ = client_->register_schema(opts.subject, SchemaType::Json, schema);
            columns_ = parse_columns(opts.columns);
            for (const auto& c : columns_) {
                if (const auto ps = decimal_precision_scale(c.code)) {
                    decimal_fields_[c.name] = ps->second;
                }
            }
        } else {
            const auto latest = client_->latest(opts.subject);
            if (latest.type != SchemaType::Json) {
                throw std::runtime_error("json-schema sink: subject '" + opts.subject +
                                         "' holds a " + schema_type_name(latest.type) +
                                         " schema, not JSON");
            }
            id_ = latest.id;
        }
    }

    std::string from_json(std::string_view json) override {
        if (columns_.empty()) {
            return frame(id_, json);  // registry-held schema: pass the row through as written
        }
        auto obj = clink::config::parse_object(json);
        if (!obj.has_value()) {
            throw std::runtime_error("json-schema sink: row is not a JSON object");
        }
        // Exact digits for decimal columns: the generic parse rounds a numeral
        // to a double, the raw token keeps what the row carried.
        const auto raw = decimal_fields_.empty()
                             ? std::map<std::string, std::string>{}
                             : clink::config::raw_number_tokens(json, decimal_fields_);
        std::string out;
        out.reserve(json.size());
        out.push_back('{');
        bool first = true;
        for (const auto& c : columns_) {
            const auto it = obj->find(c.name);
            if (it == obj->end()) {
                continue;
            }
            if (!first) {
                out.push_back(',');
            }
            first = false;
            clink::config::JsonValue{c.name}.serialize_into(out);
            out.push_back(':');
            if (const auto r = raw.find(c.name); r != raw.end()) {
                out += r->second;
            } else {
                it->second.serialize_into(out);
            }
        }
        out.push_back('}');
        return frame(id_, out);
    }
    [[nodiscard]] std::int32_t schema_id() const noexcept override { return id_; }
    [[nodiscard]] Format format() const noexcept override { return Format::JsonSchema; }

private:
    std::shared_ptr<Client> client_;
    std::int32_t id_{-1};
    std::vector<ColumnSpec> columns_;
    std::map<std::string, int> decimal_fields_;
};

}  // namespace

std::unique_ptr<ValueDecoder> make_json_schema_decoder(const FormatOptions& /*opts*/,
                                                       std::shared_ptr<Client> client) {
    return std::make_unique<JsonSchemaDecoder>(std::move(client));
}

std::unique_ptr<ValueEncoder> make_json_schema_encoder(const FormatOptions& opts,
                                                       std::shared_ptr<Client> client) {
    return std::make_unique<JsonSchemaEncoder>(opts, std::move(client));
}

}  // namespace clink::schema_registry::detail
