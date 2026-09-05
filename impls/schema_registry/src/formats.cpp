#include "clink/schema_registry/formats.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>

#include "format_impls.hpp"

namespace clink::schema_registry {

const char* format_name(Format f) noexcept {
    switch (f) {
        case Format::Avro:
            return "avro";
        case Format::Protobuf:
            return "protobuf";
        case Format::JsonSchema:
            return "json-schema";
    }
    return "avro";
}

std::optional<Format> parse_format(std::string_view text) {
    std::string lower(text);
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (lower == "avro") {
        return Format::Avro;
    }
    if (lower == "protobuf") {
        return Format::Protobuf;
    }
    if (lower == "json-schema" || lower == "json_schema" || lower == "jsonschema") {
        return Format::JsonSchema;
    }
    return std::nullopt;
}

bool format_compiled_in(Format f) noexcept {
    switch (f) {
        case Format::Avro:
#ifdef CLINK_SCHEMA_REGISTRY_HAS_AVRO
            return true;
#else
            return false;
#endif
        case Format::Protobuf:
#ifdef CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF
            return true;
#else
            return false;
#endif
        case Format::JsonSchema:
            return true;
    }
    return false;
}

std::vector<std::string> supported_format_names() {
    std::vector<std::string> out;
    for (const auto f : {Format::Avro, Format::Protobuf, Format::JsonSchema}) {
        if (format_compiled_in(f)) {
            out.emplace_back(format_name(f));
        }
    }
    return out;
}

SchemaType schema_type_for(Format f) noexcept {
    switch (f) {
        case Format::Avro:
            return SchemaType::Avro;
        case Format::Protobuf:
            return SchemaType::Protobuf;
        case Format::JsonSchema:
            return SchemaType::Json;
    }
    return SchemaType::Avro;
}

namespace {

[[noreturn]] void not_compiled_in(Format f) {
    const char* need = f == Format::Avro ? "avro-cpp (CLINK_WITH_AVRO)"
                                         : "libprotobuf + libprotoc (CLINK_WITH_PROTOBUF)";
    throw std::runtime_error(std::string{"format='"} + format_name(f) +
                             "' is not compiled into this clink build; it needs " + need);
}

}  // namespace

std::unique_ptr<ValueDecoder> make_decoder(const FormatOptions& opts,
                                           std::shared_ptr<Client> client) {
    if (!client) {
        throw std::runtime_error("make_decoder: a schema registry client is required");
    }
    switch (opts.format) {
        case Format::JsonSchema:
            return detail::make_json_schema_decoder(opts, std::move(client));
        case Format::Avro:
#ifdef CLINK_SCHEMA_REGISTRY_HAS_AVRO
            return detail::make_avro_decoder(opts, std::move(client));
#else
            not_compiled_in(opts.format);
#endif
        case Format::Protobuf:
#ifdef CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF
            return detail::make_protobuf_decoder(opts, std::move(client));
#else
            not_compiled_in(opts.format);
#endif
    }
    not_compiled_in(opts.format);
}

std::unique_ptr<ValueEncoder> make_encoder(const FormatOptions& opts,
                                           std::shared_ptr<Client> client) {
    if (!client) {
        throw std::runtime_error("make_encoder: a schema registry client is required");
    }
    if (opts.subject.empty()) {
        throw std::runtime_error(std::string{"format='"} + format_name(opts.format) +
                                 "' sink: a registry subject is required (schema_registry_subject, "
                                 "default '<topic>-value')");
    }
    switch (opts.format) {
        case Format::JsonSchema:
            return detail::make_json_schema_encoder(opts, std::move(client));
        case Format::Avro:
#ifdef CLINK_SCHEMA_REGISTRY_HAS_AVRO
            return detail::make_avro_encoder(opts, std::move(client));
#else
            not_compiled_in(opts.format);
#endif
        case Format::Protobuf:
#ifdef CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF
            return detail::make_protobuf_encoder(opts, std::move(client));
#else
            not_compiled_in(opts.format);
#endif
    }
    not_compiled_in(opts.format);
}

ParsedFormatOptions parse_format_options(const ParamLookup& param,
                                         const std::string& topic,
                                         const char* connector_name) {
    ParsedFormatOptions out;
    const std::string fmt = param("format", "");
    const auto f = parse_format(fmt);
    if (!f.has_value()) {
        return out;  // json / text / unset: not a registry format
    }
    const std::string who = connector_name == nullptr ? "connector" : connector_name;
    if (!format_compiled_in(*f)) {
        const char* need = *f == Format::Avro ? "avro-cpp (CLINK_WITH_AVRO)"
                                              : "libprotobuf + libprotoc (CLINK_WITH_PROTOBUF)";
        out.error =
            who + ": format='" + fmt + "' is not compiled into this clink build; it needs " + need;
        return out;
    }
    FormatOptions o;
    o.format = *f;
    o.client.url = param("schema_registry_url", "");
    if (o.client.url.empty()) {
        out.error = who + ": format='" + fmt +
                    "' is registry-framed and requires 'schema_registry_url' "
                    "(for example 'http://localhost:8081')";
        return out;
    }
    o.client.basic_auth = param("schema_registry_auth", "");
    o.client.bearer_token = param("schema_registry_token", "");
    o.client.verify_tls = param("schema_registry_verify_tls", "true") != "false";
    if (const auto t = param("schema_registry_timeout_ms", ""); !t.empty()) {
        try {
            const int ms = std::stoi(t);
            if (ms <= 0) {
                throw std::invalid_argument("non-positive");
            }
            o.client.rw_timeout_ms = ms;
            o.client.connect_timeout_ms = std::min(ms, 5000);
        } catch (const std::exception&) {
            out.error =
                who + ": schema_registry_timeout_ms must be a positive integer (got '" + t + "')";
            return out;
        }
    }
    o.subject = param("schema_registry_subject", topic.empty() ? "" : topic + "-value");
    o.auto_register = param("schema_registry_auto_register", "true") != "false";
    o.columns = param("schema_columns", "");
    o.record_name = param("schema_registry_record_name", "");
    o.record_namespace = param("schema_registry_namespace", "clink");
    o.message_name = param("schema_registry_message", "");
    out.options = std::move(o);
    return out;
}

namespace detail {

std::vector<ColumnSpec> parse_columns(const std::string& columns) {
    std::vector<ColumnSpec> out;
    std::size_t pos = 0;
    while (pos < columns.size()) {
        auto semi = columns.find(';', pos);
        if (semi == std::string::npos) {
            semi = columns.size();
        }
        const std::string entry = columns.substr(pos, semi - pos);
        const auto colon = entry.rfind(':');
        if (colon != std::string::npos && colon > 0) {
            out.push_back(ColumnSpec{entry.substr(0, colon), entry.substr(colon + 1)});
        }
        pos = semi + 1;
    }
    return out;
}

std::optional<std::pair<int, int>> decimal_precision_scale(const std::string& code) {
    if (code.rfind("dec_", 0) != 0) {
        return std::nullopt;
    }
    const auto rest = code.substr(4);
    const auto us = rest.find('_');
    if (us == std::string::npos) {
        return std::nullopt;
    }
    try {
        return std::make_pair(std::stoi(rest.substr(0, us)), std::stoi(rest.substr(us + 1)));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

}  // namespace detail

}  // namespace clink::schema_registry
