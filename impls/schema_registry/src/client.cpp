#include "clink/schema_registry/client.hpp"

#include <algorithm>
#include <cctype>
#include <mutex>
#include <unordered_map>

#include "clink/config/json.hpp"
#include "clink/core/base64.hpp"
#include "clink/http_connector/http_request.hpp"

namespace clink::schema_registry {

const char* schema_type_name(SchemaType type) noexcept {
    switch (type) {
        case SchemaType::Avro:
            return "AVRO";
        case SchemaType::Protobuf:
            return "PROTOBUF";
        case SchemaType::Json:
            return "JSON";
    }
    return "AVRO";
}

std::optional<SchemaType> parse_schema_type(std::string_view text) {
    std::string upper(text);
    std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    if (upper.empty() || upper == "AVRO") {
        return SchemaType::Avro;
    }
    if (upper == "PROTOBUF") {
        return SchemaType::Protobuf;
    }
    if (upper == "JSON") {
        return SchemaType::Json;
    }
    return std::nullopt;
}

RegistryError::RegistryError(int http_status, int registry_code, const std::string& what)
    : std::runtime_error(what), http_status_(http_status), registry_code_(registry_code) {}

namespace {

constexpr const char* kContentType = "application/vnd.schemaregistry.v1+json";

std::string percent_encode(std::string_view s) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (const unsigned char c : s) {
        if (std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.' || c == '~') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0xf]);
        }
    }
    return out;
}

// scheme://[user:pass@]host[:port][/prefix] -> (base_url, path prefix, userinfo)
struct SplitUrl {
    std::string base;
    std::string prefix;
    std::string userinfo;
};

SplitUrl split_url(const std::string& url) {
    SplitUrl s;
    const auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        throw RegistryError(
            0, 0, "schema registry url must start with http:// or https://: '" + url + "'");
    }
    std::string rest = url.substr(scheme_end + 3);
    std::string scheme = url.substr(0, scheme_end);
    const auto slash = rest.find('/');
    std::string authority = slash == std::string::npos ? rest : rest.substr(0, slash);
    std::string prefix = slash == std::string::npos ? "" : rest.substr(slash);
    while (!prefix.empty() && prefix.back() == '/') {
        prefix.pop_back();
    }
    const auto at = authority.rfind('@');
    if (at != std::string::npos) {
        s.userinfo = authority.substr(0, at);
        authority = authority.substr(at + 1);
    }
    if (authority.empty()) {
        throw RegistryError(0, 0, "schema registry url has no host: '" + url + "'");
    }
    s.base = scheme + "://" + authority;
    s.prefix = prefix;
    return s;
}

std::vector<SchemaReference> references_from(const clink::config::JsonValue& v) {
    std::vector<SchemaReference> refs;
    if (!v.is_object() || !v.contains("references") || !v.at("references").is_array()) {
        return refs;
    }
    for (const auto& r : v.at("references").as_array()) {
        if (!r.is_object()) {
            continue;
        }
        SchemaReference ref;
        ref.name = r.string_or("name", "");
        ref.subject = r.string_or("subject", "");
        ref.version = static_cast<int>(r.int_or("version", -1));
        refs.push_back(std::move(ref));
    }
    return refs;
}

clink::config::JsonValue references_to(const std::vector<SchemaReference>& refs) {
    clink::config::JsonArray arr;
    for (const auto& r : refs) {
        clink::config::JsonObject o;
        o.emplace("name", clink::config::JsonValue{r.name});
        o.emplace("subject", clink::config::JsonValue{r.subject});
        o.emplace("version", clink::config::JsonValue{static_cast<std::int64_t>(r.version)});
        arr.emplace_back(std::move(o));
    }
    return clink::config::JsonValue{std::move(arr)};
}

}  // namespace

struct Client::Impl {
    ClientOptions opts;
    std::string prefix;
    clink::http_connector::HttpRequest http;
    std::mutex mu;
    std::unordered_map<std::int32_t, RegisteredSchema> by_id;

    explicit Impl(ClientOptions o, SplitUrl split, clink::http_connector::HttpRequest::Options ho)
        : opts(std::move(o)), prefix(std::move(split.prefix)), http(std::move(ho)) {}

    [[nodiscard]] std::string describe() const { return opts.url; }

    // Turn a non-2xx / transport failure into a RegistryError carrying the
    // registry's own error code and message when the body has them.
    [[noreturn]] void fail(const std::string& what,
                           const clink::http_connector::HttpResponse& r) const {
        if (r.status == 0) {
            throw RegistryError(
                0, 0, "schema registry " + describe() + ": " + what + ": " + r.error);
        }
        int code = 0;
        std::string message;
        try {
            const auto body = clink::config::parse(r.body);
            if (body.is_object()) {
                code = static_cast<int>(body.int_or("error_code", 0));
                message = body.string_or("message", "");
            }
        } catch (const std::exception&) {
            // Not JSON: keep the raw body as the message.
        }
        if (message.empty()) {
            message = r.body.substr(0, 200);
        }
        throw RegistryError(r.status,
                            code,
                            "schema registry " + describe() + ": " + what + ": HTTP " +
                                std::to_string(r.status) +
                                (code != 0 ? " (error_code " + std::to_string(code) + ")" : "") +
                                (message.empty() ? "" : ": " + message));
    }

    clink::config::JsonValue get_json(const std::string& path, const std::string& what) {
        const auto r = http.get(prefix + path);
        if (r.status < 200 || r.status >= 300) {
            fail(what, r);
        }
        try {
            return clink::config::parse(r.body);
        } catch (const std::exception& e) {
            throw RegistryError(r.status,
                                0,
                                "schema registry " + describe() + ": " + what +
                                    ": response is not JSON: " + e.what());
        }
    }

    RegisteredSchema schema_from(const clink::config::JsonValue& v,
                                 const std::string& what,
                                 std::int32_t fallback_id) const {
        if (!v.is_object() || !v.contains("schema") || !v.at("schema").is_string()) {
            throw RegistryError(200,
                                0,
                                "schema registry " + describe() + ": " + what +
                                    ": response has no 'schema' string");
        }
        RegisteredSchema s;
        s.id = static_cast<std::int32_t>(v.int_or("id", fallback_id));
        s.version = static_cast<int>(v.int_or("version", -1));
        s.subject = v.string_or("subject", "");
        const auto type = parse_schema_type(v.string_or("schemaType", ""));
        if (!type.has_value()) {
            throw RegistryError(200,
                                0,
                                "schema registry " + describe() + ": " + what +
                                    ": unknown schemaType '" + v.string_or("schemaType", "") + "'");
        }
        s.type = *type;
        s.schema = v.at("schema").as_string();
        s.references = references_from(v);
        return s;
    }

    static std::string body_for(SchemaType type,
                                const std::string& schema,
                                const std::vector<SchemaReference>& references) {
        clink::config::JsonObject o;
        o.emplace("schema", clink::config::JsonValue{schema});
        // Avro is the registry's default and older registries reject an
        // explicit schemaType they do not know, so only name the others.
        if (type != SchemaType::Avro) {
            o.emplace("schemaType", clink::config::JsonValue{std::string{schema_type_name(type)}});
        }
        if (!references.empty()) {
            o.emplace("references", references_to(references));
        }
        return clink::config::JsonValue{std::move(o)}.serialize();
    }
};

Client::Client(ClientOptions opts) {
    auto split = split_url(opts.url);
    clink::http_connector::HttpRequest::Options ho;
    ho.base_url = split.base;
    ho.connect_timeout_ms = opts.connect_timeout_ms;
    ho.rw_timeout_ms = opts.rw_timeout_ms;
    ho.verify_tls = opts.verify_tls;
    ho.headers["Accept"] = std::string{kContentType} + ", application/json";
    std::string basic = opts.basic_auth.empty() ? split.userinfo : opts.basic_auth;
    if (!basic.empty()) {
        ho.headers["Authorization"] = "Basic " + clink::base64_encode(basic);
    } else if (!opts.bearer_token.empty()) {
        ho.headers["Authorization"] = "Bearer " + opts.bearer_token;
    }
    impl_ = std::make_unique<Impl>(std::move(opts), std::move(split), std::move(ho));
}

Client::~Client() = default;

const ClientOptions& Client::options() const noexcept {
    return impl_->opts;
}

RegisteredSchema Client::schema_by_id(std::int32_t id) {
    {
        std::lock_guard lk(impl_->mu);
        if (auto it = impl_->by_id.find(id); it != impl_->by_id.end()) {
            return it->second;
        }
    }
    const std::string what = "fetching schema id " + std::to_string(id);
    auto v = impl_->get_json("/schemas/ids/" + std::to_string(id), what);
    auto s = impl_->schema_from(v, what, id);
    s.id = id;
    std::lock_guard lk(impl_->mu);
    impl_->by_id.emplace(id, s);
    return s;
}

RegisteredSchema Client::latest(const std::string& subject) {
    const std::string what = "reading the latest version of subject '" + subject + "'";
    auto v = impl_->get_json("/subjects/" + percent_encode(subject) + "/versions/latest", what);
    auto s = impl_->schema_from(v, what, -1);
    if (s.subject.empty()) {
        s.subject = subject;
    }
    return s;
}

RegisteredSchema Client::version(const std::string& subject, int version) {
    const std::string what =
        "reading version " + std::to_string(version) + " of subject '" + subject + "'";
    auto v = impl_->get_json(
        "/subjects/" + percent_encode(subject) + "/versions/" + std::to_string(version), what);
    auto s = impl_->schema_from(v, what, -1);
    if (s.subject.empty()) {
        s.subject = subject;
    }
    return s;
}

std::int32_t Client::register_schema(const std::string& subject,
                                     SchemaType type,
                                     const std::string& schema,
                                     const std::vector<SchemaReference>& references) {
    const std::string what = "registering a " + std::string{schema_type_name(type)} +
                             " schema under subject '" + subject + "'";
    const auto r =
        impl_->http.post(impl_->prefix + "/subjects/" + percent_encode(subject) + "/versions",
                         Impl::body_for(type, schema, references),
                         kContentType);
    if (r.status < 200 || r.status >= 300) {
        impl_->fail(what, r);
    }
    clink::config::JsonValue v;
    try {
        v = clink::config::parse(r.body);
    } catch (const std::exception& e) {
        throw RegistryError(r.status,
                            0,
                            "schema registry " + impl_->describe() + ": " + what +
                                ": response is not JSON: " + e.what());
    }
    if (!v.is_object() || !v.contains("id") || !v.at("id").is_number()) {
        throw RegistryError(
            r.status,
            0,
            "schema registry " + impl_->describe() + ": " + what + ": response has no 'id'");
    }
    const auto id = static_cast<std::int32_t>(v.at("id").as_int());
    RegisteredSchema s;
    s.id = id;
    s.subject = subject;
    s.type = type;
    s.schema = schema;
    s.references = references;
    std::lock_guard lk(impl_->mu);
    impl_->by_id.emplace(id, std::move(s));
    return id;
}

std::optional<RegisteredSchema> Client::lookup(const std::string& subject,
                                               SchemaType type,
                                               const std::string& schema,
                                               const std::vector<SchemaReference>& references) {
    const std::string what = "looking up a schema under subject '" + subject + "'";
    const auto r = impl_->http.post(impl_->prefix + "/subjects/" + percent_encode(subject),
                                    Impl::body_for(type, schema, references),
                                    kContentType);
    if (r.status == 404) {
        return std::nullopt;
    }
    if (r.status < 200 || r.status >= 300) {
        impl_->fail(what, r);
    }
    clink::config::JsonValue v;
    try {
        v = clink::config::parse(r.body);
    } catch (const std::exception& e) {
        throw RegistryError(r.status,
                            0,
                            "schema registry " + impl_->describe() + ": " + what +
                                ": response is not JSON: " + e.what());
    }
    auto s = impl_->schema_from(v, what, -1);
    if (s.subject.empty()) {
        s.subject = subject;
    }
    return s;
}

}  // namespace clink::schema_registry
