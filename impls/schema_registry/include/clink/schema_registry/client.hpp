#pragma once
//
// A client for the Confluent Schema Registry REST API, the subset a broker
// connector needs: fetch a schema by id (decode side), read or register a
// subject's schema (encode side). Speaks to Confluent Schema Registry and to
// the API-compatible registries (Redpanda, Karapace, Apicurio's ccompat
// endpoint). Plain HTTP or HTTPS; basic auth or a bearer token.
//
// Schemas are immutable per id, so ids are cached for the client's lifetime.
// Subject lookups are not cached: a subject's latest version can change.

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace clink::schema_registry {

// The registry's own type vocabulary. A response without `schemaType` is Avro.
enum class SchemaType { Avro, Protobuf, Json };
const char* schema_type_name(SchemaType type) noexcept;  // "AVRO", "PROTOBUF", "JSON"
std::optional<SchemaType> parse_schema_type(std::string_view text);

// A reference from one schema to another registered under its own subject
// (Protobuf imports, Avro named types, JSON Schema $ref).
struct SchemaReference {
    std::string name;     // the name the referring schema uses (e.g. "other.proto")
    std::string subject;  // where the referenced schema is registered
    int version{-1};
};

struct RegisteredSchema {
    std::int32_t id{-1};
    int version{-1};      // -1 when fetched by id (the registry does not return it there)
    std::string subject;  // empty when fetched by id
    SchemaType type{SchemaType::Avro};
    std::string schema;  // the schema text as registered: JSON for Avro and JSON Schema, .proto for
                         // Protobuf
    std::vector<SchemaReference> references;
};

// Any failed registry call. http_status is 0 for a transport failure (the
// registry unreachable, TLS refused, timeout); registry_code is the body's
// `error_code` when the registry sent one (40401 subject not found, 40403
// schema not found, 409 incompatible, 422 invalid schema ...), else 0.
class RegistryError : public std::runtime_error {
public:
    RegistryError(int http_status, int registry_code, const std::string& what);
    [[nodiscard]] int http_status() const noexcept { return http_status_; }
    [[nodiscard]] int registry_code() const noexcept { return registry_code_; }

private:
    int http_status_;
    int registry_code_;
};

struct ClientOptions {
    // http://host:8081, https://registry.example.com, or with a path prefix
    // (https://host/apis/ccompat/v7). Credentials in the URL's userinfo part
    // (https://key:secret@host) are honoured as basic auth.
    std::string url;
    std::string basic_auth;    // "user:password"
    std::string bearer_token;  // used when basic_auth is empty
    bool verify_tls{true};
    int connect_timeout_ms{5000};
    int rw_timeout_ms{30000};
};

class Client {
public:
    explicit Client(ClientOptions opts);
    ~Client();
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) = delete;
    Client& operator=(Client&&) = delete;

    // GET /schemas/ids/{id}. Cached: a registered id never changes meaning.
    RegisteredSchema schema_by_id(std::int32_t id);
    // GET /subjects/{subject}/versions/latest
    RegisteredSchema latest(const std::string& subject);
    // GET /subjects/{subject}/versions/{version}
    RegisteredSchema version(const std::string& subject, int version);
    // POST /subjects/{subject}/versions. Returns the id; the registry answers
    // with the existing id when the same schema is already registered under
    // the subject, so this is idempotent.
    std::int32_t register_schema(const std::string& subject,
                                 SchemaType type,
                                 const std::string& schema,
                                 const std::vector<SchemaReference>& references = {});
    // POST /subjects/{subject} (a lookup, not a registration): the registered
    // id and version of exactly this schema under the subject, or nullopt when
    // it is not registered there.
    std::optional<RegisteredSchema> lookup(const std::string& subject,
                                           SchemaType type,
                                           const std::string& schema,
                                           const std::vector<SchemaReference>& references = {});

    [[nodiscard]] const ClientOptions& options() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::schema_registry
