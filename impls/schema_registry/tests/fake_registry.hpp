#pragma once
//
// An in-process Confluent Schema Registry double on clink's own HttpServer:
// the routes the client uses, the registry's JSON shapes and error codes,
// and a request log the tests assert on (which paths were hit, which
// Authorization header arrived). Schemas register with incrementing ids;
// re-registering an identical schema under a subject returns its id, as the
// real registry does.

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/http/http_server.hpp"

namespace clink::schema_registry::test {

class FakeRegistry {
public:
    struct Stored {
        std::int32_t id;
        int version;
        std::string subject;
        std::string type;  // "AVRO" | "PROTOBUF" | "JSON"
        std::string schema;
        clink::config::JsonValue references;  // array or null
    };

    FakeRegistry() {
        server_.get("/schemas/ids/:id",
                    [this](const clink::http::HttpRequest& r) { return by_id_(r); });
        server_.get("/subjects/:subject/versions/latest",
                    [this](const clink::http::HttpRequest& r) { return version_(r, -1); });
        server_.get("/subjects/:subject/versions/:version",
                    [this](const clink::http::HttpRequest& r) {
                        return version_(r, std::stoi(r.path_params.at("version")));
                    });
        server_.post("/subjects/:subject/versions",
                     [this](const clink::http::HttpRequest& r) { return register_(r); });
        server_.post("/subjects/:subject",
                     [this](const clink::http::HttpRequest& r) { return lookup_(r); });
        port_ = server_.start("127.0.0.1", 0);
    }
    ~FakeRegistry() { server_.stop(); }

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    // Seed a schema directly (no HTTP), as if a producer had registered it.
    std::int32_t add(const std::string& subject,
                     const std::string& type,
                     const std::string& schema,
                     clink::config::JsonValue references = clink::config::JsonValue{}) {
        std::lock_guard lk(mu_);
        return add_locked_(subject, type, schema, std::move(references));
    }

    [[nodiscard]] std::vector<std::string> requests() const {
        std::lock_guard lk(mu_);
        return requests_;
    }
    [[nodiscard]] std::string last_authorization() const {
        std::lock_guard lk(mu_);
        return last_auth_;
    }
    [[nodiscard]] std::size_t schema_count() const {
        std::lock_guard lk(mu_);
        return by_id_store_.size();
    }
    // Make every response fail with this status (0 = normal) - for the
    // error-path tests.
    void fail_with(int status) {
        std::lock_guard lk(mu_);
        fail_status_ = status;
    }

private:
    using Req = clink::http::HttpRequest;
    using Res = clink::http::HttpResponse;

    static Res error(int status, int code, const std::string& message) {
        clink::config::JsonObject o;
        o.emplace("error_code", clink::config::JsonValue{static_cast<std::int64_t>(code)});
        o.emplace("message", clink::config::JsonValue{message});
        Res r;
        r.status = status;
        r.content_type = "application/vnd.schemaregistry.v1+json";
        r.body = clink::config::JsonValue{std::move(o)}.serialize();
        return r;
    }
    static Res ok(clink::config::JsonObject o) {
        Res r;
        r.content_type = "application/vnd.schemaregistry.v1+json";
        r.body = clink::config::JsonValue{std::move(o)}.serialize();
        return r;
    }
    static clink::config::JsonObject describe(const Stored& s, bool with_subject) {
        clink::config::JsonObject o;
        if (with_subject) {
            o.emplace("subject", clink::config::JsonValue{s.subject});
            o.emplace("version", clink::config::JsonValue{static_cast<std::int64_t>(s.version)});
            o.emplace("id", clink::config::JsonValue{static_cast<std::int64_t>(s.id)});
        }
        if (s.type != "AVRO") {
            o.emplace("schemaType",
                      clink::config::JsonValue{s.type});  // the real registry omits AVRO
        }
        if (s.references.is_array()) {
            o.emplace("references", s.references);
        }
        o.emplace("schema", clink::config::JsonValue{s.schema});
        return o;
    }

    void log_(const Req& r) {
        requests_.push_back(r.method + " " + r.path);
        if (const auto it = r.headers.find("authorization"); it != r.headers.end()) {
            last_auth_ = it->second;
        }
    }

    std::int32_t add_locked_(const std::string& subject,
                             const std::string& type,
                             const std::string& schema,
                             clink::config::JsonValue references) {
        auto& versions = subjects_[subject];
        for (const auto& v : versions) {
            if (v.schema == schema && v.type == type) {
                return v.id;
            }
        }
        Stored s{next_id_++,
                 static_cast<int>(versions.size()) + 1,
                 subject,
                 type,
                 schema,
                 std::move(references)};
        versions.push_back(s);
        by_id_store_.emplace(s.id, s);
        return s.id;
    }

    Res by_id_(const Req& r) {
        std::lock_guard lk(mu_);
        log_(r);
        if (fail_status_ != 0) {
            return error(fail_status_, 50001, "injected failure");
        }
        const auto id = std::stoi(r.path_params.at("id"));
        const auto it = by_id_store_.find(id);
        if (it == by_id_store_.end()) {
            return error(404, 40403, "Schema " + std::to_string(id) + " not found");
        }
        return ok(describe(it->second, false));
    }

    Res version_(const Req& r, int version) {
        std::lock_guard lk(mu_);
        log_(r);
        if (fail_status_ != 0) {
            return error(fail_status_, 50001, "injected failure");
        }
        const auto& subject = r.path_params.at("subject");
        const auto it = subjects_.find(subject);
        if (it == subjects_.end() || it->second.empty()) {
            return error(404, 40401, "Subject '" + subject + "' not found.");
        }
        const auto& versions = it->second;
        if (version == -1) {
            return ok(describe(versions.back(), true));
        }
        if (version < 1 || static_cast<std::size_t>(version) > versions.size()) {
            return error(404, 40402, "Version " + std::to_string(version) + " not found.");
        }
        return ok(describe(versions[static_cast<std::size_t>(version) - 1], true));
    }

    Res register_(const Req& r) {
        std::lock_guard lk(mu_);
        log_(r);
        if (fail_status_ != 0) {
            return error(fail_status_, 50001, "injected failure");
        }
        const auto body = clink::config::parse(r.body);
        if (!body.is_object() || !body.contains("schema")) {
            return error(422, 42201, "Invalid schema");
        }
        const std::string type = body.string_or("schemaType", "AVRO");
        const auto id = add_locked_(
            r.path_params.at("subject"),
            type,
            body.at("schema").as_string(),
            body.contains("references") ? body.at("references") : clink::config::JsonValue{});
        clink::config::JsonObject o;
        o.emplace("id", clink::config::JsonValue{static_cast<std::int64_t>(id)});
        return ok(std::move(o));
    }

    Res lookup_(const Req& r) {
        std::lock_guard lk(mu_);
        log_(r);
        if (fail_status_ != 0) {
            return error(fail_status_, 50001, "injected failure");
        }
        const auto body = clink::config::parse(r.body);
        const auto& subject = r.path_params.at("subject");
        const auto it = subjects_.find(subject);
        if (it == subjects_.end()) {
            return error(404, 40401, "Subject '" + subject + "' not found.");
        }
        const std::string type = body.string_or("schemaType", "AVRO");
        for (const auto& v : it->second) {
            if (v.schema == body.string_or("schema", "") && v.type == type) {
                return ok(describe(v, true));
            }
        }
        return error(404, 40403, "Schema not found");
    }

    clink::http::HttpServer server_;
    std::uint16_t port_{0};
    mutable std::mutex mu_;
    std::int32_t next_id_{1};
    std::map<std::string, std::vector<Stored>> subjects_;
    std::map<std::int32_t, Stored> by_id_store_;
    std::vector<std::string> requests_;
    std::string last_auth_;
    int fail_status_{0};
};

}  // namespace clink::schema_registry::test
