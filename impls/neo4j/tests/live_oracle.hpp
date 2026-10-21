#pragma once

// The live suites' own Cypher client, over the Query API with plain JSON
// parameters and typed results: independent of the sink's batching and the
// lookup's encoding, so what a case reads back is not the code under test
// reporting on itself. Shared by test_neo4j_live.cpp and
// test_neo4j_lookup_live.cpp.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <httplib.h>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::neo4j::testing {

using clink::config::JsonArray;
using clink::config::JsonValue;

inline std::string env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

// host:port of an http(s)://host:port endpoint.
inline std::pair<std::string, int> host_port(const std::string& endpoint) {
    const auto scheme_end = endpoint.find("://");
    const auto rest = endpoint.substr(scheme_end + 3);
    const auto colon = rest.rfind(':');
    return {rest.substr(0, colon), std::stoi(rest.substr(colon + 1))};
}

class Oracle {
public:
    Oracle(std::string endpoint, std::string password, std::string ca_file = {})
        : endpoint_(std::move(endpoint)),
          password_(std::move(password)),
          ca_file_(std::move(ca_file)) {}

    // The typed rows of `statement` against `database`; throws on any error.
    std::vector<JsonArray> rows(const std::string& statement,
                                const std::string& params_json = "{}",
                                const std::string& database = "neo4j") const {
        httplib::Client cli(endpoint_);
        if (!ca_file_.empty()) {
            cli.set_ca_cert_path(ca_file_);
        }
        cli.set_basic_auth("neo4j", password_);
        cli.set_read_timeout(std::chrono::seconds{60});
        std::string body = "{\"statement\":";
        JsonValue{statement}.serialize_into(body);
        body += ",\"parameters\":" + params_json + "}";
        const auto res = cli.Post("/db/" + database + "/query/v2",
                                  httplib::Headers{{"Accept", "application/vnd.neo4j.query"}},
                                  body,
                                  "application/json");
        if (!res) {
            throw std::runtime_error("oracle: " + httplib::to_string(res.error()));
        }
        const JsonValue reply = clink::config::parse(res->body);
        if (reply.contains("errors")) {
            throw std::runtime_error("oracle: " + res->body);
        }
        std::vector<JsonArray> out;
        for (const auto& r : reply.at("data").at("values").as_array()) {
            out.push_back(r.as_array());
        }
        return out;
    }

    void run(const std::string& statement,
             const std::string& params_json = "{}",
             const std::string& database = "neo4j") const {
        (void)rows(statement, params_json, database);
    }

    std::int64_t count(const std::string& statement) const {
        const auto r = rows(statement);
        return std::stoll(r.at(0).at(0).at("_value").as_string());
    }

    // The first error code the server answers `statement` with when it is sent
    // with accessMode `mode` ("READ" or "WRITE"), or "" when it runs.
    std::string error_code(const std::string& statement,
                           const std::string& mode,
                           const std::string& database = "neo4j") const {
        httplib::Client cli(endpoint_);
        if (!ca_file_.empty()) {
            cli.set_ca_cert_path(ca_file_);
        }
        cli.set_basic_auth("neo4j", password_);
        std::string body = "{\"statement\":";
        JsonValue{statement}.serialize_into(body);
        body += ",\"accessMode\":\"" + mode + "\"}";
        const auto res = cli.Post("/db/" + database + "/query/v2",
                                  httplib::Headers{{"Accept", "application/vnd.neo4j.query"}},
                                  body,
                                  "application/json");
        if (!res) {
            throw std::runtime_error("oracle: " + httplib::to_string(res.error()));
        }
        const JsonValue reply = clink::config::parse(res->body);
        if (!reply.contains("errors") || reply.at("errors").as_array().empty()) {
            return {};
        }
        return reply.at("errors").as_array().front().at("code").as_string();
    }

    bool reachable() const {
        try {
            run("RETURN 1");
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

private:
    std::string endpoint_;
    std::string password_;
    std::string ca_file_;
};

inline std::string text(const JsonValue& typed) {
    const auto& v = typed.at("_value");
    return v.is_string() ? v.as_string() : v.serialize(0);
}

}  // namespace clink::neo4j::testing
