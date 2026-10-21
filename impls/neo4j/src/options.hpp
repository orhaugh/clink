#pragma once

#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "types.hpp"

namespace clink::neo4j {

// A refusal with a stable code ("neo4j.option_invalid", ...) for a test or a
// dashboard to key on, beside a message for a person.
class Neo4jSinkError : public std::runtime_error {
public:
    Neo4jSinkError(std::string code, const std::string& message)
        : std::runtime_error(message), code_(std::move(code)) {}
    [[nodiscard]] const std::string& code() const noexcept { return code_; }

private:
    std::string code_;
};

namespace code {
inline constexpr char kOptionInvalid[] = "neo4j.option_invalid";
inline constexpr char kSecretUnset[] = "neo4j.secret_unset";
inline constexpr char kGuaranteeUnsupported[] = "neo4j.guarantee_unsupported";
inline constexpr char kTypeUnsupported[] = "neo4j.type_unsupported";
inline constexpr char kTlsUnavailable[] = "neo4j.tls_unavailable";
inline constexpr char kTlsVerify[] = "neo4j.tls_verify";
inline constexpr char kStatementRefused[] = "neo4j.statement_refused";
inline constexpr char kAuthentication[] = "neo4j.authentication";
inline constexpr char kAuthorisation[] = "neo4j.authorisation";
inline constexpr char kServerRefused[] = "neo4j.server_refused";
inline constexpr char kRetriesExhausted[] = "neo4j.retries_exhausted";
inline constexpr char kOutcomeUnknown[] = "neo4j.outcome_unknown";
inline constexpr char kValueInvalid[] = "neo4j.value_invalid";
inline constexpr char kRowTooLarge[] = "neo4j.row_too_large";
inline constexpr char kCancelled[] = "neo4j.cancelled";
}  // namespace code

struct SinkOptions {
    std::string endpoint;  // scheme://host[:port], no path
    bool https{false};
    std::string database{"neo4j"};
    std::string statement;
    // Exactly one of: basic (username + password), bearer, none.
    std::string username;
    std::string password;
    std::string bearer_token;
    // https: the PEM CAs that may sign the server's certificate, in place of
    // the system's. The certificate must also name the endpoint's host; that
    // check has no off switch.
    std::string tls_ca_file;
    std::uint64_t batch_size{1000};
    std::uint64_t batch_bytes{4ULL << 20};
    std::uint64_t flush_interval_ms{1000};
    std::uint64_t connect_timeout_ms{5000};
    std::uint64_t request_timeout_ms{30000};
    std::uint64_t retry_window_ms{60000};
    std::uint64_t max_response_bytes{1ULL << 20};
    std::vector<Column> columns;
    std::uint32_t subtask{0};
    std::uint32_t parallelism{1};

    // The request path, the database percent-encoded.
    [[nodiscard]] std::string query_path() const;
};

// Every option the sink reads, refused when malformed, out of range or
// meaningless here; keys the SQL planner puts on every sink op are accepted
// and ignored. A value may be written env://VAR and is then read from the
// environment; a refusal names such a value by its reference only.
[[nodiscard]] SinkOptions parse_options(const std::map<std::string, std::string>& params,
                                        std::uint32_t subtask,
                                        std::uint32_t parallelism);

// The options a refusal lists as accepted.
[[nodiscard]] const std::vector<std::string>& option_names();

}  // namespace clink::neo4j
