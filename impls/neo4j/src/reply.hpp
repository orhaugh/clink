#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/http_connector/http_request.hpp"

namespace clink::neo4j {

struct ServerError {
    std::string code;     // Neo.ClientError.Statement.SyntaxError, ...
    std::string message;  // as the server wrote it
};

// What a Query API reply body says, read as far as the sink needs it.
struct Reply {
    bool parsed{false};  // the body is a JSON object
    std::string parse_error;
    std::vector<ServerError> errors;
    // "bookmarks" holds at least one string: the server reports bookmarks
    // only for a transaction it has finished, so with no errors this is the
    // proof of a commit.
    bool has_bookmarks{false};
    bool has_fields{false};
    std::vector<std::string> fields;        // data.fields: the statement's result columns
    std::optional<std::string> query_type;  // r, w, rw, s (servers from 2026.07)
    bool has_plan{false};
    std::vector<std::string> plan_operators;  // every operatorType in queryPlan, "@..." dropped
    std::vector<std::pair<std::string, std::string>> notifications;  // code, description
    std::string server_version;  // from the plan's runtime-version, when present
};

[[nodiscard]] Reply parse_reply(std::string_view body);

// How the server's own error codes are treated. Retryable follows the official
// Neo4j driver (6.x exceptions.py): every TransientError but
// Transaction.Terminated and Transaction.LockClientStopped, plus
// Cluster.NotALeader, General.ForbiddenOnReadOnlyDatabase and
// Security.AuthorizationExpired. Everything else is permanent; the classes
// only choose the message.
enum class ErrorClass : std::uint8_t {
    Retryable,
    Authentication,
    Authorisation,
    Statement,
    Data,
    Other,
};

[[nodiscard]] ErrorClass classify_code(std::string_view code) noexcept;

// True for the codes whose message can quote a value from the batch (a
// constraint violation names the conflicting property value; a type error
// prints the value it met). Their messages are withheld from logs and errors.
[[nodiscard]] bool message_may_quote_values(std::string_view code) noexcept;

// "<code>: <message>", or the code and a note when the message is withheld.
[[nodiscard]] std::string describe(const ServerError& e);

// One attempt to commit a batch, judged from the whole exchange. The status
// alone never decides: a reply streams its errors after a 202 status line.
enum class Verdict : std::uint8_t {
    Committed,     // a complete reply with bookmarks and no errors
    RolledBack,    // the server reported an error it rolled back, and a retry may succeed
    Refused,       // the server reported an error it rolled back, and a retry cannot succeed
    NotSent,       // the request never reached the server whole; nothing was applied
    Unknown,       // the request reached the server and the outcome was lost
    UnknownFinal,  // as Unknown, and a retry would end the same way (the reply is too large)
    TlsRefused,    // the server's certificate or name did not verify
};

[[nodiscard]] const char* to_string(Verdict v) noexcept;

struct Outcome {
    Verdict verdict{Verdict::Unknown};
    ErrorClass error_class{ErrorClass::Other};  // for Refused and RolledBack
    std::string code;                           // the server's code, when it gave one
    std::string detail;                         // for logs and errors; never row values
};

// A batch commit.
[[nodiscard]] Outcome judge_commit(const clink::http_connector::BoundedResponse& r);

// The open-time EXPLAIN: Committed here means a complete reply with no errors
// (an EXPLAIN commits nothing, so bookmarks are not required); `reply` gets the
// parsed body.
[[nodiscard]] Outcome judge_check(const clink::http_connector::BoundedResponse& r, Reply& reply);

// What the EXPLAIN of the configured statement says against the parameter
// contract: one line per problem, empty when the statement is accepted.
[[nodiscard]] std::vector<std::string> statement_problems(const Reply& explain);

}  // namespace clink::neo4j
