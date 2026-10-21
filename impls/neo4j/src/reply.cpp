#include "reply.hpp"

#include <algorithm>
#include <array>
#include <exception>

#include "clink/config/json.hpp"

namespace clink::neo4j {

namespace {

using clink::config::JsonValue;
using clink::http_connector::BoundedResponse;

const JsonValue* member(const JsonValue& obj, std::string_view key) {
    if (!obj.is_object()) {
        return nullptr;
    }
    const auto& o = obj.as_object();
    const auto it = o.find(std::string(key));
    return it == o.end() ? nullptr : &it->second;
}

std::string string_member(const JsonValue& obj, std::string_view key) {
    const auto* v = member(obj, key);
    return v != nullptr && v->is_string() ? v->as_string() : std::string{};
}

// A plan node: {"operatorType": "Merge@neo4j", "arguments": {...},
// "children": [...]}. Typed JSON wraps argument values as {"$type", "_value"}.
void walk_plan(const JsonValue& node, Reply& out) {
    if (!node.is_object()) {
        return;
    }
    std::string op = string_member(node, "operatorType");
    if (const auto at = op.find('@'); at != std::string::npos) {
        op.resize(at);
    }
    if (!op.empty()) {
        out.plan_operators.push_back(op);
    }
    if (out.server_version.empty()) {
        if (const auto* args = member(node, "arguments")) {
            if (const auto* rv = member(*args, "runtime-version")) {
                out.server_version =
                    rv->is_string() ? rv->as_string() : string_member(*rv, "_value");
            }
        }
    }
    if (const auto* children = member(node, "children");
        children != nullptr && children->is_array()) {
        for (const auto& c : children->as_array()) {
            walk_plan(c, out);
        }
    }
}

bool starts(std::string_view s, std::string_view prefix) {
    return s.starts_with(prefix);
}

Outcome refused_by_status(int status, Verdict otherwise, std::string detail) {
    Outcome o;
    o.detail = std::move(detail);
    if (status == 401) {
        o.verdict = Verdict::Refused;
        o.error_class = ErrorClass::Authentication;
        o.detail = "HTTP 401: the credentials were not accepted";
    } else if (status == 403) {
        o.verdict = Verdict::Refused;
        o.error_class = ErrorClass::Authorisation;
        o.detail = "HTTP 403: the user may not run this statement on this database";
    } else if (status == 404) {
        o.verdict = Verdict::Refused;
        o.detail = "HTTP 404: no Query API at this endpoint and database";
    } else if (status == 429) {
        // Throttled before the statement ran.
        o.verdict = Verdict::RolledBack;
        o.error_class = ErrorClass::Retryable;
        o.detail = "HTTP 429: the server asked for fewer requests";
    } else if (status >= 300 && status < 400) {
        o.verdict = Verdict::Refused;
        o.detail = "HTTP " + std::to_string(status) +
                   ": the endpoint redirects; point endpoint at the server itself";
    } else if (status >= 400 && status < 500 && status != 408) {
        o.verdict = Verdict::Refused;
        o.detail = "HTTP " + std::to_string(status) + ": " + o.detail;
    } else {
        o.verdict = otherwise;
        o.detail = "HTTP " + std::to_string(status) + ": " + o.detail;
    }
    return o;
}

Outcome transport(const BoundedResponse& r) {
    Outcome o;
    using F = BoundedResponse::Failure;
    switch (r.failure) {
        case F::Connect:
            o.verdict = Verdict::NotSent;
            o.detail = "could not connect: " + r.error;
            break;
        case F::TlsHandshake:
            o.verdict = Verdict::NotSent;
            o.detail = "the TLS handshake failed: " + r.error;
            break;
        case F::TlsVerify:
            o.verdict = Verdict::TlsRefused;
            o.detail =
                "the server's certificate did not verify (check tls_ca_file and that the "
                "certificate names the endpoint's host): " +
                r.error;
            break;
        case F::Write:
            o.verdict = Verdict::NotSent;
            o.detail = "the request could not be written: " + r.error;
            break;
        case F::TooLarge:
            o.verdict = Verdict::UnknownFinal;
            o.detail =
                "the reply passed max_response_bytes; the sink stopped reading it, so "
                "whether the batch committed is unknown";
            break;
        default:
            o.verdict = r.request_written ? Verdict::Unknown : Verdict::NotSent;
            o.detail = (r.request_written ? "the reply was lost after the request was sent: "
                                          : "the request failed: ") +
                       r.error;
            break;
    }
    return o;
}

}  // namespace

Reply parse_reply(std::string_view body) {
    Reply out;
    JsonValue root;
    try {
        root = clink::config::parse(body);
    } catch (const std::exception& e) {
        out.parse_error = e.what();
        return out;
    }
    if (!root.is_object()) {
        out.parse_error = "the reply is not a JSON object";
        return out;
    }
    out.parsed = true;
    if (const auto* errors = member(root, "errors"); errors != nullptr && errors->is_array()) {
        for (const auto& e : errors->as_array()) {
            out.errors.push_back(
                ServerError{string_member(e, "code"), string_member(e, "message")});
        }
    }
    if (const auto* b = member(root, "bookmarks"); b != nullptr && b->is_array()) {
        const auto& arr = b->as_array();
        out.has_bookmarks = std::any_of(arr.begin(), arr.end(), [](const JsonValue& v) {
            return v.is_string() && !v.as_string().empty();
        });
        for (const auto& v : arr) {
            if (v.is_string() && !v.as_string().empty()) {
                out.bookmark = v.as_string();
                break;
            }
        }
    }
    if (const auto* data = member(root, "data")) {
        if (const auto* fields = member(*data, "fields"); fields != nullptr && fields->is_array()) {
            out.has_fields = true;
            for (const auto& f : fields->as_array()) {
                out.fields.push_back(f.is_string() ? f.as_string() : f.serialize(0));
            }
        }
        if (const auto* values = member(*data, "values"); values != nullptr && values->is_array()) {
            const auto& rows = values->as_array();
            out.has_values = std::all_of(
                rows.begin(), rows.end(), [](const JsonValue& row) { return row.is_array(); });
            out.row_count = rows.size();
            if (out.has_values && !rows.empty()) {
                out.first_row = rows.front().as_array();
            }
        }
    }
    if (const auto* qt = member(root, "queryType"); qt != nullptr && qt->is_string()) {
        out.query_type = qt->as_string();
    }
    if (const auto* plan = member(root, "queryPlan"); plan != nullptr && plan->is_object()) {
        out.has_plan = true;
        walk_plan(*plan, out);
    }
    if (const auto* notes = member(root, "notifications"); notes != nullptr && notes->is_array()) {
        for (const auto& n : notes->as_array()) {
            out.notifications.emplace_back(string_member(n, "code"),
                                           string_member(n, "description"));
        }
    }
    return out;
}

ErrorClass classify_code(std::string_view code) noexcept {
    if (code == "Neo.TransientError.Transaction.Terminated" ||
        code == "Neo.TransientError.Transaction.LockClientStopped") {
        return ErrorClass::Other;
    }
    if (starts(code, "Neo.TransientError.") || code == "Neo.ClientError.Cluster.NotALeader" ||
        code == "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" ||
        code == "Neo.ClientError.Security.AuthorizationExpired") {
        return ErrorClass::Retryable;
    }
    if (code == "Neo.ClientError.Security.Forbidden" ||
        code == "Neo.ClientError.Security.CredentialsExpired") {
        return ErrorClass::Authorisation;
    }
    if (starts(code, "Neo.ClientError.Security.")) {
        return ErrorClass::Authentication;
    }
    if (starts(code, "Neo.ClientError.Schema.")) {
        return ErrorClass::Data;
    }
    if (starts(code, "Neo.ClientError.Statement.") || starts(code, "Neo.ClientError.Procedure.")) {
        return message_may_quote_values(code) ? ErrorClass::Data : ErrorClass::Statement;
    }
    return ErrorClass::Other;
}

bool message_may_quote_values(std::string_view code) noexcept {
    static constexpr std::array<std::string_view, 5> kStatic{
        "Neo.ClientError.Statement.SyntaxError",
        "Neo.ClientError.Statement.SemanticError",
        "Neo.ClientError.Statement.ParameterMissing",
        "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
        "Neo.ClientError.Statement.NotSystemDatabaseError",
    };
    if (std::find(kStatic.begin(), kStatic.end(), code) != kStatic.end()) {
        return false;
    }
    return starts(code, "Neo.ClientError.Schema.") || starts(code, "Neo.ClientError.Statement.") ||
           code == "Neo.ClientError.Procedure.ProcedureCallFailed";
}

std::string describe(const ServerError& e) {
    const std::string code = e.code.empty() ? std::string("(no code)") : e.code;
    if (message_may_quote_values(e.code)) {
        return code +
               " (the server's message is withheld because it can quote values from the batch)";
    }
    return e.message.empty() ? code : code + ": " + e.message;
}

const char* to_string(Verdict v) noexcept {
    switch (v) {
        case Verdict::Committed:
            return "committed";
        case Verdict::RolledBack:
            return "rolled_back";
        case Verdict::Refused:
            return "refused";
        case Verdict::NotSent:
            return "not_sent";
        case Verdict::Unknown:
            return "unknown";
        case Verdict::UnknownFinal:
            return "unknown";
        case Verdict::TlsRefused:
            return "tls_refused";
    }
    return "unknown";
}

Outcome judge_commit(const BoundedResponse& r) {
    if (!r.complete) {
        return transport(r);
    }
    const Reply reply = parse_reply(r.body);
    if (!reply.parsed) {
        return refused_by_status(r.status,
                                 Verdict::Unknown,
                                 "the reply is not a Neo4j JSON reply (cut short, or from "
                                 "something other than the server), so whether the batch "
                                 "committed is unknown");
    }
    if (!reply.errors.empty()) {
        const auto& e = reply.errors.front();
        Outcome o;
        o.code = e.code;
        o.detail = describe(e);
        if (reply.has_bookmarks) {
            // Both a commit and a failure: believe neither.
            o.verdict = Verdict::Unknown;
            o.detail = "the reply reports both a commit and an error (" + o.detail + ")";
            return o;
        }
        o.error_class = classify_code(e.code);
        o.verdict = o.error_class == ErrorClass::Retryable ? Verdict::RolledBack : Verdict::Refused;
        return o;
    }
    if (r.status >= 200 && r.status < 300) {
        if (reply.has_bookmarks) {
            return Outcome{Verdict::Committed, ErrorClass::Other, {}, {}};
        }
        return Outcome{Verdict::Unknown,
                       ErrorClass::Other,
                       {},
                       "HTTP " + std::to_string(r.status) +
                           ": the reply has no bookmarks and no errors, so whether the batch "
                           "committed is unknown"};
    }
    return refused_by_status(r.status, Verdict::Unknown, "a reply without a Neo4j error");
}

Outcome judge_check(const BoundedResponse& r, Reply& reply) {
    if (!r.complete) {
        return transport(r);
    }
    reply = parse_reply(r.body);
    if (!reply.parsed) {
        return refused_by_status(r.status, Verdict::Unknown, "the reply is not a Neo4j JSON reply");
    }
    if (!reply.errors.empty()) {
        const auto& e = reply.errors.front();
        Outcome o;
        o.code = e.code;
        o.detail = describe(e);
        o.error_class = classify_code(e.code);
        o.verdict = o.error_class == ErrorClass::Retryable ? Verdict::RolledBack : Verdict::Refused;
        return o;
    }
    if (r.status >= 200 && r.status < 300) {
        return Outcome{Verdict::Committed, ErrorClass::Other, {}, {}};
    }
    return refused_by_status(r.status, Verdict::Unknown, "a reply without a Neo4j error");
}

Outcome judge_read(const BoundedResponse& r, Reply& reply) {
    if (!r.complete) {
        Outcome o = transport(r);
        if (r.failure == BoundedResponse::Failure::TooLarge) {
            o.detail = "the reply passed max_response_bytes, and a lookup reads at most one row";
        } else if (o.verdict == Verdict::Unknown) {
            o.detail = "the reply was lost after the request was sent: " + r.error;
        }
        return o;
    }
    reply = parse_reply(r.body);
    if (!reply.parsed) {
        return refused_by_status(r.status,
                                 Verdict::Unknown,
                                 "the reply is not a Neo4j JSON reply (cut short, or from "
                                 "something other than the server)");
    }
    if (!reply.errors.empty()) {
        const auto& e = reply.errors.front();
        Outcome o;
        o.code = e.code;
        o.detail = describe(e);
        o.error_class = classify_code(e.code);
        o.verdict = o.error_class == ErrorClass::Retryable ? Verdict::RolledBack : Verdict::Refused;
        return o;
    }
    if (r.status < 200 || r.status >= 300) {
        return refused_by_status(r.status, Verdict::Unknown, "a reply without a Neo4j error");
    }
    if (!reply.has_fields || !reply.has_values) {
        return Outcome{Verdict::Refused,
                       ErrorClass::Other,
                       {},
                       "HTTP " + std::to_string(r.status) +
                           ": the reply has no result columns or rows, so it is not a Query API "
                           "result"};
    }
    if (reply.row_count > 0 && reply.first_row.size() != reply.fields.size()) {
        return Outcome{Verdict::Refused,
                       ErrorClass::Other,
                       {},
                       "the reply's row holds " + std::to_string(reply.first_row.size()) +
                           " values for " + std::to_string(reply.fields.size()) +
                           " result columns"};
    }
    return Outcome{Verdict::Committed, ErrorClass::Other, {}, {}};
}

std::vector<std::string> lookup_statement_problems(const Reply& explain,
                                                   const std::vector<std::string>& columns) {
    std::vector<std::string> out;
    if (explain.query_type && *explain.query_type != "r") {
        out.push_back("the server classes the statement as query type '" + *explain.query_type +
                      "', not a read (r); a lookup must only read");
    }
    // Writes and schema commands, by the operators that plan them; the query
    // type says the same on servers that report one.
    static constexpr std::array<std::string_view, 10> kWrites{"Create",
                                                              "Merge",
                                                              "LockingMerge",
                                                              "Delete",
                                                              "DetachDelete",
                                                              "Set",
                                                              "Remove",
                                                              "Foreach",
                                                              "Drop",
                                                              "DoNothingIfExists"};
    bool write = false;
    bool procedure = false;
    bool load_csv = false;
    bool in_transactions = false;
    for (const auto& op : explain.plan_operators) {
        write = write || std::any_of(kWrites.begin(), kWrites.end(), [&](std::string_view w) {
                    return starts(op, w);
                });
        procedure = procedure || starts(op, "ProcedureCall");
        load_csv = load_csv || starts(op, "LoadCSV");
        in_transactions = in_transactions || op.find("TransactionForeach") != std::string::npos ||
                          op.find("TransactionApply") != std::string::npos;
    }
    if (write) {
        out.push_back("the statement's plan writes to the graph or changes the schema");
    }
    if (procedure) {
        out.push_back(
            "the statement calls a procedure; a lookup refuses procedure calls, because whether "
            "a procedure only reads cannot be established from the plan");
    }
    if (load_csv) {
        out.push_back(
            "the statement uses LOAD CSV, which reads a file or URL from the server; a lookup "
            "reads only the graph");
    }
    if (in_transactions) {
        out.push_back("the statement uses CALL { ... } IN TRANSACTIONS");
    }
    if (!explain.has_plan && !explain.query_type) {
        out.push_back(
            "the server reported neither a query type nor a plan for the statement, so it "
            "cannot be checked as read-only");
    }
    std::vector<std::string> missing;
    for (const auto& c : columns) {
        if (std::find(explain.fields.begin(), explain.fields.end(), c) == explain.fields.end()) {
            missing.push_back(c);
        }
    }
    std::vector<std::string> extra;
    for (const auto& f : explain.fields) {
        if (std::find(columns.begin(), columns.end(), f) == columns.end()) {
            extra.push_back(f);
        }
    }
    const auto joined = [](const std::vector<std::string>& v) {
        std::string s;
        for (const auto& x : v) {
            s += (s.empty() ? "" : ", ") + x;
        }
        return s;
    };
    if (!missing.empty() || !extra.empty()) {
        out.push_back("the statement returns (" + joined(explain.fields) +
                      ") but the lookup table's result columns are (" + joined(columns) +
                      "); RETURN each declared column by name with AS, and not the key or a "
                      "metadata column");
    }
    for (const auto& [code, description] : explain.notifications) {
        if (code == "Neo.ClientNotification.Statement.ParameterNotProvided") {
            out.push_back("the statement uses parameters other than $key (" + description + ")");
        }
    }
    return out;
}

std::vector<std::string> scan_operators(const Reply& explain) {
    std::vector<std::string> out;
    for (const auto& op : explain.plan_operators) {
        if (op == "AllNodesScan" || op.find("ByLabelScan") != std::string::npos ||
            op.find("ByTypeScan") != std::string::npos ||
            op.find("RelationshipTypeScan") != std::string::npos) {
            out.push_back(op);
        }
    }
    return out;
}

std::vector<std::string> statement_problems(const Reply& explain) {
    std::vector<std::string> out;
    if (explain.has_fields && !explain.fields.empty()) {
        std::string cols;
        for (const auto& f : explain.fields) {
            cols += (cols.empty() ? "" : ", ") + f;
        }
        out.push_back("the statement returns columns (" + cols +
                      "); the sink reads no results, so end it without RETURN");
    }
    for (const auto& op : explain.plan_operators) {
        if (op.find("TransactionForeach") != std::string::npos ||
            op.find("TransactionApply") != std::string::npos) {
            out.push_back(
                "the statement uses CALL { ... } IN TRANSACTIONS, which commits its own "
                "transactions, so a batch would no longer commit or roll back as one");
            break;
        }
    }
    if (explain.query_type) {
        if (*explain.query_type == "r") {
            out.push_back(
                "the statement only reads (query type r), so the sink would write nothing");
        } else if (*explain.query_type == "s") {
            out.push_back(
                "the statement changes the schema (query type s); create constraints and indexes "
                "before the job starts, not once per batch");
        }
    }
    for (const auto& [code, description] : explain.notifications) {
        if (code == "Neo.ClientNotification.Statement.ParameterNotProvided") {
            out.push_back("the statement uses parameters other than $rows (" + description + ")");
        }
    }
    return out;
}

}  // namespace clink::neo4j
