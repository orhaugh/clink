#include "clink/kafka/txn_resume.hpp"

#include <chrono>
#include <cstring>
#include <thread>

namespace clink::kafka {

namespace wire {

namespace {

void put_i8(std::vector<std::byte>& out, std::int8_t v) {
    out.push_back(static_cast<std::byte>(v));
}
void put_i16(std::vector<std::byte>& out, std::int16_t v) {
    out.push_back(static_cast<std::byte>((static_cast<std::uint16_t>(v) >> 8) & 0xFF));
    out.push_back(static_cast<std::byte>(static_cast<std::uint16_t>(v) & 0xFF));
}
void put_i32(std::vector<std::byte>& out, std::int32_t v) {
    for (int i = 3; i >= 0; --i) {
        out.push_back(static_cast<std::byte>((static_cast<std::uint32_t>(v) >> (i * 8)) & 0xFF));
    }
}
void put_i64(std::vector<std::byte>& out, std::int64_t v) {
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<std::byte>((static_cast<std::uint64_t>(v) >> (i * 8)) & 0xFF));
    }
}
void put_string(std::vector<std::byte>& out, const std::string& s) {
    put_i16(out, static_cast<std::int16_t>(s.size()));
    for (const char c : s) {
        out.push_back(static_cast<std::byte>(c));
    }
}

// Request header (non-flexible): api_key, api_version, correlation_id,
// nullable client_id. The size prefix is prepended at the end.
std::vector<std::byte> frame(std::int16_t api_key,
                             std::int16_t api_version,
                             std::int32_t correlation_id,
                             const std::vector<std::byte>& request_body) {
    std::vector<std::byte> payload;
    put_i16(payload, api_key);
    put_i16(payload, api_version);
    put_i32(payload, correlation_id);
    put_string(payload, "clink-txn-resume");
    payload.insert(payload.end(), request_body.begin(), request_body.end());

    std::vector<std::byte> out;
    put_i32(out, static_cast<std::int32_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

// Bounds-checked big-endian reader over a response body.
class Reader {
public:
    explicit Reader(const std::vector<std::byte>& b) : b_(b) {}

    std::optional<std::int16_t> i16() {
        if (pos_ + 2 > b_.size()) {
            return std::nullopt;
        }
        const auto v = static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(static_cast<std::uint8_t>(b_[pos_])) << 8) |
            static_cast<std::uint8_t>(b_[pos_ + 1]));
        pos_ += 2;
        return static_cast<std::int16_t>(v);
    }
    std::optional<std::int32_t> i32() {
        if (pos_ + 4 > b_.size()) {
            return std::nullopt;
        }
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v = (v << 8) | static_cast<std::uint8_t>(b_[pos_ + static_cast<std::size_t>(i)]);
        }
        pos_ += 4;
        return static_cast<std::int32_t>(v);
    }
    // Nullable string: length -1 = null (returned as empty).
    std::optional<std::string> string() {
        const auto len = i16();
        if (!len.has_value()) {
            return std::nullopt;
        }
        if (*len < 0) {
            return std::string{};
        }
        const auto n = static_cast<std::size_t>(*len);
        if (pos_ + n > b_.size()) {
            return std::nullopt;
        }
        std::string s(reinterpret_cast<const char*>(b_.data() + pos_), n);
        pos_ += n;
        return s;
    }

private:
    const std::vector<std::byte>& b_;
    std::size_t pos_{0};
};

}  // namespace

std::vector<std::byte> api_versions_request_v0(std::int32_t correlation_id) {
    return frame(kApiVersionsKey, 0, correlation_id, {});
}

std::vector<std::byte> find_coordinator_request(std::int16_t version,
                                                std::int32_t correlation_id,
                                                const std::string& txn_id) {
    std::vector<std::byte> body;
    put_string(body, txn_id);
    put_i8(body, 1);  // coordinator_type 1 = transaction
    return frame(kFindCoordinatorKey, version, correlation_id, body);
}

std::vector<std::byte> end_txn_request_v1(std::int32_t correlation_id,
                                          const TxnIdentity& txn,
                                          bool commit) {
    std::vector<std::byte> body;
    put_string(body, txn.transactional_id);
    put_i64(body, txn.producer_id);
    put_i16(body, txn.producer_epoch);
    put_i8(body, commit ? 1 : 0);
    return frame(kEndTxnKey, 1, correlation_id, body);
}

std::vector<std::byte> sasl_handshake_request_v1(std::int32_t correlation_id,
                                                 const std::string& mechanism) {
    std::vector<std::byte> body;
    put_string(body, mechanism);
    return frame(kSaslHandshakeKey, 1, correlation_id, body);
}

std::vector<std::byte> plain_auth_bytes(const std::string& username, const std::string& password) {
    // SASL/PLAIN (RFC 4616): [authzid] NUL authcid NUL passwd; empty authzid.
    std::vector<std::byte> out;
    out.push_back(std::byte{0});
    for (const char c : username) {
        out.push_back(static_cast<std::byte>(c));
    }
    out.push_back(std::byte{0});
    for (const char c : password) {
        out.push_back(static_cast<std::byte>(c));
    }
    return out;
}

std::vector<std::byte> sasl_authenticate_request_v1(std::int32_t correlation_id,
                                                    const std::vector<std::byte>& auth_bytes) {
    std::vector<std::byte> body;
    put_i32(body, static_cast<std::int32_t>(auth_bytes.size()));
    body.insert(body.end(), auth_bytes.begin(), auth_bytes.end());
    return frame(kSaslAuthenticateKey, 1, correlation_id, body);
}

std::optional<ApiVersionsResponse> parse_api_versions_response_v0(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id) {
    Reader r(body);
    const auto corr = r.i32();
    if (!corr.has_value() || *corr != expected_correlation_id) {
        return std::nullopt;
    }
    ApiVersionsResponse out;
    const auto err = r.i16();
    if (!err.has_value()) {
        return std::nullopt;
    }
    out.error_code = *err;
    const auto count = r.i32();
    if (!count.has_value() || *count < 0) {
        return std::nullopt;
    }
    for (std::int32_t i = 0; i < *count; ++i) {
        const auto key = r.i16();
        const auto min = r.i16();
        const auto max = r.i16();
        if (!key.has_value() || !min.has_value() || !max.has_value()) {
            return std::nullopt;
        }
        if (*key == kFindCoordinatorKey) {
            out.find_coordinator = ApiRange{*min, *max};
        } else if (*key == kEndTxnKey) {
            out.end_txn = ApiRange{*min, *max};
        }
    }
    return out;
}

std::optional<FindCoordinatorResponse> parse_find_coordinator_response(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id) {
    Reader r(body);
    const auto corr = r.i32();
    if (!corr.has_value() || *corr != expected_correlation_id) {
        return std::nullopt;
    }
    // v1/v2: throttle_time_ms, error_code, error_message, node_id, host, port.
    if (!r.i32().has_value()) {  // throttle
        return std::nullopt;
    }
    FindCoordinatorResponse out;
    const auto err = r.i16();
    if (!err.has_value()) {
        return std::nullopt;
    }
    out.error_code = *err;
    if (!r.string().has_value()) {  // error_message (nullable)
        return std::nullopt;
    }
    if (!r.i32().has_value()) {  // node_id
        return std::nullopt;
    }
    const auto host = r.string();
    const auto port = r.i32();
    if (!host.has_value() || !port.has_value()) {
        return std::nullopt;
    }
    out.host = *host;
    out.port = *port;
    return out;
}

std::optional<std::int16_t> parse_end_txn_response(const std::vector<std::byte>& body,
                                                   std::int32_t expected_correlation_id) {
    Reader r(body);
    const auto corr = r.i32();
    if (!corr.has_value() || *corr != expected_correlation_id) {
        return std::nullopt;
    }
    if (!r.i32().has_value()) {  // throttle_time_ms (v1)
        return std::nullopt;
    }
    return r.i16();
}

std::optional<SaslHandshakeResponse> parse_sasl_handshake_response_v1(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id) {
    Reader r(body);
    const auto corr = r.i32();
    if (!corr.has_value() || *corr != expected_correlation_id) {
        return std::nullopt;
    }
    // v1: error_code, mechanisms ARRAY<STRING>. No throttle field.
    SaslHandshakeResponse out;
    const auto err = r.i16();
    if (!err.has_value()) {
        return std::nullopt;
    }
    out.error_code = *err;
    const auto count = r.i32();
    if (!count.has_value() || *count < 0) {
        return std::nullopt;
    }
    for (std::int32_t i = 0; i < *count; ++i) {
        const auto m = r.string();
        if (!m.has_value()) {
            return std::nullopt;
        }
        out.mechanisms.push_back(*m);
    }
    return out;
}

std::optional<SaslAuthenticateResponse> parse_sasl_authenticate_response_v1(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id) {
    Reader r(body);
    const auto corr = r.i32();
    if (!corr.has_value() || *corr != expected_correlation_id) {
        return std::nullopt;
    }
    // v1: error_code, error_message (nullable), auth_bytes, session_lifetime_ms.
    // The tail past error_message is deliberately not consumed.
    SaslAuthenticateResponse out;
    const auto err = r.i16();
    if (!err.has_value()) {
        return std::nullopt;
    }
    out.error_code = *err;
    const auto msg = r.string();
    if (!msg.has_value()) {
        return std::nullopt;
    }
    out.error_message = *msg;
    return out;
}

std::string error_name(std::int16_t code) {
    switch (code) {
        case 0:
            return "NONE";
        case 14:
            return "COORDINATOR_LOAD_IN_PROGRESS";
        case 15:
            return "COORDINATOR_NOT_AVAILABLE";
        case 16:
            return "NOT_COORDINATOR";
        case 33:
            return "UNSUPPORTED_SASL_MECHANISM";
        case 34:
            return "ILLEGAL_SASL_STATE";
        case 35:
            return "UNSUPPORTED_VERSION";
        case 47:
            return "INVALID_PRODUCER_EPOCH";
        case 48:
            return "INVALID_TXN_STATE";
        case 53:
            return "TRANSACTIONAL_ID_AUTHORIZATION_FAILED";
        case 58:
            return "SASL_AUTHENTICATION_FAILED";
        case 90:
            return "PRODUCER_FENCED";
        default:
            return "error " + std::to_string(code);
    }
}

}  // namespace wire

namespace {

using clink::network::Connection;

// One request/response exchange. nullopt = transport failure (the response
// body excludes the size prefix).
std::optional<std::vector<std::byte>> roundtrip(Connection& conn,
                                                const std::vector<std::byte>& request) {
    if (!conn.send_all(request.data(), request.size())) {
        return std::nullopt;
    }
    std::byte size_buf[4];
    if (!conn.recv_all(size_buf, 4)) {
        return std::nullopt;
    }
    std::uint32_t size = 0;
    for (int i = 0; i < 4; ++i) {
        size = (size << 8) | static_cast<std::uint8_t>(size_buf[i]);
    }
    // A response to these three tiny requests has no business being large;
    // a huge length here is a framing error or a hostile peer, and the cap
    // keeps recovery from allocating on a bad length (the decode_submit_
    // job_ack lesson).
    constexpr std::uint32_t kMaxResponse = 1u << 20;
    if (size == 0 || size > kMaxResponse) {
        return std::nullopt;
    }
    std::vector<std::byte> body(size);
    if (!conn.recv_all(body.data(), body.size())) {
        return std::nullopt;
    }
    return body;
}

bool retriable(std::int16_t code) {
    return code == 14 || code == 15 || code == 16;
}

// The SASL step, run once per connection before any other request. Three
// outcomes: authenticated (nullopt), a FINAL refusal (credentials do not
// improve on retry - carries the outcome to return), or a transport
// failure (carries a detail; the caller's retry loop decides).
struct SaslStepFailure {
    bool transport{false};
    ResumeOutcome outcome;  // meaningful when !transport
    std::string detail;     // meaningful when transport
};
std::optional<SaslStepFailure> authenticate(Connection& conn,
                                            const ResumeAuth& auth,
                                            std::int32_t& corr) {
    using Status = ResumeOutcome::Status;
    if (!auth.enabled()) {
        return std::nullopt;
    }
    ++corr;
    const auto hs_body = roundtrip(conn, wire::sasl_handshake_request_v1(corr, auth.mechanism));
    if (!hs_body.has_value()) {
        return SaslStepFailure{
            .transport = true, .outcome = {}, .detail = "SaslHandshake transport failure"};
    }
    const auto hs = wire::parse_sasl_handshake_response_v1(*hs_body, corr);
    if (!hs.has_value()) {
        return SaslStepFailure{
            .transport = true, .outcome = {}, .detail = "SaslHandshake response did not parse"};
    }
    if (hs->error_code != 0) {
        std::string enabled;
        for (const auto& m : hs->mechanisms) {
            enabled += (enabled.empty() ? "" : ", ") + m;
        }
        return SaslStepFailure{
            .transport = false,
            .outcome = {Status::Refused,
                        "SaslHandshake: " + wire::error_name(hs->error_code) + " (mechanism '" +
                            auth.mechanism + "'; broker enables: " +
                            (enabled.empty() ? "<none listed>" : enabled) + ")"},
            .detail = {}};
    }
    ++corr;
    const auto au_body = roundtrip(conn,
                                   wire::sasl_authenticate_request_v1(
                                       corr, wire::plain_auth_bytes(auth.username, auth.password)));
    if (!au_body.has_value()) {
        return SaslStepFailure{
            .transport = true, .outcome = {}, .detail = "SaslAuthenticate transport failure"};
    }
    const auto au = wire::parse_sasl_authenticate_response_v1(*au_body, corr);
    if (!au.has_value()) {
        return SaslStepFailure{
            .transport = true, .outcome = {}, .detail = "SaslAuthenticate response did not parse"};
    }
    if (au->error_code != 0) {
        return SaslStepFailure{
            .transport = false,
            .outcome = {Status::Refused,
                        "SaslAuthenticate: " + wire::error_name(au->error_code) +
                            (au->error_message.empty() ? "" : " (" + au->error_message + ")")},
            .detail = {}};
    }
    return std::nullopt;
}

}  // namespace

ResumeOutcome resume_commit(const std::string& bootstrap_host,
                            std::uint16_t bootstrap_port,
                            const TxnIdentity& txn,
                            const ConnectFn& connect,
                            const ResumeAuth& auth) {
    using Status = ResumeOutcome::Status;
    if (!txn.complete()) {
        return {Status::Refused,
                "handle carries no producer identity (pid/epoch were never captured); "
                "nothing can be resumed"};
    }
    // A mechanism this module does not speak is refused HERE, before a byte
    // goes out: silently proceeding unauthenticated would be a downgrade.
    if (auth.enabled() && auth.mechanism != "PLAIN") {
        return {Status::Refused,
                "SASL mechanism '" + auth.mechanism +
                    "' is not spoken by the resume path (PLAIN only); refusing rather than "
                    "downgrading to unauthenticated"};
    }

    // Bounded retry around the coordinator dance: leadership can be mid-move
    // at exactly the moment recovery runs (that is what recovery means).
    constexpr int kAttempts = 3;
    std::string last_detail;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        auto bootstrap = connect(bootstrap_host, bootstrap_port);
        if (bootstrap == nullptr) {
            last_detail = "cannot connect to bootstrap " + bootstrap_host + ":" +
                          std::to_string(bootstrap_port);
            continue;
        }
        (void)bootstrap->set_recv_timeout(std::chrono::milliseconds(5000));

        // 0. Authenticate this connection, when the caller has credentials.
        std::int32_t corr = 0;
        if (const auto sasl = authenticate(*bootstrap, auth, corr); sasl.has_value()) {
            if (!sasl->transport) {
                return sasl->outcome;  // refusals are final
            }
            last_detail = sasl->detail + " against bootstrap";
            continue;
        }

        // 1. What does this broker still speak?
        ++corr;
        const auto av_body = roundtrip(*bootstrap, wire::api_versions_request_v0(corr));
        if (!av_body.has_value()) {
            last_detail = "ApiVersions transport failure against bootstrap";
            continue;
        }
        const auto av = wire::parse_api_versions_response_v0(*av_body, corr);
        if (!av.has_value()) {
            last_detail = "ApiVersions response did not parse";
            continue;
        }
        if (av->error_code != 0) {
            return {Status::Unsupported, "ApiVersions: " + wire::error_name(av->error_code)};
        }
        if (!av->find_coordinator.has_value() || !av->end_txn.has_value()) {
            return {Status::Unsupported, "broker lists no FindCoordinator/EndTxn support"};
        }
        // Non-flexible versions only: FindCoordinator 1..2, EndTxn exactly 1.
        std::int16_t fc_version = 0;
        if (av->find_coordinator->min <= 1 && av->find_coordinator->max >= 1) {
            fc_version = 1;
        } else if (av->find_coordinator->min <= 2 && av->find_coordinator->max >= 2) {
            fc_version = 2;
        } else {
            return {Status::Unsupported,
                    "broker FindCoordinator range [" + std::to_string(av->find_coordinator->min) +
                        "," + std::to_string(av->find_coordinator->max) +
                        "] excludes the non-flexible versions this module speaks"};
        }
        if (av->end_txn->min > 1 || av->end_txn->max < 1) {
            return {Status::Unsupported,
                    "broker EndTxn range [" + std::to_string(av->end_txn->min) + "," +
                        std::to_string(av->end_txn->max) +
                        "] excludes v1 (the non-flexible version this module speaks)"};
        }

        // 2. Which broker coordinates this transactional.id?
        ++corr;
        const auto fc_body = roundtrip(
            *bootstrap, wire::find_coordinator_request(fc_version, corr, txn.transactional_id));
        if (!fc_body.has_value()) {
            last_detail = "FindCoordinator transport failure";
            continue;
        }
        const auto fc = wire::parse_find_coordinator_response(*fc_body, corr);
        if (!fc.has_value()) {
            last_detail = "FindCoordinator response did not parse";
            continue;
        }
        if (fc->error_code != 0) {
            last_detail = "FindCoordinator: " + wire::error_name(fc->error_code);
            if (retriable(fc->error_code)) {
                continue;
            }
            return {Status::Refused, last_detail};
        }

        // 3. Commit, on the coordinator itself. The bootstrap connection has
        // served its purpose; release it before dialing rather than holding
        // a socket across the exchange that matters.
        bootstrap.reset();
        auto coord = connect(fc->host, static_cast<std::uint16_t>(fc->port));
        if (coord == nullptr) {
            last_detail =
                "cannot connect to coordinator " + fc->host + ":" + std::to_string(fc->port);
            continue;
        }
        (void)coord->set_recv_timeout(std::chrono::milliseconds(5000));
        // The coordinator connection is a fresh session: authenticate it too.
        if (const auto sasl = authenticate(*coord, auth, corr); sasl.has_value()) {
            if (!sasl->transport) {
                return sasl->outcome;
            }
            last_detail = sasl->detail + " against coordinator";
            continue;
        }
        ++corr;
        const auto et_body =
            roundtrip(*coord, wire::end_txn_request_v1(corr, txn, /*commit=*/true));
        if (!et_body.has_value()) {
            last_detail = "EndTxn transport failure";
            continue;
        }
        const auto code = wire::parse_end_txn_response(*et_body, corr);
        if (!code.has_value()) {
            last_detail = "EndTxn response did not parse";
            continue;
        }
        if (*code == 0) {
            return {Status::Committed,
                    "EndTxn(commit) accepted for '" + txn.transactional_id + "' pid " +
                        std::to_string(txn.producer_id) + " epoch " +
                        std::to_string(txn.producer_epoch)};
        }
        last_detail = "EndTxn: " + wire::error_name(*code);
        if (retriable(*code)) {
            continue;
        }
        return {Status::Refused, last_detail};
    }
    return {Status::TransportError, last_detail + " (after " + std::to_string(3) + " attempts)"};
}

}  // namespace clink::kafka
