#include "clink/kafka/txn_resume.hpp"

#ifdef CLINK_KAFKA_RESUME_TLS
#include "clink/kafka/scram.hpp"
#endif

#include <chrono>
#include <cstring>
#include <exception>
#include <string>
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
    std::optional<std::vector<std::byte>> bytes(std::size_t n) {
        if (pos_ + n > b_.size()) {
            return std::nullopt;
        }
        std::vector<std::byte> out(b_.begin() + static_cast<std::ptrdiff_t>(pos_),
                                   b_.begin() + static_cast<std::ptrdiff_t>(pos_ + n));
        pos_ += n;
        return out;
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

    // --- flexible (compact) protocol pieces, for KIP-664 admin replies ---

    std::optional<std::int64_t> i64() {
        if (pos_ + 8 > b_.size()) {
            return std::nullopt;
        }
        std::uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<std::uint8_t>(b_[pos_ + static_cast<std::size_t>(i)]);
        }
        pos_ += 8;
        return static_cast<std::int64_t>(v);
    }

    std::optional<std::uint64_t> uvarint() {
        std::uint64_t v = 0;
        int shift = 0;
        while (pos_ < b_.size() && shift <= 63) {
            const auto byte = static_cast<std::uint8_t>(b_[pos_++]);
            v |= static_cast<std::uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                return v;
            }
            shift += 7;
        }
        return std::nullopt;
    }

    // COMPACT_STRING: uvarint length+1 (0 = null, returned as empty).
    std::optional<std::string> compact_string() {
        const auto len_plus_one = uvarint();
        if (!len_plus_one.has_value()) {
            return std::nullopt;
        }
        if (*len_plus_one == 0) {
            return std::string{};
        }
        const auto n = static_cast<std::size_t>(*len_plus_one - 1);
        if (pos_ + n > b_.size()) {
            return std::nullopt;
        }
        std::string s(reinterpret_cast<const char*>(b_.data() + pos_), n);
        pos_ += n;
        return s;
    }

    // Tagged-field buffer: uvarint count, then per field uvarint tag +
    // uvarint size + size bytes. Everything here is skipped - this module
    // consumes no tagged fields.
    bool skip_tags() {
        const auto count = uvarint();
        if (!count.has_value()) {
            return false;
        }
        for (std::uint64_t i = 0; i < *count; ++i) {
            if (!uvarint().has_value()) {
                return false;
            }
            const auto size = uvarint();
            if (!size.has_value() || pos_ + *size > b_.size()) {
                return false;
            }
            pos_ += static_cast<std::size_t>(*size);
        }
        return true;
    }

private:
    const std::vector<std::byte>& b_;
    std::size_t pos_{0};
};

void put_uvarint(std::vector<std::byte>& out, std::uint64_t v) {
    while (v >= 0x80) {
        out.push_back(static_cast<std::byte>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    out.push_back(static_cast<std::byte>(v));
}

void put_compact_string(std::vector<std::byte>& out, const std::string& s) {
    put_uvarint(out, s.size() + 1);
    for (const char c : s) {
        out.push_back(static_cast<std::byte>(c));
    }
}

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

std::vector<std::byte> describe_transactions_request_v0(std::int32_t correlation_id,
                                                        const std::string& txn_id) {
    // Flexible protocol: request header v2 (classic nullable client_id
    // string PLUS a tagged-field buffer), compact body. The one flexible
    // request this module speaks - KIP-664's admin keys have no
    // non-flexible versions.
    std::vector<std::byte> payload;
    put_i16(payload, kDescribeTransactionsKey);
    put_i16(payload, 0);
    put_i32(payload, correlation_id);
    put_string(payload, "clink-txn-resume");
    put_uvarint(payload, 0);  // header tag buffer
    // Body: transactional_ids COMPACT_ARRAY<COMPACT_STRING> + tag buffer.
    put_uvarint(payload, 2);  // array length + 1
    put_compact_string(payload, txn_id);
    put_uvarint(payload, 0);  // body tag buffer

    std::vector<std::byte> out;
    put_i32(out, static_cast<std::int32_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
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
        } else if (*key == kDescribeTransactionsKey) {
            out.describe_transactions = ApiRange{*min, *max};
        }
    }
    return out;
}

std::optional<DescribeTransactionsState> parse_describe_transactions_response_v0(
    const std::vector<std::byte>& body, std::int32_t expected_correlation_id) {
    Reader r(body);
    const auto corr = r.i32();
    if (!corr.has_value() || *corr != expected_correlation_id) {
        return std::nullopt;
    }
    // Flexible response header v1: a tag buffer follows the correlation id.
    if (!r.skip_tags()) {
        return std::nullopt;
    }
    // Body v0: throttle_time_ms, transaction_states COMPACT_ARRAY of
    // [error_code, transactional_id, state, timeout_ms, start_time_ms,
    // producer_id, producer_epoch, topics COMPACT_ARRAY, TAG_BUFFER],
    // TAG_BUFFER. One id was asked about; the first entry answers, and the
    // remainder (topics onward) is not consumed.
    if (!r.i32().has_value()) {  // throttle
        return std::nullopt;
    }
    const auto states_plus_one = r.uvarint();
    if (!states_plus_one.has_value() || *states_plus_one < 2) {
        return std::nullopt;  // null or empty array: no answer
    }
    DescribeTransactionsState out;
    const auto err = r.i16();
    if (!err.has_value()) {
        return std::nullopt;
    }
    out.error_code = *err;
    if (!r.compact_string().has_value()) {  // transactional_id (echoed)
        return std::nullopt;
    }
    const auto state = r.compact_string();
    if (!state.has_value()) {
        return std::nullopt;
    }
    out.state = *state;
    if (!r.i32().has_value()) {  // timeout_ms
        return std::nullopt;
    }
    if (!r.i64().has_value()) {  // start_time_ms
        return std::nullopt;
    }
    const auto pid = r.i64();
    const auto epoch = r.i16();
    if (!pid.has_value() || !epoch.has_value()) {
        return std::nullopt;
    }
    out.producer_id = *pid;
    out.producer_epoch = *epoch;
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
    // auth_bytes: BYTES (i32 length, -1 = null). Absent entirely (a terse
    // fake's truncated tail) parses as empty rather than failing - PLAIN
    // needs nothing from it, and SCRAM's brokers always send it.
    if (const auto len = r.i32(); len.has_value() && *len > 0) {
        out.auth_bytes = r.bytes(static_cast<std::size_t>(*len)).value_or(std::vector<std::byte>{});
    }
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
        case 51:
            return "CONCURRENT_TRANSACTIONS";
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
    // One SaslAuthenticate round: send payload, surface transport/refusal,
    // return the server's auth_bytes on success.
    const auto sasl_round = [&](const std::vector<std::byte>& payload)
        -> std::pair<std::optional<SaslStepFailure>, std::vector<std::byte>> {
        ++corr;
        const auto au_body = roundtrip(conn, wire::sasl_authenticate_request_v1(corr, payload));
        if (!au_body.has_value()) {
            return {SaslStepFailure{.transport = true,
                                    .outcome = {},
                                    .detail = "SaslAuthenticate transport failure"},
                    {}};
        }
        const auto au = wire::parse_sasl_authenticate_response_v1(*au_body, corr);
        if (!au.has_value()) {
            return {SaslStepFailure{.transport = true,
                                    .outcome = {},
                                    .detail = "SaslAuthenticate response did not parse"},
                    {}};
        }
        if (au->error_code != 0) {
            return {
                SaslStepFailure{
                    .transport = false,
                    .outcome = {Status::Refused,
                                "SaslAuthenticate: " + wire::error_name(au->error_code) +
                                    (au->error_message.empty() ? ""
                                                               : " (" + au->error_message + ")")},
                    .detail = {}},
                {}};
        }
        return {std::nullopt, au->auth_bytes};
    };

    if (auth.mechanism == "PLAIN") {
        auto [failure, ignored] = sasl_round(wire::plain_auth_bytes(auth.username, auth.password));
        (void)ignored;
        return failure;
    }

#ifdef CLINK_KAFKA_RESUME_TLS
    // SCRAM-SHA-256 (RFC 5802/7677): two rounds, and the SERVER is verified
    // too - a broker that cannot prove knowledge of the credentials via the
    // ServerSignature is refused, not trusted.
    const auto as_bytes = [](const std::string& s) {
        return std::vector<std::byte>(reinterpret_cast<const std::byte*>(s.data()),
                                      reinterpret_cast<const std::byte*>(s.data()) + s.size());
    };
    const auto first = scram::client_first(auth.username, scram::random_nonce());
    auto [f1, server_first_bytes] = sasl_round(as_bytes(first.full));
    if (f1.has_value()) {
        return f1;
    }
    const std::string server_first_msg(reinterpret_cast<const char*>(server_first_bytes.data()),
                                       server_first_bytes.size());
    const auto server_first = scram::parse_server_first(server_first_msg);
    if (!server_first.has_value()) {
        return SaslStepFailure{
            .transport = false,
            .outcome = {Status::Refused,
                        "SCRAM server-first message was malformed or demanded a KDF below the "
                        "RFC 7677 floor: " +
                            server_first_msg},
            .detail = {}};
    }
    const auto final_msg = scram::client_final(auth.password, first, *server_first);
    if (!final_msg.has_value()) {
        return SaslStepFailure{
            .transport = false,
            .outcome = {Status::Refused,
                        "SCRAM server nonce did not extend the client's (reflected or foreign "
                        "nonce); refusing"},
            .detail = {}};
    }
    auto [f2, server_final_bytes] = sasl_round(as_bytes(final_msg->message));
    if (f2.has_value()) {
        return f2;
    }
    const std::string server_final_msg(reinterpret_cast<const char*>(server_final_bytes.data()),
                                       server_final_bytes.size());
    const auto server_sig = scram::parse_server_final_signature(server_final_msg);
    if (!server_sig.has_value() || *server_sig != final_msg->expected_server_signature) {
        return SaslStepFailure{
            .transport = false,
            .outcome = {Status::Refused,
                        "SCRAM server signature mismatch: the broker did not prove knowledge "
                        "of the credentials; refusing rather than trusting it"},
            .detail = {}};
    }
    return std::nullopt;
#else
    return SaslStepFailure{
        .transport = false,
        .outcome = {Status::Refused,
                    "SASL mechanism '" + auth.mechanism +
                        "' needs the crypto that ships with clink::tls, which this build "
                        "lacks; refusing rather than downgrading"},
        .detail = {}};
#endif
}

// What an exchange needs the bootstrap to advertise beyond FindCoordinator.
struct Needs {
    bool end_txn{false};   // EndTxn v1
    bool describe{false};  // DescribeTransactions v0
};

// An authenticated connection to the transactional id's coordinator, the
// versions the bootstrap advertised, and the last correlation id used.
struct CoordinatorLink {
    std::unique_ptr<Connection> conn;
    std::int32_t corr{0};
    wire::ApiVersionsResponse versions;
};

// The dance every exchange here shares: [SASL] -> ApiVersions ->
// FindCoordinator(transactional.id) on the bootstrap, then a fresh
// connection to the coordinator -> [SASL]. Three outcomes: the link; a FINAL
// answer retrying cannot change (`final`: a refused authentication, a
// non-retriable FindCoordinator error, versions this module does not speak),
// carried as the ResumeOutcome status it maps to; or a transport failure
// (neither), which the caller's retry loop decides.
struct DanceOutcome {
    std::optional<CoordinatorLink> link;
    bool final{false};
    ResumeOutcome::Status status{ResumeOutcome::Status::TransportError};
    std::string detail;
};

DanceOutcome reach_coordinator(const std::string& bootstrap_host,
                               std::uint16_t bootstrap_port,
                               const std::string& transactional_id,
                               const ConnectFn& connect,
                               const ResumeAuth& auth,
                               Needs needs) {
    using Status = ResumeOutcome::Status;
    const auto transport = [](std::string detail) {
        return DanceOutcome{std::nullopt, false, Status::TransportError, std::move(detail)};
    };
    const auto final_answer = [](Status status, std::string detail) {
        return DanceOutcome{std::nullopt, true, status, std::move(detail)};
    };

    // A failed connect returns nullptr or throws (see ConnectFn); the reason a
    // throw gives, a certificate that does not name the broker, say, is the
    // one an operator needs, so it goes into the detail.
    const auto dial = [&connect](const std::string& host,
                                 std::uint16_t port,
                                 std::string& why) -> std::unique_ptr<network::Connection> {
        try {
            return connect(host, port);
        } catch (const std::exception& e) {
            why = std::string{": "} + e.what();
            return nullptr;
        }
    };
    std::string why;
    auto bootstrap = dial(bootstrap_host, bootstrap_port, why);
    if (bootstrap == nullptr) {
        return transport("cannot connect to bootstrap " + bootstrap_host + ":" +
                         std::to_string(bootstrap_port) + why);
    }
    (void)bootstrap->set_recv_timeout(std::chrono::milliseconds(5000));

    // 0. Authenticate this connection, when the caller has credentials.
    std::int32_t corr = 0;
    if (const auto sasl = authenticate(*bootstrap, auth, corr); sasl.has_value()) {
        if (!sasl->transport) {
            return final_answer(sasl->outcome.status, sasl->outcome.detail);  // refusals are final
        }
        return transport(sasl->detail + " against bootstrap");
    }

    // 1. What does this broker still speak?
    ++corr;
    const auto av_body = roundtrip(*bootstrap, wire::api_versions_request_v0(corr));
    if (!av_body.has_value()) {
        return transport("ApiVersions transport failure against bootstrap");
    }
    const auto av = wire::parse_api_versions_response_v0(*av_body, corr);
    if (!av.has_value()) {
        return transport("ApiVersions response did not parse");
    }
    if (av->error_code != 0) {
        return final_answer(Status::Unsupported,
                            "ApiVersions: " + wire::error_name(av->error_code));
    }
    if (!av->find_coordinator.has_value() || (needs.end_txn && !av->end_txn.has_value())) {
        return final_answer(Status::Unsupported,
                            needs.end_txn ? "broker lists no FindCoordinator/EndTxn support"
                                          : "broker lists no FindCoordinator support");
    }
    // Non-flexible versions only: FindCoordinator 1..2, EndTxn exactly 1.
    std::int16_t fc_version = 0;
    if (av->find_coordinator->min <= 1 && av->find_coordinator->max >= 1) {
        fc_version = 1;
    } else if (av->find_coordinator->min <= 2 && av->find_coordinator->max >= 2) {
        fc_version = 2;
    } else {
        return final_answer(Status::Unsupported,
                            "broker FindCoordinator range [" +
                                std::to_string(av->find_coordinator->min) + "," +
                                std::to_string(av->find_coordinator->max) +
                                "] excludes the non-flexible versions this module speaks");
    }
    if (needs.end_txn && (av->end_txn->min > 1 || av->end_txn->max < 1)) {
        return final_answer(Status::Unsupported,
                            "broker EndTxn range [" + std::to_string(av->end_txn->min) + "," +
                                std::to_string(av->end_txn->max) +
                                "] excludes v1 (the non-flexible version this module speaks)");
    }
    // DescribeTransactions has only flexible versions; v0 is the one spoken.
    if (needs.describe &&
        (!av->describe_transactions.has_value() || av->describe_transactions->min > 0)) {
        return final_answer(Status::Unsupported, "broker lists no DescribeTransactions v0 support");
    }

    // 2. Which broker coordinates this transactional.id?
    ++corr;
    const auto fc_body =
        roundtrip(*bootstrap, wire::find_coordinator_request(fc_version, corr, transactional_id));
    if (!fc_body.has_value()) {
        return transport("FindCoordinator transport failure");
    }
    const auto fc = wire::parse_find_coordinator_response(*fc_body, corr);
    if (!fc.has_value()) {
        return transport("FindCoordinator response did not parse");
    }
    if (fc->error_code != 0) {
        auto detail = "FindCoordinator: " + wire::error_name(fc->error_code);
        if (retriable(fc->error_code)) {
            return transport(std::move(detail));
        }
        return final_answer(Status::Refused, std::move(detail));
    }

    // 3. The coordinator itself. The bootstrap connection has served its
    // purpose; release it before dialing rather than holding a socket across
    // the exchange that matters.
    bootstrap.reset();
    auto coord = dial(fc->host, static_cast<std::uint16_t>(fc->port), why);
    if (coord == nullptr) {
        return transport("cannot connect to coordinator " + fc->host + ":" +
                         std::to_string(fc->port) + why);
    }
    (void)coord->set_recv_timeout(std::chrono::milliseconds(5000));
    // The coordinator connection is a fresh session: authenticate it too.
    if (const auto sasl = authenticate(*coord, auth, corr); sasl.has_value()) {
        if (!sasl->transport) {
            return final_answer(sasl->outcome.status, sasl->outcome.detail);
        }
        return transport(sasl->detail + " against coordinator");
    }
    return DanceOutcome{CoordinatorLink{std::move(coord), corr, *av}, false, Status::Committed, {}};
}

// DescribeTransactions v0 on an established coordinator link. nullopt, with
// `detail` set, when no state came back: a transport failure, an unparseable
// response, or a broker error.
std::optional<DescribeState> describe_on(CoordinatorLink& link,
                                         const std::string& transactional_id,
                                         std::string& detail) {
    ++link.corr;
    const auto dt_body =
        roundtrip(*link.conn, wire::describe_transactions_request_v0(link.corr, transactional_id));
    if (!dt_body.has_value()) {
        detail = "DescribeTransactions transport failure";
        return std::nullopt;
    }
    const auto dt = wire::parse_describe_transactions_response_v0(*dt_body, link.corr);
    if (!dt.has_value()) {
        detail = "DescribeTransactions response did not parse";
        return std::nullopt;
    }
    if (dt->error_code != 0) {
        detail = "DescribeTransactions: " + wire::error_name(dt->error_code);
        return std::nullopt;
    }
    return DescribeState{
        .state = dt->state, .producer_id = dt->producer_id, .producer_epoch = dt->producer_epoch};
}

std::string describe_text(const DescribeState& d) {
    return "state=" + d.state + " pid=" + std::to_string(d.producer_id) +
           " epoch=" + std::to_string(d.producer_epoch);
}

// A mechanism this module does not speak is refused before a byte goes out:
// silently proceeding unauthenticated would be a downgrade.
std::optional<std::string> unspoken_mechanism(const ResumeAuth& auth) {
    if (auth.enabled() && auth.mechanism != "PLAIN" && auth.mechanism != "SCRAM-SHA-256") {
        return "SASL mechanism '" + auth.mechanism +
               "' is not spoken by the resume path (PLAIN and SCRAM-SHA-256 only); "
               "refusing rather than downgrading to unauthenticated";
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
    if (const auto refusal = unspoken_mechanism(auth); refusal.has_value()) {
        return {Status::Refused, *refusal};
    }

    // Bounded retry around the coordinator dance: leadership can be mid-move
    // at exactly the moment recovery runs (that is what recovery means).
    constexpr int kAttempts = 3;
    std::string last_detail;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        auto dance = reach_coordinator(bootstrap_host,
                                       bootstrap_port,
                                       txn.transactional_id,
                                       connect,
                                       auth,
                                       Needs{.end_txn = true});
        if (!dance.link.has_value()) {
            if (dance.final) {
                return {dance.status, dance.detail};
            }
            last_detail = dance.detail;
            continue;
        }
        auto& link = *dance.link;
        const auto& av = link.versions;

        // Commit, on the coordinator itself.
        ++link.corr;
        const auto et_body =
            roundtrip(*link.conn, wire::end_txn_request_v1(link.corr, txn, /*commit=*/true));
        if (!et_body.has_value()) {
            last_detail = "EndTxn transport failure";
            continue;
        }
        const auto code = wire::parse_end_txn_response(*et_body, link.corr);
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
        // A fenced EndTxn is ambiguous BY CONSTRUCTION: the staged epoch
        // comes from librdkafka's periodic statistics callback while the
        // broker bumps the producer epoch on every commit, so a handle
        // staged mid-run is chronically one commit stale and a COMMITTED
        // transaction reads as fenced (qual01-20260818d: 4,560 duplicates
        // from exactly this misreading). The transaction's actual state
        // disambiguates. CompleteCommit under our producer id with an
        // epoch at or above the staged one is the commit landing; this is
        // sound because every resolution walk runs before anything
        // redeploys, so no successor can have produced that state.
        if ((*code == 47 /*INVALID_PRODUCER_EPOCH*/ || *code == 90 /*PRODUCER_FENCED*/) &&
            av.describe_transactions.has_value() && av.describe_transactions->min <= 0) {
            std::string dt_detail;
            const auto dt = describe_on(link, txn.transactional_id, dt_detail);
            if (dt.has_value() && dt->state == "CompleteCommit" &&
                dt->producer_id == txn.producer_id && dt->producer_epoch >= txn.producer_epoch) {
                return {Status::Committed,
                        "DescribeTransactions proves CompleteCommit for '" + txn.transactional_id +
                            "' pid " + std::to_string(txn.producer_id) + " (staged epoch " +
                            std::to_string(txn.producer_epoch) + " was stale, broker epoch " +
                            std::to_string(dt->producer_epoch) +
                            "); the fenced EndTxn was a stale-epoch artefact, not a verdict"};
            }
            if (dt.has_value()) {
                last_detail += "; DescribeTransactions: " + describe_text(*dt);
            } else if (dt_detail.rfind("DescribeTransactions: ", 0) == 0) {
                last_detail += "; " + dt_detail;  // the broker answered with an error
            }
        }
        return {Status::Refused, last_detail};
    }
    return {Status::TransportError,
            last_detail + " (after " + std::to_string(kAttempts) + " attempts)"};
}

std::optional<DescribeState> describe_transaction_state(const std::string& bootstrap_host,
                                                        std::uint16_t bootstrap_port,
                                                        const std::string& transactional_id,
                                                        const ConnectFn& connect,
                                                        const ResumeAuth& auth) {
    // The read-only half of resume_commit's dance. Single attempt; the
    // caller polls (it exists to BE a wait condition).
    auto dance = reach_coordinator(
        bootstrap_host, bootstrap_port, transactional_id, connect, auth, Needs{.describe = true});
    if (!dance.link.has_value()) {
        return std::nullopt;
    }
    std::string detail;
    return describe_on(*dance.link, transactional_id, detail);
}

OrphanSettlement settle_orphan(const std::string& bootstrap_host,
                               std::uint16_t bootstrap_port,
                               const TxnIdentity& staged,
                               const ConnectFn& connect,
                               const ResumeAuth& auth,
                               const std::function<void()>& before_abort) {
    using Status = OrphanSettlement::Status;
    OrphanSettlement out;
    out.floor_epoch = staged.producer_epoch;
    if (!staged.complete()) {
        out.detail =
            "the orphan's handle carries no producer identity (pid/epoch were never captured); "
            "it cannot be described or ended";
        return out;
    }
    if (const auto refusal = unspoken_mechanism(auth); refusal.has_value()) {
        out.detail = *refusal;
        return out;
    }
    const auto verdict = [&out](Status status, const DescribeState& d, std::string detail) {
        out.status = status;
        out.state = d.state;
        out.producer_id = d.producer_id;
        out.producer_epoch = d.producer_epoch;
        out.detail = std::move(detail);
        return out;
    };

    // Bounded rounds. A refused abort costs one: the next round describes
    // again, which is where the verdict comes from. Each round redoes the
    // dance, since a coordinator that refused or dropped a request may also
    // have moved.
    constexpr int kRounds = 4;
    std::int16_t floor_epoch = staged.producer_epoch;
    std::string last_detail;
    std::string refused;  // the last abort's refusal, reported beside the verdict
    for (int round = 0; round < kRounds; ++round) {
        if (round > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        auto dance = reach_coordinator(bootstrap_host,
                                       bootstrap_port,
                                       staged.transactional_id,
                                       connect,
                                       auth,
                                       Needs{.end_txn = true, .describe = true});
        if (!dance.link.has_value()) {
            // No coordinator to ask: unreachable, refused credentials, or
            // versions this module does not speak. The caller's attempts and
            // deadline are the retry; rounds here would multiply its wait by
            // kRounds against a broker that is down, and an open that waits
            // that long is cancelled by the restart before it can refuse.
            last_detail = dance.detail;
            break;
        }
        auto& link = *dance.link;
        std::string dt_detail;
        const auto observed = describe_on(link, staged.transactional_id, dt_detail);
        if (!observed.has_value()) {
            last_detail = dt_detail;
            continue;
        }
        out.state = observed->state;
        out.producer_id = observed->producer_id;
        out.producer_epoch = observed->producer_epoch;
        switch (next_settle_step(*observed, staged.producer_id, floor_epoch)) {
            case SettleStep::Committed:
                return verdict(Status::Committed,
                               *observed,
                               "DescribeTransactions proves the orphan's commit (" +
                                   describe_text(*observed) + ", floor epoch " +
                                   std::to_string(floor_epoch) + ")" + refused);
            case SettleStep::NotCommitted:
                return verdict(Status::NotCommitted,
                               *observed,
                               "DescribeTransactions finds the orphan decided without a commit (" +
                                   describe_text(*observed) +
                                   (observed->producer_id == staged.producer_id
                                        ? ""
                                        : ", not the staged producer id " +
                                              std::to_string(staged.producer_id)) +
                                   ")" + refused);
            case SettleStep::Retry:
                last_detail = "DescribeTransactions reported a state this module does not know (" +
                              describe_text(*observed) + ")";
                continue;
            case SettleStep::AbortWith:
                break;
        }

        // Undecided: end it under the identity the describe just reported.
        // The staged epoch rides librdkafka's statistics and can trail the
        // broker's, and an abort under a trailing epoch would be fenced
        // without deciding anything.
        floor_epoch = observed->producer_epoch;
        out.floor_epoch = floor_epoch;
        if (before_abort) {
            before_abort();
        }
        const TxnIdentity ongoing{.transactional_id = staged.transactional_id,
                                  .producer_id = observed->producer_id,
                                  .producer_epoch = observed->producer_epoch};
        ++link.corr;
        const auto et_body =
            roundtrip(*link.conn, wire::end_txn_request_v1(link.corr, ongoing, /*commit=*/false));
        if (!et_body.has_value()) {
            last_detail =
                "EndTxn(abort) transport failure after the request was sent; the next describe "
                "learns whether it landed";
            continue;
        }
        const auto code = wire::parse_end_txn_response(*et_body, link.corr);
        if (!code.has_value()) {
            last_detail = "EndTxn(abort) response did not parse";
            continue;
        }
        if (*code == 0) {
            return verdict(Status::Aborted,
                           *observed,
                           "EndTxn(abort) accepted for '" + staged.transactional_id + "' pid " +
                               std::to_string(observed->producer_id) + " epoch " +
                               std::to_string(observed->producer_epoch));
        }
        // CONCURRENT_TRANSACTIONS (a decision completing), INVALID_TXN_STATE
        // or a fenced epoch (a commit that got there first), or anything
        // else: the code is not the verdict, the next describe is.
        last_detail = "EndTxn(abort) answered " + wire::error_name(*code) + "; describing again";
        refused = "; the abort under epoch " + std::to_string(observed->producer_epoch) +
                  " was refused with " + wire::error_name(*code);
    }
    out.status = Status::TransportError;
    out.detail = last_detail + " (no verdict)";
    return out;
}

}  // namespace clink::kafka
