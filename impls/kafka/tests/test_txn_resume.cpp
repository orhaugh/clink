// The prepared-transaction resume path, hermetically: the wire encoding
// pinned byte-for-byte, the parsers against truncation and mismatched
// correlation, and the whole resume_commit exchange - plus the registered
// kafka_2pc resolver - driven against a fake broker speaking scripted
// Kafka-protocol bytes over a real socket. The live half (a genuine orphan
// committed on a real broker) is test_txn_resume_live.cpp.

#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/kafka/txn_resume.hpp"
#include "clink/runtime/network/connection.hpp"

namespace {

using clink::kafka::ResumeOutcome;
using clink::kafka::TxnIdentity;
namespace wire = clink::kafka::wire;

// --- wire goldens -------------------------------------------------------------

TEST(TxnResumeWire, EndTxnRequestV1IsPinnedByteForByte) {
    TxnIdentity txn;
    txn.transactional_id = "t1";
    txn.producer_id = 42;
    txn.producer_epoch = 3;
    const auto frame = wire::end_txn_request_v1(/*correlation_id=*/7, txn, /*commit=*/true);

    // size(4) | api_key 26 | version 1 | correlation 7 |
    // client_id "clink-txn-resume" | body: "t1", pid 42, epoch 3, commit 1.
    const std::vector<std::uint8_t> expected = {
        0x00, 0x00, 0x00, 0x29,                                         // size = 41
        0x00, 0x1A,                                                     // api_key 26
        0x00, 0x01,                                                     // version 1
        0x00, 0x00, 0x00, 0x07,                                         // correlation 7
        0x00, 0x10, 'c',  'l',  'i',  'n',  'k',  '-',  't', 'x', 'n',  // client id
        '-',  'r',  'e',  's',  'u',  'm',  'e',                        //
        0x00, 0x02, 't',  '1',                                          // transactional_id
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,                 // producer_id 42
        0x00, 0x03,                                                     // epoch 3
        0x01,                                                           // commit
    };
    ASSERT_EQ(frame.size(), expected.size());
    for (std::size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(static_cast<std::uint8_t>(frame[i]), expected[i]) << "byte " << i;
    }
}

TEST(TxnResumeWire, ParsersRefuseTruncationAndForeignCorrelation) {
    TxnIdentity txn;
    txn.transactional_id = "t";
    txn.producer_id = 1;
    txn.producer_epoch = 0;
    // A well-formed EndTxn response: corr 9, throttle 0, error 0.
    std::vector<std::byte> ok;
    const auto put32 = [&](std::uint32_t v) {
        for (int i = 3; i >= 0; --i) {
            ok.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    };
    put32(9);
    put32(0);
    ok.push_back(std::byte{0});
    ok.push_back(std::byte{0});

    EXPECT_EQ(wire::parse_end_txn_response(ok, 9).value_or(-1), 0);
    EXPECT_FALSE(wire::parse_end_txn_response(ok, 10).has_value())
        << "a response for someone else's correlation id must not be believed";
    std::vector<std::byte> truncated(ok.begin(), ok.end() - 1);
    EXPECT_FALSE(wire::parse_end_txn_response(truncated, 9).has_value());
}

TEST(TxnResumeWire, ErrorNamesCoverTheCodesThisPathMeets) {
    EXPECT_EQ(wire::error_name(0), "NONE");
    EXPECT_EQ(wire::error_name(90), "PRODUCER_FENCED");
    EXPECT_EQ(wire::error_name(48), "INVALID_TXN_STATE");
    EXPECT_EQ(wire::error_name(35), "UNSUPPORTED_VERSION");
    EXPECT_EQ(wire::error_name(1234), "error 1234");
}

// --- a fake broker over a real socket ----------------------------------------

// Reads one request frame; returns {api_key, correlation_id}.
struct ParsedRequest {
    std::int16_t api_key{-1};
    std::int32_t correlation{0};
    std::vector<std::uint8_t> body;  // everything after client_id
};

bool read_exact(int fd, void* buf, std::size_t n) {
    auto* p = static_cast<std::uint8_t*>(buf);
    std::size_t got = 0;
    while (got < n) {
        const auto r = ::recv(fd, p + got, n - got, 0);
        if (r <= 0) {
            return false;
        }
        got += static_cast<std::size_t>(r);
    }
    return true;
}

bool read_request(int fd, ParsedRequest& out) {
    std::uint8_t hdr[4];
    if (!read_exact(fd, hdr, 4)) {
        return false;
    }
    const std::uint32_t size = (std::uint32_t(hdr[0]) << 24) | (std::uint32_t(hdr[1]) << 16) |
                               (std::uint32_t(hdr[2]) << 8) | std::uint32_t(hdr[3]);
    std::vector<std::uint8_t> payload(size);
    if (size == 0 || size > (1u << 20) || !read_exact(fd, payload.data(), size)) {
        return false;
    }
    out.api_key = static_cast<std::int16_t>((payload[0] << 8) | payload[1]);
    out.correlation = static_cast<std::int32_t>(
        (std::uint32_t(payload[4]) << 24) | (std::uint32_t(payload[5]) << 16) |
        (std::uint32_t(payload[6]) << 8) | std::uint32_t(payload[7]));
    const std::size_t client_len = (std::size_t(payload[8]) << 8) | payload[9];
    out.body.assign(payload.begin() + 10 + static_cast<std::ptrdiff_t>(client_len), payload.end());
    return true;
}

void send_response(int fd, std::int32_t correlation, const std::vector<std::uint8_t>& body) {
    std::vector<std::uint8_t> out;
    const auto put32 = [&](std::uint32_t v) {
        for (int i = 3; i >= 0; --i) {
            out.push_back(static_cast<std::uint8_t>((v >> (i * 8)) & 0xFF));
        }
    };
    put32(static_cast<std::uint32_t>(body.size() + 4));
    put32(static_cast<std::uint32_t>(correlation));
    out.insert(out.end(), body.begin(), body.end());
    (void)::send(fd, out.data(), out.size(), 0);
}

std::vector<std::uint8_t> api_versions_body(std::int16_t fc_min,
                                            std::int16_t fc_max,
                                            std::int16_t et_min,
                                            std::int16_t et_max) {
    std::vector<std::uint8_t> b;
    const auto put16 = [&](std::int16_t v) {
        b.push_back(static_cast<std::uint8_t>((static_cast<std::uint16_t>(v) >> 8) & 0xFF));
        b.push_back(static_cast<std::uint8_t>(static_cast<std::uint16_t>(v) & 0xFF));
    };
    put16(0);  // error_code
    b.insert(b.end(), {0, 0, 0, 2});
    put16(wire::kFindCoordinatorKey);
    put16(fc_min);
    put16(fc_max);
    put16(wire::kEndTxnKey);
    put16(et_min);
    put16(et_max);
    return b;
}

std::vector<std::uint8_t> find_coordinator_body(std::int16_t error,
                                                const std::string& host,
                                                std::int32_t port) {
    std::vector<std::uint8_t> b;
    const auto put16 = [&](std::int16_t v) {
        b.push_back(static_cast<std::uint8_t>((static_cast<std::uint16_t>(v) >> 8) & 0xFF));
        b.push_back(static_cast<std::uint8_t>(static_cast<std::uint16_t>(v) & 0xFF));
    };
    const auto put32 = [&](std::int32_t v) {
        for (int i = 3; i >= 0; --i) {
            b.push_back(
                static_cast<std::uint8_t>((static_cast<std::uint32_t>(v) >> (i * 8)) & 0xFF));
        }
    };
    put32(0);      // throttle
    put16(error);  // error_code
    put16(-1);     // error_message = null
    put32(1);      // node_id
    put16(static_cast<std::int16_t>(host.size()));
    for (const char c : host) {
        b.push_back(static_cast<std::uint8_t>(c));
    }
    put32(port);
    return b;
}

std::vector<std::uint8_t> end_txn_body(std::int16_t error) {
    return {0,
            0,
            0,
            0,  // throttle
            static_cast<std::uint8_t>((static_cast<std::uint16_t>(error) >> 8) & 0xFF),
            static_cast<std::uint8_t>(static_cast<std::uint16_t>(error) & 0xFF)};
}

std::vector<std::uint8_t> sasl_handshake_body(std::int16_t error,
                                              const std::vector<std::string>& mechanisms) {
    std::vector<std::uint8_t> b;
    const auto put16 = [&](std::int16_t v) {
        b.push_back(static_cast<std::uint8_t>((static_cast<std::uint16_t>(v) >> 8) & 0xFF));
        b.push_back(static_cast<std::uint8_t>(static_cast<std::uint16_t>(v) & 0xFF));
    };
    put16(error);
    b.push_back(0);
    b.push_back(0);
    b.push_back(0);
    b.push_back(static_cast<std::uint8_t>(mechanisms.size()));
    for (const auto& m : mechanisms) {
        put16(static_cast<std::int16_t>(m.size()));
        for (const char c : m) {
            b.push_back(static_cast<std::uint8_t>(c));
        }
    }
    return b;
}

std::vector<std::uint8_t> sasl_authenticate_body(std::int16_t error, const std::string& message) {
    std::vector<std::uint8_t> b;
    const auto put16 = [&](std::int16_t v) {
        b.push_back(static_cast<std::uint8_t>((static_cast<std::uint16_t>(v) >> 8) & 0xFF));
        b.push_back(static_cast<std::uint8_t>(static_cast<std::uint16_t>(v) & 0xFF));
    };
    put16(error);
    if (message.empty()) {
        put16(-1);  // error_message = null
    } else {
        put16(static_cast<std::int16_t>(message.size()));
        for (const char c : message) {
            b.push_back(static_cast<std::uint8_t>(c));
        }
    }
    // auth_bytes (empty) + session_lifetime_ms (v1 tail the parser ignores).
    b.insert(b.end(), {0, 0, 0, 0});
    b.insert(b.end(), {0, 0, 0, 0, 0, 0, 0, 0});
    return b;
}

// What the fake broker demands and answers for SASL. Defaults reproduce the
// pre-SASL broker exactly (nothing required, nothing scripted).
struct FakeSasl {
    // A required broker REFUSES any non-SASL request on an unauthenticated
    // connection by closing it - the observable shape of a SASL-only
    // listener, which is what makes the no-credentials arm honest.
    bool required{false};
    std::int16_t handshake_error{0};
    std::int16_t auth_error{0};
    std::string auth_error_message;
    std::vector<std::string> mechanisms{"PLAIN"};
};

// A broker whose behaviour is the script: for each accepted connection it
// answers requests until the peer hangs up. Records the EndTxn bodies it saw
// so tests can assert what actually went over the wire.
class FakeBroker {
public:
    // end_txn_error: the code EndTxn answers with. fc/et ranges: what
    // ApiVersions advertises.
    explicit FakeBroker(std::int16_t end_txn_error = 0,
                        std::int16_t fc_min = 0,
                        std::int16_t fc_max = 4,
                        std::int16_t et_min = 0,
                        std::int16_t et_max = 4,
                        FakeSasl sasl = {})
        : end_txn_error_(end_txn_error),
          fc_min_(fc_min),
          fc_max_(fc_max),
          et_min_(et_min),
          et_max_(et_max),
          sasl_(std::move(sasl)) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        (void)::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        (void)::listen(listen_fd_, 4);
        socklen_t len = sizeof(addr);
        (void)::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        accept_thread_ = std::thread([this] { accept_loop_(); });
    }

    ~FakeBroker() {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
        for (auto& t : serve_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    [[nodiscard]] std::vector<std::vector<std::uint8_t>> end_txn_bodies() const {
        std::lock_guard lock(mu_);
        return end_txn_bodies_;
    }
    // The SaslAuthenticate auth_bytes each connection presented, in arrival
    // order - one entry per authenticated connection.
    [[nodiscard]] std::vector<std::vector<std::uint8_t>> auth_bytes_seen() const {
        std::lock_guard lock(mu_);
        return auth_bytes_seen_;
    }

private:
    void accept_loop_() {
        while (true) {
            const int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) {
                return;  // listener closed
            }
            // One thread per connection: a real broker serves the bootstrap
            // and coordinator connections concurrently, and so must this.
            std::lock_guard lock(mu_);
            serve_threads_.emplace_back([this, fd] {
                serve_(fd);
                ::close(fd);
            });
        }
    }

    void serve_(int fd) {
        ParsedRequest req;
        bool authenticated = false;
        while (read_request(fd, req)) {
            if (req.api_key == wire::kSaslHandshakeKey) {
                send_response(fd,
                              req.correlation,
                              sasl_handshake_body(sasl_.handshake_error, sasl_.mechanisms));
                if (sasl_.handshake_error != 0) {
                    return;
                }
            } else if (req.api_key == wire::kSaslAuthenticateKey) {
                {
                    std::lock_guard lock(mu_);
                    auth_bytes_seen_.push_back(req.body);
                }
                send_response(fd,
                              req.correlation,
                              sasl_authenticate_body(sasl_.auth_error, sasl_.auth_error_message));
                if (sasl_.auth_error != 0) {
                    return;
                }
                authenticated = true;
            } else if (sasl_.required && !authenticated) {
                // A SASL-only listener drops unauthenticated traffic; the
                // client observes a closed connection, not a Kafka error.
                return;
            } else if (req.api_key == wire::kApiVersionsKey) {
                send_response(
                    fd, req.correlation, api_versions_body(fc_min_, fc_max_, et_min_, et_max_));
            } else if (req.api_key == wire::kFindCoordinatorKey) {
                // The coordinator is this same fake broker.
                send_response(fd, req.correlation, find_coordinator_body(0, "127.0.0.1", port_));
            } else if (req.api_key == wire::kEndTxnKey) {
                {
                    std::lock_guard lock(mu_);
                    end_txn_bodies_.push_back(req.body);
                }
                send_response(fd, req.correlation, end_txn_body(end_txn_error_));
            } else {
                return;
            }
        }
    }

    std::int16_t end_txn_error_;
    std::int16_t fc_min_, fc_max_, et_min_, et_max_;
    FakeSasl sasl_;
    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::thread accept_thread_;
    std::vector<std::thread> serve_threads_;
    mutable std::mutex mu_;
    std::vector<std::vector<std::uint8_t>> end_txn_bodies_;
    std::vector<std::vector<std::uint8_t>> auth_bytes_seen_;
};

clink::kafka::ConnectFn plain_connect() {
    return [](const std::string& host, std::uint16_t port) {
        return clink::network::connect_plain(host, port);
    };
}

TxnIdentity identity() {
    TxnIdentity txn;
    txn.transactional_id = "resume-me";
    txn.producer_id = 42;
    txn.producer_epoch = 3;
    return txn;
}

// --- resume_commit against the fake broker ------------------------------------

TEST(TxnResume, HappyPathCommitsAndSendsTheExactIdentity) {
    FakeBroker broker(/*end_txn_error=*/0);
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect());
    ASSERT_TRUE(outcome.committed()) << outcome.detail;

    const auto bodies = broker.end_txn_bodies();
    ASSERT_EQ(bodies.size(), 1u);
    // body: string "resume-me" (2+9), pid 42 (8), epoch 3 (2), commit 1 (1).
    const auto& b = bodies[0];
    ASSERT_EQ(b.size(), 2u + 9u + 8u + 2u + 1u);
    EXPECT_EQ(std::string(b.begin() + 2, b.begin() + 11), "resume-me");
    EXPECT_EQ(b[18], 42u) << "producer_id low byte";
    EXPECT_EQ(b[20], 3u) << "epoch low byte";
    EXPECT_EQ(b[21], 1u) << "committed flag - this path must never send an abort";
}

TEST(TxnResume, AFencedProducerIsRefusedNeverClaimedCommitted) {
    FakeBroker broker(/*end_txn_error=*/90);  // PRODUCER_FENCED
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect());
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused);
    EXPECT_NE(outcome.detail.find("PRODUCER_FENCED"), std::string::npos) << outcome.detail;
}

TEST(TxnResume, ABrokerWithoutTheSpokenVersionsIsUnsupported) {
    // Broker only speaks FindCoordinator v3+ (flexible) - this module does
    // not, and must say so rather than guess at an encoding.
    FakeBroker broker(/*end_txn_error=*/0, /*fc_min=*/3, /*fc_max=*/5);
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect());
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Unsupported);
    EXPECT_TRUE(broker.end_txn_bodies().empty()) << "must not fire EndTxn at a guessed version";
}

TEST(TxnResume, AnIncompleteIdentityIsRefusedWithoutTouchingTheNetwork) {
    TxnIdentity txn;
    txn.transactional_id = "never-captured";
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", 1, txn, [](const std::string&, std::uint16_t) {
            ADD_FAILURE() << "no connection may be attempted without an identity";
            return std::unique_ptr<clink::network::Connection>{};
        });
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused);
    EXPECT_NE(outcome.detail.find("no producer identity"), std::string::npos);
}

TEST(TxnResume, AnUnreachableBrokerIsATransportErrorAfterBoundedAttempts) {
    // Port 1: connection refused immediately, three bounded attempts.
    const auto outcome = clink::kafka::resume_commit("127.0.0.1", 1, identity(), plain_connect());
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::TransportError);
}

// --- the registered resolver end to end ---------------------------------------

TEST(TxnResume, TheRegisteredResolverDrivesTheWholeChainFromAHandle) {
    const auto resolver = clink::connectors::TxnResumeRegistry::instance().find("kafka_2pc");
    ASSERT_TRUE(resolver.has_value()) << "kafka install() must register the resolver";

    FakeBroker broker(/*end_txn_error=*/0);
    const std::string handle =
        "{\"v\":\"1\",\"resolver\":\"kafka_2pc\",\"bootstrap\":\"127.0.0.1:" +
        std::to_string(broker.port()) +
        "\",\"transactional_id\":\"resume-me\",\"producer_id\":\"42\","
        "\"producer_epoch\":\"3\",\"ckpt\":\"5\"}";
    const auto result = (*resolver)(handle);
    EXPECT_TRUE(result.committed) << result.detail;
    EXPECT_EQ(broker.end_txn_bodies().size(), 1u);
}

TEST(TxnResume, TheResolverRefusesAHandleWithoutIdentityOrShape) {
    const auto resolver = clink::connectors::TxnResumeRegistry::instance().find("kafka_2pc");
    ASSERT_TRUE(resolver.has_value());

    const auto no_identity = (*resolver)(
        R"({"v":"1","resolver":"kafka_2pc","bootstrap":"127.0.0.1:9","transactional_id":"t",)"
        R"("producer_id":"-1","producer_epoch":"-1","ckpt":"5"})");
    EXPECT_FALSE(no_identity.committed);
    EXPECT_NE(no_identity.detail.find("no producer identity"), std::string::npos);

    const auto garbage = (*resolver)("not json at all");
    EXPECT_FALSE(garbage.committed);
    EXPECT_NE(garbage.detail.find("did not parse"), std::string::npos);
}

// --- SASL ----------------------------------------------------------------------

TEST(TxnResumeWire, SaslFramesArePinnedByteForByte) {
    const auto hs = wire::sasl_handshake_request_v1(/*correlation_id=*/9, "PLAIN");
    const std::vector<std::uint8_t> hs_expected = {
        0x00, 0x00, 0x00, 0x21,                                     // size = 33
        0x00, 0x11,                                                 // api_key 17
        0x00, 0x01,                                                 // version 1
        0x00, 0x00, 0x00, 0x09,                                     // correlation 9
        0x00, 0x10, 'c',  'l',  'i', 'n', 'k', '-', 't', 'x', 'n',  // client id
        '-',  'r',  'e',  's',  'u', 'm', 'e',                      //
        0x00, 0x05, 'P',  'L',  'A', 'I', 'N',                      // mechanism
    };
    ASSERT_EQ(hs.size(), hs_expected.size());
    for (std::size_t i = 0; i < hs_expected.size(); ++i) {
        EXPECT_EQ(static_cast<std::uint8_t>(hs[i]), hs_expected[i]) << "byte " << i;
    }

    // RFC 4616 PLAIN: NUL user NUL pass, then the BYTES wrapper.
    const auto plain = wire::plain_auth_bytes("u", "pw");
    const std::vector<std::uint8_t> plain_expected = {0x00, 'u', 0x00, 'p', 'w'};
    ASSERT_EQ(plain.size(), plain_expected.size());
    for (std::size_t i = 0; i < plain_expected.size(); ++i) {
        EXPECT_EQ(static_cast<std::uint8_t>(plain[i]), plain_expected[i]) << "byte " << i;
    }

    const auto au = wire::sasl_authenticate_request_v1(/*correlation_id=*/10, plain);
    const std::vector<std::uint8_t> au_expected = {
        0x00, 0x00, 0x00, 0x23,                                     // size = 35
        0x00, 0x24,                                                 // api_key 36
        0x00, 0x01,                                                 // version 1
        0x00, 0x00, 0x00, 0x0A,                                     // correlation 10
        0x00, 0x10, 'c',  'l',  'i', 'n', 'k', '-', 't', 'x', 'n',  // client id
        '-',  'r',  'e',  's',  'u', 'm', 'e',                      //
        0x00, 0x00, 0x00, 0x05,                                     // auth_bytes len
        0x00, 'u',  0x00, 'p',  'w',                                // auth_bytes
    };
    ASSERT_EQ(au.size(), au_expected.size());
    for (std::size_t i = 0; i < au_expected.size(); ++i) {
        EXPECT_EQ(static_cast<std::uint8_t>(au[i]), au_expected[i]) << "byte " << i;
    }
}

TEST(TxnResumeWire, SaslParsersRefuseTruncationAndForeignCorrelation) {
    const auto hs_ok = sasl_handshake_body(0, {"PLAIN", "SCRAM-SHA-256"});
    std::vector<std::byte> hs_body;
    hs_body.insert(hs_body.end(), {std::byte{0}, std::byte{0}, std::byte{0}, std::byte{5}});
    for (const auto b : hs_ok) {
        hs_body.push_back(static_cast<std::byte>(b));
    }
    const auto parsed = wire::parse_sasl_handshake_response_v1(hs_body, 5);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->error_code, 0);
    ASSERT_EQ(parsed->mechanisms.size(), 2U);
    EXPECT_EQ(parsed->mechanisms[1], "SCRAM-SHA-256");
    EXPECT_FALSE(wire::parse_sasl_handshake_response_v1(hs_body, 6).has_value())
        << "a foreign correlation id must not parse";
    std::vector<std::byte> truncated(hs_body.begin(), hs_body.begin() + 7);
    EXPECT_FALSE(wire::parse_sasl_handshake_response_v1(truncated, 5).has_value());

    const auto au_err = sasl_authenticate_body(58, "bad credentials");
    std::vector<std::byte> au_body;
    au_body.insert(au_body.end(), {std::byte{0}, std::byte{0}, std::byte{0}, std::byte{9}});
    for (const auto b : au_err) {
        au_body.push_back(static_cast<std::byte>(b));
    }
    const auto au_parsed = wire::parse_sasl_authenticate_response_v1(au_body, 9);
    ASSERT_TRUE(au_parsed.has_value());
    EXPECT_EQ(au_parsed->error_code, 58);
    EXPECT_EQ(au_parsed->error_message, "bad credentials");
    EXPECT_FALSE(wire::parse_sasl_authenticate_response_v1(au_body, 10).has_value());
}

TEST(TxnResume, AnAuthenticatedResumeCommitsAndPresentsPlainCredentials) {
    FakeBroker broker(/*end_txn_error=*/0,
                      /*fc_min=*/0,
                      /*fc_max=*/4,
                      /*et_min=*/0,
                      /*et_max=*/4,
                      FakeSasl{.required = true});
    clink::kafka::ResumeAuth auth;
    auth.mechanism = "PLAIN";
    auth.username = "alice";
    auth.password = "secret";
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect(), auth);
    ASSERT_TRUE(outcome.committed()) << outcome.detail;

    // BOTH connections authenticated (bootstrap + coordinator), each with
    // the exact RFC 4616 payload.
    const auto seen = broker.auth_bytes_seen();
    ASSERT_EQ(seen.size(), 2U) << "each connection is a fresh session and must authenticate";
    const std::vector<std::uint8_t> expected_auth = {
        0x00, 'a', 'l', 'i', 'c', 'e', 0x00, 's', 'e', 'c', 'r', 'e', 't'};
    for (const auto& bytes : seen) {
        // The request body is the BYTES wrapper (4-byte length) + payload.
        ASSERT_EQ(bytes.size(), expected_auth.size() + 4);
        for (std::size_t i = 0; i < expected_auth.size(); ++i) {
            EXPECT_EQ(bytes[i + 4], expected_auth[i]) << "auth byte " << i;
        }
    }
}

TEST(TxnResume, AnUnknownMechanismIsRefusedBeforeAnyByteIsSent) {
    bool dialed = false;
    const clink::kafka::ConnectFn refuse_dial =
        [&dialed](const std::string&,
                  std::uint16_t) -> std::unique_ptr<clink::network::Connection> {
        dialed = true;
        return nullptr;
    };
    clink::kafka::ResumeAuth auth;
    auth.mechanism = "SCRAM-SHA-256";
    auth.username = "alice";
    auth.password = "secret";
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", 9092, identity(), refuse_dial, auth);
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused);
    EXPECT_NE(outcome.detail.find("PLAIN only"), std::string::npos) << outcome.detail;
    EXPECT_FALSE(dialed) << "an unspoken mechanism must refuse locally, not probe the network";
}

TEST(TxnResume, AHandshakeRefusalSurfacesTheBrokersEnabledMechanisms) {
    FakeBroker broker(/*end_txn_error=*/0,
                      /*fc_min=*/0,
                      /*fc_max=*/4,
                      /*et_min=*/0,
                      /*et_max=*/4,
                      FakeSasl{.required = true,
                               .handshake_error = 33,  // UNSUPPORTED_SASL_MECHANISM
                               .auth_error = 0,
                               .auth_error_message = {},
                               .mechanisms = {"SCRAM-SHA-512"}});
    clink::kafka::ResumeAuth auth;
    auth.mechanism = "PLAIN";
    auth.username = "alice";
    auth.password = "secret";
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect(), auth);
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused);
    EXPECT_NE(outcome.detail.find("UNSUPPORTED_SASL_MECHANISM"), std::string::npos)
        << outcome.detail;
    EXPECT_NE(outcome.detail.find("SCRAM-SHA-512"), std::string::npos)
        << "the refusal must name what WOULD work: " << outcome.detail;
}

TEST(TxnResume, BadCredentialsAreRefusedWithTheBrokersMessage) {
    FakeBroker broker(/*end_txn_error=*/0,
                      /*fc_min=*/0,
                      /*fc_max=*/4,
                      /*et_min=*/0,
                      /*et_max=*/4,
                      FakeSasl{.required = true,
                               .handshake_error = 0,
                               .auth_error = 58,  // SASL_AUTHENTICATION_FAILED
                               .auth_error_message = "invalid credentials for alice",
                               .mechanisms = {"PLAIN"}});
    clink::kafka::ResumeAuth auth;
    auth.mechanism = "PLAIN";
    auth.username = "alice";
    auth.password = "wrong";
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect(), auth);
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused)
        << "bad credentials must be FINAL, never retried or claimed committed";
    EXPECT_NE(outcome.detail.find("SASL_AUTHENTICATION_FAILED"), std::string::npos)
        << outcome.detail;
    EXPECT_NE(outcome.detail.find("invalid credentials for alice"), std::string::npos)
        << outcome.detail;
    EXPECT_TRUE(broker.end_txn_bodies().empty())
        << "no EndTxn may be attempted on an unauthenticated connection";
}

TEST(TxnResume, NoCredentialsAgainstASaslOnlyBrokerFallsBackAsTransportError) {
    FakeBroker broker(/*end_txn_error=*/0,
                      /*fc_min=*/0,
                      /*fc_max=*/4,
                      /*et_min=*/0,
                      /*et_max=*/4,
                      FakeSasl{.required = true});
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect());
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::TransportError)
        << "a SASL-only listener drops unauthenticated traffic; the resume must fall back, "
           "never guess: "
        << outcome.detail;
    EXPECT_TRUE(broker.end_txn_bodies().empty());
}

}  // namespace
