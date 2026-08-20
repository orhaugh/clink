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

#ifdef CLINK_KAFKA_RESUME_TLS
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>

#include "clink/kafka/scram.hpp"
#include "clink/runtime/network/network_socket.hpp"
#include "clink/runtime/network/tls_connection.hpp"
#include "clink/runtime/network/tls_socket.hpp"
#endif

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
                                            std::int16_t et_max,
                                            std::int16_t dt_min = 0,
                                            std::int16_t dt_max = 0) {
    std::vector<std::uint8_t> b;
    const auto put16 = [&](std::int16_t v) {
        b.push_back(static_cast<std::uint8_t>((static_cast<std::uint16_t>(v) >> 8) & 0xFF));
        b.push_back(static_cast<std::uint8_t>(static_cast<std::uint16_t>(v) & 0xFF));
    };
    put16(0);  // error_code
    b.insert(b.end(), {0, 0, 0, 3});
    put16(wire::kFindCoordinatorKey);
    put16(fc_min);
    put16(fc_max);
    put16(wire::kEndTxnKey);
    put16(et_min);
    put16(et_max);
    put16(wire::kDescribeTransactionsKey);
    put16(dt_min);
    put16(dt_max);
    return b;
}

// DescribeTransactions v0 response, exactly as far as the client's parser
// consumes it: flexible header tag buffer, throttle, a one-entry states
// array of [error, echoed id, state, timeout, start_time, pid, epoch].
// The unconsumed remainder (topics array, tag buffers) is deliberately
// absent - the parser must not require it.
std::vector<std::uint8_t> describe_transactions_body(std::int16_t error,
                                                     const std::string& txn_id,
                                                     const std::string& state,
                                                     std::int64_t pid,
                                                     std::int16_t epoch) {
    std::vector<std::uint8_t> b;
    const auto put16 = [&](std::int16_t v) {
        b.push_back(static_cast<std::uint8_t>((static_cast<std::uint16_t>(v) >> 8) & 0xFF));
        b.push_back(static_cast<std::uint8_t>(static_cast<std::uint16_t>(v) & 0xFF));
    };
    const auto put64 = [&](std::int64_t v) {
        for (int i = 7; i >= 0; --i) {
            b.push_back(
                static_cast<std::uint8_t>((static_cast<std::uint64_t>(v) >> (i * 8)) & 0xFF));
        }
    };
    const auto put_compact = [&](const std::string& s) {
        b.push_back(static_cast<std::uint8_t>(s.size() + 1));  // uvarint, short strings only
        for (const char c : s) {
            b.push_back(static_cast<std::uint8_t>(c));
        }
    };
    b.push_back(0);                   // header tag buffer
    b.insert(b.end(), {0, 0, 0, 0});  // throttle
    b.push_back(2);                   // states: COMPACT_ARRAY length+1
    put16(error);
    put_compact(txn_id);
    put_compact(state);
    b.insert(b.end(), {0, 0, 0x3A, 0x98});  // timeout_ms 15000
    put64(1);                               // start_time_ms
    put64(pid);
    put16(epoch);
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

std::vector<std::uint8_t> sasl_authenticate_body(std::int16_t error,
                                                 const std::string& message,
                                                 const std::string& auth_payload = "") {
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
    // auth_bytes (SCRAM's server messages ride here) + session_lifetime_ms.
    const auto n = static_cast<std::uint32_t>(auth_payload.size());
    b.push_back(static_cast<std::uint8_t>((n >> 24) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((n >> 16) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((n >> 8) & 0xFF));
    b.push_back(static_cast<std::uint8_t>(n & 0xFF));
    for (const char c : auth_payload) {
        b.push_back(static_cast<std::uint8_t>(c));
    }
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
    // SCRAM scripting: answer round 1 with a server-first extending the
    // client's nonce, round 2 with this v= payload (garbage by default -
    // the forged-server arm's whole point).
    bool scram{false};
    std::string scram_server_final{"v=Zm9yZ2Vkc2lnbmF0dXJlZm9yZ2Vk"};
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
                        FakeSasl sasl = {},
                        std::int16_t dt_min = 0,
                        std::int16_t dt_max = 0)
        : end_txn_error_(end_txn_error),
          fc_min_(fc_min),
          fc_max_(fc_max),
          et_min_(et_min),
          et_max_(et_max),
          dt_min_(dt_min),
          dt_max_(dt_max),
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
    // Script what DescribeTransactions answers (defaults: CompleteCommit
    // under pid 42 epoch 3 - the identity() fixture).
    void set_describe(std::string state, std::int64_t pid, std::int16_t epoch) {
        std::lock_guard lock(mu_);
        describe_state_ = std::move(state);
        describe_pid_ = pid;
        describe_epoch_ = epoch;
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
                if (sasl_.scram) {
                    // The request body is BYTES: 4-byte length then the SCRAM
                    // message. Round 1 carries "n,,n=...,r=<nonce>"; extract
                    // the nonce and answer with a server-first that extends
                    // it. Round 2 gets the scripted server-final. A fake
                    // needs no real crypto to script the FORGED-server arm -
                    // a wrong signature is just a wrong string, and the
                    // client must refuse it.
                    const std::string payload(req.body.begin() + 4, req.body.end());
                    std::string reply;
                    if (payload.rfind("n,,", 0) == 0) {
                        const auto r_pos = payload.rfind(",r=");
                        const std::string client_nonce =
                            r_pos == std::string::npos ? "" : payload.substr(r_pos + 3);
                        reply = "r=" + client_nonce + "srvext,s=c2FsdHNhbHRzYWx0c2FsdA==,i=4096";
                    } else {
                        reply = sasl_.scram_server_final;
                        authenticated = true;
                    }
                    send_response(fd, req.correlation, sasl_authenticate_body(0, "", reply));
                } else {
                    send_response(
                        fd,
                        req.correlation,
                        sasl_authenticate_body(sasl_.auth_error, sasl_.auth_error_message));
                    if (sasl_.auth_error != 0) {
                        return;
                    }
                    authenticated = true;
                }
            } else if (sasl_.required && !authenticated) {
                // A SASL-only listener drops unauthenticated traffic; the
                // client observes a closed connection, not a Kafka error.
                return;
            } else if (req.api_key == wire::kApiVersionsKey) {
                send_response(
                    fd,
                    req.correlation,
                    api_versions_body(fc_min_, fc_max_, et_min_, et_max_, dt_min_, dt_max_));
            } else if (req.api_key == wire::kDescribeTransactionsKey) {
                std::string state;
                std::int64_t pid = 0;
                std::int16_t epoch = 0;
                {
                    std::lock_guard lock(mu_);
                    state = describe_state_;
                    pid = describe_pid_;
                    epoch = describe_epoch_;
                }
                send_response(fd,
                              req.correlation,
                              describe_transactions_body(0, "resume-me", state, pid, epoch));
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
    std::int16_t dt_min_, dt_max_;
    std::string describe_state_{"CompleteCommit"};
    std::int64_t describe_pid_{42};
    std::int16_t describe_epoch_{3};
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
    // The fenced disambiguation asks DescribeTransactions; when the state
    // record does not prove OUR commit (here: the id is Empty under a
    // foreign identity), the refusal must stand.
    broker.set_describe("Empty", -1, -1);
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect());
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused);
    EXPECT_NE(outcome.detail.find("PRODUCER_FENCED"), std::string::npos) << outcome.detail;
}

TEST(TxnResume, AFencedEndTxnIsDisambiguatedByDescribeWhenTheCommitLanded) {
    // The staged epoch is chronically one commit stale (it rides the
    // statistics callback while the broker bumps per commit), so a
    // COMMITTED transaction can answer EndTxn with PRODUCER_FENCED. The
    // resolver must then read CompleteCommit under our producer id - at
    // or above the staged epoch - as the commit landing, not a refusal:
    // the refusal path replays the committed interval as duplicates.
    FakeBroker broker(/*end_txn_error=*/90);
    broker.set_describe("CompleteCommit", 42, /*epoch=*/4);
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect());
    EXPECT_TRUE(outcome.committed()) << outcome.detail;
    EXPECT_NE(outcome.detail.find("stale-epoch artefact"), std::string::npos) << outcome.detail;
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

// --- the read-only describe probe + the pre-fence orphan verdict -------------
//
// describe_transaction_state is what the 2PC sink's open() asks BEFORE its
// init_transactions fences a predecessor whose walk ended unresolved, and
// what the recovery gates poll while awaiting broker-side expiry. It must
// never mutate broker state, so its coverage here is the read path only:
// the dialogue, the transport fallback, and the version guard.

TEST(TxnResume, TheDescribeProbeReadsStateAndIdentity) {
    FakeBroker broker;
    const auto st = clink::kafka::describe_transaction_state(
        "127.0.0.1", broker.port(), "resume-me", plain_connect());
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(st->state, "CompleteCommit");
    EXPECT_EQ(st->producer_id, 42);
    EXPECT_EQ(st->producer_epoch, 3);
    EXPECT_TRUE(broker.end_txn_bodies().empty())
        << "the READ-ONLY probe must never send EndTxn - an EndTxn executes a commit";
}

TEST(TxnResume, TheDescribeProbeIsNulloptAgainstAnUnreachableBroker) {
    const auto st =
        clink::kafka::describe_transaction_state("127.0.0.1", 1, "resume-me", plain_connect());
    EXPECT_FALSE(st.has_value());
}

TEST(TxnResume, TheDescribeProbeRefusesABrokerWithoutTheVersion) {
    // DescribeTransactions advertised only from v1: this module speaks v0
    // and must answer "no verdict" rather than guess at an encoding.
    FakeBroker broker(/*end_txn_error=*/0,
                      /*fc_min=*/0,
                      /*fc_max=*/4,
                      /*et_min=*/0,
                      /*et_max=*/4,
                      FakeSasl{},
                      /*dt_min=*/1,
                      /*dt_max=*/4);
    const auto st = clink::kafka::describe_transaction_state(
        "127.0.0.1", broker.port(), "resume-me", plain_connect());
    EXPECT_FALSE(st.has_value());
}

// The verdict mapping the sink applies to what describe returns. Proving a
// commit arms replay suppression for the orphan's panes; a false positive
// here is DATA LOSS (suppressing panes never published), a false negative
// is the rig-night duplicate. Every state the Kafka transaction
// coordinator can report is pinned, under matching and foreign identities.
TEST(TxnResume, TheOrphanVerdictProvesOnlyCommitDecisionsUnderTheStagedIdentity) {
    using clink::kafka::DescribeState;
    using clink::kafka::orphan_commit_proven;
    const auto observed = [](std::string state, std::int64_t pid = 42, std::int16_t epoch = 3) {
        DescribeState d;
        d.state = std::move(state);
        d.producer_id = pid;
        d.producer_epoch = epoch;
        return d;
    };
    // Commit decisions under the staged identity: proven.
    EXPECT_TRUE(orphan_commit_proven(observed("CompleteCommit"), 42, 3));
    EXPECT_TRUE(orphan_commit_proven(observed("PrepareCommit"), 42, 3));
    // Undecided or abort-side states: never proven - the init's abort and
    // the restore's replay are the legitimate outcome.
    EXPECT_FALSE(orphan_commit_proven(observed("Ongoing"), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed("PrepareAbort"), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed("CompleteAbort"), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed("Empty"), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed("Dead"), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed("PrepareEpochFence"), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed(""), 42, 3));
    // A commit decision under a FOREIGN identity describes some other
    // generation's transaction: proving it would suppress panes this
    // orphan never published.
    EXPECT_FALSE(orphan_commit_proven(observed("CompleteCommit", 43, 3), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed("CompleteCommit", 42, 4), 42, 3));
    EXPECT_FALSE(orphan_commit_proven(observed("CompleteCommit", -1, -1), 42, 3));
}

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
    auth.mechanism = "GSSAPI";
    auth.username = "alice";
    auth.password = "secret";
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", 9092, identity(), refuse_dial, auth);
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused);
    EXPECT_NE(outcome.detail.find("PLAIN and SCRAM-SHA-256 only"), std::string::npos)
        << outcome.detail;
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

#ifdef CLINK_KAFKA_RESUME_TLS

// --- SCRAM-SHA-256 ---------------------------------------------------------------
//
// The whole exchange against RFC 7677's published test vector (user /
// pencil), so the crypto is pinned by the RFC rather than by this
// implementation's own output - the only non-circular gate a from-scratch
// SCRAM client can have.

TEST(TxnResumeScram, TheRfc7677VectorIsReproducedExactly) {
    namespace scram = clink::kafka::scram;
    const auto first = scram::client_first("user", "rOprNGfwEbeRWgbNEkqO");
    EXPECT_EQ(first.full, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
    EXPECT_EQ(first.bare, "n=user,r=rOprNGfwEbeRWgbNEkqO");

    const std::string server_first_msg =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
        "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const auto server_first = scram::parse_server_first(server_first_msg);
    ASSERT_TRUE(server_first.has_value());
    EXPECT_EQ(server_first->iterations, 4096U);
    EXPECT_EQ(server_first->nonce, "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0");

    const auto final = scram::client_final("pencil", first, *server_first);
    ASSERT_TRUE(final.has_value());
    EXPECT_EQ(final->message,
              "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
              "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");

    // The v= the RFC's server sends must equal OUR computed ServerSignature:
    // the mutual-authentication half of the vector.
    const auto server_sig =
        scram::parse_server_final_signature("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    ASSERT_TRUE(server_sig.has_value());
    EXPECT_EQ(*server_sig, final->expected_server_signature);
}

TEST(TxnResumeScram, AForgedServerSignatureIsRefusedNeverTrusted) {
    // The fake completes both SCRAM rounds happily but signs the final
    // message with garbage - a broker (or an interceptor) that never knew
    // the credentials. Trusting it and proceeding would hand EndTxn to an
    // unauthenticated peer.
    FakeBroker broker(/*end_txn_error=*/0,
                      /*fc_min=*/0,
                      /*fc_max=*/4,
                      /*et_min=*/0,
                      /*et_max=*/4,
                      FakeSasl{.required = true,
                               .handshake_error = 0,
                               .auth_error = 0,
                               .auth_error_message = {},
                               .mechanisms = {"SCRAM-SHA-256"},
                               .scram = true});
    clink::kafka::ResumeAuth auth;
    auth.mechanism = "SCRAM-SHA-256";
    auth.username = "alice";
    auth.password = "secret";
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), plain_connect(), auth);
    EXPECT_EQ(outcome.status, ResumeOutcome::Status::Refused) << outcome.detail;
    EXPECT_NE(outcome.detail.find("server signature mismatch"), std::string::npos)
        << outcome.detail;
    EXPECT_TRUE(broker.end_txn_bodies().empty())
        << "EndTxn went to a server that never proved knowledge of the credentials";
}

TEST(TxnResumeScram, AttackShapesAreRefusedAtParseAndComputeTime) {
    namespace scram = clink::kafka::scram;
    const auto first = scram::client_first("user", "clientnonce");

    // A server nonce that does not EXTEND the client's is a reflection.
    const auto foreign = scram::parse_server_first("r=somebodyelse,s=c2FsdA==,i=4096");
    ASSERT_TRUE(foreign.has_value());
    EXPECT_FALSE(scram::client_final("pw", first, *foreign).has_value());

    // An iteration count below RFC 7677's 4096 floor is a KDF downgrade.
    EXPECT_FALSE(scram::parse_server_first("r=clientnonceX,s=c2FsdA==,i=1").has_value());

    // e= is an explicit server error, never a signature.
    EXPECT_FALSE(scram::parse_server_final_signature("e=other-error").has_value());
}

// --- TLS ------------------------------------------------------------------------
//
// The resume over a REAL TLS session: a fake broker behind
// accept_tls_connection, a self-signed certificate generated with the
// openssl CLI (the tls impl's own fixture pattern), and the client
// verifying against that certificate as its CA. Gated on the build
// carrying clink::tls, exactly like the resolver branch under test.

std::filesystem::path resume_tls_cert_dir() {
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_resume_tls_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const auto cert = dir / "cert.pem";
    const auto key = dir / "key.pem";
    const std::string cmd = "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + key.string() +
                            " -out " + cert.string() +
                            " -days 1 -subj /CN=127.0.0.1"
                            " -addext subjectAltName=IP:127.0.0.1 > /dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) {
        return {};
    }
    return dir;
}

// One-connection-at-a-time TLS broker scripting the happy resume dialogue.
// Serial accept is enough here: the resume releases its bootstrap
// connection before dialing the coordinator.
class TlsResumeBroker {
public:
    explicit TlsResumeBroker(const std::filesystem::path& cert_dir)
        : ctx_(std::make_shared<clink::network::TlsServerContext>(
              (cert_dir / "cert.pem").string(), (cert_dir / "key.pem").string())) {
        std::uint16_t bound = 0;
        listen_fd_ = clink::network::NetworkSocket::listen_on(bound, "127.0.0.1");
        port_ = bound;
        accept_thread_ = std::thread([this] { accept_loop_(); });
    }

    ~TlsResumeBroker() {
        // Order matters: the flag first, so the accept loop can tell the
        // listener teardown (accept_tls_connection THROWS on a dead
        // listener, same as on a failed client handshake) from a client
        // it should keep serving past.
        stopping_.store(true, std::memory_order_release);
        clink::network::NetworkSocket::shutdown_read(listen_fd_);
        clink::network::NetworkSocket::close(listen_fd_);
        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
    }

    TlsResumeBroker(const TlsResumeBroker&) = delete;
    TlsResumeBroker& operator=(const TlsResumeBroker&) = delete;
    TlsResumeBroker(TlsResumeBroker&&) = delete;
    TlsResumeBroker& operator=(TlsResumeBroker&&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    [[nodiscard]] std::size_t end_txn_count() const {
        std::lock_guard lock(mu_);
        return end_txn_count_;
    }

private:
    void accept_loop_() {
        while (!stopping_.load(std::memory_order_acquire)) {
            std::unique_ptr<clink::network::Connection> conn;
            try {
                conn = clink::network::accept_tls_connection(listen_fd_, ctx_);
            } catch (const std::exception&) {
                continue;  // a failed handshake ends that client, not the broker
            }
            if (conn == nullptr) {
                return;
            }
            serve_(*conn);
        }
    }

    void serve_(clink::network::Connection& conn) {
        while (true) {
            std::byte hdr[4];
            if (!conn.recv_all(hdr, 4)) {
                return;
            }
            std::uint32_t size = 0;
            for (int i = 0; i < 4; ++i) {
                size = (size << 8) | static_cast<std::uint8_t>(hdr[i]);
            }
            if (size == 0 || size > (1U << 20)) {
                return;
            }
            std::vector<std::byte> payload(size);
            if (!conn.recv_all(payload.data(), payload.size())) {
                return;
            }
            const auto api_key =
                static_cast<std::int16_t>((static_cast<std::uint8_t>(payload[0]) << 8) |
                                          static_cast<std::uint8_t>(payload[1]));
            const auto correlation = static_cast<std::int32_t>(
                (std::uint32_t(static_cast<std::uint8_t>(payload[4])) << 24) |
                (std::uint32_t(static_cast<std::uint8_t>(payload[5])) << 16) |
                (std::uint32_t(static_cast<std::uint8_t>(payload[6])) << 8) |
                std::uint32_t(static_cast<std::uint8_t>(payload[7])));

            std::vector<std::uint8_t> body;
            if (api_key == wire::kApiVersionsKey) {
                body = api_versions_body(0, 4, 0, 4);
            } else if (api_key == wire::kFindCoordinatorKey) {
                body = find_coordinator_body(0, "127.0.0.1", port_);
            } else if (api_key == wire::kEndTxnKey) {
                {
                    std::lock_guard lock(mu_);
                    ++end_txn_count_;
                }
                body = end_txn_body(0);
            } else {
                return;
            }
            std::vector<std::byte> out;
            const std::uint32_t total = static_cast<std::uint32_t>(body.size() + 4);
            for (int i = 3; i >= 0; --i) {
                out.push_back(static_cast<std::byte>((total >> (i * 8)) & 0xFF));
            }
            for (int i = 3; i >= 0; --i) {
                out.push_back(static_cast<std::byte>(
                    (static_cast<std::uint32_t>(correlation) >> (i * 8)) & 0xFF));
            }
            for (const auto b : body) {
                out.push_back(static_cast<std::byte>(b));
            }
            if (!conn.send_all(out.data(), out.size())) {
                return;
            }
        }
    }

    std::shared_ptr<clink::network::TlsServerContext> ctx_;
    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::thread accept_thread_;
    std::atomic<bool> stopping_{false};
    mutable std::mutex mu_;
    std::size_t end_txn_count_{0};
};

TEST(TxnResumeTls, TheResumeCommitsOverAVerifiedTlsSession) {
    if (std::system("openssl version > /dev/null 2>&1") != 0) {
        GTEST_SKIP() << "openssl CLI not available for the cert fixture";
    }
    const auto cert_dir = resume_tls_cert_dir();
    ASSERT_FALSE(cert_dir.empty()) << "self-signed cert generation failed";
    TlsResumeBroker broker(cert_dir);

    auto ctx = std::make_shared<clink::network::TlsClientContext>((cert_dir / "cert.pem").string());
    const clink::kafka::ConnectFn tls_connect =
        [ctx](const std::string& host,
              std::uint16_t port) -> std::unique_ptr<clink::network::Connection> {
        try {
            return clink::network::connect_tls_connection(host, port, ctx);
        } catch (const std::exception&) {
            return nullptr;
        }
    };
    const auto outcome =
        clink::kafka::resume_commit("127.0.0.1", broker.port(), identity(), tls_connect);
    EXPECT_TRUE(outcome.committed()) << outcome.detail;
    EXPECT_EQ(broker.end_txn_count(), 1U);
    std::error_code ec;
    std::filesystem::remove_all(cert_dir, ec);
}

TEST(TxnResumeTls, TheResolverDialsTlsWhenTheEnvironmentNamesACa) {
    if (std::system("openssl version > /dev/null 2>&1") != 0) {
        GTEST_SKIP() << "openssl CLI not available for the cert fixture";
    }
    const auto cert_dir = resume_tls_cert_dir();
    ASSERT_FALSE(cert_dir.empty());
    TlsResumeBroker broker(cert_dir);
    ::setenv("CLINK_KAFKA_RESUME_TLS_CA", (cert_dir / "cert.pem").c_str(), 1);

    const auto resolver = clink::connectors::TxnResumeRegistry::instance().find("kafka_2pc");
    ASSERT_TRUE(resolver.has_value());
    const std::string handle = "{\"v\":1,\"resolver\":\"kafka_2pc\",\"bootstrap\":\"127.0.0.1:" +
                               std::to_string(broker.port()) +
                               "\",\"transactional_id\":\"resume-me\",\"producer_id\":\"42\","
                               "\"producer_epoch\":\"3\",\"ckpt\":\"2\"}";
    const auto result = (*resolver)(handle);
    ::unsetenv("CLINK_KAFKA_RESUME_TLS_CA");
    EXPECT_TRUE(result.committed) << result.detail;
    EXPECT_EQ(broker.end_txn_count(), 1U)
        << "the resolver must have dialed THROUGH the TLS session, not around it";
    std::error_code ec;
    std::filesystem::remove_all(cert_dir, ec);
}

TEST(TxnResumeTls, AnUnusableCaRefusesLoudlyInsteadOfDowngradingToPlaintext) {
    // A plaintext fake broker stands ready: a silent downgrade WOULD
    // succeed against it, which is exactly what must not happen.
    FakeBroker broker(/*end_txn_error=*/0);
    ::setenv("CLINK_KAFKA_RESUME_TLS_CA", "/nonexistent/resume-ca.pem", 1);
    const auto resolver = clink::connectors::TxnResumeRegistry::instance().find("kafka_2pc");
    ASSERT_TRUE(resolver.has_value());
    const std::string handle = "{\"v\":1,\"resolver\":\"kafka_2pc\",\"bootstrap\":\"127.0.0.1:" +
                               std::to_string(broker.port()) +
                               "\",\"transactional_id\":\"resume-me\",\"producer_id\":\"42\","
                               "\"producer_epoch\":\"3\",\"ckpt\":\"2\"}";
    const auto result = (*resolver)(handle);
    ::unsetenv("CLINK_KAFKA_RESUME_TLS_CA");
    EXPECT_FALSE(result.committed);
    EXPECT_NE(result.detail.find("could not be built"), std::string::npos) << result.detail;
    EXPECT_TRUE(broker.end_txn_bodies().empty())
        << "the resolver dialed PLAINTEXT after TLS was requested - the downgrade defect";
}

#endif  // CLINK_KAFKA_RESUME_TLS

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
