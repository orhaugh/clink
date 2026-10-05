// A TLS coordinator's accept thread must not be held by any one client.
//
// The coordinator admits every connection on a single accept thread, and with
// TLS on that thread used to run each handshake as a blocking SSL_accept with
// no deadline. A client that connected and never completed the handshake - a
// port scanner, a plain-TCP client, a slowloris dribbling a ClientHello - then
// held it for ever: no worker could register behind it, and Coordinator::stop()
// waited for ever to join it. Each connection is now admitted on a thread of
// its own, the handshake runs non-blocking against Config::handshake_timeout
// and the coordinator's stop wake, and the whole of admission (handshake and
// first frame) has a deadline that stop() can cut short. These tests drive the
// real coordinator, the TLS accept factory clink_node installs, and a real
// worker over TLS.

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/runtime/network/tls_connection.hpp"
#include "clink/runtime/network/tls_socket.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

// Long enough that a pass is never a slow machine and a fail is never a fast
// one: every healthy path here finishes in well under a second, and the
// broken one never finishes at all.
constexpr auto kFailureBound = 10s;

// A self-signed certificate and key in a fresh directory, from the openssl CLI.
// Empty when the CLI is unavailable.
struct TlsAcceptCertDir {
    std::filesystem::path dir;
    TlsAcceptCertDir() {
        const auto d = std::filesystem::temp_directory_path() /
                       ("clink_tls_accept_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(d);
        const std::string cmd = "openssl req -x509 -newkey rsa:2048 -nodes -keyout " +
                                (d / "key.pem").string() + " -out " + (d / "cert.pem").string() +
                                " -days 1 -subj /CN=localhost > /dev/null 2>&1";
        if (std::system(cmd.c_str()) == 0) {
            dir = d;
        } else {
            std::filesystem::remove_all(d);
        }
    }
    ~TlsAcceptCertDir() {
        if (!dir.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    }
    TlsAcceptCertDir(const TlsAcceptCertDir&) = delete;
    TlsAcceptCertDir& operator=(const TlsAcceptCertDir&) = delete;
    [[nodiscard]] std::string cert() const { return (dir / "cert.pem").string(); }
    [[nodiscard]] std::string key() const { return (dir / "key.pem").string(); }
};

// The factory clink_node installs for --tls-cert, handed the coordinator's
// handshake deadline and stop wake.
void install_tls_accept(Coordinator& coordinator,
                        const std::shared_ptr<network::TlsServerContext>& ctx) {
    coordinator.set_accept_factory([ctx](const Coordinator::AcceptRequest& req) {
        return network::handshake_accepted_tls_connection(
            req.fd,
            ctx,
            network::TlsAcceptOptions{.handshake_timeout = req.handshake_timeout,
                                      .wake = req.wake});
    });
}

void use_tls_to_coordinator(Worker& worker, const std::shared_ptr<network::TlsClientContext>& ctx) {
    worker.set_connect_factory([ctx](const std::string& host, std::uint16_t port) {
        return network::connect_tls_connection(host, port, ctx);
    });
}

bool await_tls_condition(const std::function<bool()>& cond, std::chrono::milliseconds bound) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (!cond()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

std::uint64_t handshake_failures() {
    return MetricsRegistry::global().counter(metrics::kCoordinatorHandshakeFailures).value();
}

// How the stalling client behaves once its TCP connection is up.
enum class Stall : std::uint8_t {
    // Sends nothing: a port scanner, or a plain-TCP client on a TLS port.
    Silent,
    // Sends the start of a ClientHello and stops: a record header announcing a
    // 512-byte handshake record, then the first few bytes of it, so the server
    // has begun the handshake and is waiting for the rest.
    PartialClientHello,
};

std::unique_ptr<network::Connection> connect_stalling_client(std::uint16_t port, Stall how) {
    auto conn = network::connect_plain("127.0.0.1", port);
    if (conn == nullptr || how == Stall::Silent) {
        return conn;
    }
    constexpr std::array<std::uint8_t, 11> kPartialHello{
        0x16,
        0x03,
        0x01,
        0x02,
        0x00,  // handshake record, TLS 1.0 framing, 512 bytes
        0x01,
        0x00,
        0x01,
        0xfc,  // ClientHello, 508 bytes
        0x03,
        0x03,  // client_version TLS 1.2, and nothing more
    };
    if (!conn->send_all(reinterpret_cast<const std::byte*>(kPartialHello.data()),
                        kPartialHello.size())) {
        return nullptr;
    }
    return conn;
}

class TlsCoordinatorAccept : public ::testing::Test {
protected:
    void SetUp() override {
        if (!network::TlsServerContext::is_real_implementation()) {
            GTEST_SKIP() << "built without OpenSSL";
        }
        if (certs_.dir.empty()) {
            GTEST_SKIP() << "openssl CLI unavailable, cannot make a self-signed certificate";
        }
        server_ctx_ = std::make_shared<network::TlsServerContext>(certs_.cert(), certs_.key());
        client_ctx_ = std::make_shared<network::TlsClientContext>(certs_.cert());
    }

    // A client that stalls the handshake must not stop a worker registering
    // behind it, and the stalled connection must be closed at the handshake
    // deadline and counted once.
    void expect_worker_registers_behind(Stall how) {
        Coordinator::Config cfg;
        cfg.handshake_timeout = 300ms;
        Coordinator coordinator(cfg);
        install_tls_accept(coordinator, server_ctx_);
        const auto port = coordinator.start();
        coordinator.expect_workers({"w-tls"});
        const auto failures_before = handshake_failures();

        // First in the listener's queue, so the accept thread reaches it first.
        auto stalled = connect_stalling_client(port, how);
        ASSERT_NE(stalled, nullptr);

        Worker worker("w-tls", "127.0.0.1");
        worker.register_role("noop", [](const DeploymentTask&) {});
        use_tls_to_coordinator(worker, client_ctx_);
        auto registering = std::async(std::launch::async,
                                      [&] { worker.connect_to_coordinator("127.0.0.1", port); });
        if (registering.wait_for(kFailureBound) != std::future_status::ready) {
            ADD_FAILURE() << "a worker could not register for " << kFailureBound.count()
                          << "s behind a client that stalled its TLS handshake: that "
                             "handshake holds up admission";
            stalled->close();  // fails the held handshake, so the worker gets through
            registering.wait();
            worker.stop();
            coordinator.stop();
            return;
        }
        ASSERT_NO_THROW(registering.get());
        EXPECT_TRUE(coordinator.await_registrations(5s));

        // The coordinator closed the stalled connection rather than leaving it
        // open: its next read sees end of stream, not the recv timeout.
        ASSERT_TRUE(stalled->set_recv_timeout(kFailureBound));
        std::byte b{};
        const auto read_from = std::chrono::steady_clock::now();
        EXPECT_FALSE(stalled->recv_all(&b, 1));
        EXPECT_LT(std::chrono::steady_clock::now() - read_from, kFailureBound / 2)
            << "the stalled connection was left open after its handshake deadline";
        // Counted just after the close, on the connection's admission thread.
        EXPECT_TRUE(await_tls_condition([&] { return handshake_failures() > failures_before; },
                                        kFailureBound))
            << "a timed-out handshake must be counted";
        EXPECT_EQ(handshake_failures() - failures_before, 1u)
            << "a timed-out handshake must be counted once per connection";

        stalled->close();
        worker.stop();
        coordinator.stop();
    }

    // A client that stalls the handshake must not stop Coordinator::stop()
    // returning, however long the handshake deadline: stop() wakes the
    // handshake as it wakes the accept wait.
    void expect_stop_returns_during(Stall how) {
        namespace fault = clink::fault;
        Coordinator::Config cfg;
        cfg.handshake_timeout = 10min;  // only the stop wake can end this handshake in time
        Coordinator coordinator(cfg);
        install_tls_accept(coordinator, server_ctx_);
        const auto port = coordinator.start();
        // Counts the accept thread reaching the handshake, and changes nothing.
        fault::Registry::instance().reset();
        fault::ScopedFault observe{fault::Rule{.point = fault::points::kTlsAcceptBeforeHandshake,
                                               .action = fault::Action::Observe}};

        auto stalled = connect_stalling_client(port, how);
        ASSERT_NE(stalled, nullptr);
        // Wait until the accept thread has taken the connection into its
        // handshake, so stop() has a handshake to wake rather than an idle wait.
        ASSERT_TRUE(await_tls_condition(
            [] {
                return fault::Registry::instance().hits(fault::points::kTlsAcceptBeforeHandshake) >=
                       1;
            },
            kFailureBound))
            << "the accept thread never took the stalled connection";

        auto stopping = std::async(std::launch::async, [&] { coordinator.stop(); });
        if (stopping.wait_for(kFailureBound) != std::future_status::ready) {
            ADD_FAILURE() << "Coordinator::stop() did not return within " << kFailureBound.count()
                          << "s while a client stalled its TLS handshake: stop() is joining an "
                             "accept thread that the handshake holds";
            stalled->close();  // fails the held handshake, so the accept thread exits
        }
        stopping.get();
        stalled->close();
    }

    TlsAcceptCertDir certs_;
    std::shared_ptr<network::TlsServerContext> server_ctx_;
    std::shared_ptr<network::TlsClientContext> client_ctx_;
};

}  // namespace

TEST_F(TlsCoordinatorAccept, ASilentClientDoesNotStopAWorkerRegistering) {
    expect_worker_registers_behind(Stall::Silent);
}

TEST_F(TlsCoordinatorAccept, APartialClientHelloDoesNotStopAWorkerRegistering) {
    expect_worker_registers_behind(Stall::PartialClientHello);
}

TEST_F(TlsCoordinatorAccept, StopReturnsWhileASilentClientHoldsAHandshake) {
    expect_stop_returns_during(Stall::Silent);
}

TEST_F(TlsCoordinatorAccept, StopReturnsWhileAPartialClientHelloHoldsAHandshake) {
    expect_stop_returns_during(Stall::PartialClientHello);
}

// One step further in: a client that completes the handshake and then sends
// nothing. The coordinator bounds a new connection's first frame with
// heartbeat_timeout through Connection::set_recv_timeout, which the TLS
// connection did not implement, so on TLS that read had no deadline and this
// client held the accept thread exactly as the stalled handshake did.
TEST_F(TlsCoordinatorAccept, AClientSilentAfterItsHandshakeDoesNotStopAWorkerRegistering) {
    Coordinator::Config cfg;
    cfg.heartbeat_timeout = 300ms;  // the first-frame deadline
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-tls"});

    auto silent = network::connect_tls_connection("127.0.0.1", port, client_ctx_);
    ASSERT_NE(silent, nullptr);

    Worker worker("w-tls", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    use_tls_to_coordinator(worker, client_ctx_);
    auto registering =
        std::async(std::launch::async, [&] { worker.connect_to_coordinator("127.0.0.1", port); });
    if (registering.wait_for(kFailureBound) != std::future_status::ready) {
        ADD_FAILURE() << "a worker could not register behind a TLS client that sent no first "
                         "frame: the accept thread is held by that read";
        silent->close();
    }
    ASSERT_NO_THROW(registering.get());
    EXPECT_TRUE(coordinator.await_registrations(5s));
    EXPECT_EQ(coordinator.client_session_count(), 0u);

    silent->close();
    worker.stop();
    coordinator.stop();
}

// The deadline bounds the whole handshake, not each read: a client that sends
// one byte of its ClientHello at a time, each well inside the deadline, is still
// dropped once the deadline passes.
TEST_F(TlsCoordinatorAccept, ADribblingClientIsDroppedAtTheHandshakeDeadline) {
    Coordinator::Config cfg;
    cfg.handshake_timeout = 300ms;
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    const auto failures_before = handshake_failures();

    auto dribbler = network::connect_plain("127.0.0.1", port);
    ASSERT_NE(dribbler, nullptr);
    // A 512-byte record header, then one body byte at a time: the handshake
    // never completes, and every read the server makes succeeds well inside
    // the deadline.
    constexpr std::array<std::uint8_t, 5> kHeader{0x16, 0x03, 0x01, 0x02, 0x00};
    ASSERT_TRUE(
        dribbler->send_all(reinterpret_cast<const std::byte*>(kHeader.data()), kHeader.size()));
    const auto started = std::chrono::steady_clock::now();
    const std::byte one{0x01};
    bool closed = false;
    // Until the coordinator closes the connection (a send fails), or the bound.
    // 200 bytes at 50 ms is 10 s, and the record wants 512, so it never completes.
    for (int i = 0; i < 200 && !closed; ++i) {
        std::this_thread::sleep_for(50ms);
        closed = !dribbler->send_all(&one, 1);
    }
    const auto elapsed = std::chrono::steady_clock::now() - started;
    EXPECT_TRUE(closed) << "a client sending its ClientHello a byte at a time was never dropped";
    EXPECT_LT(elapsed, 5s) << "the handshake deadline is not a deadline on the whole handshake";
    EXPECT_TRUE(await_tls_condition([&] { return handshake_failures() > failures_before; }, 5s));

    dribbler->close();
    coordinator.stop();
}

// Past the handshake: a client that completes it, sends a first-frame header
// claiming 32 KiB, then one TLS record of one byte every 50 ms. Each read the
// coordinator makes returns well inside its per-read bound (heartbeat_timeout),
// so only a deadline on the whole of admission drops it.
TEST_F(TlsCoordinatorAccept, AClientDribblingItsFirstFrameIsDroppedAtTheAdmissionDeadline) {
    Coordinator::Config cfg;
    cfg.handshake_timeout = 300ms;
    cfg.heartbeat_timeout = 300ms;  // admission deadline: 600 ms from the accept
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();

    auto dribbler = network::connect_tls_connection("127.0.0.1", port, client_ctx_);
    ASSERT_NE(dribbler, nullptr);
    // 32 KiB: under kMaxFirstFrameBytes, so the coordinator reads it.
    constexpr std::array<std::uint8_t, 4> k32KiB{0x00, 0x00, 0x80, 0x00};
    ASSERT_TRUE(
        dribbler->send_all(reinterpret_cast<const std::byte*>(k32KiB.data()), k32KiB.size()));
    const auto started = std::chrono::steady_clock::now();
    const std::byte one{0x01};
    bool closed = false;
    // Until the coordinator closes the connection (a send fails), or 10 s.
    for (int i = 0; i < 200 && !closed; ++i) {
        std::this_thread::sleep_for(50ms);
        closed = !dribbler->send_all(&one, 1);
    }
    EXPECT_TRUE(closed) << "a TLS client sending its first frame a byte at a time was never "
                           "dropped: the first frame is bounded per read, not as a whole";
    EXPECT_LT(std::chrono::steady_clock::now() - started, 5s);
    EXPECT_EQ(coordinator.client_session_count(), 0u);

    dribbler->close();
    coordinator.stop();
}

// stop() must not wait on a first-frame read either, however long the bounds
// on it: it shuts the connection down under the read.
TEST_F(TlsCoordinatorAccept, StopReturnsWhileAClientHoldsItsFirstFrame) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.handshake_timeout = 10min;
    cfg.heartbeat_timeout = 10min;  // only stop() can end this read in time
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    fault::Registry::instance().reset();
    fault::ScopedFault observe{
        fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeFirstFrame,
                    .action = fault::Action::Observe}};

    auto holder = network::connect_tls_connection("127.0.0.1", port, client_ctx_);
    ASSERT_NE(holder, nullptr);
    // 32 KiB: under kMaxFirstFrameBytes, so the coordinator reads it.
    constexpr std::array<std::uint8_t, 4> k32KiB{0x00, 0x00, 0x80, 0x00};
    ASSERT_TRUE(holder->send_all(reinterpret_cast<const std::byte*>(k32KiB.data()), k32KiB.size()));
    // Past the handshake and committed to the first-frame read, so stop() has
    // that read to end rather than a handshake.
    ASSERT_TRUE(await_tls_condition(
        [] {
            return fault::Registry::instance().hits(
                       fault::points::kCoordinatorAdmissionBeforeFirstFrame) >= 1;
        },
        kFailureBound))
        << "the coordinator never reached the first-frame read";

    auto stopping = std::async(std::launch::async, [&] { coordinator.stop(); });
    if (stopping.wait_for(kFailureBound) != std::future_status::ready) {
        ADD_FAILURE() << "Coordinator::stop() did not return within " << kFailureBound.count()
                      << "s while a TLS client held its first frame";
        holder->close();  // ends the held read, so stop() can finish
    }
    stopping.get();
    holder->close();
}

// Handshakes no longer run one after another: stalling clients accepted ahead
// of a worker cost it nothing, where each used to cost it a whole
// handshake_timeout.
TEST_F(TlsCoordinatorAccept, StallingHandshakesAheadOfAWorkerDoNotDelayIt) {
    Coordinator::Config cfg;
    cfg.handshake_timeout = 5s;
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-tls"});

    // Ahead of the worker in the listener's queue: four of them were twenty
    // seconds of handshakes in sequence.
    std::vector<std::unique_ptr<network::Connection>> stalled;
    for (int i = 0; i < 4; ++i) {
        stalled.push_back(connect_stalling_client(port, Stall::Silent));
        ASSERT_NE(stalled.back(), nullptr);
    }

    Worker worker("w-tls", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    use_tls_to_coordinator(worker, client_ctx_);
    auto registering =
        std::async(std::launch::async, [&] { worker.connect_to_coordinator("127.0.0.1", port); });
    // Less than one handshake_timeout: the worker waited for none of them.
    if (registering.wait_for(cfg.handshake_timeout) != std::future_status::ready) {
        ADD_FAILURE() << "a worker waited longer than one handshake_timeout behind clients "
                         "stalling their handshakes: handshakes run one at a time";
        for (auto& c : stalled) {
            c->close();
        }
    }
    ASSERT_NO_THROW(registering.get());
    EXPECT_TRUE(coordinator.await_registrations(5s));

    for (auto& c : stalled) {
        c->close();
    }
    worker.stop();
    coordinator.stop();
}

// At max_pending_connections the oldest admission is closed to make room, so a
// flood of stalling clients cannot keep a worker out however many there are.
TEST_F(TlsCoordinatorAccept, AFullAdmissionSetEvictsItsOldestConnection) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.handshake_timeout = 10min;  // only eviction can end these handshakes in time
    cfg.max_pending_connections = 2;
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-tls"});
    fault::Registry::instance().reset();
    fault::ScopedFault observe{fault::Rule{.point = fault::points::kTlsAcceptBeforeHandshake,
                                           .action = fault::Action::Observe}};

    auto oldest = connect_stalling_client(port, Stall::Silent);
    ASSERT_NE(oldest, nullptr);
    ASSERT_TRUE(await_tls_condition(
        [] {
            return fault::Registry::instance().hits(fault::points::kTlsAcceptBeforeHandshake) >= 1;
        },
        kFailureBound));
    auto newer = connect_stalling_client(port, Stall::Silent);
    ASSERT_NE(newer, nullptr);
    ASSERT_TRUE(await_tls_condition(
        [] {
            return fault::Registry::instance().hits(fault::points::kTlsAcceptBeforeHandshake) >= 2;
        },
        kFailureBound))
        << "both stalling clients must be in their handshakes before the worker arrives";

    // The set is full: the worker's connection evicts the oldest.
    Worker worker("w-tls", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    use_tls_to_coordinator(worker, client_ctx_);
    auto registering =
        std::async(std::launch::async, [&] { worker.connect_to_coordinator("127.0.0.1", port); });
    if (registering.wait_for(kFailureBound) != std::future_status::ready) {
        ADD_FAILURE() << "a worker could not register with the admission set full";
        oldest->close();
        newer->close();
    }
    ASSERT_NO_THROW(registering.get());
    EXPECT_TRUE(coordinator.await_registrations(5s));

    // The oldest was closed, not left to its ten-minute deadline: its read
    // sees the end of the stream at once rather than the recv timeout.
    ASSERT_TRUE(oldest->set_recv_timeout(kFailureBound));
    std::byte b{};
    const auto read_from = std::chrono::steady_clock::now();
    EXPECT_FALSE(oldest->recv_all(&b, 1));
    EXPECT_LT(std::chrono::steady_clock::now() - read_from, kFailureBound / 2)
        << "the oldest admission was not evicted when the set was full";

    oldest->close();
    newer->close();
    worker.stop();
    coordinator.stop();
}

// The flood that made eviction a lockout, from the worker's own address so
// only progress can tell them apart: silent connections, each newer than a
// worker whose handshake is done. Each must evict another silent connection,
// never the worker.
TEST_F(TlsCoordinatorAccept, AWorkerPastItsHandshakeIsNotEvictedByAFloodFromItsOwnAddress) {
    namespace fault = clink::fault;
    Coordinator::Config cfg;
    cfg.handshake_timeout = 10min;  // only eviction ends the flood's handshakes
    cfg.heartbeat_timeout = 10min;
    cfg.max_pending_connections = 2;
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-tls"});
    fault::Registry::instance().reset();
    // The worker's admission, held once its handshake is done and before its
    // first frame is read. The flood never gets that far.
    fault::ScopedFault hold{
        fault::Rule{.point = fault::points::kCoordinatorAdmissionBeforeFirstFrame,
                    .ordinal = 1,
                    .action = fault::Action::Block}};
    fault::Registry::instance().arm(fault::Rule{.point = fault::points::kTlsAcceptBeforeHandshake,
                                                .action = fault::Action::Observe});
    const auto hits = [](const char* point) { return fault::Registry::instance().hits(point); };

    Worker worker("w-tls", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    use_tls_to_coordinator(worker, client_ctx_);
    auto registering =
        std::async(std::launch::async, [&] { worker.connect_to_coordinator("127.0.0.1", port); });
    ASSERT_TRUE(await_tls_condition(
        [&] { return hits(fault::points::kCoordinatorAdmissionBeforeFirstFrame) >= 1; },
        kFailureBound))
        << "the worker never got past its handshake";

    // Twice the cap, one at a time, each into its handshake before the next.
    std::vector<std::unique_ptr<network::Connection>> flood;
    for (std::uint64_t i = 0; i < 4; ++i) {
        flood.push_back(connect_stalling_client(port, Stall::Silent));
        ASSERT_NE(flood.back(), nullptr);
        ASSERT_TRUE(await_tls_condition(
            [&] { return hits(fault::points::kTlsAcceptBeforeHandshake) >= 2 + i; },
            kFailureBound));
    }

    fault::Registry::instance().release(fault::points::kCoordinatorAdmissionBeforeFirstFrame);
    if (registering.wait_for(kFailureBound) != std::future_status::ready) {
        ADD_FAILURE() << "the worker did not register once released";
        for (auto& c : flood) {
            c->close();
        }
    }
    EXPECT_NO_THROW(registering.get())
        << "a worker past its handshake was evicted by newer connections from its own address "
           "that had sent nothing";
    EXPECT_TRUE(coordinator.await_registrations(5s));
    EXPECT_EQ(hits(fault::points::kCoordinatorAdmissionBeforeFirstFrame), 1u)
        << "the worker registered, but only on a second connection: its first was evicted";

    for (auto& c : flood) {
        c->close();
    }
    worker.stop();
    coordinator.stop();
}

// The CPU a handshake costs the server is spent in its SSL_accept steps, now
// on as many admission threads as there are connections. A server context
// bounds how many steps compute at once; a step that cannot get a slot within
// its handshake deadline fails the handshake.
TEST_F(TlsCoordinatorAccept, AHandshakeStepBeyondTheContextsLimitWaitsForASlot) {
    namespace fault = clink::fault;
    server_ctx_->set_max_concurrent_handshake_steps(1);
    Coordinator::Config cfg;
    cfg.handshake_timeout = 300ms;
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    const auto failures_before = handshake_failures();
    fault::Registry::instance().reset();
    // The first handshake's first step, holding the only slot.
    fault::ScopedFault hold{fault::Rule{
        .point = fault::points::kTlsHandshakeStep, .ordinal = 1, .action = fault::Action::Block}};
    const auto steps = [] {
        return fault::Registry::instance().hits(fault::points::kTlsHandshakeStep);
    };

    auto first = std::async(std::launch::async, [&]() -> std::unique_ptr<network::Connection> {
        try {
            return network::connect_tls_connection("127.0.0.1", port, client_ctx_);
        } catch (const std::exception&) {
            return nullptr;  // its deadline passed while it was held
        }
    });
    ASSERT_TRUE(await_tls_condition([&] { return steps() >= 1; }, kFailureBound));

    std::unique_ptr<network::Connection> second;
    try {
        second = network::connect_tls_connection("127.0.0.1", port, client_ctx_);
    } catch (const std::exception&) {
        second = nullptr;
    }
    EXPECT_EQ(second, nullptr) << "a second handshake completed while the context's only "
                                  "handshake slot was held";
    EXPECT_EQ(steps(), 1u) << "a second handshake step computed while the only slot was held";
    EXPECT_TRUE(
        await_tls_condition([&] { return handshake_failures() > failures_before; }, kFailureBound))
        << "a handshake that could not get a slot must be counted as failed";

    fault::Registry::instance().release(fault::points::kTlsHandshakeStep);
    if (first.wait_for(kFailureBound) != std::future_status::ready) {
        ADD_FAILURE() << "the held handshake never finished once released";
    }
    (void)first.get();
    coordinator.stop();
}

// A handshake waiting for a handshake slot used to wait on until one freed,
// which with no handshake deadline was never: neither the shutdown that evicts
// it nor the stop wake can reach a condition variable. And an evicted
// admission stopped counting against max_pending_connections at once, so each
// new connection evicted one more and left one more thread and socket waiting
// for a slot. An evicted handshake must let go of its thread and its socket
// while the slot it was waiting for is still held.
TEST_F(TlsCoordinatorAccept, AHandshakeEvictedWhileWaitingForASlotLetsGoWithoutTheSlot) {
    namespace fault = clink::fault;
    server_ctx_->set_max_concurrent_handshake_steps(1);
    Coordinator::Config cfg;
    cfg.handshake_timeout = 0ms;  // no handshake deadline: only eviction ends these
    cfg.heartbeat_timeout = 10min;
    cfg.max_pending_connections = 2;
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    const auto failures_before = handshake_failures();
    // Declared before the fault, so a failed assertion releases the held
    // handshake before it waits for these clients.
    std::vector<std::future<void>> clients;
    fault::Registry::instance().reset();
    // The first handshake's first step, holding the only slot until released.
    fault::ScopedFault hold{fault::Rule{
        .point = fault::points::kTlsHandshakeStep, .ordinal = 1, .action = fault::Action::Block}};
    fault::Registry::instance().arm(fault::Rule{.point = fault::points::kTlsAcceptBeforeHandshake,
                                                .action = fault::Action::Observe});
    const auto hits = [](const char* point) { return fault::Registry::instance().hits(point); };
    const auto failures = [&] { return handshake_failures() - failures_before; };

    // Real TLS clients, each sending a whole ClientHello.
    const auto connect = [&] {
        clients.push_back(std::async(std::launch::async, [&] {
            try {
                (void)network::connect_tls_connection("127.0.0.1", port, client_ctx_);
            } catch (const std::exception&) {
                // Evicted: the coordinator closed it.
            }
        }));
    };

    connect();  // holds the slot
    ASSERT_TRUE(await_tls_condition([&] { return hits(fault::points::kTlsHandshakeStep) >= 1; },
                                    kFailureBound));
    // Each of the rest goes from its accept straight to the wait for a slot
    // (the first step comes before any wait on the client), and each one past
    // the cap evicts the oldest still counted: the slot holder first, then the
    // waiters in turn.
    connect();
    ASSERT_TRUE(await_tls_condition(
        [&] { return hits(fault::points::kTlsAcceptBeforeHandshake) >= 2; }, kFailureBound));
    connect();  // evicts the slot holder, which is blocked and cannot let go
    ASSERT_TRUE(await_tls_condition(
        [&] { return hits(fault::points::kTlsAcceptBeforeHandshake) >= 3; }, kFailureBound));
    for (std::uint64_t evicted_waiters = 1; evicted_waiters <= 2; ++evicted_waiters) {
        connect();  // evicts the oldest waiter
        ASSERT_TRUE(await_tls_condition(
            [&] { return hits(fault::points::kTlsAcceptBeforeHandshake) >= 3 + evicted_waiters; },
            kFailureBound));
        // Counted once the admission has closed both of its descriptors.
        EXPECT_TRUE(
            await_tls_condition([&] { return failures() >= evicted_waiters; }, kFailureBound))
            << "a handshake evicted while it waited for a handshake slot kept its thread and its "
               "socket until a slot freed: the wait cannot be reached by the eviction";
    }
    EXPECT_EQ(hits(fault::points::kTlsHandshakeStep), 1u)
        << "a handshake step computed while the only slot was held";

    // The slot holder, evicted before its step ran, gives up once it has the
    // slot rather than computing a step for a connection already dropped.
    fault::Registry::instance().release(fault::points::kTlsHandshakeStep);
    EXPECT_TRUE(await_tls_condition([&] { return failures() >= 3; }, kFailureBound));
    for (auto& c : clients) {
        if (c.wait_for(kFailureBound) != std::future_status::ready) {
            ADD_FAILURE() << "a client's handshake never ended";
        }
    }
    coordinator.stop();
}

// The handshake deadline is computed in the TLS layer too, so it must
// saturate there as well: about 317 years overflowed the clock into the past,
// and every handshake failed at once.
TEST_F(TlsCoordinatorAccept, AHandshakeTimeoutTooLargeForTheClockIsNoDeadline) {
    Coordinator::Config cfg;
    cfg.handshake_timeout = std::chrono::milliseconds{10'000'000'000'000};
    Coordinator coordinator(cfg);
    install_tls_accept(coordinator, server_ctx_);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-tls"});

    Worker worker("w-tls", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    use_tls_to_coordinator(worker, client_ctx_);
    ASSERT_NO_THROW(worker.connect_to_coordinator("127.0.0.1", port))
        << "a handshake timeout too large for the clock failed every handshake at once";
    EXPECT_TRUE(coordinator.await_registrations(5s));

    worker.stop();
    coordinator.stop();
}
