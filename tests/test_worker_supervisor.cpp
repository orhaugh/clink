#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include <gtest/gtest.h>
#include <sys/socket.h>

#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/worker_supervisor.hpp"
#include "clink/runtime/network/connection.hpp"

using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

template <typename Predicate>
bool await(Predicate&& predicate, std::chrono::milliseconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return predicate();
}

}  // namespace

TEST(WorkerSupervisor, HeartbeatLeaseDetectsASilentCoordinatorWithoutEof) {
    int sockets[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    auto server = std::shared_ptr<clink::network::Connection>(
        clink::network::make_plain_connection(sockets[1]).release());
    std::jthread silent_coordinator([server] {
        auto registration = read_frame(*server);
        if (!registration.has_value()) {
            return;
        }
        MessageReader reader(std::move(*registration));
        if (static_cast<MessageKind>(reader.read_u8()) != MessageKind::Register) {
            return;
        }
        (void)decode_register(reader);
        const auto ack =
            encode_frame(MessageKind::RegisterAck,
                         RegisterAckMsg{.ok = true, .message = "", .coordinator_epoch = 7});
        if (!send_frame(*server, ack)) {
            return;
        }
        // Continue reading valid heartbeats while deliberately withholding
        // HeartbeatAck. The socket remains open, proving the worker's lease
        // does not depend on TCP EOF or a failed write.
        while (read_frame(*server).has_value()) {
        }
    });

    Worker::Config config;
    config.heartbeat_interval = 25ms;
    config.coordinator_heartbeat_timeout = 100ms;
    Worker worker("lease-worker", "127.0.0.1", config);
    auto client_fd = std::make_shared<std::atomic<int>>(sockets[0]);
    worker.set_connect_factory([client_fd](const std::string&, std::uint16_t) {
        const int fd = client_fd->exchange(-1, std::memory_order_acq_rel);
        return clink::network::make_plain_connection(fd);
    });
    worker.connect_to_coordinator("unused", 1);

    EXPECT_TRUE(await([&] { return worker.disconnected(); }, 2s))
        << "the bidirectional heartbeat lease did not expire";
    worker.stop();
}

TEST(WorkerSupervisor, RegistrationHandshakeTimesOutWhenPeerStaysSilent) {
    int sockets[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    auto silent_server = std::shared_ptr<clink::network::Connection>(
        clink::network::make_plain_connection(sockets[1]).release());

    Worker::Config config;
    config.heartbeat_interval = 0ms;
    config.coordinator_heartbeat_timeout = 75ms;
    Worker worker("handshake-worker", "127.0.0.1", config);
    auto client_fd = std::make_shared<std::atomic<int>>(sockets[0]);
    worker.set_connect_factory([client_fd](const std::string&, std::uint16_t) {
        const int fd = client_fd->exchange(-1, std::memory_order_acq_rel);
        return clink::network::make_plain_connection(fd);
    });

    const auto started = std::chrono::steady_clock::now();
    try {
        worker.connect_to_coordinator("unused", 1);
        FAIL() << "a silent registration peer was accepted";
    } catch (const WorkerConnectionError& e) {
        EXPECT_TRUE(e.retryable());
    }
    EXPECT_LT(std::chrono::steady_clock::now() - started, 1s)
        << "the registration handshake remained unbounded";
    worker.stop();
    silent_server->close();
}

TEST(WorkerSupervisor, ReplacesTheControlSessionWithoutStoppingTheSupervisor) {
    Coordinator first;
    const auto first_port = first.start();
    first.expect_workers({"recovering-worker"});

    std::mutex endpoint_mu;
    LeaderEndpoint endpoint{.host = "127.0.0.1", .port = first_port, .epoch = 0};
    WorkerSupervisor::Config config;
    config.worker.heartbeat_interval = 50ms;
    config.worker.coordinator_heartbeat_timeout = 500ms;
    config.discovery_timeout = 50ms;
    config.initial_backoff = 10ms;
    config.max_backoff = 100ms;

    WorkerSupervisor supervisor("recovering-worker",
                                "127.0.0.1",
                                config,
                                [&](std::chrono::milliseconds) -> std::optional<LeaderEndpoint> {
                                    std::lock_guard lock(endpoint_mu);
                                    return endpoint;
                                });

    std::atomic<bool> shutdown{false};
    std::optional<WorkerSupervisor::RunResult> result;
    std::jthread runner([&](std::stop_token stop_token) {
        result = supervisor.run([&] {
            return shutdown.load(std::memory_order_acquire) || stop_token.stop_requested();
        });
    });

    ASSERT_TRUE(first.await_registrations(2s));
    ASSERT_TRUE(await([&] { return supervisor.active_worker() != nullptr; }));
    auto first_session = supervisor.active_worker();
    ASSERT_NE(first_session, nullptr);
    EXPECT_EQ(supervisor.snapshot().session_number, 1U);

    // Bring up the replacement before ending the old coordinator so discovery
    // can move immediately. The supervisor thread itself is never restarted.
    Coordinator second;
    const auto second_port = second.start();
    second.expect_workers({"recovering-worker"});
    {
        std::lock_guard lock(endpoint_mu);
        endpoint.port = second_port;
    }
    first.stop();

    ASSERT_TRUE(second.await_registrations(5s));
    ASSERT_TRUE(await([&] {
        auto now = supervisor.active_worker();
        return now != nullptr && now != first_session;
    }));
    EXPECT_TRUE(first_session->disconnected());
    const auto recovered = supervisor.snapshot();
    EXPECT_TRUE(recovered.connected);
    EXPECT_EQ(recovered.session_number, 2U);
    EXPECT_EQ(recovered.successful_reconnections, 1U);
    EXPECT_EQ(recovered.coordinator_port, second_port);

    shutdown.store(true, std::memory_order_release);
    runner.join();
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->fatal) << result->error;
    EXPECT_EQ(supervisor.active_worker(), nullptr);
    second.stop();
}
