// The outbox's abort over TLS (src/cluster/outbox.hpp; the plain TCP cases
// are in tests/test_outbox.cpp). A writer blocked in SSL_write must be woken
// by abort as one blocked in send is: the transport's close shuts the socket
// down under it.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/runtime/network/network_socket.hpp"
#include "clink/runtime/network/tls_connection.hpp"
#include "clink/runtime/network/tls_socket.hpp"

#include "src/cluster/outbox.hpp"

namespace {

using clink::cluster::FrameClass;
using clink::cluster::Outbox;
using clink::cluster::OutboxState;
using clink::cluster::OutboxStopReason;
using clink::cluster::PostResult;
using clink::network::NetworkSocket;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// A self-signed certificate and key in a fresh directory, from the openssl
// CLI; empty when it is not available.
std::filesystem::path tls_outbox_self_signed_cert() {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_tls_outbox_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const std::string cmd = "openssl req -x509 -newkey rsa:2048 -nodes -keyout " +
                            (dir / "key.pem").string() + " -out " + (dir / "cert.pem").string() +
                            " -days 1 -subj /CN=localhost > /dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) {
        return {};
    }
    return dir;
}

// A TLS peer that connects and then reads nothing until it is let go, when
// its connection closes. Let go and joined on destruction, so a failed
// assertion cannot leave its thread running.
class TlsOutboxSilentPeer {
public:
    TlsOutboxSilentPeer(std::uint16_t port, std::shared_ptr<clink::network::TlsClientContext> ctx)
        : thread_([this, port, ctx = std::move(ctx)] { run_(port, ctx); }) {}
    ~TlsOutboxSilentPeer() {
        let_go();
        thread_.join();
    }
    TlsOutboxSilentPeer(const TlsOutboxSilentPeer&) = delete;
    TlsOutboxSilentPeer& operator=(const TlsOutboxSilentPeer&) = delete;
    TlsOutboxSilentPeer(TlsOutboxSilentPeer&&) = delete;
    TlsOutboxSilentPeer& operator=(TlsOutboxSilentPeer&&) = delete;

    void let_go() {
        {
            std::lock_guard lock(mu_);
            let_go_ = true;
        }
        cv_.notify_all();
    }

private:
    void run_(std::uint16_t port, const std::shared_ptr<clink::network::TlsClientContext>& ctx) {
        try {
            const auto conn = clink::network::connect_tls_connection("127.0.0.1", port, ctx);
            std::unique_lock lock(mu_);
            cv_.wait(lock, [this] { return let_go_; });
        } catch (const std::exception&) {
            // The server side's assertions say what went wrong.
        }
    }

    std::mutex mu_;
    std::condition_variable cv_;
    bool let_go_{false};
    std::thread thread_;
};

// Runs an outbox's writer on a thread of its own, as an owner does.
class TlsOutboxWriterThread {
public:
    explicit TlsOutboxWriterThread(std::shared_ptr<Outbox> outbox)
        : outbox_(std::move(outbox)), thread_([this] { run_(); }) {}
    ~TlsOutboxWriterThread() {
        outbox_->abort("the test is over");
        thread_.join();
    }
    TlsOutboxWriterThread(const TlsOutboxWriterThread&) = delete;
    TlsOutboxWriterThread& operator=(const TlsOutboxWriterThread&) = delete;
    TlsOutboxWriterThread(TlsOutboxWriterThread&&) = delete;
    TlsOutboxWriterThread& operator=(TlsOutboxWriterThread&&) = delete;

    bool returned_within(Clock::duration bound) {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, bound, [this] { return returned_; });
    }

private:
    void run_() {
        outbox_->run_writer();
        std::lock_guard lock(mu_);
        returned_ = true;
        cv_.notify_all();
    }

    std::shared_ptr<Outbox> outbox_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool returned_{false};
    std::thread thread_;
};

}  // namespace

TEST(TlsOutbox, AbortFreesAWriterBlockedInSslWrite) {
    if (!clink::network::TlsServerContext::is_real_implementation()) {
        GTEST_SKIP() << "Built without OpenSSL";
    }
    const auto cert_dir = tls_outbox_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable, can't fixture self-signed cert";
    }
    auto server_ctx = std::make_shared<clink::network::TlsServerContext>(
        (cert_dir / "cert.pem").string(), (cert_dir / "key.pem").string());
    auto client_ctx =
        std::make_shared<clink::network::TlsClientContext>((cert_dir / "cert.pem").string());
    std::uint16_t port = 0;
    const int listener = NetworkSocket::listen_on(port, "127.0.0.1");
    ASSERT_GE(listener, 0);

    TlsOutboxSilentPeer peer(port, client_ctx);
    std::shared_ptr<clink::network::Connection> local;
    try {
        local = clink::network::accept_tls_connection(listener, server_ctx);
    } catch (const std::exception& e) {
        ADD_FAILURE() << "the TLS accept failed: " << e.what();
    }
    NetworkSocket::close(listener);
    ASSERT_NE(local, nullptr);

    auto outbox = std::make_shared<Outbox>(local, "worker 'tls'");
    TlsOutboxWriterThread writer(outbox);
    // Far more than the two sockets' buffers hold, so SSL_write blocks.
    ASSERT_EQ(outbox->post(std::make_shared<const std::vector<std::byte>>(
                               std::size_t{64} * 1024 * 1024, std::byte{0x5a}),
                           FrameClass::Bulk),
              PostResult::Queued);
    const auto stall_deadline = Clock::now() + 10s;
    while (outbox->stalled_for(Clock::now()) < 200ms && Clock::now() < stall_deadline) {
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_GE(outbox->stalled_for(Clock::now()), 200ms) << "the writer was never held in SSL_write";

    outbox->abort("worker lost: the test gave it up");
    // Well inside the five seconds after which Darwin's zero-window probe lets
    // one more chunk through, so a writer that returned only at that chunk
    // fails here; far longer than the wake takes.
    const bool returned = writer.returned_within(2s);
    // Either way the peer goes now, which also frees a writer abort did not.
    peer.let_go();
    EXPECT_TRUE(returned) << "abort did not wake a writer blocked in SSL_write";

    const auto s = outbox->snapshot();
    EXPECT_EQ(s.state, OutboxState::Stopped);
    EXPECT_EQ(s.stop_reason, OutboxStopReason::Aborted);
    EXPECT_EQ(s.frames_discarded, 1U);
    std::filesystem::remove_all(cert_dir);
}
