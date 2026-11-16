// A cancelled subtask exits even when the peer it sends to has stopped
// reading and never closed its socket.
//
// A Worker killed by its kernel normally closes its sockets, and a sender
// blocked on it is woken by the FIN or RST. A Worker whose host vanished, or
// whose close was lost, sends nothing: the sender's credit wait, or its
// send() into a full socket buffer, then has nothing left to wake it. The
// sink runner's cancel only closes its input channel, so until the network
// sink watched the task's cancel signal, that subtask never exited. The
// Worker's drain waits for every subtask before it re-registers, so its
// slots stayed unavailable and a job needing them stayed parked for capacity.
//
// The peer here accepts the connection and then neither reads nor closes,
// which is what such a sender sees. Each case gives the cancel five seconds,
// then breaks the connection itself so a failure ends the test instead of
// hanging it.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/network/local_data_plane.hpp"
#include "clink/runtime/network/network_bridge.hpp"
#include "clink/runtime/network/network_channel.hpp"
#include "clink/runtime/network/network_socket.hpp"
#include "clink/runtime/network/wire.hpp"

using namespace clink;
using namespace clink::network;
using namespace std::chrono_literals;

namespace {

// Emits batches of 256 as fast as the sink takes them, until cancelled.
class NetworkSinkCancelFloodSource final : public Source<std::int64_t> {
public:
    bool produce(Emitter<std::int64_t>& out) override {
        if (this->cancelled()) {
            return false;
        }
        Batch<std::int64_t> b;
        for (int i = 0; i < 256; ++i) {
            b.emplace(next_++);
        }
        return out.emit_data(std::move(b));
    }
    std::string name() const override { return "network_sink_cancel.source"; }

private:
    std::int64_t next_{0};
};

// Accepts one connection, optionally grants `grant` credits, and then neither
// reads nor closes until reset() or destruction.
class NetworkSinkCancelSilentPeer {
public:
    explicit NetworkSinkCancelSilentPeer(std::uint32_t grant) {
        listener_ = NetworkSocket::listen_on(port_);
        if (listener_ < 0) {
            throw std::runtime_error("silent peer: listen failed");
        }
        accept_thread_ = std::thread([this, grant] {
            const int fd = NetworkSocket::accept_one(listener_);
            if (fd < 0) {
                return;
            }
            if (grant > 0) {
                std::vector<std::byte> body;
                body.push_back(static_cast<std::byte>(Kind::CreditUpdate));
                put_u32_be(body, grant);
                std::vector<std::byte> header;
                put_u32_be(header, static_cast<std::uint32_t>(body.size()));
                (void)NetworkSocket::send_all(fd, header.data(), header.size());
                (void)NetworkSocket::send_all(fd, body.data(), body.size());
            }
            fd_.store(fd, std::memory_order_release);
        });
    }

    ~NetworkSinkCancelSilentPeer() {
        if (fd_.load(std::memory_order_acquire) < 0) {
            // Nobody connected: connect to ourselves so the accept returns.
            const int self = NetworkSocket::connect_to("127.0.0.1", port_);
            if (accept_thread_.joinable()) {
                accept_thread_.join();
            }
            if (self >= 0) {
                ::close(self);
            }
        } else if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
        reset();
        ::close(listener_);
    }

    NetworkSinkCancelSilentPeer(const NetworkSinkCancelSilentPeer&) = delete;
    NetworkSinkCancelSilentPeer& operator=(const NetworkSinkCancelSilentPeer&) = delete;

    std::uint16_t port() const noexcept { return port_; }
    bool accepted() const noexcept { return fd_.load(std::memory_order_acquire) >= 0; }

    // Bytes the sender has put in our receive buffer that we have not read.
    std::size_t unread_bytes() const {
        int n = 0;
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd < 0 || ::ioctl(fd, FIONREAD, &n) != 0) {
            return 0;
        }
        return static_cast<std::size_t>(n);
    }

    // Abortive close (RST), the close a sender CAN hear.
    void reset() {
        const int fd = fd_.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) {
            ::linger l{};
            l.l_onoff = 1;
            l.l_linger = 0;
            ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));
            ::close(fd);
        }
    }

private:
    std::uint16_t port_{0};
    int listener_{-1};
    std::atomic<int> fd_{-1};
    std::thread accept_thread_;
};

bool network_sink_cancel_await(const std::function<bool()>& pred,
                               std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(5ms);
    }
    return pred();
}

// True once the peer's unread byte count has been non-zero and unchanged for
// 300 ms: the sender has stopped sending, on credit or on a full buffer.
bool network_sink_cancel_await_stall(const NetworkSinkCancelSilentPeer& peer,
                                     std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t last = 0;
    auto steady_since = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() < deadline) {
        const auto now_bytes = peer.unread_bytes();
        const auto now = std::chrono::steady_clock::now();
        if (now_bytes != last || now_bytes == 0) {
            last = now_bytes;
            steady_since = now;
        } else if (now - steady_since >= 300ms) {
            return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

struct NetworkSinkCancelOutcome {
    bool stalled = false;
    bool exited_on_cancel = false;
    std::vector<LocalExecutor::OperatorError> errors_before_cancel;
};

// Runs flood source -> NetworkBridgeSink -> silent peer, cancels the task once
// the sink has stalled, and reports whether the task exited on the cancel.
NetworkSinkCancelOutcome network_sink_cancel_run(std::uint32_t grant) {
    ScopedDisableLocalDataPlane no_local;
    NetworkSinkCancelSilentPeer peer(grant);
    auto token = std::make_shared<std::atomic<bool>>(false);

    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<NetworkSinkCancelFloodSource>());
    dag.add_sink<std::int64_t>(
        src,
        std::make_shared<NetworkBridgeSink<std::int64_t>>("127.0.0.1", peer.port(), int64_codec()));
    JobConfig cfg;
    cfg.external_cancel_token = token;
    NetworkSinkCancelOutcome outcome;
    {
        LocalExecutor exec(std::move(dag), cfg);
        exec.start();
        // Whatever happens below, the executor's destructor must not meet a
        // sink still blocked on the peer: the cancel, then the RST, release it.
        struct Release {
            std::shared_ptr<std::atomic<bool>> token;
            NetworkSinkCancelSilentPeer& peer;
            ~Release() {
                token->store(true, std::memory_order_release);
                peer.reset();
            }
        };
        const Release release{.token = token, .peer = peer};

        if (!network_sink_cancel_await([&] { return peer.accepted(); }, 5s)) {
            return outcome;
        }
        outcome.stalled = network_sink_cancel_await_stall(peer, 10s);
        if (!outcome.stalled) {
            return outcome;
        }
        token->store(true, std::memory_order_release);
        auto done = std::async(std::launch::async, [&exec] { exec.await_termination(); });
        outcome.exited_on_cancel = done.wait_for(5s) == std::future_status::ready;
        if (!outcome.exited_on_cancel) {
            peer.reset();
            done.wait();
        }
        outcome.errors_before_cancel = exec.operator_errors_before_cancel();
    }
    return outcome;
}

}  // namespace

TEST(NetworkSinkCancel, ASinkWaitingForCreditFromASilentPeerExitsOnCancel) {
    // 1024 credits: four batches of 256, then the fifth waits for a grant
    // the peer never sends.
    const auto outcome = network_sink_cancel_run(1024);
    ASSERT_TRUE(outcome.stalled) << "the sink never stalled on the peer, so this tests nothing";
    EXPECT_TRUE(outcome.exited_on_cancel)
        << "the cancelled task stayed blocked waiting for credit from a peer that "
           "neither reads nor closes; only breaking the connection released it";
    EXPECT_TRUE(outcome.errors_before_cancel.empty())
        << "the task failed before it was cancelled: "
        << outcome.errors_before_cancel.front().second;
}

TEST(NetworkSinkCancel, ASinkBlockedInSendToASilentPeerExitsOnCancel) {
    // Credit is no limit here; the socket buffers fill and send() blocks.
    const auto outcome = network_sink_cancel_run(0x7fffffffU);
    ASSERT_TRUE(outcome.stalled) << "the sink never stalled on the peer, so this tests nothing";
    EXPECT_TRUE(outcome.exited_on_cancel)
        << "the cancelled task stayed blocked in send() to a peer that neither reads nor "
           "closes; only breaking the connection released it";
    EXPECT_TRUE(outcome.errors_before_cancel.empty())
        << "the task failed before it was cancelled: "
        << outcome.errors_before_cancel.front().second;
}

TEST(NetworkSinkCancel, APeerThatKeepsReadingStillSeesTheStreamEndAsCancelled) {
    // The cancel must not turn into a clean finish on the wire: a downstream
    // aligner that reads it as end-of-input fires every open window.
    ScopedDisableLocalDataPlane no_local;
    NetworkChannelSource<std::int64_t> rx(/*port=*/0, int64_codec());
    const auto port = rx.listen();
    std::thread acceptor([&rx] { rx.accept(); });

    auto token = std::make_shared<std::atomic<bool>>(false);
    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<NetworkSinkCancelFloodSource>());
    dag.add_sink<std::int64_t>(
        src, std::make_shared<NetworkBridgeSink<std::int64_t>>("127.0.0.1", port, int64_codec()));
    JobConfig cfg;
    cfg.external_cancel_token = token;
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();
    acceptor.join();

    std::size_t records = 0;
    while (records < 10'000) {
        auto e = rx.pop();
        ASSERT_TRUE(e.has_value()) << "the stream ended before the cancel";
        if (e->is_data()) {
            records += e->as_data().size();
        }
    }
    token->store(true, std::memory_order_release);
    while (rx.pop().has_value()) {
    }
    exec.await_termination();
    EXPECT_TRUE(rx.close_cancelled()) << "a cancelled sender's stream ended as a clean finish";
    EXPECT_TRUE(exec.operator_errors_before_cancel().empty());
}
