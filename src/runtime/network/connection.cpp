#include "clink/runtime/network/connection.hpp"

#include <atomic>
#include <mutex>

#include <sys/socket.h>
#include <sys/time.h>

#include "clink/runtime/network/network_socket.hpp"

namespace clink::network {

namespace {

class PlainTcpConnection final : public Connection {
public:
    explicit PlainTcpConnection(int fd) noexcept : fd_(fd) {}
    // The actual ::close happens HERE and only here. Connections are held
    // by shared_ptr, and every thread that touches the socket holds a
    // reference across the call, so by the time the destructor runs no
    // reader can be inside recv on this fd - which is the condition
    // ::close needs: closing a descriptor another thread is blocked on is
    // an fd-reuse hazard, and TSan rightly reports the close/recv pair as
    // a race at the descriptor level even with the fd variable atomic.
    ~PlainTcpConnection() override {
        const int fd = fd_.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) {
            NetworkSocket::close(fd);
        }
    }

    PlainTcpConnection(const PlainTcpConnection&) = delete;
    PlainTcpConnection& operator=(const PlainTcpConnection&) = delete;

    // Serialize sends: a single connection is written from multiple threads (the
    // coordinator sends TriggerCheckpoint from both the periodic checkpoint loop and the
    // client-triggered savepoint path, plus deploy/cancel/heartbeat). Without this
    // lock two frames interleave byte-wise on the socket, the peer reads a
    // misframed stream, and a decode throws "MessageReader: truncated payload"
    // (uncaught -> the process aborts). recv is single-reader per connection so it
    // needs no lock; send + recv are independent directions.
    bool send_all(const std::byte* buf, std::size_t len) override {
        std::lock_guard<std::mutex> lk(send_mu_);
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd < 0 || closed_.load(std::memory_order_acquire))
            return false;
        return NetworkSocket::send_all(fd, buf, len);
    }

    bool recv_all(std::byte* buf, std::size_t len) override {
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd < 0 || closed_.load(std::memory_order_acquire))
            return false;
        return NetworkSocket::recv_all(fd, buf, len);
    }

    bool set_recv_timeout(std::chrono::milliseconds timeout) override {
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd < 0) {
            return false;
        }
        // Zero clears it (blocking again), which is what the coordinator does
        // once a connection is admitted and handed to its own thread.
        struct timeval tv{};
        tv.tv_sec = static_cast<decltype(tv.tv_sec)>(timeout.count() / 1000);
        tv.tv_usec = static_cast<decltype(tv.tv_usec)>((timeout.count() % 1000) * 1000);
        return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
    }

    void shutdown_write() override {
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd >= 0)
            NetworkSocket::shutdown_write(fd);
    }

    void shutdown_read() override {
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd >= 0)
            NetworkSocket::shutdown_read(fd);
    }

    // close() is the cross-thread WAKE, not the resource release. Reader
    // threads block in recv_all on this fd while another thread closes the
    // connection (the NetworkChannelSource defect family): shutting both
    // directions down wakes the blocked reader with an error and refuses
    // new IO, while the descriptor itself stays valid until the destructor
    // - after every referencing thread has let go - actually closes it.
    // Exactly one closer performs the shutdown.
    void close() override {
        if (closed_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd >= 0) {
            NetworkSocket::shutdown_read(fd);
            NetworkSocket::shutdown_write(fd);
        }
    }

    bool is_open() const noexcept override { return fd_.load(std::memory_order_acquire) >= 0; }

private:
    std::atomic<int> fd_;
    std::atomic<bool> closed_{false};
    std::mutex send_mu_;
};

}  // namespace

std::unique_ptr<Connection> make_plain_connection(int fd) {
    if (fd < 0)
        return nullptr;
    return std::make_unique<PlainTcpConnection>(fd);
}

std::unique_ptr<Connection> connect_plain(const std::string& host, std::uint16_t port) {
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0)
        return nullptr;
    return std::make_unique<PlainTcpConnection>(fd);
}

}  // namespace clink::network
