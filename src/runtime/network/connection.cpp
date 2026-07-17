#include "clink/runtime/network/connection.hpp"

#include <mutex>

#include "clink/runtime/network/network_socket.hpp"

namespace clink::network {

namespace {

class PlainTcpConnection final : public Connection {
public:
    explicit PlainTcpConnection(int fd) noexcept : fd_(fd) {}
    ~PlainTcpConnection() override { close(); }

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
        if (fd_ < 0)
            return false;
        return NetworkSocket::send_all(fd_, buf, len);
    }

    bool recv_all(std::byte* buf, std::size_t len) override {
        if (fd_ < 0)
            return false;
        return NetworkSocket::recv_all(fd_, buf, len);
    }

    void shutdown_write() override {
        if (fd_ >= 0)
            NetworkSocket::shutdown_write(fd_);
    }

    void shutdown_read() override {
        if (fd_ >= 0)
            NetworkSocket::shutdown_read(fd_);
    }

    void close() override {
        if (fd_ >= 0) {
            NetworkSocket::close(fd_);
            fd_ = -1;
        }
    }

    bool is_open() const noexcept override { return fd_ >= 0; }

private:
    int fd_;
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
