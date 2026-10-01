#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include <clickhouse/base/socket.h>
#include <clickhouse/client.h>

namespace clink::clickhouse::native {

// Shared by one client's CountingSocketFactory (owned by the client) and the
// transport (owned by the writer). Outlives both through shared_ptr; the
// wrapper streams hold it too, and never touch the socket in their
// destructors (the client destroys its socket before its streams).
class SocketControl {
public:
    // Sticky. Every later read, write or connect through this control throws
    // std::system_error(ECONNABORTED); the live fd, if any, is shut down
    // (SHUT_RDWR) under mu_, which wakes a recv or send blocked on another
    // thread.
    void poison() noexcept;
    [[nodiscard]] bool poisoned() const noexcept;
    [[nodiscard]] std::uint64_t bytes_written() const noexcept;
    [[nodiscard]] std::uint64_t bytes_read() const noexcept;

    // Used by the wrapper socket only.
    void attach_fd(int fd) noexcept;  // under mu_
    void detach_fd() noexcept;        // under mu_, before the inner socket closes the fd
    void add_written(std::size_t n) noexcept;
    void add_read(std::size_t n) noexcept;

private:
    mutable std::mutex mu_;
    int fd_{-1};
    std::atomic<bool> poisoned_{false};
    std::atomic<std::uint64_t> written_{0};
    std::atomic<std::uint64_t> read_{0};
};

// Wraps NonSecureSocketFactory, or the vendored SSLSocketFactory when `tls`,
// and returns a socket whose streams count bytes and honour the poison.
class CountingSocketFactory final : public ::clickhouse::SocketFactory {
public:
    CountingSocketFactory(const ::clickhouse::ClientOptions& opts,
                          bool tls,
                          std::shared_ptr<SocketControl> control);
    ~CountingSocketFactory() override;
    CountingSocketFactory(const CountingSocketFactory&) = delete;
    CountingSocketFactory& operator=(const CountingSocketFactory&) = delete;

    std::unique_ptr<::clickhouse::SocketBase> connect(
        const ::clickhouse::ClientOptions& opts, const ::clickhouse::Endpoint& endpoint) override;
    // Never sleeps: ping_before_query is off, so only the client's retry guard
    // would call it.
    void sleepFor(const std::chrono::milliseconds& /*duration*/) override {}

private:
    std::unique_ptr<::clickhouse::NonSecureSocketFactory> inner_;
    std::shared_ptr<SocketControl> control_;
};

// The fd behind a ::clickhouse::Socket, whose handle is protected. Exposed for
// tests.
[[nodiscard]] int socket_fd(const ::clickhouse::Socket& socket) noexcept;

}  // namespace clink::clickhouse::native
