#include "native/socket_factory.hpp"

#include <utility>

#include "native/errors.hpp"

namespace clink::clickhouse::native {

void SocketControl::poison() noexcept {
    poisoned_.store(true, std::memory_order_release);
}

bool SocketControl::poisoned() const noexcept {
    return poisoned_.load(std::memory_order_acquire);
}

std::uint64_t SocketControl::bytes_written() const noexcept {
    return written_.load(std::memory_order_relaxed);
}

std::uint64_t SocketControl::bytes_read() const noexcept {
    return read_.load(std::memory_order_relaxed);
}

void SocketControl::attach_fd(int /*fd*/) noexcept {}

void SocketControl::detach_fd() noexcept {}

void SocketControl::add_written(std::size_t n) noexcept {
    written_.fetch_add(n, std::memory_order_relaxed);
}

void SocketControl::add_read(std::size_t n) noexcept {
    read_.fetch_add(n, std::memory_order_relaxed);
}

CountingSocketFactory::CountingSocketFactory(const ::clickhouse::ClientOptions& /*opts*/,
                                             bool /*tls*/,
                                             std::shared_ptr<SocketControl> control)
    : control_(std::move(control)) {
    not_implemented("CountingSocketFactory");
}

CountingSocketFactory::~CountingSocketFactory() = default;

std::unique_ptr<::clickhouse::SocketBase> CountingSocketFactory::connect(
    const ::clickhouse::ClientOptions& /*opts*/, const ::clickhouse::Endpoint& /*endpoint*/) {
    not_implemented("CountingSocketFactory::connect");
}

int socket_fd(const ::clickhouse::Socket& /*socket*/) noexcept {
    return -1;
}

}  // namespace clink::clickhouse::native
