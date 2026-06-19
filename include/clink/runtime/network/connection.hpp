#pragma once

// Connection is the cluster-side abstraction over a transport (plain TCP
// or TLS). JM and TM hold one of these per peer instead of bare int fds
// so the TLS variant can be slotted in without forking the cluster
// machinery.
//
// Why an abstraction rather than two parallel code paths: a JM accepting
// connections has dozens of call sites (frame readers, watchdog, cancel
// broadcast, deploy dispatch). Each one would need a switch on "is this
// peer TLS?". The PIMPL interface here keeps all that ignorance of the
// transport in one place.
//
// shutdown_read is exposed even on TLS where the semantics are weaker
// (TLS doesn't have a true unidirectional half-close); the watchdog
// uses it to interrupt blocked recv() during cancellation. On TLS the
// implementation does a hard close, which is fine for the watchdog
// use case (the connection is going away anyway).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace clink::network {

class Connection {
public:
    virtual ~Connection() = default;

    // Send all `len` bytes. Returns true on success.
    virtual bool send_all(const std::byte* buf, std::size_t len) = 0;

    // Receive exactly `len` bytes. Returns true on success, false on
    // peer close or transport error.
    virtual bool recv_all(std::byte* buf, std::size_t len) = 0;

    // Half-close send side (graceful EOF to peer). Best-effort.
    virtual void shutdown_write() = 0;

    // Interrupt a blocked recv() on this connection. On TLS this is a
    // hard close; on plain TCP it's shutdown(SHUT_RD). Used by the
    // watchdog to break readers when a peer is declared lost.
    virtual void shutdown_read() = 0;

    // Final tear-down. Idempotent. After close(), is_open() returns false
    // and further send/recv calls return false.
    virtual void close() = 0;

    virtual bool is_open() const noexcept = 0;
};

// Wrap an already-accepted int fd as a plain-TCP Connection. Takes
// ownership: close() closes the fd.
std::unique_ptr<Connection> make_plain_connection(int fd);

// Plain-TCP connect to host:port; returns nullptr on failure.
std::unique_ptr<Connection> connect_plain(const std::string& host, std::uint16_t port);

}  // namespace clink::network
