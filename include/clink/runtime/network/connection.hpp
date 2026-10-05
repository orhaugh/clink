#pragma once

// Connection is the cluster-side abstraction over a transport (plain TCP
// or TLS). coordinator and worker hold one of these per peer instead of bare int fds
// so the TLS variant can be slotted in without forking the cluster
// machinery.
//
// Why an abstraction rather than two parallel code paths: a coordinator accepting
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

#include <chrono>
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

    // Bound how long a single recv() on this connection may block.
    //
    // The coordinator sets it while it reads a new peer's FIRST frame, so a
    // peer that opens a socket and sends nothing is dropped at the first read
    // that times out.
    //
    // Returns false when the transport cannot set one. Bounds each recv(), not
    // a whole recv_all: a peer dribbling one byte per window can still stretch
    // a transfer. The coordinator's admission deadline is what bounds the
    // whole first frame (Coordinator::Config::handshake_timeout); it ends the
    // read with shutdown_read, so a transport that cannot set this is still
    // bounded.
    virtual bool set_recv_timeout(std::chrono::milliseconds /*timeout*/) { return false; }

    // Bound how long a single send() on this connection may block; zero
    // clears it. A send that times out fails, and the connection is not to be
    // used again (part of a frame may have gone).
    //
    // The coordinator sets it while it replies to a peer it has not yet
    // admitted: a peer that never reads would otherwise hold the thread
    // replying to it, and whatever that thread holds, in send() for as long
    // as it keeps the connection open. Returns false when the transport
    // cannot set one. Like set_recv_timeout, it bounds each send(), not a
    // whole send_all; the coordinator's replies to an unadmitted peer are
    // small enough to fit a fresh socket's send buffer, so one send() is the
    // whole of each.
    virtual bool set_send_timeout(std::chrono::milliseconds /*timeout*/) { return false; }
};

// Wrap an already-accepted int fd as a plain-TCP Connection. Takes
// ownership: close() closes the fd.
std::unique_ptr<Connection> make_plain_connection(int fd);

// Plain-TCP connect to host:port; returns nullptr on failure.
std::unique_ptr<Connection> connect_plain(const std::string& host, std::uint16_t port);

}  // namespace clink::network
