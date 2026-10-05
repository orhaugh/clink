#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

namespace clink::network {

// Default bind host for the data plane (NetworkBridgeSource /
// NetworkChannelSource). Reads CLINK_DATA_BIND_HOST when set,
// otherwise 127.0.0.1 - the safe single-host default. Docker
// compose, k8s, or any multi-host deployment sets the env var to
// 0.0.0.0 so subtask data-plane ports are reachable across
// containers/nodes.
inline std::string default_data_bind_host() {
    if (const char* env = std::getenv("CLINK_DATA_BIND_HOST"); env != nullptr && *env != '\0') {
        return env;
    }
    return "127.0.0.1";
}

class AcceptWake;

// Thin RAII-free wrappers around POSIX socket APIs. Returns are -1 on
// failure; the higher-level NetworkChannelSink/Source classes translate to
// exceptions. Splitting these out of the templated NetworkChannel<T> lets
// us keep the system-call code in a .cpp instead of forcing every
// translation unit that includes network_channel.hpp to pull in
// <sys/socket.h>.
class NetworkSocket {
public:
    // Connect to host:port over TCP; returns the connected fd or -1.
    static int connect_to(const std::string& host, std::uint16_t port);

    // Bind to bind_host:port and listen. If port == 0, the OS picks one
    // and writes it back via the out-param.
    //
    // bind_host accepts:
    //   "127.0.0.1"  - loopback only (default; safe for single-host tests)
    //   "0.0.0.0"    - all interfaces (required for multi-machine clusters)
    //   "1.2.3.4"    - bind to a specific local IPv4 address
    //
    // Note: binding non-loopback exposes the port to the network. Pair
    // with TLS / mTLS for any deployment beyond a trusted local network.
    static int listen_on(std::uint16_t& port, std::string_view bind_host = "127.0.0.1");

    // Block until a single connection arrives, returning the accepted fd.
    // The accepted fd is always blocking, even from a non-blocking
    // listener (an accepted socket inherits O_NONBLOCK on Darwin and the
    // BSDs, not on Linux). On a non-blocking listener with nothing pending
    // this returns -1 with errno EAGAIN/EWOULDBLOCK.
    //
    // Nothing can wake this call portably, so a thread parked here must not
    // be stopped by closing the listener under it: see the overload below.
    static int accept_one(int listener_fd);

    // Wait for a connection on listener_fd OR for `wake` to be woken, and
    // accept it. Returns the accepted (blocking) fd, or -1 with errno
    // ECANCELED once woken (a connection accepted after the wake is closed,
    // not returned); any other -1 is a real accept failure with
    // errno preserved. Puts the listener in non-blocking mode, so a
    // connection that disappears between the readiness check and accept()
    // sends this back to waiting rather than parking it in accept().
    //
    // This is the only correct way to stop a thread waiting on a listener:
    // wake it, join it, and only THEN close the listener. Closing the
    // listener to wake the thread is not: on Darwin, a close() that lands
    // while the other thread is entering accept() can miss it, and then
    // both block - accept() asleep, and close() waiting uninterruptibly for
    // accept() to let go of the descriptor - until a connection happens to
    // arrive. On Linux close() does not wake accept() at all, and a
    // descriptor closed under a thread about to use it can be reissued to
    // another listener in the same process before that thread arrives.
    static int accept_one(int listener_fd, const AcceptWake& wake);

    // The waiting half of the overload above, for a caller whose accept is
    // done elsewhere (the coordinator's pluggable accept factory). True when
    // a connection is pending; false once woken (errno ECANCELED) or if the
    // wait fails (errno preserved). Also puts the listener in non-blocking
    // mode, so the accept_one(listener_fd) that follows returns EAGAIN
    // rather than parking if the connection has gone in the meantime.
    static bool wait_for_connection(int listener_fd, const AcceptWake& wake);

    // Send all `len` bytes of `buf`. Returns true on success.
    static bool send_all(int fd, const std::byte* buf, std::size_t len);

    // Receive exactly `len` bytes into `buf`. Returns true on success,
    // false on connection close or error.
    static bool recv_all(int fd, std::byte* buf, std::size_t len);

    // Best-effort half-close on send side (signals EOF to the peer
    // without freeing the fd). Useful for graceful shutdown.
    static void shutdown_write(int fd);

    // Half-close on receive side. A blocked recv() from another thread
    // returns 0; used to interrupt a NetworkChannelSource::pop() during
    // cancellation.
    static void shutdown_read(int fd);

    static void close(int fd);
};

// Wakes a thread waiting in NetworkSocket::accept_one(listener, wake), so
// its owner can join it before closing the listener. A wake that lands
// before the thread starts waiting is not lost: every later wait returns at
// once too. Created before the accepting thread starts and destroyed after
// it has been joined.
//
// How it wakes differs by platform, so that a waiting receiver costs no
// descriptors where it matters. On Linux, wake() shuts the listener down
// (shutdown(SHUT_RD)), which wakes a poll() or accept() on it, persists for
// later waits, and leaves the descriptor open. Darwin and the BSDs ignore
// shutdown() on a listener, so there it is a self-pipe whose byte is never
// read. Either way the listener is never closed by the wake.
//
// Because the Linux wake acts on the listener's descriptor, wake() must not
// be called once the owner has closed the listener (the number may already
// belong to another socket): release the AcceptWake before closing it.
class AcceptWake {
public:
    // Throws std::runtime_error if the pipe cannot be created (Darwin/BSD).
    explicit AcceptWake(int listener_fd);
    ~AcceptWake();
    AcceptWake(const AcceptWake&) = delete;
    AcceptWake& operator=(const AcceptWake&) = delete;
    AcceptWake(AcceptWake&&) = delete;
    AcceptWake& operator=(AcceptWake&&) = delete;

    // Idempotent and safe from any thread.
    void wake() noexcept;

    [[nodiscard]] bool woken() const noexcept { return woken_.load(std::memory_order_acquire); }

    // The pipe's read end, for poll(); -1 on Linux, where the listener
    // itself is what becomes ready.
    [[nodiscard]] int fd() const noexcept { return read_fd_; }

private:
    // Read only by the Linux wake.
    [[maybe_unused]] int listener_fd_;
    std::atomic<bool> woken_{false};
    int read_fd_{-1};
    int write_fd_{-1};
};

}  // namespace clink::network
