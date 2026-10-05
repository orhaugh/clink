#pragma once

#include <atomic>
#include <chrono>
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
    // Connect to host:port over TCP; returns the connected fd or -1. Bounded
    // only by the kernel's own connect timeout (about two minutes with
    // Linux's default SYN retries).
    static int connect_to(const std::string& host, std::uint16_t port);

    // What connect_within came to.
    struct ConnectAttempt {
        // The connected socket, blocking, as connect_to returns it; -1 when
        // the connect failed.
        int fd{-1};
        // When fd is -1 and the host resolved: the connect's error, and
        // ETIMEDOUT when `deadline` passed with the connect still under way.
        int error{0};
        // The host did not resolve; nothing was attempted.
        bool unresolved{false};
        // The deadline the connect ran under, for a caller that holds what
        // follows it (a handshake) to the same one; time_point::max() when
        // there was none.
        std::chrono::steady_clock::time_point deadline{
            std::chrono::steady_clock::time_point::max()};
    };

    // As connect_to, within `timeout` of the host resolving (zero or less:
    // no deadline). The connect runs non-blocking and its completion is
    // waited for in poll(), then read from SO_ERROR. The deadline starts
    // after name resolution, which has resolver timeouts of its own (five
    // seconds a nameserver under glibc's defaults) that nothing here can cut
    // short, and which would otherwise spend the whole bound before a packet
    // was sent.
    static ConnectAttempt connect_within(const std::string& host,
                                         std::uint16_t port,
                                         std::chrono::milliseconds timeout);

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
    // As above, and false with errno ETIMEDOUT once `deadline` passes with
    // nothing pending, for a caller that has other work due by then (the
    // coordinator's admission deadlines). time_point::max() waits without one.
    static bool wait_for_connection(int listener_fd,
                                    const AcceptWake& wake,
                                    std::chrono::steady_clock::time_point deadline);

    // now() + timeout, saturating: a timeout too large to add to the clock
    // (milliseconds::max(), say, as "no limit") gives time_point::max(),
    // which every wait here reads as no deadline, rather than overflowing
    // into a deadline in the past. Zero or less gives now().
    static std::chrono::steady_clock::time_point deadline_after(std::chrono::milliseconds timeout);

    // What wait_ready saw.
    enum class WaitResult : std::uint8_t {
        Ready,     // fd is readable (or writable), or has an error or hangup to report
        Woken,     // `wake` was woken; checked first, so it wins over Ready
        TimedOut,  // `deadline` passed first
        Failed,    // poll() failed, errno preserved
    };

    // Wait until `fd` is ready to read (or, with want_write, to write), until
    // `wake` is woken, or until `deadline`, whichever comes first. For a
    // thread that must do bounded I/O on an accepted connection before it
    // can go back to its listener, such as a non-blocking TLS handshake on
    // an accept thread: the deadline stops one peer from holding the thread,
    // and the wake lets the owner stop it at once. `wake` may be null (no
    // wake) and `deadline` may be time_point::max() (no deadline). A wake
    // that came before the call returns Woken at once.
    static WaitResult wait_ready(int fd,
                                 bool want_write,
                                 const AcceptWake* wake,
                                 std::chrono::steady_clock::time_point deadline);

    // Wait until `wake` is woken or `deadline` passes, and nothing else: a
    // pending connection on the wake's listener does not end it. True once
    // woken. For an accept thread that must not call accept() for a while
    // (it failed for want of a descriptor, and the connection is still
    // pending, so waiting on the listener would return at once) but must
    // still stop when told to.
    static bool wait_for_wake(const AcceptWake& wake,
                              std::chrono::steady_clock::time_point deadline);

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
    // wait_ready waits on the wake alongside another descriptor.
    friend class NetworkSocket;

    // Read only on Linux: by the wake, and by wait_ready, which polls it
    // with no events requested, so a pending connection does not register
    // and only the shutdown that is the wake does (as POLLHUP).
    [[maybe_unused]] int listener_fd_;
    std::atomic<bool> woken_{false};
    int read_fd_{-1};
    int write_fd_{-1};
};

}  // namespace clink::network
