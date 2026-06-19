#pragma once

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
    static int accept_one(int listener_fd);

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

}  // namespace clink::network
