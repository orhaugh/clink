#pragma once

// Host ports for the test containers' published ports.
//
// The Docker daemon binds a published port, not the test, so a fixture cannot
// hold the port until the container takes it, the way ReservedPort holds one
// for a clink process it spawns. What a fixture can do, and every fixture here
// does through start_on_free_ports():
//
//   - take only a port that is free when it is chosen: one a socket can bind
//     on 0.0.0.0 and on :: (the daemon publishes on both), without
//     SO_REUSEADDR, so a port still in TIME_WAIT counts as taken;
//   - choose from a range below both kernels' ephemeral ranges (Linux 32768
//     to 60999, macOS 49152 to 65535), so the kernel never gives one of these
//     ports to an outgoing connection between the check and the bind: only a
//     process that binds it explicitly can take it;
//   - when the daemon still finds a port taken, because something bound it in
//     the moment between, remove the half-made container and start again on
//     fresh ports rather than fail the test.

#include <cerrno>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "docker_command.hpp"

namespace clink::test {

inline constexpr std::uint16_t kFirstContainerPort = 20000;
inline constexpr std::uint16_t kLastContainerPort = 32767;

// Whether a TCP socket of `family` can bind `port` on that family's wildcard
// address. A host without the family (no IPv6) cannot have the port taken on
// it, so it counts as free there.
inline bool port_binds(int family, std::uint16_t port) {
    const int fd = ::socket(family, SOCK_STREAM, 0);
    if (fd < 0) {
        return errno == EAFNOSUPPORT;
    }
    int rc = -1;
    if (family == AF_INET6) {
        // Only the IPv6 wildcard: the IPv4 one is checked on its own.
        int one = 1;
        (void)::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof(one));
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_any;
        addr.sin6_port = htons(port);
        rc = ::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    } else {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
        rc = ::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    }
    const int err = errno;
    ::close(fd);
    return rc == 0 || (family == AF_INET6 && err == EADDRNOTAVAIL);
}

// A host port nothing holds on IPv4 or IPv6, from the container range and not
// in `exclude`. Throws when a few hundred random candidates all turn out
// taken.
inline std::uint16_t free_host_port(const std::vector<std::uint16_t>& exclude = {}) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(kFirstContainerPort, kLastContainerPort);
    for (int i = 0; i < 400; ++i) {
        const auto port = static_cast<std::uint16_t>(dist(gen));
        bool excluded = false;
        for (const auto e : exclude) {
            excluded = excluded || e == port;
        }
        if (!excluded && port_binds(AF_INET, port) && port_binds(AF_INET6, port)) {
            return port;
        }
    }
    throw std::runtime_error("no free host port in " + std::to_string(kFirstContainerPort) + "-" +
                             std::to_string(kLastContainerPort) + " after 400 candidates");
}

// Whether a failed `docker run`'s output says a published host port was
// taken: the daemon's wording differs between Docker Engine and Docker
// Desktop, and over versions.
inline bool docker_port_taken(const std::string& out) {
    return out.find("address already in use") != std::string::npos ||
           out.find("port is already allocated") != std::string::npos ||
           out.find("failed to bind host port") != std::string::npos ||
           out.find("ports are not available") != std::string::npos;
}

struct StartedContainer {
    std::vector<std::uint16_t> ports;
    std::string name;
};

// Runs the `docker run` that `command(ports, name)` builds, on `count` free
// host ports, for the container `name_for(ports)` names. When the daemon finds
// a port taken, the half-made container is removed and the run starts again
// on fresh ports, up to `attempts` times. Returns the ports and name the
// container started with. Throws, with docker's output, on any other failure,
// or when every attempt met a taken port.
template <typename NameFor, typename Command>
StartedContainer start_on_free_ports(const std::string& who,
                                     std::size_t count,
                                     const NameFor& name_for,
                                     const Command& command,
                                     int attempts = 8) {
    std::string last;
    for (int attempt = 1; attempt <= attempts; ++attempt) {
        StartedContainer c;
        for (std::size_t i = 0; i < count; ++i) {
            c.ports.push_back(free_host_port(c.ports));
        }
        c.name = name_for(c.ports);
        const auto started = run_docker_command(command(c.ports, c.name));
        if (started.status == 0) {
            return c;
        }
        // A run that fails to publish its port leaves the container created
        // but not started; --rm does not remove a container that never ran.
        (void)run_docker_command("docker rm -f " + c.name);
        if (!docker_port_taken(started.out)) {
            throw std::runtime_error(who + ": docker run failed (exit " +
                                     std::to_string(started.status) + "): " + started.out);
        }
        last = started.out;
    }
    throw std::runtime_error(who + ": docker run found a host port taken on all " +
                             std::to_string(attempts) + " attempts; last: " + last);
}

// The usual naming: a prefix and the first host port, which is unique among
// running containers because the port is.
inline auto named_by_port(std::string prefix) {
    return [prefix = std::move(prefix)](const std::vector<std::uint16_t>& ports) {
        return prefix + std::to_string(ports.front());
    };
}

}  // namespace clink::test
