// The test containers' host ports (docker_ports.hpp): a port is taken only when
// it binds on IPv4 and IPv6, from a range the kernel never gives to outgoing
// connections; a port held by anything is never handed out; and a docker run
// that finds its port taken starts again on fresh ports, while any other
// failure ends at once. The retry is driven through shell commands standing in
// for docker run, so no container is needed.

#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "tests/integration/docker_ports.hpp"

namespace {

using clink::test::docker_port_taken;
using clink::test::free_host_port;
using clink::test::kFirstContainerPort;
using clink::test::kLastContainerPort;
using clink::test::port_binds;
using clink::test::start_on_free_ports;

// A listener on `port` on the wildcard address of `family`, closed with the
// object; -1 when the family is unavailable here.
class Listener {
public:
    Listener(int family, std::uint16_t port) {
        fd_ = ::socket(family, SOCK_STREAM, 0);
        if (fd_ < 0) {
            return;
        }
        int rc = -1;
        if (family == AF_INET6) {
            int one = 1;
            (void)::setsockopt(fd_, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof(one));
            sockaddr_in6 addr{};
            addr.sin6_family = AF_INET6;
            addr.sin6_addr = in6addr_any;
            addr.sin6_port = htons(port);
            rc = ::bind(fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
        } else {
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_ANY);
            addr.sin_port = htons(port);
            rc = ::bind(fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
        }
        if (rc != 0 || ::listen(fd_, 1) != 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    ~Listener() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;
    [[nodiscard]] bool listening() const noexcept { return fd_ >= 0; }

private:
    int fd_{-1};
};

}  // namespace

TEST(DockerPorts, AFreePortIsFromTheRangeAndBindsOnBothFamilies) {
    for (int i = 0; i < 20; ++i) {
        const auto port = free_host_port();
        EXPECT_GE(port, kFirstContainerPort);
        EXPECT_LE(port, kLastContainerPort);
        EXPECT_TRUE(port_binds(AF_INET, port)) << port;
        EXPECT_TRUE(port_binds(AF_INET6, port)) << port;
    }
    const auto a = free_host_port();
    const auto b = free_host_port({a});
    EXPECT_NE(a, b);
}

TEST(DockerPorts, APortHeldOnEitherFamilyIsNotFree) {
    const auto v4 = free_host_port();
    {
        const Listener held(AF_INET, v4);
        ASSERT_TRUE(held.listening());
        EXPECT_FALSE(port_binds(AF_INET, v4)) << "a port with an IPv4 listener counted as free";
    }
    EXPECT_TRUE(port_binds(AF_INET, v4)) << "the port was not free again once released";

    const auto v6 = free_host_port();
    const Listener held6(AF_INET6, v6);
    if (!held6.listening()) {
        GTEST_SKIP() << "this host has no IPv6";
    }
    EXPECT_FALSE(port_binds(AF_INET6, v6)) << "a port with an IPv6 listener counted as free";
    EXPECT_TRUE(port_binds(AF_INET, v6)) << "the IPv4 check must see only IPv4";
}

TEST(DockerPorts, TheDaemonsWordingsForATakenPortAreRecognised) {
    // As Docker Engine on the CI runners and Docker Desktop on macOS wrote them.
    EXPECT_TRUE(docker_port_taken(
        "docker: Error response from daemon: failed to set up container networking: driver "
        "failed programming external connectivity on endpoint clink_test_pg_57474: failed to "
        "bind host port for 0.0.0.0:57474:172.17.0.2:5432/tcp: address already in use"));
    EXPECT_TRUE(docker_port_taken(
        "docker: Error response from daemon: ports are not available: exposing port TCP "
        "0.0.0.0:53342 -> 127.0.0.1:0: listen tcp 0.0.0.0:53342: bind: address already in use"));
    EXPECT_TRUE(docker_port_taken("Bind for 0.0.0.0:5432 failed: port is already allocated"));
    EXPECT_FALSE(docker_port_taken(
        "Unable to find image 'clink/none:1' locally\ndocker: Error response from daemon: pull "
        "access denied"));
}

TEST(DockerPorts, ATakenPortStartsAgainOnFreshPortsAndOtherFailuresEndAtOnce) {
    const auto name = [](const std::vector<std::uint16_t>& ports) {
        return "clink_test_ports_probe_" + std::to_string(ports.front());
    };
    {
        // Taken twice, then started: three runs, each on ports of its own.
        std::vector<std::vector<std::uint16_t>> tried;
        const auto started = start_on_free_ports(
            "probe", 2, name, [&](const std::vector<std::uint16_t>& ports, const std::string&) {
                tried.push_back(ports);
                return tried.size() < 3
                           ? std::string{"echo 'bind: address already in use'; exit 125"}
                           : std::string{"true"};
            });
        ASSERT_EQ(tried.size(), 3U);
        EXPECT_EQ(started.ports, tried.back());
        EXPECT_EQ(started.name, name(tried.back()));
        std::set<std::uint16_t> distinct;
        for (const auto& ports : tried) {
            ASSERT_EQ(ports.size(), 2U);
            EXPECT_NE(ports[0], ports[1]) << "one run was given the same port twice";
            distinct.insert(ports.begin(), ports.end());
        }
        EXPECT_GT(distinct.size(), 2U) << "a retry reused the ports that were taken";
    }
    {
        // Any other failure is not retried, and says what docker said.
        int runs = 0;
        try {
            (void)start_on_free_ports(
                "probe", 1, name, [&](const std::vector<std::uint16_t>&, const std::string&) {
                    ++runs;
                    return std::string{"echo 'pull access denied'; exit 125"};
                });
            FAIL() << "a failed run was taken for a started one";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("pull access denied"), std::string::npos)
                << e.what();
        }
        EXPECT_EQ(runs, 1);
    }
    {
        // A port taken on every attempt ends after the attempts allowed.
        int runs = 0;
        try {
            (void)start_on_free_ports(
                "probe",
                1,
                name,
                [&](const std::vector<std::uint16_t>&, const std::string&) {
                    ++runs;
                    return std::string{"echo 'port is already allocated'; exit 125"};
                },
                3);
            FAIL() << "a run that never started was taken for a started one";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("taken on all 3 attempts"), std::string::npos)
                << e.what();
        }
        EXPECT_EQ(runs, 3);
    }
}
