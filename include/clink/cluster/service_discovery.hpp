#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace clink::cluster {

// Endpoint of a Coordinator that Workers should connect to.
struct CoordinatorEndpoint {
    std::string host;
    std::uint16_t port{};
};

// Abstraction so workers don't need a hardcoded coordinator address. Useful for K8s,
// Consul, etcd, etc., where the coordinator's endpoint is not known until launch
// time. Concrete implementations only have to satisfy two operations:
//
//   - discover_coordinator(timeout): block (or poll) until the endpoint
//     is known. Return nullopt on timeout.
//
//   - register_coordinator(endpoint): publish the endpoint so other
//     processes can discover it. coordinator side calls this after it knows
//     its bound port. Default is no-op for read-only backends.
class ServiceDiscovery {
public:
    ServiceDiscovery() = default;
    virtual ~ServiceDiscovery() = default;
    ServiceDiscovery(const ServiceDiscovery&) = delete;
    ServiceDiscovery& operator=(const ServiceDiscovery&) = delete;
    ServiceDiscovery(ServiceDiscovery&&) = delete;
    ServiceDiscovery& operator=(ServiceDiscovery&&) = delete;

    virtual std::optional<CoordinatorEndpoint> discover_coordinator(
        std::chrono::milliseconds timeout) = 0;

    virtual void register_coordinator(const CoordinatorEndpoint& /*ep*/) {}
};

// Hardcoded endpoint - useful for tests and simple two-host deployments
// where DNS / static config already does the work.
class StaticConfigDiscovery final : public ServiceDiscovery {
public:
    StaticConfigDiscovery(std::string host, std::uint16_t port);
    std::optional<CoordinatorEndpoint> discover_coordinator(
        std::chrono::milliseconds timeout) override;

private:
    CoordinatorEndpoint ep_;
};

// Reads endpoint from environment variables. Defaults to CLINK_COORDINATOR_HOST
// and CLINK_COORDINATOR_PORT, but the variable names can be overridden - handy
// for K8s where service env vars follow the {NAME}_HOST / {NAME}_PORT
// convention.
//
// register_coordinator sets the variables in the calling process's env
// (so an in-process coordinator can publish for an in-process worker to discover).
// In real K8s deployments, the orchestrator sets the env vars; the coordinator
// process never publishes anywhere.
class EnvVarDiscovery final : public ServiceDiscovery {
public:
    EnvVarDiscovery() = default;
    EnvVarDiscovery(std::string host_var, std::string port_var);

    std::optional<CoordinatorEndpoint> discover_coordinator(
        std::chrono::milliseconds timeout) override;

    void register_coordinator(const CoordinatorEndpoint& ep) override;

private:
    std::string host_var_{"CLINK_COORDINATOR_HOST"};
    std::string port_var_{"CLINK_COORDINATOR_PORT"};
};

// Reads endpoint from a file containing a single line "host:port". Polls
// (every 50ms) until the file appears, then parses. Models the workflow
// where a coordinator (Consul, etcd, K8s ConfigMap volume mount) writes
// the coordinator's endpoint to a known path on disk.
//
// register_coordinator writes the file atomically (write to ".tmp" then
// rename) so a partial read can never produce a malformed endpoint.
class FileDiscovery final : public ServiceDiscovery {
public:
    explicit FileDiscovery(std::filesystem::path path);

    std::optional<CoordinatorEndpoint> discover_coordinator(
        std::chrono::milliseconds timeout) override;

    void register_coordinator(const CoordinatorEndpoint& ep) override;

private:
    std::filesystem::path path_;
};

}  // namespace clink::cluster
