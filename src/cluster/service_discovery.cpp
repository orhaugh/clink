#include "clink/cluster/service_discovery.hpp"

#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace clink::cluster {

// ----- StaticConfigDiscovery -----

StaticConfigDiscovery::StaticConfigDiscovery(std::string host, std::uint16_t port)
    : ep_{.host = std::move(host), .port = port} {}

std::optional<JobManagerEndpoint> StaticConfigDiscovery::discover_job_manager(
    std::chrono::milliseconds /*timeout*/) {
    return ep_;
}

// ----- EnvVarDiscovery -----

EnvVarDiscovery::EnvVarDiscovery(std::string host_var, std::string port_var)
    : host_var_(std::move(host_var)), port_var_(std::move(port_var)) {}

std::optional<JobManagerEndpoint> EnvVarDiscovery::discover_job_manager(
    std::chrono::milliseconds timeout) {
    // Poll: orchestrators sometimes inject env after the process starts
    // (K8s sidecars, init containers writing to /etc/env, etc.). Most of
    // the time the very first read succeeds.
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        const char* host = std::getenv(host_var_.c_str());
        const char* port = std::getenv(port_var_.c_str());
        if (host != nullptr && port != nullptr && *host != '\0' && *port != '\0') {
            try {
                return JobManagerEndpoint{.host = std::string{host},
                                          .port = static_cast<std::uint16_t>(std::stoi(port))};
            } catch (const std::exception&) {
                // Malformed port - give up, don't keep polling on bad input.
                return std::nullopt;
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return std::nullopt;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void EnvVarDiscovery::register_job_manager(const JobManagerEndpoint& ep) {
    ::setenv(host_var_.c_str(), ep.host.c_str(), 1);
    ::setenv(port_var_.c_str(), std::to_string(ep.port).c_str(), 1);
}

// ----- FileDiscovery -----

FileDiscovery::FileDiscovery(std::filesystem::path path) : path_(std::move(path)) {}

std::optional<JobManagerEndpoint> FileDiscovery::discover_job_manager(
    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        std::error_code ec;
        if (std::filesystem::exists(path_, ec) && !ec) {
            std::ifstream in(path_);
            std::string line;
            if (in && std::getline(in, line)) {
                // Reverse-find the colon - IPv6 literals contain colons,
                // so the *last* one is the host/port separator.
                const auto sep = line.rfind(':');
                if (sep != std::string::npos) {
                    try {
                        std::string host = line.substr(0, sep);
                        std::string port = line.substr(sep + 1);
                        return JobManagerEndpoint{
                            .host = std::move(host),
                            .port = static_cast<std::uint16_t>(std::stoi(port))};
                    } catch (const std::exception& e) {
                        // Partial / malformed line - keep polling: writer
                        // may still be flushing. Atomic rename in our own
                        // register_job_manager prevents this, but external
                        // writers may not be atomic.
                        (void)e;
                    }
                }
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return std::nullopt;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void FileDiscovery::register_job_manager(const JobManagerEndpoint& ep) {
    // Atomic-rename pattern: write to ".tmp" then rename so a concurrent
    // reader never sees a half-written line.
    auto tmp = path_;
    tmp += ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        if (!out) {
            throw std::runtime_error("FileDiscovery: cannot open " + tmp.string());
        }
        out << ep.host << ":" << ep.port << "\n";
    }
    std::error_code ec;
    std::filesystem::rename(tmp, path_, ec);
    if (ec) {
        throw std::runtime_error("FileDiscovery: rename failed: " + ec.message());
    }
}

}  // namespace clink::cluster
