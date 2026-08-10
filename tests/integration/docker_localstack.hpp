#pragma once

#include <chrono>
#include <cstdlib>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

namespace clink::test {

// RAII wrapper that spins up a LocalStack container (S3 only) via the Docker
// CLI for the lifetime of the object, then stops it on destruction. The same
// image the repo's integration-services compose file pins, so machines that
// have run the live suite already have it. Picks a random high port; the
// readiness probe is LocalStack's own health endpoint, the same condition the
// compose healthcheck uses.
//
// Usage mirrors DockerPostgres / DockerKafka:
//   if (!DockerLocalstack::docker_available()) GTEST_SKIP();
//   DockerLocalstack s3;
//   ... use s3.endpoint() with AWS_ACCESS_KEY_ID/SECRET set to any value ...
class DockerLocalstack {
public:
    DockerLocalstack() {
        port_ = pick_port();
        container_name_ = "clink_test_ls_" + std::to_string(port_);
        const std::string cmd = "docker run -d --rm -p " + std::to_string(port_) +
                                ":4566 -e SERVICES=s3 --name " + container_name_ +
                                " localstack/localstack:3 > /dev/null 2>&1";
        if (std::system(cmd.c_str()) != 0) {
            throw std::runtime_error("DockerLocalstack: docker run failed");
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
        const std::string probe = "curl -fsS http://127.0.0.1:" + std::to_string(port_) +
                                  "/_localstack/health > /dev/null 2>&1";
        while (std::system(probe.c_str()) != 0) {
            if (std::chrono::steady_clock::now() >= deadline) {
                stop();
                throw std::runtime_error("DockerLocalstack: never became healthy");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    ~DockerLocalstack() { stop(); }
    DockerLocalstack(const DockerLocalstack&) = delete;
    DockerLocalstack& operator=(const DockerLocalstack&) = delete;
    DockerLocalstack(DockerLocalstack&&) = delete;
    DockerLocalstack& operator=(DockerLocalstack&&) = delete;

    [[nodiscard]] std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_);
    }

    static bool docker_available() {
        return std::system("docker info > /dev/null 2>&1") == 0 &&
               std::system("curl --version > /dev/null 2>&1") == 0;
    }

private:
    void stop() noexcept {
        if (!container_name_.empty()) {
            const std::string cmd = "docker stop " + container_name_ + " > /dev/null 2>&1";
            std::system(cmd.c_str());
            container_name_.clear();
        }
    }

    static int pick_port() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(20000, 60000);
        return dist(gen);
    }

    int port_{0};
    std::string container_name_;
};

}  // namespace clink::test
