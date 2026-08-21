#pragma once

// Throwaway MinIO container for S3-facing integration tests: one bucket
// namespace per suite on a random host port, torn down on destruction.
//
// MinIO rather than LocalStack, and the choice is load-bearing for the
// exactly-once suites: LocalStack's CompleteMultipartUpload is not atomic.
// Duplicate in-flight completes for one uploadId - exactly what an SDK
// retrying into a frozen endpoint produces, and what two recovery
// incarnations legitimately racing produce - can BOTH return 200, and the
// late one re-completes after the parts were deleted, overwriting the good
// object with a zero-byte one. Real S3 serialises the complete and answers
// every duplicate with NoSuchUpload (which S3Sink2PC's HeadObject fallback
// handles); MinIO matches that behaviour, so a verdict against MinIO is
// evidence about the engine rather than about the store's races.
//
// Usage:
//   if (!DockerMinio::docker_available()) GTEST_SKIP();
//   DockerMinio s3;
//   ... use s3.endpoint() with AWS_ACCESS_KEY_ID/SECRET set to
//   s3.access_key() / s3.secret_key() ...
#include <chrono>
#include <cstdlib>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace clink::test {

class DockerMinio {
public:
    DockerMinio() {
        port_ = pick_port();
        container_name_ = "clink_test_minio_" + std::to_string(port_);
        // CLINK_TEST_S3_TRACE=1 turns on MinIO's per-request HTTP trace so
        // `docker logs` shows every S3 API call - the forensic view when an
        // exactly-once verdict needs the store's side of the story.
        const bool trace = std::getenv("CLINK_TEST_S3_TRACE") != nullptr;
        const std::string cmd =
            "docker run -d --rm -p " + std::to_string(port_) +
            ":9000 -e MINIO_ROOT_USER=" + std::string{kAccessKey} +
            " -e MINIO_ROOT_PASSWORD=" + std::string{kSecretKey} + " " +
            (trace ? std::string{"-e MINIO_HTTP_TRACE=/dev/stderr "} : std::string{}) + "--name " +
            container_name_ +
            " minio/minio:RELEASE.2025-09-07T16-13-09Z server /data > /dev/null 2>&1";
        if (std::system(cmd.c_str()) != 0) {
            throw std::runtime_error("DockerMinio: docker run failed");
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
        const std::string probe = "curl -fsS http://127.0.0.1:" + std::to_string(port_) +
                                  "/minio/health/ready > /dev/null 2>&1";
        while (std::system(probe.c_str()) != 0) {
            if (std::chrono::steady_clock::now() >= deadline) {
                stop();
                throw std::runtime_error("DockerMinio: never became healthy");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    ~DockerMinio() { stop(); }
    DockerMinio(const DockerMinio&) = delete;
    DockerMinio& operator=(const DockerMinio&) = delete;
    DockerMinio(DockerMinio&&) = delete;
    DockerMinio& operator=(DockerMinio&&) = delete;

    [[nodiscard]] std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_);
    }

    // MinIO validates credentials (unlike LocalStack); tests must export
    // these as AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
    [[nodiscard]] static std::string access_key() { return std::string{kAccessKey}; }
    [[nodiscard]] static std::string secret_key() { return std::string{kSecretKey}; }

    static bool docker_available() {
        return std::system("docker info > /dev/null 2>&1") == 0 &&
               std::system("curl --version > /dev/null 2>&1") == 0;
    }

    // Freeze / thaw the store - the local model of an object-store outage
    // composing with recovery, same contract as DockerKafka's and
    // DockerPostgres's pause: near-instant, and connections HANG rather
    // than refuse, so SDK calls run their own bounded retries against the
    // frozen socket.
    void pause() {
        if (container_name_.empty()) {
            return;
        }
        const std::string cmd = "docker pause " + container_name_ + " > /dev/null 2>&1";
        (void)std::system(cmd.c_str());
    }

    void unpause() {
        if (container_name_.empty()) {
            return;
        }
        const std::string cmd = "docker unpause " + container_name_ + " > /dev/null 2>&1";
        (void)std::system(cmd.c_str());
    }

private:
    void stop() noexcept {
        if (!container_name_.empty()) {
            // Unpause first: a test dying mid-outage strands a paused
            // container that docker stop/kill refuse on some engine
            // versions (the DockerKafka teardown lesson).
            const std::string unpause_cmd =
                "docker unpause " + container_name_ + " > /dev/null 2>&1";
            std::system(unpause_cmd.c_str());
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

    static constexpr std::string_view kAccessKey = "clink-minio-test";
    static constexpr std::string_view kSecretKey = "clink-minio-test-secret";

    int port_ = 0;
    std::string container_name_;
};

}  // namespace clink::test
