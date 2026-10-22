#pragma once

// Throwaway MinIO container for S3-facing integration tests: one bucket
// namespace per suite on a host port checked free (docker_ports.hpp), torn
// down on destruction.
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
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "docker_command.hpp"
#include "docker_ports.hpp"

namespace clink::test {

class DockerMinio {
public:
    DockerMinio() {
        const auto started = start_on_free_ports(
            "DockerMinio",
            1,
            named_by_port("clink_test_minio_"),
            [&](const std::vector<std::uint16_t>& ports, const std::string& name) {
                return "docker run -d --rm -p " + std::to_string(ports[0]) +
                       ":9000 -e MINIO_ROOT_USER=" + std::string{kAccessKey} +
                       " -e MINIO_ROOT_PASSWORD=" + std::string{kSecretKey} + " --name " + name +
                       " " + std::string{kImage} + " server /data";
            });
        port_ = started.ports[0];
        container_name_ = started.name;
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
        // CLINK_TEST_S3_TRACE=1 records every S3 call the store serves - the
        // forensic view when an exactly-once verdict needs the store's side
        // of the story.
        if (std::getenv("CLINK_TEST_S3_TRACE") != nullptr) {
            start_trace();
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
    // MinIO dropped MINIO_HTTP_TRACE, which named a file to trace to, in
    // 2019; the server streams its call trace to an admin client instead.
    // One runs inside the container, where the image ships mcli, and writes
    // a line per call to a host file named on stderr. The file outlives the
    // container, so the trace is still there after teardown; the client
    // ends with the server.
    void start_trace() {
        trace_file_ =
            (std::filesystem::temp_directory_path() / (container_name_ + ".s3trace")).string();
        const std::string client = "docker exec " + container_name_ +
                                   " /bin/sh -c 'mcli alias set local http://127.0.0.1:9000 " +
                                   std::string{kAccessKey} + " " + std::string{kSecretKey} +
                                   " > /dev/null && exec mcli admin trace --no-color "
                                   "--disable-pager local' > '" +
                                   trace_file_ + "' 2>&1 &";
        (void)std::system(client.c_str());
        // Live once it shows a call made after it started.
        const std::string probe = "curl -s -o /dev/null http://127.0.0.1:" + std::to_string(port_) +
                                  "/" + std::string{kTraceProbe};
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        for (;;) {
            (void)std::system(probe.c_str());
            std::ifstream in(trace_file_);
            const std::string seen{std::istreambuf_iterator<char>(in),
                                   std::istreambuf_iterator<char>()};
            if (seen.find(kTraceProbe) != std::string::npos) {
                std::fprintf(stderr, "DockerMinio: S3 calls traced to %s\n", trace_file_.c_str());
                return;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                stop();
                throw std::runtime_error("DockerMinio: the S3 trace did not start: " + seen);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    }

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

    // MinIO no longer publishes images: the minio/minio repository is gone
    // from Docker Hub and quay.io refuses anonymous pulls. This is SILO,
    // Pigsty's fork of the MinIO server under a new name, which keeps the S3
    // API, the MINIO_* settings, the /minio/health routes and `server /data`.
    // It completes uploads as described above: measured on this release,
    // eight completes of one upload in flight together gave one success and
    // seven NoSuchUpload, with the object intact, in each of 100 rounds.
    // Pinned by digest, so a re-pushed tag cannot change the store under the
    // suite; the qualification rigs pin the same release by tag.
    static constexpr std::string_view kImage =
        "pgsty/silo:RELEASE.2026-10-18T00-00-00Z"
        "@sha256:2636fc4155d9a7e41ff58cdb65ddd13ba9304e67184e13c25bc5cb34016269d7";
    static constexpr std::string_view kAccessKey = "clink-minio-test";
    static constexpr std::string_view kSecretKey = "clink-minio-test-secret";
    // A bucket nothing creates: the trace's first line, never a test's.
    static constexpr std::string_view kTraceProbe = "clink-trace-probe";

    int port_ = 0;
    std::string container_name_;
    std::string trace_file_;
};

}  // namespace clink::test
