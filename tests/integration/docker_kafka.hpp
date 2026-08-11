#pragma once

#include <chrono>
#include <cstdlib>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

namespace clink::test {

// RAII wrapper that spins up a single-node Redpanda broker via the Docker CLI
// for the lifetime of the object, then stops it on destruction. Kafka-API
// compatible, transactions enabled out of the box in dev-container mode, one
// process - which is why it is the test broker here rather than a
// two-container ZooKeeper+Kafka stack. Picks a random high port to avoid
// collisions across concurrent runs; the advertised address carries the host
// port so clients bootstrap correctly through the mapping.
//
// Usage mirrors DockerPostgres:
//   if (!DockerKafka::docker_available()) GTEST_SKIP();
//   DockerKafka kafka;
//   ... use kafka.brokers() ...
class DockerKafka {
public:
    DockerKafka() {
        port_ = pick_port();
        container_name_ = "clink_test_rp_" + std::to_string(port_);
        // Two listeners: `internal` keeps in-container rpk working (rpk dials
        // the ADVERTISED address, which for a single mapped listener would be
        // the host port - unreachable from inside the container), `external`
        // is what the host-side test clients bootstrap through.
        const std::string cmd =
            "docker run -d --rm -p " + std::to_string(port_) + ":19092 --name " + container_name_ +
            " redpandadata/redpanda:v24.2.7 redpanda start"
            " --mode dev-container --smp 1"
            " --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092"
            " --advertise-kafka-addr internal://127.0.0.1:9092,external://127.0.0.1:" +
            std::to_string(port_) + " > /dev/null 2>&1";
        if (std::system(cmd.c_str()) != 0) {
            throw std::runtime_error("DockerKafka: docker run failed");
        }
        // Readiness is a broker-side condition, not a duration: rpk ships in
        // the image and exits 0 only once the cluster answers. `cluster
        // info` alone is NOT enough - it answers before the transaction
        // coordinator is ready, and a transactional producer that connects
        // in that window burns a full init_transactions timeout plus a
        // restart cycle (observed as a first-checkpoint stall in the
        // exactly-once suite). `cluster health` waits for the controller
        // and leadership to settle, which is the precondition the
        // transactional path actually needs.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
        const std::string probe =
            "docker exec " + container_name_ + " rpk cluster info > /dev/null 2>&1";
        while (std::system(probe.c_str()) != 0) {
            if (std::chrono::steady_clock::now() >= deadline) {
                stop();
                throw std::runtime_error("DockerKafka: broker never became ready");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        const std::string health = "docker exec " + container_name_ +
                                   " rpk cluster health --exit-when-healthy --watch"
                                   " > /dev/null 2>&1";
        if (std::system(health.c_str()) != 0) {
            stop();
            throw std::runtime_error("DockerKafka: cluster never reported healthy");
        }
    }

    ~DockerKafka() { stop(); }
    DockerKafka(const DockerKafka&) = delete;
    DockerKafka& operator=(const DockerKafka&) = delete;
    DockerKafka(DockerKafka&&) = delete;
    DockerKafka& operator=(DockerKafka&&) = delete;

    [[nodiscard]] std::string brokers() const { return "127.0.0.1:" + std::to_string(port_); }

    // Create a topic explicitly (1 partition unless asked otherwise), so a
    // test does not depend on auto-creation racing the first produce.
    void create_topic(const std::string& name, int partitions = 1) const {
        const std::string cmd = "docker exec " + container_name_ + " rpk topic create " + name +
                                " -p " + std::to_string(partitions) + " > /dev/null 2>&1";
        if (std::system(cmd.c_str()) != 0) {
            throw std::runtime_error("DockerKafka: rpk topic create " + name + " failed");
        }
    }

    static bool docker_available() { return std::system("docker info > /dev/null 2>&1") == 0; }

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
