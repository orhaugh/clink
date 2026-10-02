#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>

namespace clink::test {

// RAII wrapper around ClickHouse servers run through the Docker CLI, on the
// pattern of DockerPostgres, for tests that must fault a real server: kill it,
// restart it, and read what landed.
//
// Two shapes, both built from docker/integration-services.yml so that the
// images stay pinned by digest in one place and the configuration files are
// the ones the live suites use:
//   * one server, the line's `clickhouse-<line>` service image, no Keeper;
//   * two replicas of one shard sharing a standalone Keeper, the line's
//     `clickhouse-<line>-r1` / `-r2` / `clickhouse-keeper-<line>` services,
//     with docker/clickhouse/replicated/*.xml mounted as the profile mounts
//     them. Those replicas default Replicated tables to async_insert=1, so a
//     table that must take synchronous inserts sets async_insert = 0 itself.
//
// Unlike the Postgres and Kafka helpers, the containers run without --rm: a
// test that kills a server with `docker kill -s KILL` starts the same
// container again, with its data. The destructor removes every container,
// with its volumes, and the network, whatever state they are in. Each server's
// native port is published on a fixed host port chosen at construction, so it
// survives a restart.
//
// SQL runs through the container's own clickhouse-client, so the test binary
// needs no ClickHouse client library.
struct DockerClickHouseOptions {
    // The release line, as the service names spell it: "26.3" or "26.8".
    std::string line{"26.8"};
    // 1: a single server. 2: two replicas and a Keeper.
    int replicas{2};
    std::chrono::seconds startup_timeout{180};
};

class DockerClickHouse {
public:
    using Options = DockerClickHouseOptions;

    struct Result {
        int status{-1};
        std::string out;
    };

    explicit DockerClickHouse(Options opts = {}) : opts_(std::move(opts)) {
        if (opts_.replicas != 1 && opts_.replicas != 2) {
            throw std::invalid_argument("DockerClickHouse: replicas must be 1 or 2");
        }
        static std::atomic<int> counter{0};
        std::string tag = opts_.line;
        for (char& c : tag) {
            if (c == '.') {
                c = '-';
            }
        }
        tag_ = tag;
        prefix_ = "clink_test_ch_" + std::to_string(::getpid()) + "_" +
                  std::to_string(counter.fetch_add(1)) + "_" + tag;
        try {
            start_all();
        } catch (...) {
            remove_all();
            throw;
        }
    }

    ~DockerClickHouse() { remove_all(); }

    DockerClickHouse(const DockerClickHouse&) = delete;
    DockerClickHouse& operator=(const DockerClickHouse&) = delete;
    DockerClickHouse(DockerClickHouse&&) = delete;
    DockerClickHouse& operator=(DockerClickHouse&&) = delete;

    [[nodiscard]] const std::string& line() const noexcept { return opts_.line; }
    [[nodiscard]] int replicas() const noexcept { return opts_.replicas; }
    [[nodiscard]] bool replicated() const noexcept { return opts_.replicas == 2; }

    // The host port of server `i`'s native protocol (0-based).
    [[nodiscard]] int port(int i = 0) const {
        return servers_.at(static_cast<std::size_t>(i)).port;
    }
    [[nodiscard]] const std::string& container(int i = 0) const {
        return servers_.at(static_cast<std::size_t>(i)).name;
    }

    // Run SQL on server `i`; the output is TSV. Throws on a failed query.
    std::string query(const std::string& sql, int i = 0) const {
        const Result r = try_query(sql, i);
        if (r.status != 0) {
            throw std::runtime_error("DockerClickHouse::query on " + container(i) + " failed (" +
                                     std::to_string(r.status) + "): " + r.out + "\nSQL: " + sql);
        }
        return r.out;
    }

    // The first line of the output, without its newline.
    std::string scalar(const std::string& sql, int i = 0) const {
        std::string out = query(sql, i);
        const auto nl = out.find('\n');
        if (nl != std::string::npos) {
            out.resize(nl);
        }
        return out;
    }

    [[nodiscard]] Result try_query(const std::string& sql, int i = 0) const {
        return run("docker exec " + container(i) + " clickhouse-client --multiquery --query " +
                   shell_quote(sql) + " 2>&1");
    }

    // The faults. kill() is SIGKILL: no shutdown, no flush. stop() and
    // restart() are graceful, so the server flushes its system logs.
    void kill(int i = 0) const { (void)run("docker kill -s KILL " + container(i) + " 2>&1"); }
    void stop(int i = 0) const { (void)run("docker stop -t 30 " + container(i) + " 2>&1"); }
    void start(int i = 0) const {
        const Result r = run("docker start " + container(i) + " 2>&1");
        if (r.status != 0) {
            throw std::runtime_error("DockerClickHouse: docker start " + container(i) +
                                     " failed: " + r.out);
        }
        await_ready(i);
    }
    void restart(int i = 0) const {
        const Result r = run("docker restart -t 30 " + container(i) + " 2>&1");
        if (r.status != 0) {
            throw std::runtime_error("DockerClickHouse: docker restart " + container(i) +
                                     " failed: " + r.out);
        }
        await_ready(i);
    }

    // True once server `i` answers a query.
    [[nodiscard]] bool ready(int i = 0) const { return try_query("SELECT 1", i).status == 0; }

    void await_ready(int i = 0) const {
        const auto deadline = std::chrono::steady_clock::now() + opts_.startup_timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            // A replica is usable once it also reaches its Keeper.
            const std::string probe =
                replicated() ? "SELECT count() FROM system.zookeeper WHERE path = '/'" : "SELECT 1";
            if (try_query(probe, i).status == 0) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        throw std::runtime_error("DockerClickHouse: " + container(i) +
                                 " did not become ready in time");
    }

    static bool docker_available() { return std::system("docker info > /dev/null 2>&1") == 0; }

    // The image a service of docker/integration-services.yml names, so the
    // digest pins live in that file only.
    static std::string image_for(const std::string& service) {
        std::ifstream in(compose_file());
        if (!in) {
            throw std::runtime_error("DockerClickHouse: cannot read " + compose_file().string());
        }
        const std::string header = "  " + service + ":";
        std::string line;
        bool in_service = false;
        while (std::getline(in, line)) {
            if (line == header) {
                in_service = true;
                continue;
            }
            if (!in_service) {
                continue;
            }
            // The next service, at the same indentation, ends this one.
            if (line.size() > 2 && line[0] == ' ' && line[1] == ' ' && line[2] != ' ' &&
                line[2] != '#') {
                break;
            }
            const auto pos = line.find("image:");
            if (pos != std::string::npos) {
                std::string image = line.substr(pos + 6);
                image.erase(0, image.find_first_not_of(' '));
                image.erase(image.find_last_not_of(" \r") + 1);
                return image;
            }
        }
        throw std::runtime_error("DockerClickHouse: no image for service " + service + " in " +
                                 compose_file().string());
    }

    static std::filesystem::path repo_root() {
        // tests/integration/docker_clickhouse.hpp
        return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path();
    }

    static std::filesystem::path compose_file() {
        return repo_root() / "docker" / "integration-services.yml";
    }

private:
    struct Server {
        std::string name;
        int port{0};
    };

    static std::string shell_quote(const std::string& s) {
        std::string out = "'";
        for (const char c : s) {
            if (c == '\'') {
                out += "'\\''";
            } else {
                out += c;
            }
        }
        out += "'";
        return out;
    }

    static Result run(const std::string& cmd) {
        Result r;
        FILE* pipe = ::popen(cmd.c_str(), "r");
        if (pipe == nullptr) {
            return r;
        }
        std::array<char, 4096> buf{};
        std::size_t n = 0;
        while ((n = std::fread(buf.data(), 1, buf.size(), pipe)) > 0) {
            r.out.append(buf.data(), n);
        }
        const int status = ::pclose(pipe);
        r.status = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        return r;
    }

    // A free loopback port, released for docker to bind. The window between
    // the release and `docker run` is small; a collision fails the run loudly.
    static int free_port() {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        int port = 0;
        if (fd >= 0 && ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
            socklen_t len = sizeof(addr);
            ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
            port = ntohs(addr.sin_port);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        if (port == 0) {
            throw std::runtime_error("DockerClickHouse: no free port");
        }
        return port;
    }

    void must(const std::string& cmd, const std::string& what) {
        const Result r = run(cmd + " 2>&1");
        if (r.status != 0) {
            throw std::runtime_error("DockerClickHouse: " + what + " failed: " + r.out);
        }
    }

    void start_all() {
        const auto conf = repo_root() / "docker" / "clickhouse";
        if (!replicated()) {
            Server s{prefix_, free_port()};
            servers_.push_back(s);
            must("docker run -d --name " + s.name +
                     " -e CLICKHOUSE_SKIP_USER_SETUP=1 -p 127.0.0.1:" + std::to_string(s.port) +
                     ":9000 " + image_for("clickhouse-" + tag_),
                 "docker run " + s.name);
            await_ready(0);
            return;
        }
        network_ = prefix_ + "_net";
        must("docker network create " + network_, "docker network create");
        keeper_ = prefix_ + "_keeper";
        must("docker run -d --name " + keeper_ + " --hostname " + keeper_ + " --network " +
                 network_ + " -v " + (conf / "replicated" / "keeper.xml").string() +
                 ":/etc/clickhouse-keeper/keeper_config.xml:ro " +
                 image_for("clickhouse-keeper-" + tag_),
             "docker run " + keeper_);
        for (int r = 1; r <= 2; ++r) {
            Server s{prefix_ + "_r" + std::to_string(r), free_port()};
            servers_.push_back(s);
            must("docker run -d --name " + s.name + " --hostname " + s.name + " --network " +
                     network_ +
                     " -e CLICKHOUSE_SKIP_USER_SETUP=1 -e CLINK_CLICKHOUSE_KEEPER_HOST=" + keeper_ +
                     " -e CLINK_CLICKHOUSE_REPLICA=r" + std::to_string(r) + " -v " +
                     (conf / "replicated" / "replicated.xml").string() +
                     ":/etc/clickhouse-server/config.d/replicated.xml:ro -v " +
                     (conf / "replicated" / "replicated-merge-tree-async.xml").string() +
                     ":/etc/clickhouse-server/config.d/replicated-merge-tree-async.xml:ro -p "
                     "127.0.0.1:" +
                     std::to_string(s.port) + ":9000 " +
                     image_for("clickhouse-" + tag_ + "-r" + std::to_string(r)),
                 "docker run " + s.name);
        }
        for (int i = 0; i < 2; ++i) {
            await_ready(i);
        }
    }

    void remove_all() noexcept {
        std::string names;
        for (const auto& s : servers_) {
            names += " " + s.name;
        }
        if (!keeper_.empty()) {
            names += " " + keeper_;
        }
        if (!names.empty()) {
            (void)run("docker rm -f -v" + names + " > /dev/null 2>&1");
        }
        if (!network_.empty()) {
            (void)run("docker network rm " + network_ + " > /dev/null 2>&1");
        }
        servers_.clear();
        keeper_.clear();
        network_.clear();
    }

    Options opts_;
    std::string tag_;
    std::string prefix_;
    std::string network_;
    std::string keeper_;
    std::vector<Server> servers_;
};

}  // namespace clink::test
