#pragma once

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <libpq-fe.h>

namespace clink::test {

// RAII wrapper that spins up a Postgres container via the Docker CLI for
// the lifetime of the object, then stops it on destruction. Picks a random
// high port to avoid collisions across concurrent runs.
//
// Usage:
//   if (!DockerPostgres::available()) GTEST_SKIP();
//   DockerPostgres pg;
//   ... use pg.conninfo() ...
//
// Container is launched detached with `--rm`, so a crashing test still
// gets cleanup via Docker's own reaper.
struct DockerPostgresOptions {
    std::string          image{"postgres:16"};
    std::chrono::seconds startup_timeout{30};
    // Extra arguments appended after the image name; passed through
    // to the postgres process. Useful for tests that need
    // `wal_level=logical` or other server settings.
    std::vector<std::string> postgres_args{};
};

class DockerPostgres {
public:
    using Options = DockerPostgresOptions;

    explicit DockerPostgres(Options opts = {}) {
        port_           = pick_port();
        container_name_ = "clink_test_pg_" + std::to_string(port_);

        std::string cmd =
            "docker run -d --rm "
            "-p " + std::to_string(port_) + ":5432 "
            "-e POSTGRES_PASSWORD=test "
            "-e POSTGRES_USER=postgres "
            "-e POSTGRES_DB=postgres "
            "--name " + container_name_ + " " +
            opts.image;
        for (const auto& a : opts.postgres_args) {
            cmd += " ";
            cmd += a;
        }
        cmd += " > /tmp/clink_docker_id.txt 2>&1";
        const int rc = std::system(cmd.c_str());
        if (rc != 0) {
            throw std::runtime_error("docker run failed (rc=" + std::to_string(rc) + ")");
        }

        // Poll libpq until the server accepts connections or we time out.
        const auto deadline = std::chrono::steady_clock::now() + opts.startup_timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            PGconn* test_conn = PQconnectdb(conninfo().c_str());
            const auto status = PQstatus(test_conn);
            PQfinish(test_conn);
            if (status == CONNECTION_OK) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{200});
        }
        stop();
        throw std::runtime_error("Postgres container did not become ready in time");
    }

    ~DockerPostgres() { stop(); }

    DockerPostgres(const DockerPostgres&)            = delete;
    DockerPostgres& operator=(const DockerPostgres&) = delete;
    DockerPostgres(DockerPostgres&&)                 = delete;
    DockerPostgres& operator=(DockerPostgres&&)      = delete;

    int                port() const noexcept { return port_; }
    const std::string& container_name() const noexcept { return container_name_; }

    std::string conninfo() const {
        return "host=127.0.0.1 port=" + std::to_string(port_) +
               " user=postgres password=test dbname=postgres";
    }

    // Run a SQL string on the container. Throws on error.
    void exec(const std::string& sql) const {
        std::unique_ptr<PGconn, decltype(&PQfinish)> conn(PQconnectdb(conninfo().c_str()),
                                                          &PQfinish);
        if (PQstatus(conn.get()) != CONNECTION_OK) {
            throw std::runtime_error("DockerPostgres::exec: connect failed: " +
                                     std::string{PQerrorMessage(conn.get())});
        }
        std::unique_ptr<PGresult, decltype(&PQclear)> r(PQexec(conn.get(), sql.c_str()), &PQclear);
        const auto status = PQresultStatus(r.get());
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            throw std::runtime_error("DockerPostgres::exec: " +
                                     std::string{PQerrorMessage(conn.get())});
        }
    }

    static bool docker_available() {
        // `docker info` exits 0 only when the daemon is reachable.
        return std::system("docker info > /dev/null 2>&1") == 0;
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
        std::random_device              rd;
        std::mt19937                    gen(rd());
        std::uniform_int_distribution<> dist(50000, 59999);
        return dist(gen);
    }

    int         port_{0};
    std::string container_name_;
};

}  // namespace clink::test
