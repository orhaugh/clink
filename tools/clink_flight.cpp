// clink flight-sql - serve the embedded engine over Arrow Flight SQL, so
// any Flight SQL / ADBC / JDBC client can run clink SQL over one wire
// protocol:
//
//   clink flight-sql --port=32010 [--host=H] [--file=init.sql]
//                    [--catalog-dir=D] [--parallelism=N]
//                    [--checkpoint-dir=D] [--checkpoint-interval-ms=N]
//                    [--state-backend=URI]
//
// Statement queries run bare SELECTs and stream typed Arrow batches back
// (changelog plans carry a leading row_kind column); statement updates run
// DDL / INSERT scripts synchronously. --file runs an init script (DDL)
// before serving. No authentication in v1: bind loopback (the default) or
// front with a proxy.
//
// Exit codes: 0 = clean shutdown (SIGINT/SIGTERM), 2 = error.

#include <csignal>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>

#include <arrow/flight/server.h>

#include "clink/embed/embedded_engine.hpp"
#include "clink/embed/flight_sql_server.hpp"

namespace {

std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

void usage() {
    std::cerr << "Usage: clink flight-sql [--port=32010] [--host=127.0.0.1] [--file=init.sql]\n"
              << "                        [--catalog-dir=D] [--parallelism=N]\n"
              << "                        [--checkpoint-dir=D] [--checkpoint-interval-ms=N]\n"
              << "                        [--state-backend=URI]\n"
              << "\n"
              << "Serve the embedded engine over Arrow Flight SQL: queries run bare\n"
              << "SELECTs and stream typed Arrow batches (changelog plans carry a\n"
              << "leading row_kind column); updates run DDL / INSERT synchronously.\n"
              << "--file runs an init script before serving. No authentication in\n"
              << "v1: bind loopback (default) or front with a proxy.\n"
              << "\n"
              << "Exit codes: 0 = clean shutdown, 2 = error.\n";
}

}  // namespace

int clink_cmd_flight_sql(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        usage();
        return 0;
    }
    try {
        clink::embed::EngineOptions opts;
        opts.catalog_dir = get_arg(argc, argv, "catalog-dir");
        opts.checkpoint_dir = get_arg(argc, argv, "checkpoint-dir");
        opts.state_backend_uri = get_arg(argc, argv, "state-backend");
        if (const auto v = get_arg(argc, argv, "parallelism"); !v.empty()) {
            opts.parallelism = static_cast<std::uint32_t>(std::stoul(v));
        }
        if (const auto v = get_arg(argc, argv, "checkpoint-interval-ms"); !v.empty()) {
            opts.checkpoint_interval_ms = std::stoll(v);
        }
        clink::embed::EmbeddedEngine engine{std::move(opts)};

        if (const auto init = get_arg(argc, argv, "file"); !init.empty()) {
            std::ifstream in(init);
            if (!in) {
                std::cerr << "clink flight-sql: cannot open --file " << init << "\n";
                return 2;
            }
            std::stringstream ss;
            ss << in.rdbuf();
            if (engine.execute_script(ss.str()) != 0) {
                std::cerr << "clink flight-sql: init script failed\n";
                return 2;
            }
        }

        const auto host = get_arg(argc, argv, "host", "127.0.0.1");
        int port = 32010;
        if (const auto v = get_arg(argc, argv, "port"); !v.empty()) {
            port = std::stoi(v);
        }
        auto location_r = arrow::flight::Location::ForGrpcTcp(host, port);
        if (!location_r.ok()) {
            std::cerr << "clink flight-sql: " << location_r.status().ToString() << "\n";
            return 2;
        }
        clink::embed::ClinkFlightSqlServer server{engine};
        arrow::flight::FlightServerOptions fopts{*location_r};
        if (auto st = server.Init(fopts); !st.ok()) {
            std::cerr << "clink flight-sql: init failed: " << st.ToString() << "\n";
            return 2;
        }
        if (auto st = server.SetShutdownOnSignals({SIGINT, SIGTERM}); !st.ok()) {
            std::cerr << "clink flight-sql: " << st.ToString() << "\n";
            return 2;
        }
        std::cout << "flight-sql: serving grpc+tcp://" << host << ":" << server.port()
                  << " (Ctrl-C to stop)\n";
        if (auto st = server.Serve(); !st.ok()) {
            std::cerr << "clink flight-sql: " << st.ToString() << "\n";
            return 2;
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "clink flight-sql: " << e.what() << "\n";
        return 2;
    }
}
