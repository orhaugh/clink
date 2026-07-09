// clink_submit_sql: SQL-to-JobGraphSpec compiler.
//
// Reads a SQL file (or inline -e expression) and runs it through the
// shared SQL script runner (clink/sql/script_runner.hpp):
//   * DDL statements fold into an in-memory (optionally persistent) catalog
//   * INSERT INTO ... SELECT compiles to a JobGraphSpec, printed to stdout
//     or POSTed to a running JM with --jm-host/--jm-port
//
// Usage:
//   clink_submit_sql --file path/to/job.sql
//   clink_submit_sql -e "CREATE TABLE ...; INSERT INTO ..."
//   clink_submit_sql --explain --file job.sql   (prints LogicalPlan tree)

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>

#include "clink/sql/catalog.hpp"
#include "clink/sql/script_runner.hpp"
#ifdef CLINK_LINKED_WASM
#include "clink/wasm/install.hpp"
#endif

namespace {

struct Args {
    std::string file;
    std::string inline_sql;
    std::string catalog_dir;
    std::string jm_host;
    std::uint16_t jm_port = 0;
    std::string job_name;
    std::string state_backend;  // per-job state backend URI (empty = cluster default)
    bool explain = false;
    std::uint32_t parallelism = 1;  // uniform op parallelism (>1 fans the plan out)
};

void print_usage() {
    std::cerr
        << "Usage: clink_submit_sql --file <path> | -e <sql>\n"
        << "                        [--catalog-dir <dir>]\n"
        << "                        [--jm-host <host> --jm-port <port> [--name <job>]]\n"
        << "                        [--explain]\n"
        << "\n"
        << "  --file <path>       Read SQL from a file.\n"
        << "  -e <sql>            Read SQL from the command line.\n"
        << "  --catalog-dir <dir> Persistent catalog dir. Loaded at startup; tables\n"
        << "                      registered via CREATE TABLE auto-save as JSON-per-table.\n"
        << "  --jm-host <host>    JM HTTP host. When set with --jm-port, INSERT statements\n"
        << "  --jm-port <port>    POST the compiled JobGraphSpec to the JM instead of\n"
        << "                      printing JSON. The JM runs the job and replies with a\n"
        << "                      job id.\n"
        << "  --name <job>        Job name (default 'sql_job'). Only used with --jm-host.\n"
        << "  --state-backend <uri>  Per-job state backend URI, overriding the cluster default.\n"
        << "                      A disaggregated tier (e.g. remote-read://...) activates the\n"
        << "                      async KeyedState path. Only used with --jm-host.\n"
        << "  --parallelism <n>   Uniform op parallelism (default 1). >1 fans every op out to\n"
        << "  -p <n>              n subtasks; keyed ops hash-partition by key, sources split\n"
        << "                      partitions across subtasks (Kafka needs >= n partitions).\n"
        << "                      Assumes a parallelizable plan (the common SQL shapes).\n"
        << "  --explain           Print the LogicalPlan tree instead of the JobGraphSpec JSON.\n";
}

Args parse_args(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--file" || arg == "-f") {
            if (++i >= argc) {
                std::cerr << "error: --file requires a path\n";
                std::exit(2);
            }
            a.file = argv[i];
        } else if (arg == "-e") {
            if (++i >= argc) {
                std::cerr << "error: -e requires a SQL string\n";
                std::exit(2);
            }
            a.inline_sql = argv[i];
        } else if (arg == "--catalog-dir") {
            if (++i >= argc) {
                std::cerr << "error: --catalog-dir requires a path\n";
                std::exit(2);
            }
            a.catalog_dir = argv[i];
        } else if (arg == "--jm-host") {
            if (++i >= argc) {
                std::cerr << "error: --jm-host requires a host\n";
                std::exit(2);
            }
            a.jm_host = argv[i];
        } else if (arg == "--jm-port") {
            if (++i >= argc) {
                std::cerr << "error: --jm-port requires a port\n";
                std::exit(2);
            }
            a.jm_port = static_cast<std::uint16_t>(std::stoi(argv[i]));
        } else if (arg == "--name") {
            if (++i >= argc) {
                std::cerr << "error: --name requires a string\n";
                std::exit(2);
            }
            a.job_name = argv[i];
        } else if (arg == "--state-backend") {
            if (++i >= argc) {
                std::cerr << "error: --state-backend requires a URI\n";
                std::exit(2);
            }
            a.state_backend = argv[i];
        } else if (arg == "--parallelism" || arg == "-p") {
            if (++i >= argc) {
                std::cerr << "error: --parallelism requires a number\n";
                std::exit(2);
            }
            a.parallelism = static_cast<std::uint32_t>(std::stoul(argv[i]));
            if (a.parallelism < 1) {
                std::cerr << "error: --parallelism must be >= 1\n";
                std::exit(2);
            }
        } else if (arg == "--explain") {
            a.explain = true;
        } else if (arg == "--help" || arg == "-h") {
            print_usage();
            std::exit(0);
        } else {
            std::cerr << "error: unknown argument: " << arg << "\n";
            print_usage();
            std::exit(2);
        }
    }
    if (a.file.empty() == a.inline_sql.empty()) {
        std::cerr << "error: exactly one of --file or -e is required\n";
        print_usage();
        std::exit(2);
    }
    return a;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        std::cerr << "error: cannot open " << path << "\n";
        std::exit(1);
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    std::string sql = args.file.empty() ? args.inline_sql : read_file(args.file);

#ifdef CLINK_LINKED_WASM
    // CREATE FUNCTION ... LANGUAGE wasm executes here, client-side: the
    // loader validates the module and packages its bytes into the spec the
    // JM receives, so the cluster needs no access to the local path.
    {
        clink::plugin::PluginRegistry reg;
        clink::wasm::install(reg);
    }
#endif

    clink::sql::Catalog catalog;
    if (!args.catalog_dir.empty()) {
        try {
            catalog.load_from_dir(args.catalog_dir);
            catalog.set_persistence_dir(args.catalog_dir);
        } catch (const std::exception& e) {
            std::cerr << "error: failed to load catalog from " << args.catalog_dir << ": "
                      << e.what() << "\n";
            return 1;
        }
    }

    clink::sql::ScriptRunOptions opts;
    opts.explain = args.explain;
    opts.parallelism = args.parallelism;
    opts.job_name = args.job_name;

    clink::sql::ScriptIO io{&std::cout, &std::cerr};
    auto submit = (!args.jm_host.empty() && args.jm_port != 0)
                      ? clink::sql::make_http_submit(
                            args.jm_host, args.jm_port, args.state_backend, std::cout, std::cerr)
                      : clink::sql::make_print_submit(std::cout);
    return clink::sql::run_script(sql, catalog, opts, io, submit);
}
