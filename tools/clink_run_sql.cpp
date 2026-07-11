// `clink run <file>.sql` / `clink run -e "<sql>"` - the SQL execution
// front door of the unified CLI.
//
// EMBEDDED by default: starts the whole runtime in this process (an
// in-process JobManager + TaskManager over an ephemeral loopback port,
// via clink::embed::EmbeddedEngine), runs the script, awaits the
// submitted jobs, and exits with their status. No daemons, no cluster.
// A bare SELECT prints its result rows to stdout through the synthesised
// connector='print' sink. The first Ctrl-C cancels the running jobs and
// drains them; a second Ctrl-C force-quits.
//
// With --jm-host=<h> --jm-port=<p> the same script is instead compiled
// and each job POSTed to that running JobManager (the clink_submit_sql
// path) - the same file moves from laptop to cluster by adding one flag.
// Bare SELECT is rejected remotely: its print sink would write to a
// TaskManager's stdout, not this terminal.

#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

#include "clink/embed/embedded_engine.hpp"
#include "clink/plugin/install_defaults.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/script_runner.hpp"
#ifdef CLINK_LINKED_WASM
#include "clink/wasm/install.hpp"
#endif

namespace {

volatile std::sig_atomic_t g_interrupts = 0;

void on_sigint(int /*sig*/) {
    ++g_interrupts;
    if (g_interrupts >= 2) {
        _exit(130);
    }
}

struct SqlRunArgs {
    std::string file;
    std::string inline_sql;
    std::string catalog_dir;
    std::string jm_host;
    std::uint16_t jm_port = 0;
    std::string job_name;
    std::string state_backend;
    std::string checkpoint_dir;
    std::int64_t checkpoint_interval_ms = 10'000;
    std::string capture_dir;
    std::size_t capture_records = 0;
    std::uint32_t parallelism = 1;
    std::size_t slots = 64;
    bool explain = false;
};

void usage() {
    std::cerr << "Usage: clink run <file>.sql [flags]\n"
              << "       clink run -e \"<sql>\"  [flags]\n"
              << "\n"
              << "Runs a SQL script EMBEDDED: the whole runtime starts in this process,\n"
              << "no daemons. A bare SELECT prints its rows to stdout. Ctrl-C stops and\n"
              << "drains a running pipeline (press twice to force-quit).\n"
              << "\n"
              << "  -e <sql>                    Inline SQL instead of a file.\n"
              << "  --parallelism=<n> | -p <n>  Uniform op parallelism (default 1).\n"
              << "  --state-backend=<uri>       Per-job state backend URI (e.g.\n"
              << "                              rocksdb:///tmp/state). Default: memory, or\n"
              << "                              file when --checkpoint-dir is set.\n"
              << "  --checkpoint-dir=<dir>      Enable checkpointing under this root.\n"
              << "  --checkpoint-interval-ms=<n>  Periodic checkpoint cadence (default 10000;\n"
              << "                              used only with --checkpoint-dir).\n"
              << "  --capture-dir=<dir>         Record-capture flight recorder: tee each\n"
              << "                              operator's input records into per-checkpoint\n"
              << "                              epoch files under this dir (time-travel\n"
              << "                              debugging; inspect with clink capture-cat).\n"
              << "  --capture-records=<n>       Per-epoch record cap (default 10000).\n"
              << "  --catalog-dir=<dir>         Persistent catalog (CREATE TABLE auto-saves).\n"
              << "  --slots=<n>                 In-process TaskManager slots (default 64).\n"
              << "  --name=<job>                Job-name override for submitted jobs.\n"
              << "  --explain                   Print LogicalPlans; nothing runs.\n"
              << "  --jm-host=<host> --jm-port=<port>\n"
              << "                              Submit the SAME script to a running\n"
              << "                              JobManager instead of running embedded.\n";
}

// Accepts --flag=value and --flag value; returns false on a malformed
// argument list (diagnostic already printed).
bool parse_args(int argc, char** argv, SqlRunArgs& a) {
    auto value_of =
        [&](int& i, std::string_view arg, std::string_view flag, std::string* out) -> bool {
        const std::string eq = std::string{flag} + "=";
        if (arg.starts_with(eq)) {
            *out = std::string{arg.substr(eq.size())};
            return true;
        }
        if (arg == flag) {
            if (i + 1 >= argc) {
                std::cerr << "error: " << flag << " requires a value\n";
                return false;
            }
            *out = argv[++i];
            return true;
        }
        return false;
    };
    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        std::string v;
        if (arg == "--help" || arg == "-h") {
            usage();
            std::exit(0);
        } else if (arg == "--explain") {
            a.explain = true;
        } else if (arg == "-e") {
            if (i + 1 >= argc) {
                std::cerr << "error: -e requires a SQL string\n";
                return false;
            }
            a.inline_sql = argv[++i];
        } else if (value_of(i, arg, "--file", &v) || value_of(i, arg, "-f", &v)) {
            a.file = v;
        } else if (value_of(i, arg, "--catalog-dir", &v)) {
            a.catalog_dir = v;
        } else if (value_of(i, arg, "--jm-host", &v)) {
            a.jm_host = v;
        } else if (value_of(i, arg, "--jm-port", &v)) {
            a.jm_port = static_cast<std::uint16_t>(std::stoi(v));
        } else if (value_of(i, arg, "--name", &v)) {
            a.job_name = v;
        } else if (value_of(i, arg, "--state-backend", &v)) {
            a.state_backend = v;
        } else if (value_of(i, arg, "--checkpoint-dir", &v)) {
            a.checkpoint_dir = v;
        } else if (value_of(i, arg, "--checkpoint-interval-ms", &v)) {
            a.checkpoint_interval_ms = std::stoll(v);
        } else if (value_of(i, arg, "--capture-dir", &v)) {
            a.capture_dir = v;
        } else if (value_of(i, arg, "--capture-records", &v)) {
            a.capture_records = static_cast<std::size_t>(std::stoull(v));
        } else if (value_of(i, arg, "--parallelism", &v) || value_of(i, arg, "-p", &v)) {
            a.parallelism = static_cast<std::uint32_t>(std::stoul(v));
            if (a.parallelism < 1) {
                std::cerr << "error: --parallelism must be >= 1\n";
                return false;
            }
        } else if (value_of(i, arg, "--slots", &v)) {
            a.slots = static_cast<std::size_t>(std::stoul(v));
        } else if (!arg.starts_with("-") && arg.ends_with(".sql")) {
            a.file = std::string{arg};
        } else {
            std::cerr << "error: unknown argument: " << arg << "\n";
            usage();
            return false;
        }
    }
    if (a.file.empty() == a.inline_sql.empty()) {
        std::cerr << "error: exactly one of <file>.sql or -e is required\n";
        usage();
        return false;
    }
    return true;
}

std::string read_file(const std::string& path, bool* ok) {
    std::ifstream in(path);
    if (!in) {
        std::cerr << "error: cannot open " << path << "\n";
        *ok = false;
        return {};
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    *ok = true;
    return ss.str();
}

}  // namespace

int clink_cmd_run_sql(int argc, char** argv) {
    SqlRunArgs args;
    if (!parse_args(argc, argv, args)) {
        return 2;
    }
    bool ok = true;
    const std::string sql = args.file.empty() ? args.inline_sql : read_file(args.file, &ok);
    if (!ok) {
        return 1;
    }

    // Remote or explain-only: no in-process cluster needed - the shared
    // script runner compiles (and POSTs) exactly as clink_submit_sql does.
    if (!args.jm_host.empty() || args.explain) {
        if (!args.jm_host.empty() && args.jm_port == 0) {
            std::cerr << "error: --jm-host requires --jm-port\n";
            return 2;
        }
#ifdef CLINK_LINKED_WASM
        // CREATE FUNCTION ... LANGUAGE wasm executes here, client-side (the
        // loader validates the module and packages its bytes into the spec);
        // the embedded branch below gets this via EmbeddedEngine instead.
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
        auto submit =
            !args.jm_host.empty()
                ? clink::sql::make_http_submit(
                      args.jm_host, args.jm_port, args.state_backend, std::cout, std::cerr)
                : clink::sql::make_print_submit(std::cout);
        return clink::sql::run_script(sql, catalog, opts, io, submit);
    }

    // Embedded: the whole runtime in this process.
    clink::embed::EngineOptions eopts;
    eopts.parallelism = args.parallelism;
    eopts.slots = args.slots;
    eopts.state_backend_uri = args.state_backend;
    eopts.checkpoint_dir = args.checkpoint_dir;
    eopts.checkpoint_interval_ms = args.checkpoint_interval_ms;
    eopts.capture_dir = args.capture_dir;
    eopts.capture_records = args.capture_records;
    eopts.catalog_dir = args.catalog_dir;
    eopts.job_name = args.job_name;
    try {
        // Embedded execution reaches every linked connector, same as a
        // cluster node: install the impls' factories before the engine
        // materialises any job (idempotent).
        clink::plugin::PluginRegistry impl_reg;
        clink::plugin::install_defaults(impl_reg);
        clink::embed::EmbeddedEngine engine{std::move(eopts)};
        if (int rc = engine.execute_script(sql); rc != 0) {
            return rc;
        }
        if (engine.job_count() == 0) {
            return 0;  // pure-DDL / catalog-only script
        }
        std::signal(SIGINT, on_sigint);
        const bool run_ok = engine.await_all([] { return g_interrupts > 0; });
        std::signal(SIGINT, SIG_DFL);
        return run_ok ? 0 : 1;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
