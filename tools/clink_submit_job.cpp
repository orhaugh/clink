// clink_submit_job - submit a compiled clink job (a .so built with
// CLINK_REGISTER_JOB) to a running Coordinator. Mirrors ` run`.
//
// Usage:
//   clink_submit_job --job=/path/to/my_job.so
//                      --coordinator-host=127.0.0.1 --coordinator-port=6123
//                      [--wait-timeout-s=N] [--name=<label>]
//
// Flow:
//   1. dlopen the job .so locally so its clink_plugin_register fires
//      (this populates the submitter's process-wide RunnerRegistry -
//      not strictly required, but matches the local-test config and
//      catches build-fn errors before any network round-trip).
//   2. Look up clink_job_build, call it to fetch the JobGraphSpec
//      JSON pointer + length.
//   3. JobSubmitter::submit(graph_json, plugin_paths = [<abs .so path>],
//                           opts). The coordinator ships the .so to every worker
//      assigned a task from this job; each worker dlopens it, which fires
//      the same clink_plugin_register call_once gate and makes the
//      inline op-types resolvable in that worker's RunnerRegistry.
//   4. Print the SubmitResult to stdout.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include "clink/application/job_submitter.hpp"

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
    std::cerr
        << "Usage: clink run --job=<path.so> --coordinator-host=<host> --coordinator-port=<port>\n"
        << "                          [--wait-timeout-s=N] [--name=<label>]\n"
        << "                          [--state-backend=<scheme>[:<path>]]\n"
        << "                          [--checkpoint-interval-ms=N] "
           "[--max-restarts-on-worker-loss=N]\n"
        << "       clink run <file>.sql | -e \"<sql>\"   (embedded SQL: run with --help for "
           "flags)\n"
        << "\n"
        << "Submit a clink job (a shared library built with CLINK_REGISTER_JOB)\n"
        << "to a running Coordinator. A .sql argument instead runs the script\n"
        << "EMBEDDED in this process (no cluster), or submits it to a coordinator when\n"
        << "--coordinator-host/--coordinator-port are given.\n"
        << "\n"
        << "State backend selection:\n"
        << "  --state-backend=memory           in-memory keyed state (default; no persistence)\n"
        << "  --state-backend=file[:<path>]    file-backed (default path: /var/lib/clink/state)\n"
        << "  --state-backend=rocksdb[:<path>] RocksDB-backed (default: /var/lib/clink/state)\n"
        << "  --checkpoint-dir=<uri>           low-level escape hatch (sets the raw URI "
           "directly)\n";
}

// Translate --state-backend=<scheme>[:<path>] into a checkpoint-dir
// URI compatible with the StateBackendFactory. Returns the composed
// URI, or std::nullopt + writes to stderr on unknown schemes.
std::optional<std::string> compose_state_backend_uri(const std::string& spec) {
    constexpr const char* kDefaultPath = "/var/lib/clink/state";
    const auto colon = spec.find(':');
    const std::string scheme = (colon == std::string::npos) ? spec : spec.substr(0, colon);
    const std::string path =
        (colon == std::string::npos) ? std::string{kDefaultPath} : spec.substr(colon + 1);
    if (scheme == "memory") {
        // Memory backend ignores the path; an empty checkpoint_dir
        // disables persistence entirely (the coordinator skips checkpoint
        // triggers; the runner uses in-memory state).
        return std::string{};
    }
    if (scheme == "file") {
        // File backend accepts a bare path (legacy default) or the
        // explicit file:// URI; either resolves to the same builder.
        return path;
    }
    if (scheme == "rocksdb") {
        return std::string{"rocksdb://"} + path;
    }
    if (scheme == "forst") {
        // ForSt backend (opt-in build): the scheme resolves worker-side
        // only when the node was built with CLINK_WITH_FORST=ON - an
        // unknown scheme there fails the deploy with a clear factory
        // error, so the client passes it through like rocksdb.
        return std::string{"forst://"} + path;
    }
    std::cerr << "clink run: unknown --state-backend scheme '" << scheme
              << "' (expected one of: memory, file, rocksdb, forst)\n";
    return std::nullopt;
}

}  // namespace

// SQL front door (tools/clink_run_sql.cpp, or its stub when the build has
// no SQL frontend). `clink run` dispatches there on the argument shape.
int clink_cmd_run_sql(int argc, char** argv);

int clink_cmd_run(int argc, char** argv) {
    // SQL mode: `clink run pipeline.sql` / `clink run -e "SELECT ..."`.
    // Dispatch on the argument shape so the compiled-job (.so) path below
    // stays untouched: any positional ending .sql, or -e / --file present.
    for (int i = 1; i < argc; ++i) {
        const std::string_view a = argv[i];
        if (a == "-e" || a == "--file" || a.starts_with("--file=")) {
            return clink_cmd_run_sql(argc, argv);
        }
        if (!a.starts_with("-") && a.ends_with(".sql")) {
            return clink_cmd_run_sql(argc, argv);
        }
    }

    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return argc < 2 ? 1 : 0;
    }

    const auto job_path = get_arg(argc, argv, "job");
    const auto coordinator_host = get_arg(argc, argv, "coordinator-host", "127.0.0.1");
    const auto coordinator_port_str = get_arg(argc, argv, "coordinator-port", "6123");
    const auto wait_s_str = get_arg(argc, argv, "wait-timeout-s", "30");
    const auto job_name = get_arg(argc, argv, "name", "job");
    // Checkpointing: --checkpoint-dir enables periodic barriers at
    // --checkpoint-interval-ms cadence. --restore-from-dir +
    // --restore-from-checkpoint-id resumes from a prior run's snapshot.
    //
    // --state-backend is the friendly shorthand: pass a scheme (and
    // optionally a path) and we compose the underlying URI. If both
    // --state-backend and --checkpoint-dir are given, --checkpoint-dir
    // wins (escape hatch).
    auto ckpt_dir = get_arg(argc, argv, "checkpoint-dir", "");
    const auto state_backend = get_arg(argc, argv, "state-backend", "");
    if (ckpt_dir.empty() && !state_backend.empty()) {
        auto composed = compose_state_backend_uri(state_backend);
        if (!composed.has_value()) {
            return 8;
        }
        ckpt_dir = std::move(*composed);
    }
    const auto ckpt_interval_str = get_arg(argc, argv, "checkpoint-interval-ms", "0");
    const auto restore_dir = get_arg(argc, argv, "restore-from-dir", "");
    const auto restore_id_str = get_arg(argc, argv, "restore-from-checkpoint-id", "0");
    const auto max_restarts_str = get_arg(argc, argv, "max-restarts-on-worker-loss", "0");

    if (job_path.empty()) {
        std::cerr << "clink_submit_job: --job=<path.so> is required\n";
        return 2;
    }
    const auto job_abs = std::filesystem::absolute(std::filesystem::path{job_path});
    if (!std::filesystem::exists(job_abs)) {
        std::cerr << "clink_submit_job: job .so not found: " << job_abs << "\n";
        return 2;
    }

    // Load the job .so locally: this fires clink_plugin_register
    // (call_once-gated) which runs the user's build_fn and populates
    // this process's RunnerRegistry. Catches build-fn errors before
    // we even reach out to the coordinator.
    void* handle = ::dlopen(job_abs.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        std::cerr << "clink_submit_job: dlopen failed: " << ::dlerror() << "\n";
        return 3;
    }

    using JobBuildFn = int (*)(const char**, std::size_t*);
    auto sym = ::dlsym(handle, "clink_job_build");
    if (sym == nullptr) {
        std::cerr << "clink_submit_job: .so does not export clink_job_build\n"
                  << "  (was it built with CLINK_REGISTER_JOB?)\n";
        ::dlclose(handle);
        return 4;
    }
    JobBuildFn job_build = nullptr;
    std::memcpy(&job_build, &sym, sizeof(job_build));

    using PluginRegisterFn = int (*)(void*, char*, std::size_t);
    auto reg_sym = ::dlsym(handle, "clink_plugin_register");
    if (reg_sym != nullptr) {
        PluginRegisterFn plugin_register = nullptr;
        std::memcpy(&plugin_register, &reg_sym, sizeof(plugin_register));
        char err[1024]{};
        // registry_ptr unused by REGISTER_JOB's emitted function; pass
        // nullptr.
        if (plugin_register(nullptr, err, sizeof(err)) != 0) {
            std::cerr << "clink_submit_job: build_fn failed: " << err << "\n";
            ::dlclose(handle);
            return 5;
        }
    }

    const char* graph_json_data = nullptr;
    std::size_t graph_json_size = 0;
    if (job_build(&graph_json_data, &graph_json_size) != 0) {
        std::cerr << "clink_submit_job: clink_job_build returned non-zero\n";
        ::dlclose(handle);
        return 6;
    }
    if (graph_json_data == nullptr || graph_json_size == 0) {
        std::cerr << "clink_submit_job: empty job graph\n";
        ::dlclose(handle);
        return 7;
    }
    const std::string graph_json{graph_json_data, graph_json_size};

    const auto coordinator_port = static_cast<std::uint16_t>(std::stoi(coordinator_port_str));
    clink::application::JobSubmitter submitter(coordinator_host, coordinator_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds{std::stoi(wait_s_str)};
    if (!ckpt_dir.empty()) {
        opts.checkpoint.checkpoint_dir = ckpt_dir;
        opts.checkpoint.interval_ms = std::stoll(ckpt_interval_str);
        opts.checkpoint.restore_from_dir = restore_dir;
        opts.checkpoint.restore_from_checkpoint_id =
            static_cast<std::uint64_t>(std::stoull(restore_id_str));
        opts.checkpoint.max_restarts_on_worker_loss =
            static_cast<std::uint32_t>(std::stoul(max_restarts_str));
    }

    const auto result = submitter.submit(graph_json, {job_abs.string()}, opts);

    std::cout << "submit: name=" << job_name << " completed=" << result.completed
              << " ok=" << result.ok;
    if (!result.reject_message.empty()) {
        std::cout << " reject=" << result.reject_message;
    }
    if (!result.errors.empty()) {
        std::cout << " errors=" << result.errors.front();
    }
    std::cout << "\n";

    // dlclose the .so. Leaving the .so loaded would leak across repeated
    // invocations of a long-lived submit tool; for a one-shot CLI it's
    // moot, but keep the bookkeeping tidy.
    ::dlclose(handle);

    if (!result.completed) {
        return 8;
    }
    return result.ok ? 0 : 9;
}
