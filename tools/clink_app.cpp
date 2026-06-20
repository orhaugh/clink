// clink_app -  Application Mode equivalent.
//
// Application Mode runs the user's main() inside the JobManager
// process (instead of in a separate client). The JM dlopens the user
// JAR, builds the JobGraph in-process, schedules it, awaits completion,
// and exits. clink_app mirrors that shape:
//
//   1. Start a JobManager in THIS process bound to --port.
//   2. dlopen the supplied .so (built with CLINK_REGISTER_JOB) and
//      retrieve its JobGraphSpec via clink_job_build. No wire round-
//      trip: the bundle is constructed in-process and threaded straight
//      into JobManager::submit_job.
//   3. The JM ships the .so bytes to every TM that registers, the same
//      way it does for clink_submit_job submissions.
//   4. Block on JobManager::await_job_completion(...).
//   5. Surface job_errors and exit (non-zero on any error).
//
// External TMs are required: spawn them with `clink_node --role=tm
// --jm-host=127.0.0.1 --jm-port=<port>` before or during clink_app's
// `--wait-slots-s` window. The JM will block in submit_job until enough
// slots are available (or the wait window expires).
//
// Usage:
//   clink_app --job=/path/to/my_job.so
//               [--port=N] [--bind=127.0.0.1] [--advertise=127.0.0.1]
//               [--wait-slots-s=N] [--wait-job-s=N] [--name=<label>]
//               [--no-tcp]  (don't accept external clients; in-proc only)
//
// Compared to clink_submit_job:
//   * submit_job: connects to an EXISTING JM as a client; JM is long-lived.
//     Like  "session mode" submissions.
//   * clink_app: STARTS A NEW JM bound to the job; exits when the job
//     ends. Like  "application mode".

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_bundle.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/plugin_cache.hpp"
#include "clink/cluster/plugin_loader.hpp"

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
    std::cerr << "Usage: clink run-application --job=<path.so>\n"
              << "                   [--port=N] [--bind=<host>] [--advertise=<host>]\n"
              << "                   [--wait-slots-s=N] [--wait-job-s=N] [--name=<label>]\n"
              << "                   [--state-backend=<uri>]\n"
              << "\n"
              << "Start a JM in this process, dlopen the job .so locally, submit\n"
              << "via the in-process API, wait for completion. External TMs must\n"
              << "register with this JM during the --wait-slots-s window.\n"
              << "\n"
              << "--state-backend defaults to disagg-local:// (process-local, async\n"
              << "path on, not durable). Use file:///dir or remote-read://bucket for\n"
              << "a durable run; an empty value falls back to the in-memory backend.\n";
}

}  // namespace

int clink_cmd_run_application(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return argc < 2 ? 1 : 0;
    }

    const auto job_path = get_arg(argc, argv, "job");
    const auto port_str = get_arg(argc, argv, "port", "0");
    const auto bind_host = get_arg(argc, argv, "bind", "0.0.0.0");
    const auto advertise = get_arg(argc, argv, "advertise", "127.0.0.1");
    const auto wait_slots_str = get_arg(argc, argv, "wait-slots-s", "30");
    const auto wait_job_str = get_arg(argc, argv, "wait-job-s", "300");
    const auto job_name = get_arg(argc, argv, "name", "app");
    // run-application is the local one-shot dev runner, so it defaults to the
    // disagg-local:// deferring backend: keyed state works out of the box AND
    // the async/disaggregated execution path is exercised without S3. It is
    // process-local + non-durable, which is correct for a single-process local
    // run. Override for a durable run, e.g. --state-backend=remote-read://bkt
    // or --state-backend=file:///var/clink. An empty value falls back to the
    // legacy memory backend. Applied via the JM's default lever below.
    const auto state_backend = get_arg(argc, argv, "state-backend", "disagg-local://");

    if (job_path.empty()) {
        std::cerr << "clink_app: --job=<path.so> is required\n";
        return 2;
    }
    const auto job_abs = std::filesystem::absolute(std::filesystem::path{job_path});
    if (!std::filesystem::exists(job_abs)) {
        std::cerr << "clink_app: job .so not found: " << job_abs << "\n";
        return 2;
    }

    const auto bind_port = static_cast<std::uint16_t>(std::stoi(port_str));
    const auto wait_slots = std::chrono::seconds{std::stoi(wait_slots_str)};
    const auto wait_job = std::chrono::seconds{std::stoi(wait_job_str)};

    // Built-ins must be registered before the JM accepts anything that
    // references int64 / string channels. JobManager::submit_job calls
    // this internally too, but registering up front keeps the order
    // explicit and matches clink_node's behaviour.
    clink::cluster::ensure_built_ins_registered();

    // Start the JM. submit_wait_for_slots is the per-submission slot-
    // availability timeout: submit_job() blocks until either the cluster
    // has the slots the job needs, or this elapses.
    clink::cluster::JobManager::Config cfg;
    cfg.bind_host = bind_host;
    cfg.advertise_host = advertise;
    cfg.submit_wait_for_slots = wait_slots;
    cfg.default_state_backend_uri = state_backend;
    clink::cluster::JobManager jm(cfg);
    const auto bound = jm.start(bind_port);
    std::cerr << "clink_app: JM listening on " << advertise << ":" << bound << "\n";

    // Allocate the per-job bundle and load the .so INTO it. Locally, in
    // this process - no wire submit. The same .so will be shipped to
    // remote TMs as PluginBinary bytes via the Deploy message.
    auto bundle = std::make_unique<clink::cluster::JobBundle>();
    auto bundle_preg = bundle->as_plugin_registry();
    auto load_result =
        clink::cluster::PluginLoader::default_instance().load_into(job_abs.string(), bundle_preg);
    if (!load_result.ok) {
        std::cerr << "clink_app: dlopen/register failed for " << job_abs << ": "
                  << load_result.error << "\n";
        return 3;
    }

    // Retrieve the JobGraphSpec JSON via clink_job_build. The .so has
    // already run build_fn under call_once (during load_into); job_build
    // just exposes the captured JSON pointer.
    using JobBuildFn = int (*)(const char**, std::size_t*);
    JobBuildFn job_build = nullptr;
    auto sym = ::dlsym(load_result.plugin.dl_handle, "clink_job_build");
    if (sym == nullptr) {
        std::cerr << "clink_app: .so does not export clink_job_build (was it built with "
                     "CLINK_REGISTER_JOB?)\n";
        return 4;
    }
    std::memcpy(&job_build, &sym, sizeof(job_build));

    const char* graph_json_data = nullptr;
    std::size_t graph_json_size = 0;
    if (job_build(&graph_json_data, &graph_json_size) != 0 || graph_json_data == nullptr ||
        graph_json_size == 0) {
        std::cerr << "clink_app: clink_job_build returned no graph\n";
        return 5;
    }
    auto graph =
        clink::cluster::JobGraphSpec::from_json(std::string{graph_json_data, graph_json_size});

    // Read the .so bytes so the JM can ship them to every TM in Deploy.
    std::vector<clink::cluster::PluginBinary> plugins;
    plugins.push_back(clink::cluster::make_plugin_binary_from_file(job_abs.string()));

    // Submit in-process. notify_client_fd = -1 because there's no
    // separate client to push JobCompleted at. We poll
    // await_job_completion below.
    std::uint64_t job_id = 0;
    try {
        job_id = jm.submit_job(graph,
                               clink::cluster::OperatorRegistry::default_instance(),
                               std::move(plugins),
                               clink::cluster::CheckpointConfig{},
                               std::move(bundle),
                               /*notify_client_conn=*/nullptr);
    } catch (const std::exception& e) {
        std::cerr << "clink_app: submit_job threw: " << e.what() << "\n";
        return 6;
    }
    std::cerr << "clink_app: submitted job " << job_id << " (name=" << job_name << ")\n";

    if (!jm.await_job_completion(job_id, wait_job)) {
        std::cerr << "clink_app: job " << job_id << " did not complete within " << wait_job.count()
                  << "s\n";
        return 7;
    }

    const auto errors = jm.job_errors(job_id);
    if (!errors.empty()) {
        for (const auto& e : errors) {
            std::cerr << "clink_app: job error: " << e << "\n";
        }
        return 8;
    }

    std::cerr << "clink_app: job " << job_id << " completed successfully\n";
    return 0;
}
