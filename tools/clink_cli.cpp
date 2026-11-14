// clink - client CLI. Dispatches subcommands to the
// individual clink_cmd_* implementations defined in the sibling
// tools/clink_<verb>.cpp files. Mirrors ` run / cancel /
// savepoint / list` shape so muscle memory carries over.
//
// The server-side daemon (`clink_node --role=coordinator|worker`) is a separate
// binary, matching `coordinator.sh` / `worker.sh` split.

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string_view>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/plugin/install_defaults.hpp"
#include "clink/plugin/plugin.hpp"
#ifdef CLINK_CLI_HAS_SQL
#include "clink/sql/install.hpp"
#endif
#ifdef CLINK_LINKED_ROCKSDB
#include "clink/state/rocksdb_state_backend.hpp"
#endif

// Subcommand entry points. Each lives in its own .cpp under tools/.
// Signatures match the original per-binary main(): the dispatcher
// passes a synthesized argc/argv with the subcommand token sliced off,
// so existing --flag parsing inside each handler still works.
int clink_cmd_run(int argc, char** argv);
int clink_cmd_run_application(int argc, char** argv);
int clink_cmd_cancel(int argc, char** argv);
int clink_cmd_savepoint(int argc, char** argv);
int clink_cmd_stop_job(int argc, char** argv);
int clink_cmd_check_savepoint(int argc, char** argv);
int clink_cmd_state_diff(int argc, char** argv);
int clink_cmd_state_cat(int argc, char** argv);
int clink_cmd_state_sweep(int argc, char** argv);
int clink_cmd_state_export(int argc, char** argv);
int clink_cmd_state_query(int argc, char** argv);
int clink_cmd_capture_cat(int argc, char** argv);
int clink_cmd_capture_push(int argc, char** argv);
int clink_cmd_capture_fetch(int argc, char** argv);
int clink_cmd_replay(int argc, char** argv);
int clink_cmd_replay_diff(int argc, char** argv);
int clink_cmd_flight_sql(int argc, char** argv);
int clink_cmd_rescale(int argc, char** argv);
int clink_cmd_rescale_op(int argc, char** argv);
int clink_cmd_list(int argc, char** argv);
int clink_cmd_checkpoint_verify(int argc, char** argv);
int clink_cmd_incident_collect(int argc, char** argv);
int clink_cmd_incident_verify(int argc, char** argv);
int clink_cmd_incident_regress(int argc, char** argv);
int clink_cmd_incident_reconcile(int argc, char** argv);
int clink_cmd_incident_scope(int argc, char** argv);
int clink_cmd_lint(int argc, char** argv);

namespace {

// The impls declare their capability records inside their install() hooks, and
// nothing calls them on a path that does not run a job. Install the linked
// connectors into a scratch registry, exactly as `clink run` does before a
// script, so what reads the records sees the binary rather than the subset
// that self-registers.
void declare_linked_capabilities() {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry linked;
    clink::plugin::install_defaults(linked);
#ifdef CLINK_CLI_HAS_SQL
    // The SQL frontend declares the records of the connectors it registers
    // itself (delta, print, collect, changelog, queryable_state), and
    // announces itself, from its install().
    clink::sql::install(linked);
#endif
}

void usage() {
    std::cerr
        << "Usage: clink <command> [--flags...]\n"
        << "\n"
        << "Commands:\n"
        << "  run               Submit a compiled job .so to a running coordinator, or run a .sql\n"
        << "                    script embedded in this process (`clink run job.sql`).\n"
        << "  run-application   Start an in-process coordinator and run a job (alias of  "
           "run-application).\n"
        << "  cancel            Cancel an active job by id, abruptly - everything since the "
           "last checkpoint replays on restart.\n"
        << "  stop              Stop a job gracefully: drain, commit the tail at a final "
           "checkpoint, print the id to resume from.\n"
        << "  savepoint         Trigger a synchronous savepoint (alias of  savepoint).\n"
        << "  check-savepoint   Inspect state-schema version stamps inside a savepoint file.\n"
        << "  state-diff        Compare two checkpoint datasets, or two snapshots.\n"
        << "  state-cat         List a checkpoint dataset, or dump a snapshot.\n"
        << "  state-sweep       Reclaim unreferenced objects from a disaggregated store.\n"
        << "  state-export      Export a completed checkpoint of a whole job as a verified\n"
        << "                    dataset (--checkpoint-dir), or a snapshot as one file.\n"
        << "  state-query       Run SQL over a checkpoint dataset or a snapshot.\n"
        << "  capture-cat       Inspect record-capture flight-recorder epochs.\n"
        << "  capture-push      Upload a capture tree to object storage (retention).\n"
        << "  capture-fetch     Download a pushed capture tree for local replay.\n"
        << "  replay            Replay a captured epoch offline (one operator or whole job).\n"
        << "  replay-diff       Diff two replay emission dumps (first divergence).\n"
        << "  flight-sql        Serve the embedded engine over Arrow Flight SQL.\n"
        << "  rescale           Change a running job's per-role parallelism.\n"
        << "  rescale-op        Rescale ONE operator within a job to a new parallelism.\n"
        << "  list              List active and recently-completed jobs (alias of  list).\n"
        << "  checkpoint-verify Verify a checkpoint directory's integrity; --repair mints\n"
        << "                    sidecars for a directory written before they existed.\n"
        << "  incident-collect  Gather one job's incident evidence into a portable bundle.\n"
        << "  incident-verify   Check an incident bundle offline: integrity, the restore\n"
        << "                    point, state, protocol traces and replay. Exits 3 on a\n"
        << "                    partial result.\n"
        << "  incident-regress  Freeze a bundle's captured epoch as a replay regression test.\n"
        << "  incident-reconcile Read a ClickHouse destination, read-only, and say whether it\n"
        << "                    holds exactly what a bundle's sink received for one completed\n"
        << "                    checkpoint, and with --reference audit it against a\n"
        << "                    reference computed outside the job. Exits 3 on a partial\n"
        << "                    result.\n"
        << "  incident-scope    Export the scope of one checkpoint in a bundle, offline, for a\n"
        << "                    reference implementation to compute from.\n"
        << "  lint              Check a job/cluster configuration for settings that will be\n"
        << "                    ignored or that contradict each other. No cluster needed;\n"
        << "                    exits 1 on anything submission would refuse.\n"
        << "\n"
        << "Flags:\n"
        << "  --capabilities        What THIS BINARY can do: connectors compiled in, their\n"
        << "                        declared delivery guarantees, and build surfaces.\n"
        << "  --capabilities-json   The same manifest as JSON.\n"
        << "\n"
        << "Each command accepts --help to print its own flag list.\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        usage();
        return 1;
    }
    const std::string_view cmd = argv[1];
#ifdef CLINK_LINKED_ROCKSDB
    // RocksDB checkpoints in a bundle or a checkpoint dataset are read through
    // this build's RocksDB, from a scratch copy.
    clink::incident::register_directory_state_reader(
        "rocksdb", [](const std::filesystem::path& dir) {
            return clink::rocksdb_checkpoint_to_arrow(dir.string());
        });
#endif
    if (cmd == "--help" || cmd == "-h" || cmd == "help") {
        usage();
        return 0;
    }
    // The manifest describes the binary, so it must be produced AFTER the
    // built-ins have registered (that is what declares them) and must not
    // need a running cluster.
    if (cmd == "--capabilities" || cmd == "--capabilities-json") {
        // A query command must not interleave operational logs with its
        // answer - the JSON form is meant to be piped into a parser.
        // Only defaulted, so an explicit CLINK_LOG_LEVEL still wins.
        ::setenv("CLINK_LOG_LEVEL", "off", /*overwrite=*/0);
        // Without the linked connectors' records the manifest listed the six
        // built-ins and told the reader that Kafka, ClickHouse and every other
        // connector compiled into the binary were absent.
        declare_linked_capabilities();
        const auto facts = clink::connectors::current_build_facts();
        const auto caps = clink::connectors::CapabilityRegistry::instance().all();
        std::cout << (cmd == "--capabilities-json"
                          ? clink::connectors::render_manifest_json(facts, caps)
                          : clink::connectors::render_manifest_text(facts, caps))
                  << "\n";
        return 0;
    }

    // Slice off argv[1] (the subcommand) and pass the remainder down.
    // Subcommand handlers re-parse --flags from index 1, matching their
    // original per-binary behavior. We patch argv[0] to "clink <cmd>"
    // so error messages print the user-facing invocation.
    static std::string prog_name;
    prog_name = std::string{argv[0]} + " " + std::string{cmd};
    int sub_argc = argc - 1;
    char** sub_argv = argv + 1;
    sub_argv[0] = prog_name.data();

    // The inspection commands answer a question about files or a running
    // job, and the answer must not be interleaved with the engine's own
    // operational log: state-query starts an embedded engine to run its
    // SQL, and printed the engine's task lifecycle around its result rows.
    // Only defaulted, so an explicit CLINK_LOG_LEVEL still wins - the same
    // rule --capabilities applies above.
    if (cmd == "state-cat" || cmd == "state-diff" || cmd == "state-export" ||
        cmd == "state-query" || cmd == "check-savepoint" || cmd == "capture-cat" ||
        cmd == "replay-diff" || cmd == "incident-collect" || cmd == "incident-verify" ||
        cmd == "incident-regress" || cmd == "incident-reconcile" || cmd == "incident-scope") {
        ::setenv("CLINK_LOG_LEVEL", "off", /*overwrite=*/0);
    }

    if (cmd == "run") {
        return clink_cmd_run(sub_argc, sub_argv);
    }
    if (cmd == "run-application") {
        return clink_cmd_run_application(sub_argc, sub_argv);
    }
    if (cmd == "cancel") {
        return clink_cmd_cancel(sub_argc, sub_argv);
    }
    if (cmd == "savepoint") {
        return clink_cmd_savepoint(sub_argc, sub_argv);
    }
    if (cmd == "stop") {
        return clink_cmd_stop_job(sub_argc, sub_argv);
    }
    if (cmd == "check-savepoint") {
        return clink_cmd_check_savepoint(sub_argc, sub_argv);
    }
    if (cmd == "state-diff") {
        return clink_cmd_state_diff(sub_argc, sub_argv);
    }
    if (cmd == "state-sweep") {
        return clink_cmd_state_sweep(argc - 1, argv + 1);
    }
    if (cmd == "state-cat") {
        return clink_cmd_state_cat(sub_argc, sub_argv);
    }
    if (cmd == "state-export") {
        return clink_cmd_state_export(sub_argc, sub_argv);
    }
    if (cmd == "state-query") {
        return clink_cmd_state_query(sub_argc, sub_argv);
    }
    if (cmd == "capture-cat") {
        return clink_cmd_capture_cat(sub_argc, sub_argv);
    }
    if (cmd == "capture-push") {
        return clink_cmd_capture_push(sub_argc, sub_argv);
    }
    if (cmd == "capture-fetch") {
        return clink_cmd_capture_fetch(sub_argc, sub_argv);
    }
    if (cmd == "replay") {
        return clink_cmd_replay(sub_argc, sub_argv);
    }
    if (cmd == "replay-diff") {
        return clink_cmd_replay_diff(sub_argc, sub_argv);
    }
    if (cmd == "flight-sql") {
        return clink_cmd_flight_sql(sub_argc, sub_argv);
    }
    if (cmd == "rescale") {
        return clink_cmd_rescale(sub_argc, sub_argv);
    }
    if (cmd == "rescale-op") {
        return clink_cmd_rescale_op(sub_argc, sub_argv);
    }
    if (cmd == "list") {
        return clink_cmd_list(sub_argc, sub_argv);
    }
    if (cmd == "lint") {
        return clink_cmd_lint(sub_argc, sub_argv);
    }
    if (cmd == "checkpoint-verify") {
        return clink_cmd_checkpoint_verify(sub_argc, sub_argv);
    }
    if (cmd == "incident-collect") {
        // The bundle's delivery report is the analysis the coordinator runs at
        // submission, over the same capability records.
        declare_linked_capabilities();
        return clink_cmd_incident_collect(sub_argc, sub_argv);
    }
    if (cmd == "incident-verify") {
        return clink_cmd_incident_verify(sub_argc, sub_argv);
    }
    if (cmd == "incident-regress") {
        return clink_cmd_incident_regress(sub_argc, sub_argv);
    }
    if (cmd == "incident-reconcile") {
        return clink_cmd_incident_reconcile(sub_argc, sub_argv);
    }
    if (cmd == "incident-scope") {
        return clink_cmd_incident_scope(sub_argc, sub_argv);
    }

    std::cerr << "clink: unknown command '" << cmd << "'\n\n";
    usage();
    return 2;
}
