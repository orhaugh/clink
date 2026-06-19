// clink - client CLI. Dispatches subcommands to the
// individual clink_cmd_* implementations defined in the sibling
// tools/clink_<verb>.cpp files. Mirrors ` run / cancel /
// savepoint / list` shape so muscle memory carries over.
//
// The server-side daemon (`clink_node --role=jm|tm`) is a separate
// binary, matching `jobmanager.sh` / `taskmanager.sh` split.

#include <cstring>
#include <iostream>
#include <string_view>

// Subcommand entry points. Each lives in its own .cpp under tools/.
// Signatures match the original per-binary main(): the dispatcher
// passes a synthesized argc/argv with the subcommand token sliced off,
// so existing --flag parsing inside each handler still works.
int clink_cmd_run(int argc, char** argv);
int clink_cmd_run_application(int argc, char** argv);
int clink_cmd_cancel(int argc, char** argv);
int clink_cmd_savepoint(int argc, char** argv);
int clink_cmd_check_savepoint(int argc, char** argv);
int clink_cmd_rescale(int argc, char** argv);
int clink_cmd_rescale_op(int argc, char** argv);
int clink_cmd_list(int argc, char** argv);

namespace {

void usage() {
    std::cerr
        << "Usage: clink <command> [--flags...]\n"
        << "\n"
        << "Commands:\n"
        << "  run               Submit a compiled job .so to a running JM (alias of  run).\n"
        << "  run-application   Start an in-process JM and run a job (alias of  "
           "run-application).\n"
        << "  cancel            Cancel an active job by id (alias of  cancel).\n"
        << "  savepoint         Trigger a synchronous savepoint (alias of  savepoint).\n"
        << "  check-savepoint   Inspect state-schema version stamps inside a savepoint file.\n"
        << "  rescale           Change a running job's per-role parallelism.\n"
        << "  rescale-op        Rescale ONE operator within a job to a new parallelism.\n"
        << "  list              List active and recently-completed jobs (alias of  list).\n"
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
    if (cmd == "--help" || cmd == "-h" || cmd == "help") {
        usage();
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
    if (cmd == "check-savepoint") {
        return clink_cmd_check_savepoint(sub_argc, sub_argv);
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

    std::cerr << "clink: unknown command '" << cmd << "'\n\n";
    usage();
    return 2;
}
