// clink lint - check a configuration without submitting a job.
//
// The gap this closes (docs/production-hardening-plan.md W17): the config
// linter ran only at submission and behind `--profile`, so the way to find
// out whether a deployment's flags were coherent was to submit a job with
// them. On a production cluster that is not a check anyone runs.
//
// The command is only worth having if it agrees with the gate. It shares
// tools/cli_config_args.hpp with `clink run`, so the flags are parsed once,
// the CheckpointConfig is assembled once, and the same
// lint_checkpoint_config / lint_profile pair decides. A lint that reached
// its own verdict would be worse than nothing: a clean result would stop
// meaning the submission will be accepted.
//
// Exit codes are the point of the command, not the text: 0 clean, 1 errors
// found, 2 bad usage. That makes it usable in a deploy pipeline as a gate
// rather than something a human reads.

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/config_lint.hpp"

#include "cli_config_args.hpp"

namespace {

using clink::cli::get_arg;
using clink::cli::has_arg;

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
    std::cerr << "Usage: clink lint [job flags...] [cluster flags...]\n"
              << "\n"
              << "Check a configuration and report what will be ignored or contradicted,\n"
              << "without submitting anything or contacting a cluster. Takes the same flags\n"
              << "as `clink run` and `clink_node`, so a command line can be pasted in as-is;\n"
              << "flags it does not recognise are ignored rather than rejected.\n"
              << "\n"
              << "Job configuration (as `clink run`):\n"
              << "  --checkpoint-dir=<dir>                 enable checkpointing\n"
              << "  --state-backend=<scheme>[:<path>]      shorthand that composes the dir\n"
              << "  --checkpoint-interval-ms=N             periodic checkpoint cadence\n"
              << "  --restore-from-dir=<dir>               resume source\n"
              << "  --restore-from-checkpoint-id=N         resume point\n"
              << "  --max-restarts-on-worker-loss=auto|N   self-healing budget\n"
              << "  --capture-dir=<dir> --capture-records=N   flight recorder\n"
              << "  --profile=development|production       apply a profile's defaults first,\n"
              << "                                         then check its promises too\n"
              << "\n"
              << "Cluster liveness (as `clink_node --role=coordinator`):\n"
              << "  --heartbeat-timeout-ms=N     worker-loss window (node default 5000)\n"
              << "  --watchdog-interval-ms=N     liveness poll cadence (node default 200)\n"
              << "  --heartbeat-interval-ms=N    heartbeat cadence (500). NOT a clink_node\n"
              << "                               flag: the node fixes it at 500 and only the\n"
              << "                               embedded API can change it. Accepted here\n"
              << "                               because the timeout check needs it.\n"
              << "     Checked only when at least one is given, because linting the\n"
              << "     defaults of a process this command is not starting would report\n"
              << "     problems about a cluster nobody configured.\n"
              << "\n"
              << "Exit: 0 clean (warnings still print), 1 at least one error, 2 bad usage.\n";
}

}  // namespace

int clink_cmd_lint(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || has_flag(argc, argv, "h")) {
        usage();
        return 0;
    }

    auto resolved_dir = clink::cli::resolve_checkpoint_dir(argc, argv);
    if (!resolved_dir.has_value()) {
        return 2;  // unknown --state-backend scheme; already reported
    }
    auto config = clink::cli::build_checkpoint_config(argc, argv, *resolved_dir);

    auto lint = clink::cli::apply_profile_and_lint(argc, argv, config);
    if (!lint.ok) {
        return 2;
    }
    auto problems = std::move(lint.problems);

    // Liveness settings belong to the coordinator process rather than to a
    // submission, so they are linted only when the operator supplied one -
    // otherwise `clink lint --checkpoint-dir=/x` would emit findings about
    // heartbeat defaults it was never asked about.
    const bool any_liveness = has_arg(argc, argv, "heartbeat-interval-ms") ||
                              has_arg(argc, argv, "heartbeat-timeout-ms") ||
                              has_arg(argc, argv, "watchdog-interval-ms");
    if (any_liveness) {
        // Defaults for the ones not given MUST match clink_node's, or the
        // combination being linted is not the combination that would run -
        // which is the one way this command could confidently report a
        // clean cluster that then misbehaves. Taken from clink_node.cpp
        // (heartbeat_interval is a literal 500 there, timeout 5000,
        // watchdog 200) and pinned by a test.
        const auto hb = std::stoll(get_arg(argc, argv, "heartbeat-interval-ms", "500"));
        const auto to = std::stoll(get_arg(argc, argv, "heartbeat-timeout-ms", "5000"));
        const auto wd = std::stoll(get_arg(argc, argv, "watchdog-interval-ms", "200"));
        for (auto& p : clink::cluster::lint_liveness_config(hb, to, wd)) {
            problems.push_back(std::move(p));
        }
    }

    if (problems.empty()) {
        std::cout << "lint: no problems found\n";
        return 0;
    }

    std::cout << clink::cluster::render_problems(problems) << "\n";
    const bool fatal =
        std::any_of(problems.begin(), problems.end(), [](const auto& p) { return p.is_error(); });
    if (fatal) {
        // Say what the error MEANS for the operator, since the command's
        // whole purpose is to answer that before a deploy.
        std::cout << "lint: this configuration would be REFUSED at submission\n";
        return 1;
    }
    std::cout << "lint: no errors (warnings above would not block a submission)\n";
    return 0;
}
