// clink lint - check a configuration without submitting a job.
//
// The gap this closes (docs/history/production-hardening-2026-08.md W17): the config
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
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/config_lint.hpp"
#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
#include "clink/http/http_client.hpp"

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
              << "  --from-job=<host>:<port>/<id>          lint a RUNNING job's deployed\n"
              << "                                         configuration instead of flags\n"
              << "  --checkpoint-dir=<dir>                 enable checkpointing\n"
              << "  --state-backend=<scheme>[:<path>]      shorthand that composes the dir\n"
              << "  --checkpoint-interval-ms=N             periodic checkpoint cadence\n"
              << "  --restore-from-dir=<dir>               resume source\n"
              << "  --restore-from-checkpoint-id=N         resume point\n"
              << "  --max-restarts-on-worker-loss=auto|N   self-healing budget\n"
              << "\n"
              << "Job graph (optional):\n"
              << "  --graph-json=<file>                    lint the operators in a JobGraphSpec\n"
              << "  --available-slots=N                    check the graph fits that many slots\n"
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

    // --from-job=<host>:<port>/<job_id> lints what is DEPLOYED, not what was typed.
    //
    // The gap this closes (follow-up 24): every other path here assembles a
    // CheckpointConfig from flags, so a clean verdict says "this command line is
    // coherent" and never "the running job is coherent". Those diverge the moment
    // anyone edits a deployment without re-running the lint, and nothing detected it.
    //
    // The coordinator now reports a job's live checkpoint configuration on
    // /api/v1/jobs/:id (F73), so the drift is checkable at the source: fetch the
    // configuration the coordinator is ACTING on and lint that.
    clink::cluster::CheckpointConfig config;
    if (const auto from_job = get_arg(argc, argv, "from-job"); !from_job.empty()) {
        const auto slash = from_job.rfind('/');
        const auto colon =
            from_job.rfind(':', slash == std::string::npos ? from_job.size() : slash);
        if (slash == std::string::npos || colon == std::string::npos || colon >= slash) {
            std::cerr << "lint: --from-job needs <host>:<port>/<job_id>, got '" << from_job
                      << "'\n";
            return 2;
        }
        const std::string host = from_job.substr(0, colon);
        std::uint16_t port = 0;
        std::string job_id;
        try {
            port = static_cast<std::uint16_t>(
                std::stoul(from_job.substr(colon + 1, slash - colon - 1)));
            job_id = from_job.substr(slash + 1);
            (void)std::stoull(job_id);  // reject a non-numeric id before the request
        } catch (const std::exception&) {
            std::cerr << "lint: --from-job needs <host>:<port>/<job_id>, got '" << from_job
                      << "'\n";
            return 2;
        }

        clink::http::HttpClient client(host, port);
        if (const char* tok = std::getenv("CLINK_AUTH_TOKEN"); tok != nullptr) {
            client.set_bearer_token(tok);
        }
        const auto resp = client.get("/api/v1/jobs/" + job_id);
        if (resp.status == 0) {
            std::cerr << "lint: cannot reach coordinator at " << host << ":" << port << " - "
                      << resp.error << "\n";
            return 2;
        }
        if (resp.status != 200) {
            std::cerr << "lint: coordinator returned HTTP " << resp.status << " for job " << job_id
                      << "; is that job id running?\n";
            return 2;
        }

        clink::config::JsonValue doc;
        try {
            doc = clink::config::parse(resp.body);
        } catch (const std::exception& e) {
            std::cerr << "lint: could not parse the coordinator's response: " << e.what() << "\n";
            return 2;
        }
        // A coordinator too old to report its configuration must not be linted as
        // though it reported an empty one - that would read as "checkpointing is
        // disabled" and is the exact false verdict this command exists to avoid.
        if (!doc.contains("checkpoint_interval_ms")) {
            std::cerr << "lint: this coordinator does not report a job's checkpoint "
                         "configuration, so there is nothing deployed to lint. It predates "
                         "the field being published; lint the flags instead.\n";
            return 2;
        }
        config.checkpoint_dir = doc.string_or("checkpoint_dir", "");
        config.interval_ms = doc.int_or("checkpoint_interval_ms", 0);
        config.state_backend_uri = doc.string_or("state_backend_uri", "");
        config.restore_from_dir = doc.string_or("restore_from_dir", "");
        config.restore_from_checkpoint_id =
            static_cast<std::uint64_t>(doc.int_or("restore_from_checkpoint_id", 0));
        config.max_restarts_on_worker_loss =
            static_cast<std::uint32_t>(doc.int_or("max_restarts_on_worker_loss", 0));
        if (doc.bool_or("unaligned_checkpoints", false)) {
            config.alignment = clink::cluster::CheckpointAlignment::Unaligned;
        }
        std::cerr << "lint: checking job " << job_id << " as deployed on " << host << ":" << port
                  << "\n";
    } else {
        auto resolved_dir = clink::cli::resolve_checkpoint_dir(argc, argv);
        if (!resolved_dir.has_value()) {
            return 2;  // unknown --state-backend scheme; already reported
        }
        config = clink::cli::build_checkpoint_config(argc, argv, *resolved_dir);
    }

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

    // Graph-level lint, when the operator points at a graph. Two ways in: a
    // JobGraphSpec JSON file, or --available-slots to have the size checked
    // against a cluster this command cannot see for itself.
    if (const auto graph_json_path = get_arg(argc, argv, "graph-json"); !graph_json_path.empty()) {
        std::ifstream in(graph_json_path);
        if (!in) {
            std::cerr << "lint: cannot read --graph-json=" << graph_json_path << "\n";
            return 2;
        }
        const std::string body((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        std::optional<std::uint32_t> slots;
        if (has_arg(argc, argv, "available-slots")) {
            slots =
                static_cast<std::uint32_t>(std::stoul(get_arg(argc, argv, "available-slots", "0")));
        }
        try {
            const auto graph = clink::cluster::JobGraphSpec::from_json(body);
            for (auto& p : clink::cluster::lint_job_graph(graph, config, slots)) {
                problems.push_back(std::move(p));
            }

            // Cross-check against the delivery-guarantee analyser (follow-up 24).
            //
            // The two answer adjacent questions and never compared notes. The config
            // linter asks "is this configuration self-consistent"; the guarantee
            // analyser asks "what can this pipeline actually deliver". A
            // configuration can pass the first and still not support what it looks
            // like it is asking for - checkpointing enabled, an interval set, and a
            // sink with no transactional commit, which is at-least-once however
            // coherent the flags are.
            //
            // Reporting the analyser's verdict here means one command answers both,
            // and the operator does not have to know that the second analysis exists
            // to benefit from it. Same analyser the submission gate runs, so a clean
            // lint and an accepted submission cannot disagree.
            clink::connectors::GuaranteeReport report;
            const auto rejection = clink::cluster::check_delivery_guarantee(graph, config, &report);
            std::cout << "lint: end-to-end delivery guarantee: "
                      << clink::connectors::to_string(report.level);
            if (!report.limiting_factor.empty()) {
                std::cout << " (limited by " << report.limiting_factor << ")";
            }
            std::cout << "\n";
            for (const auto& warn : report.warnings) {
                std::cout << "lint:   note: " << warn << "\n";
            }
            if (!rejection.empty()) {
                // The submission gate would refuse this, so the lint must too -
                // otherwise a clean lint followed by a refused submission is exactly
                // the disagreement this command exists to rule out.
                std::cout << "lint: the pipeline cannot provide the requested guarantee:\n"
                          << rejection << "\n"
                          << "lint: this configuration would be REFUSED at submission\n";
                return 1;
            }
        } catch (const std::exception& e) {
            std::cerr << "lint: --graph-json did not parse: " << e.what() << "\n";
            return 2;
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
