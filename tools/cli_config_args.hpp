#pragma once

// Shared flag parsing and checkpoint-config assembly for the client CLI.
//
// Extracted so `clink lint` and `clink run` cannot disagree. That is the
// whole reason this header exists rather than each command parsing its own
// flags: a linter that reaches a different verdict from the gate it claims
// to preview is worse than no linter, because a clean lint then means
// nothing. Two properties have to hold, and both are easy to break by
// reimplementing:
//
//   * every flag is read into the config even when no checkpoint dir was
//     resolved, so a contradiction like `--checkpoint-interval-ms=500` with
//     no dir REACHES the linter. Dropping the flags instead is what made
//     five checks unreachable through the CLI (see build_checkpoint_config);
//   * `--max-restarts-on-worker-loss` defaults to the kRestartAuto
//     sentinel, not to 0. Writing an explicit zero made the documented
//     recovery default unreachable through the CLI (see the comment in
//     build_checkpoint_config), and a lint that wrote 0 while submission
//     wrote the sentinel would pass configs that then behaved differently.

#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/config_lint.hpp"
#include "clink/cluster/protocol.hpp"

namespace clink::cli {

inline std::string get_arg(int argc,
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

// Was the flag actually given? get_arg cannot say - a default is
// indistinguishable from an explicit value equal to it - and a profile has
// to know. An explicit `--checkpoint-interval-ms=0` means "no periodic
// checkpoints" and must survive; filling it in because it looks unset would
// be the same failure the config linter exists to catch.
inline bool has_arg(int argc, char** argv, std::string_view flag) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]}.starts_with(prefix)) {
            return true;
        }
    }
    return false;
}

// Translate --state-backend=<scheme>[:<path>] into a checkpoint-dir
// URI compatible with the StateBackendFactory. Returns the composed
// URI, or std::nullopt + writes to stderr on unknown schemes.
inline std::optional<std::string> compose_state_backend_uri(const std::string& spec) {
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
    std::cerr << "unknown --state-backend scheme '" << scheme
              << "' (expected one of: memory, file, rocksdb, forst)\n";
    return std::nullopt;
}

// Resolve the checkpoint dir the way submission does: --checkpoint-dir
// wins, --state-backend composes one, neither leaves it empty. nullopt
// means an unknown scheme was given and the caller should fail.
inline std::optional<std::string> resolve_checkpoint_dir(int argc, char** argv) {
    auto ckpt_dir = get_arg(argc, argv, "checkpoint-dir", "");
    const auto state_backend = get_arg(argc, argv, "state-backend", "");
    if (ckpt_dir.empty() && !state_backend.empty()) {
        return compose_state_backend_uri(state_backend);
    }
    return ckpt_dir;
}

// Build the CheckpointConfig a submission would send for these flags.
//
// Every field is read whether or not a checkpoint dir was resolved, and
// that is the point. The CLI used to skip the whole block when the dir was
// empty, which made five of the config linter's checks unreachable through
// the CLI - including the one its own header offers as the motivating
// example, `--checkpoint-interval-ms=500` with no `--checkpoint-dir`. The
// flags were dropped here, so the contradiction never reached the linter and
// the job ran with no checkpoints and no complaint.
//
// Populating them changes no runtime behaviour: the coordinator's trigger
// loop already skips a job whose dir is empty. What it changes is that the
// contradiction is now visible to `check_config`, so a config asking for
// something it cannot get is refused instead of silently ignored.
inline cluster::CheckpointConfig build_checkpoint_config(int argc,
                                                         char** argv,
                                                         const std::string& ckpt_dir) {
    cluster::CheckpointConfig c;
    c.checkpoint_dir = ckpt_dir;
    c.interval_ms = std::stoll(get_arg(argc, argv, "checkpoint-interval-ms", "0"));
    c.restore_from_dir = get_arg(argc, argv, "restore-from-dir", "");
    c.restore_from_checkpoint_id = static_cast<std::uint64_t>(
        std::stoull(get_arg(argc, argv, "restore-from-checkpoint-id", "0")));
    // "auto" rather than "0", which is what this defaulted to and why the
    // documented recovery default never applied.
    //
    // CheckpointConfig::max_restarts_on_worker_loss uses kRestartAuto as an
    // UNSET sentinel that resolves to self-heal when checkpoint_dir is set
    // and fail-fast otherwise. Defaulting the flag to "0" wrote an EXPLICIT
    // zero into every submission, so the sentinel was unreachable through
    // the CLI and every CLI-submitted job failed fast on the first worker
    // loss - including jobs configured with checkpointing, whose whole
    // point is to survive one.
    //
    // Found by the config linter warning about a production profile with
    // fail-fast restarts, on a command line that never mentioned restarts.
    const auto max_restarts_str = get_arg(argc, argv, "max-restarts-on-worker-loss", "auto");
    c.max_restarts_on_worker_loss = max_restarts_str == "auto"
                                        ? cluster::kRestartAuto
                                        : static_cast<std::uint32_t>(std::stoul(max_restarts_str));
    // Record-capture flight recorder: arm the per-epoch .cap tee so the run
    // is replayable offline with `clink replay`. Pairs with a checkpoint dir
    // (epochs align with checkpoints).
    c.capture_dir = get_arg(argc, argv, "capture-dir", "");
    c.capture_records =
        static_cast<std::uint64_t>(std::stoull(get_arg(argc, argv, "capture-records", "0")));
    return c;
}

// Apply --profile (if given) and collect every problem the submission gate
// would collect: the profile's own promises plus the general checks.
//
// `ok` is false only for an unparseable profile name. Errors within the
// problems list are the caller's to act on, because submission refuses on
// them and `clink lint` reports them, and those are different exit paths.
struct ProfileLintResult {
    bool ok{true};
    bool profile_given{false};
    cluster::ConfigProfile profile{cluster::ConfigProfile::Development};
    std::vector<cluster::ConfigProblem> problems;
};

inline ProfileLintResult apply_profile_and_lint(int argc,
                                                char** argv,
                                                cluster::CheckpointConfig& c) {
    ProfileLintResult out;
    const auto profile_str = get_arg(argc, argv, "profile", "");
    if (!profile_str.empty()) {
        const auto profile = cluster::profile_from_string(profile_str);
        if (!profile.has_value()) {
            std::cerr << "unknown --profile=" << profile_str
                      << " (expected 'development' or 'production')\n";
            out.ok = false;
            return out;
        }
        out.profile_given = true;
        out.profile = *profile;
        // Applied after the explicit flags so it can only fill gaps, never
        // overwrite a choice.
        cluster::apply_profile(*profile,
                               c,
                               has_arg(argc, argv, "checkpoint-dir"),
                               has_arg(argc, argv, "checkpoint-interval-ms"));
        out.problems = cluster::lint_profile(*profile, c);
    }
    // The general config checks run either way: a profile does not exempt a
    // submission from being coherent, and a config with no profile still
    // needs linting.
    for (auto& p : cluster::lint_checkpoint_config(c)) {
        out.problems.push_back(std::move(p));
    }
    return out;
}

}  // namespace clink::cli
