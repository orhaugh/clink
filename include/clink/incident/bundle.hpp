#pragma once

// Incident evidence bundles (design record 014).
//
// A bundle is a directory holding the evidence one incident needs - the
// job's checkpoint markers, its participants' snapshot sidecars and, by
// choice, their snapshots, the flight recorder's epochs, a run of protocol
// traces and the coordinator's job manifest - indexed by a versioned
// manifest that lists every file's relative path, role, size and SHA-256,
// and every piece of evidence that is absent and why.
//
// collect() gathers one from a job's local directories and publishes it by
// renaming a completed temporary directory into place. read_bundle() opens
// one as untrusted input, within bounds. verify_core() runs the checks that
// need no SQL runtime; the replay checks live with the SQL frontend
// (tools/clink_incident_replay.cpp). Every check reports passed, failed,
// unavailable or unsupported, and passed means only what the check states.
//
// The checkpoint and capture subtrees keep the engine's own layout, so the
// production functions that resolve a restore (resolve_restore_source,
// state_dir_for, restore_point_on_disk) and a replay (EpochReplay) run on a
// bundle unchanged: no check re-implements the recovery protocol.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::incident {

using JobId = std::uint64_t;

inline constexpr const char* kFormat = "clink-incident-bundle";
inline constexpr std::int64_t kVersion = 1;

// ---- verdicts and reports ---------------------------------------------------

enum class Verdict : std::uint8_t { Passed, Failed, Unavailable, Unsupported };

[[nodiscard]] const char* to_string(Verdict v) noexcept;

struct Check {
    std::string id;  // stable: "bundle.artifacts", "checkpoint.participants", ...
    Verdict verdict{Verdict::Unavailable};
    std::vector<std::string> evidence;  // bundle-relative paths, or what was read
    std::string reason;
    std::string next_action;           // empty when there is nothing to do
    std::vector<std::string> details;  // supporting facts, one per line
};

// The checks a profile requires: integrity, consistency, replay or full.
// Throws std::invalid_argument for a profile name it does not know.
[[nodiscard]] std::vector<std::string> required_checks(const std::string& profile,
                                                       bool expected_output_given);

struct Report {
    std::string bundle;
    std::string profile;
    std::vector<std::string> required;
    std::vector<Check> checks;

    // 0: every required check passed. 1: a check failed. 3: none failed, but a
    // required check is unavailable or unsupported, or did not run.
    [[nodiscard]] int exit_code() const;
    [[nodiscard]] std::string render_text() const;
    [[nodiscard]] std::string render_json() const;
};

// ---- the bundle as read -----------------------------------------------------

struct Artifact {
    std::string path;  // relative to the bundle root
    std::string role;
    std::uint64_t bytes{0};
    std::string sha256;
};

// The reader's bounds. A bundle is untrusted input.
struct ReadLimits {
    std::uint64_t max_manifest_bytes{16ULL << 20};
    std::uint64_t max_files{100000};
    std::uint64_t max_file_bytes{8ULL << 30};
    std::uint64_t max_total_bytes{64ULL << 30};
    std::size_t max_json_depth{64};
};

struct Bundle {
    std::filesystem::path root;  // absolute
    config::JsonValue manifest;
    std::vector<Artifact> artifacts;

    [[nodiscard]] JobId job() const;
    [[nodiscard]] std::uint64_t checkpoint() const;
    [[nodiscard]] bool contains(const std::string& role) const;  // the manifest's "contents"
    [[nodiscard]] const Artifact* find(const std::string& path) const;
    [[nodiscard]] std::filesystem::path checkpoint_dir() const { return root / "checkpoint"; }
    [[nodiscard]] std::filesystem::path capture_dir() const { return root / "capture"; }
    [[nodiscard]] std::filesystem::path trace_dir() const { return root / "traces"; }
};

// Thrown when a directory is not a bundle this reader may read: no
// manifest, an unknown format or version, a manifest past its bounds, or an
// artifact path that is absolute, climbs out of the bundle or is not a
// plain relative path. The message names the problem.
struct BundleRefused : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Open a bundle. Checks the manifest's own form and every artifact path's
// form; does not read or hash the artifacts (verify_core does).
[[nodiscard]] Bundle read_bundle(const std::filesystem::path& root, const ReadLimits& limits = {});

// Whether `path` is a relative path of plain components: not empty, not
// absolute, no "." or ".." component, no backslash, no NUL.
[[nodiscard]] bool safe_relative_path(const std::string& path) noexcept;

// ---- verification -----------------------------------------------------------

struct VerifyOptions {
    std::string profile{"consistency"};
    // The repository's formal/ directory (and scripts/formal-check.sh beside
    // it) for protocol.conformance; empty: CLINK_FORMAL_DIR, else unavailable.
    std::string formal_dir;
    // The TLA+ jar cache; empty: CLINK_FORMAL_TOOLS_DIR, else
    // ${CLINK_DEPS_PREFIX:-~/.clink-deps}/formal-tools. Never downloaded.
    std::string formal_tools_dir;
    ReadLimits limits;
};

// The checks that need no SQL runtime: bundle.*, checkpoint.*, state.decode,
// protocol.*. None contacts a service or writes outside the system's
// temporary directory.
[[nodiscard]] std::vector<Check> verify_core(const Bundle& bundle, const VerifyOptions& options);

// The check for a bundle that could not be read at all.
[[nodiscard]] Check refused_check(const std::string& reason);

// The checkpoint whose state a capture epoch's events were processed from,
// which a replay of the epoch restores: the checkpoint before it, except for
// the first epoch of a restored run, which starts where that run restored. A
// job's RESTORED record names the newest such run (it numbers from `next` and
// restored from `restore`); an older one's first epoch is not known. 0 is
// fresh state.
[[nodiscard]] std::uint64_t epoch_starting_checkpoint(
    std::uint64_t epoch,
    std::optional<std::uint64_t> restored_from,
    std::optional<std::uint64_t> restored_next) noexcept;

// ---- collection -------------------------------------------------------------

struct CollectOptions {
    std::string checkpoint_dir;  // the job's checkpoint directory (local filesystem)
    JobId job{0};
    std::string ha_dir;       // optional: the coordinator's HA directory
    std::string capture_dir;  // optional: the flight recorder's root
    std::string trace_dir;    // optional: one run directory of protocol traces
    // Which restore rule applies: nullopt reads requires_commit_confirmation
    // from the job manifest, and refuses to guess without one.
    std::optional<bool> commit_confirmation;
    std::optional<std::uint64_t> checkpoint;                        // override the engine's choice
    std::vector<std::uint64_t> ops;                                 // captures to keep; empty: all
    std::optional<std::pair<std::uint64_t, std::uint64_t>> epochs;  // inclusive
    bool include_state{false};
    bool include_captures{false};
    std::uint64_t max_bytes{1ULL << 30};
    std::uint64_t max_files{10000};
    std::chrono::seconds timeout{600};
    std::string out;  // the bundle directory to publish; must not exist

    // For tests: runs after each source file is copied and before it is
    // checked for change, so a test can change or delete it in between.
    std::function<void(const std::filesystem::path& source)> after_copy;
};

struct CollectResult {
    std::filesystem::path bundle;
    std::uint64_t checkpoint{0};
    std::uint64_t files{0};
    std::uint64_t bytes{0};
    double seconds{0};
    std::vector<std::string> notes;  // exclusions and anything else to say
};

// Thrown when collection cannot publish a bundle that tells the truth: the
// evidence changed or vanished while it was copied, a required piece is past
// a bound, the time ran out, or the inputs do not say which restore rule
// applies. Nothing is left behind at `out`.
struct CollectError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

[[nodiscard]] CollectResult collect(const CollectOptions& options);

// Graph parameters whose names suggest a credential, replaced in the
// collected job manifest.
[[nodiscard]] bool looks_like_credential(const std::string& param_name) noexcept;

// SHA-256 of a file's bytes, hex; reads in bounded blocks.
[[nodiscard]] std::string sha256_file(const std::filesystem::path& path);

}  // namespace clink::incident
