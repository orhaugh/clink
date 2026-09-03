#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace clink::plugin {
class PluginRegistry;
}

namespace clink::cluster {

// Result of loading one plugin shared library. `module` aliases the loader's
// process-lifetime mapping. Successful C++ plugins are deliberately not
// dlclose()d: factories, type-erased closures, TLS and module statics can all
// outlive the registry call that created them, and POSIX provides no reliable
// proof that every such reference has gone away. PluginLoader keeps one
// mapping per source path, so this safety rule is bounded rather than one
// mapping per job or worker control session.
struct LoadedPlugin {
    std::string source_path;  // Original .so path on disk.
    std::string name;         // From clink_plugin_metadata().
    std::string version;
    std::string abi_fingerprint;  // From clink_plugin_abi_fingerprint() (the default gate).
    int abi_version{0};           // From clink_plugin_abi_version() (diagnostic; 0 = legacy).
    std::string abi_hash;         // From clink_plugin_abi_hash() (informational / strict gate).
    std::string target_triple;    // From clink_plugin_target_triple().
    std::string toolchain;        // From clink_plugin_toolchain() ("" = legacy plugin).
    std::shared_ptr<void> module;
    void* dl_handle{nullptr};  // Non-owning alias of module.get(), for dlsym callers.
};

// Structured classification of a load failure. The deterministic gate
// refusals (kAbiMismatch / kTripleMismatch / kToolchainMismatch) can never
// succeed on retry with the same binaries, so callers may treat them as
// terminal for the artifact (the worker marks them fatal on deploy); the
// remaining kinds keep ordinary retry semantics - a dlopen error can be
// environmental and a register failure is the job's own logic.
enum class PluginLoadFailure : std::uint8_t {
    None,
    Dlopen,
    MissingSymbol,
    AbiMismatch,        // fingerprint (default) or exact-hash (strict/legacy) gate
    TripleMismatch,     // built for a different platform
    ToolchainMismatch,  // stdlib / sanitizer identity differs
    NullMetadata,
    RegisterFailed,
};

// True for the deterministic gate refusals that cannot succeed on a retry
// with the same binaries (ABI surface / target triple / toolchain). Deploy
// paths mark these fatal so the job fails with the cause instead of consuming
// restart budget on redeploy-and-refuse loops.
[[nodiscard]] constexpr bool is_plugin_gate_refusal(PluginLoadFailure f) noexcept {
    return f == PluginLoadFailure::AbiMismatch || f == PluginLoadFailure::TripleMismatch ||
           f == PluginLoadFailure::ToolchainMismatch;
}

// Outcome of a load attempt. Holds the parsed LoadedPlugin on success
// or a structured error otherwise. The error message is human-readable
// and safe to surface to a client via JobCompletedMsg.errors.
struct PluginLoadResult {
    bool ok{false};
    LoadedPlugin plugin;
    std::string error;
    PluginLoadFailure failure{PluginLoadFailure::None};
};

// PluginLoader is the cluster-side machinery that loads a .so from a
// file path and bridges it into the process-wide registries.
//
// Lifecycle:
//   1. The coordinator (or test harness) writes plugin bytes to a path on disk.
//   2. PluginLoader::load(path) dlopen()'s the file with RTLD_LOCAL,
//      reads the four extern "C" handshake symbols, verifies the ABI
//      hash and target triple against the cluster's own values, then
//      calls clink_plugin_register() to populate the registries.
//   3. A LoadedPlugin handle is recorded by source_path so repeated
//      load() calls for the same path are idempotent.
//
// Thread-safe. Plugin loads are serialised behind the loader's mutex
// because dlopen / global registry mutation aren't safe to run
// concurrently for the same module.
class PluginLoader {
public:
    // Load a plugin from disk. Returns ok=false on any failure
    // (dlopen, missing symbol, ABI/target mismatch, registration
    // throw). Idempotent on success: a second load() for the same
    // path returns the cached handle without re-running the register
    // hook.
    //
    // This overload writes into the *default-instance* registries
    // (the original behaviour). Use load_into(path, registry) to
    // direct registrations at a per-job bundle's PluginRegistry view.
    PluginLoadResult load(const std::string& so_path);

    // Load a plugin from disk, directing its register-hook output at
    // `registry` rather than the default singletons. Used by the coordinator/worker
    // to scope a job's registrations to its JobBundle. Idempotency
    // applies per (path, registry-identity) pair - see notes on the
    // implementation.
    // The module is mapped once per path, but its register hook runs on every
    // call so each job bundle receives an independent set of registrations.
    // Per-job callers normally retain the result for provenance even though
    // the loader itself owns the process-lifetime mapping.
    PluginLoadResult load_into(const std::string& so_path, plugin::PluginRegistry& registry);

    // Returns the LoadedPlugin matching this path, if loaded. nullptr
    // otherwise. Used by tests and diagnostics.
    const LoadedPlugin* find(const std::string& so_path) const;

    // Singleton. Process-wide because the registries it writes to are
    // also process-wide.
    static PluginLoader& default_instance();

private:
    mutable std::mutex mu_;
    // All successfully admitted module mappings. Their shared_ptr deleter is
    // intentionally a no-op; the operating system reclaims them at process
    // exit. This avoids unsafe C++ plugin unloading while bounding mappings to
    // one per cache path/content hash.
    std::unordered_map<std::string, LoadedPlugin> modules_;
    std::unordered_map<std::string, LoadedPlugin> loaded_;
};

// The cluster's structural ABI fingerprint (baked in at build time via
// abi_version.hpp). The DEFAULT gate: a plugin loads when its fingerprint equals
// this. Rotates on a real ABI/behaviour change, not on .cpp/test/doc commits.
const char* cluster_abi_fingerprint() noexcept;

// The cluster's manual ABI-break version (folded into the fingerprint). Exported
// for diagnostics.
int cluster_abi_version() noexcept;

// The cluster's own ABI hash (git commit at build time). Informational: used as
// the gate only under strict mode (CLINK_STRICT_PLUGIN_ABI=1).
const char* cluster_abi_hash() noexcept;

// The cluster's target triple (linux-x86_64 / linux-arm64 /
// darwin-arm64). Plugins built for a different triple are rejected at
// load time. The macro is defined in plugin.hpp; this function exposes
// it as a runtime string for diagnostics.
const char* cluster_target_triple() noexcept;

// The cluster's toolchain identity (stdlib, dual-ABI choice, sanitizer
// instrumentation - see kToolchain in the generated abi_version.hpp). A plugin
// exporting a different value is refused; a plugin without the symbol predates
// the fingerprint that introduced it and is refused by the fingerprint gate.
const char* cluster_toolchain() noexcept;

// The cluster's per-header surface manifest ("path=sha256" lines, from the
// generated abi_surface.hpp). Used to name the differing headers when a
// plugin's fingerprint does not match.
const char* cluster_abi_manifest() noexcept;

// True when strict plugin-ABI matching is requested (CLINK_STRICT_PLUGIN_ABI=1
// in the environment). In strict mode the loader falls back to the historic
// exact commit-hash gate instead of the fingerprint gate.
bool strict_plugin_abi_enabled() noexcept;

// Pure decision for whether a plugin is ABI-compatible with the cluster,
// factored out of PluginLoader::load so it can be unit-tested without a real
// .so. Returns an empty error when compatible, otherwise a human-readable
// rejection reason plus its PluginLoadFailure classification.
//
// Order of gates:
//   1. Default: compare structural fingerprints. Strict mode, or a legacy
//      plugin that predates the fingerprint symbol, falls back to the exact
//      commit-hash. On a fingerprint mismatch with both manifests supplied,
//      the error names the differing headers (or blames the build options
//      when the manifests are identical).
//   2. Target triple: unconditional equality.
//   3. Toolchain identity: equality iff the plugin exports the symbol
//      (plugin_has_toolchain); a legacy plugin skips this gate because the
//      fingerprint that introduced the symbol already refuses it.
struct AbiCheckInput {
    bool strict{false};                  // strict mode requested (exact-hash gate)
    bool plugin_has_fingerprint{false};  // plugin exports clink_plugin_abi_fingerprint
    std::string plugin_fingerprint;
    std::string cluster_fingerprint;
    std::string plugin_hash;
    std::string cluster_hash;
    std::string plugin_triple;  // "" (default) compares equal for legacy unit tests
    std::string cluster_triple;
    bool plugin_has_toolchain{false};  // plugin exports clink_plugin_toolchain
    std::string plugin_toolchain;
    std::string cluster_toolchain;
    std::string plugin_manifest;   // "path=sha256" lines; "" = symbol absent
    std::string cluster_manifest;  // "" = do not attempt to name the diff
};
struct AbiCheckResult {
    std::string error;  // empty = compatible
    PluginLoadFailure failure{PluginLoadFailure::None};
};
AbiCheckResult check_plugin_abi(const AbiCheckInput& in);

// Names the difference between two "path=sha256" manifests: headers whose
// hash changed, headers only one side has, capped at max_names entries with
// a "+N more" tail. Returns "" when the manifests are equal or either is
// empty. Pure; exposed for unit tests.
std::string summarise_manifest_diff(const std::string& plugin_manifest,
                                    const std::string& cluster_manifest,
                                    std::size_t max_names);

}  // namespace clink::cluster
