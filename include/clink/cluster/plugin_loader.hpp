#pragma once

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

// Result of loading one plugin shared library. The handle owns the
// dlopen()'d module; destroying the handle dlclose()'s it. While a
// handle is alive, the plugin's registered factories remain in the
// process-wide TypeRegistry / RunnerRegistry / SelectorRegistry.
//
// v1: handles are tracked but never explicitly released - we keep
// plugins loaded for the TM's lifetime. dlclose() with registered
// std::function closures pointing back into plugin code is risky
// without quiescing all in-flight subtasks first; that
// machinery is deferred.
struct LoadedPlugin {
    std::string source_path;  // Original .so path on disk.
    std::string name;         // From clink_plugin_metadata().
    std::string version;
    std::string abi_fingerprint;  // From clink_plugin_abi_fingerprint() (the default gate).
    int abi_version{0};           // From clink_plugin_abi_version() (diagnostic; 0 = legacy).
    std::string abi_hash;         // From clink_plugin_abi_hash() (informational / strict gate).
    std::string target_triple;    // From clink_plugin_target_triple().
    void* dl_handle{nullptr};     // dlopen handle - opaque.
};

// Outcome of a load attempt. Holds the parsed LoadedPlugin on success
// or a structured error otherwise. The error message is human-readable
// and safe to surface to a client via JobCompletedMsg.errors.
struct PluginLoadResult {
    bool ok{false};
    LoadedPlugin plugin;
    std::string error;
};

// PluginLoader is the cluster-side machinery that loads a .so from a
// file path and bridges it into the process-wide registries.
//
// Lifecycle:
//   1. The JM (or test harness) writes plugin bytes to a path on disk.
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
    // `registry` rather than the default singletons. Used by the JM/TM
    // to scope a job's registrations to its JobBundle. Idempotency
    // applies per (path, registry-identity) pair - see notes on the
    // implementation.
    PluginLoadResult load_into(const std::string& so_path, plugin::PluginRegistry& registry);

    // Returns the LoadedPlugin matching this path, if loaded. nullptr
    // otherwise. Used by tests and diagnostics.
    const LoadedPlugin* find(const std::string& so_path) const;

    // Singleton. Process-wide because the registries it writes to are
    // also process-wide.
    static PluginLoader& default_instance();

private:
    mutable std::mutex mu_;
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

// True when strict plugin-ABI matching is requested (CLINK_STRICT_PLUGIN_ABI=1
// in the environment). In strict mode the loader falls back to the historic
// exact commit-hash gate instead of the fingerprint gate.
bool strict_plugin_abi_enabled() noexcept;

// Pure decision for whether a plugin is ABI-compatible with the cluster,
// factored out of PluginLoader::load so it can be unit-tested without a real
// .so. Returns an empty string when compatible, otherwise a human-readable
// rejection reason. The target-triple gate is applied separately by the loader.
//
// Default: compare structural fingerprints. Strict mode, or a legacy plugin
// that predates the fingerprint symbol, falls back to the exact commit-hash.
struct AbiCheckInput {
    bool strict{false};                  // strict mode requested (exact-hash gate)
    bool plugin_has_fingerprint{false};  // plugin exports clink_plugin_abi_fingerprint
    std::string plugin_fingerprint;
    std::string cluster_fingerprint;
    std::string plugin_hash;
    std::string cluster_hash;
};
std::string check_plugin_abi(const AbiCheckInput& in);

}  // namespace clink::cluster
