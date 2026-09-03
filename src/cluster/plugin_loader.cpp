#include "clink/cluster/plugin_loader.hpp"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"  // SelectorRegistry
#include "clink/cluster/runner_helpers.hpp"     // SideOutputAttacherRegistry
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/plugin/abi_surface.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::cluster {

namespace {

// Resolve a required symbol from the dlopen handle. Returns nullptr if
// missing; caller is responsible for the error path.
template <typename Fn>
Fn dlsym_as(void* handle, const char* sym) {
    // dlsym returns a void*; per POSIX, casting void* to a function
    // pointer is conditionally supported, but works on every platform
    // we care about (Linux x86_64/arm64, Darwin arm64).
    void* p = ::dlsym(handle, sym);
    if (p == nullptr) {
        return nullptr;
    }
    Fn fn;
    static_assert(sizeof(fn) == sizeof(p), "function pointer size differs from void* size");
    std::memcpy(&fn, &p, sizeof(fn));
    return fn;
}

std::string current_dlerror() {
    const char* msg = ::dlerror();
    return msg == nullptr ? std::string{"(no dl error)"} : std::string{msg};
}

}  // namespace

const char* cluster_abi_fingerprint() noexcept {
    return ::clink::plugin::kAbiFingerprint;
}

int cluster_abi_version() noexcept {
    return ::clink::plugin::kAbiVersion;
}

const char* cluster_abi_hash() noexcept {
    return ::clink::plugin::kAbiHash;
}

const char* cluster_target_triple() noexcept {
    return CLINK_PLUGIN_TARGET_TRIPLE;
}

const char* cluster_toolchain() noexcept {
    return ::clink::plugin::kToolchain;
}

const char* cluster_abi_manifest() noexcept {
    return ::clink::plugin::kAbiSurfaceManifest;
}

bool strict_plugin_abi_enabled() noexcept {
    const char* v = std::getenv("CLINK_STRICT_PLUGIN_ABI");
    return v != nullptr && std::strcmp(v, "1") == 0;
}

namespace {

// Parse "path=sha256" lines into path -> hash. Malformed lines are skipped:
// the manifest is diagnostic material, never a gate input by itself.
std::map<std::string, std::string> parse_manifest(const std::string& manifest) {
    std::map<std::string, std::string> out;
    std::size_t pos = 0;
    while (pos < manifest.size()) {
        auto eol = manifest.find('\n', pos);
        if (eol == std::string::npos) {
            eol = manifest.size();
        }
        const std::string_view line{manifest.data() + pos, eol - pos};
        pos = eol + 1;
        const auto eq = line.find('=');
        if (eq == std::string_view::npos || eq == 0) {
            continue;
        }
        out.emplace(std::string{line.substr(0, eq)}, std::string{line.substr(eq + 1)});
    }
    return out;
}

}  // namespace

std::string summarise_manifest_diff(const std::string& plugin_manifest,
                                    const std::string& cluster_manifest,
                                    std::size_t max_names) {
    if (plugin_manifest.empty() || cluster_manifest.empty() ||
        plugin_manifest == cluster_manifest) {
        return {};
    }
    const auto plugin = parse_manifest(plugin_manifest);
    const auto cluster = parse_manifest(cluster_manifest);
    std::string out;
    std::size_t named = 0;
    std::size_t total = 0;
    auto add = [&](const std::string& path, const char* what) {
        ++total;
        if (named >= max_names) {
            return;
        }
        if (!out.empty()) {
            out += ", ";
        }
        out += path + " (" + what + ")";
        ++named;
    };
    // std::map keeps both sides sorted, so the naming order is deterministic.
    for (const auto& [path, hash] : plugin) {
        auto it = cluster.find(path);
        if (it == cluster.end()) {
            add(path, "only in plugin");
        } else if (it->second != hash) {
            add(path, "changed");
        }
    }
    for (const auto& [path, hash] : cluster) {
        if (!plugin.contains(path)) {
            add(path, "only in cluster");
        }
    }
    if (total == 0) {
        return {};
    }
    std::string head = "differing surface headers: " + out;
    if (total > named) {
        head += " and " + std::to_string(total - named) + " more";
    }
    return head;
}

AbiCheckResult check_plugin_abi(const AbiCheckInput& in) {
    AbiCheckResult r;
    // Gate 1 - the ABI surface. Strict mode, or a legacy plugin that predates
    // the fingerprint symbol: fall back to the historic exact commit-hash gate.
    if (in.strict || !in.plugin_has_fingerprint) {
        if (in.plugin_hash != in.cluster_hash) {
            const char* why =
                in.strict ? "strict mode" : "legacy plugin (no ABI-fingerprint symbol)";
            r.error = std::string{"plugin ABI hash mismatch ("} + why + "): plugin reports '" +
                      (in.plugin_hash.empty() ? "(none)" : in.plugin_hash) +
                      "', cluster expects '" + in.cluster_hash + "'";
            r.failure = PluginLoadFailure::AbiMismatch;
            return r;
        }
    } else if (in.plugin_fingerprint != in.cluster_fingerprint) {
        // Default gate: compatible iff the structural ABI fingerprints match.
        // The fingerprint hashes the declared extension surface + the ABI
        // options that surface uses + the pinned Arrow version + the manual
        // ABI version, so a difference means the plugin was built against an
        // incompatible surface and must be rebuilt against this release.
        r.error = std::string{"plugin ABI fingerprint mismatch: plugin '"} +
                  (in.plugin_fingerprint.empty() ? "(none)" : in.plugin_fingerprint) +
                  "', cluster '" + in.cluster_fingerprint +
                  "' (the declared extension surface differs; rebuild the plugin against "
                  "this release). plugin commit '" +
                  (in.plugin_hash.empty() ? "(none)" : in.plugin_hash) + "', cluster commit '" +
                  in.cluster_hash + "'";
        if (!in.plugin_manifest.empty() && !in.cluster_manifest.empty()) {
            const auto diff = summarise_manifest_diff(in.plugin_manifest, in.cluster_manifest, 5);
            if (!diff.empty()) {
                r.error += ". " + diff;
            } else {
                r.error +=
                    ". The declared surface headers are identical; the difference is "
                    "in the ABI-relevant build options, the pinned Arrow version or "
                    "CLINK_ABI_VERSION";
            }
        }
        r.failure = PluginLoadFailure::AbiMismatch;
        return r;
    }
    // Gate 2 - target triple, unconditional: a genuine binary-compat axis
    // independent of the header surface (the same headers hash to the same
    // fingerprint on every platform).
    if (in.plugin_triple != in.cluster_triple) {
        r.error = std::string{"plugin target-triple mismatch: plugin reports '"} +
                  (in.plugin_triple.empty() ? "(none)" : in.plugin_triple) +
                  "', cluster expects '" + in.cluster_triple + "'";
        r.failure = PluginLoadFailure::TripleMismatch;
        return r;
    }
    // Gate 3 - toolchain identity (stdlib, dual-ABI choice, sanitizer
    // instrumentation), checked only when the plugin exports the symbol. A
    // plugin without it predates fingerprint v2 and was already refused by
    // gate 1, so skipping here waves nothing through.
    if (in.plugin_has_toolchain && in.plugin_toolchain != in.cluster_toolchain) {
        r.error = std::string{"plugin toolchain mismatch: plugin built with '"} +
                  in.plugin_toolchain + "', cluster built with '" + in.cluster_toolchain +
                  "' (the standard library, dual-ABI choice or sanitizer instrumentation "
                  "differs; rebuild the plugin with the cluster's toolchain)";
        r.failure = PluginLoadFailure::ToolchainMismatch;
        return r;
    }
    return r;
}

PluginLoadResult PluginLoader::load(const std::string& so_path) {
    {
        // Idempotency: load() callers (the legacy default-singleton
        // path) get the cached handle without re-running the register
        // hook for the same path. Per-job callers go through load_into
        // and bypass this cache (see below) since each bundle needs
        // its own register_fn invocation.
        std::lock_guard lock(mu_);
        if (auto it = loaded_.find(so_path); it != loaded_.end()) {
            PluginLoadResult r;
            r.ok = true;
            r.plugin = it->second;
            return r;
        }
    }
    auto& tr = TypeRegistry::default_instance();
    auto& rr = RunnerRegistry::default_instance();
    auto& sr = SelectorRegistry::default_instance();
    auto& ker = KeyExtractorRegistry::default_instance();
    auto& soar = SideOutputAttacherRegistry::default_instance();
    ::clink::plugin::PluginRegistry preg(tr, rr, sr, ker, soar);
    auto result = load_into(so_path, preg);
    if (result.ok) {
        std::lock_guard lock(mu_);
        loaded_[so_path] = result.plugin;
    }
    return result;
}

PluginLoadResult PluginLoader::load_into(const std::string& so_path,
                                         ::clink::plugin::PluginRegistry& registry) {
    PluginLoadResult result;

    // Make sure built-ins are present before we touch the registries.
    // The plugin's typed registrations may reference channel types
    // (e.g. "int64") that the built-ins own.
    ensure_built_ins_registered();

    using AbiFingerprintFn = const char* (*)();
    using AbiVersionFn = int (*)();
    using AbiHashFn = const char* (*)();
    using TripleFn = const char* (*)();
    using ToolchainFn = const char* (*)();
    using ManifestFn = const char* (*)();
    using MetadataFn = const ::clink::plugin::PluginMetadata* (*)();
    using RegisterFn = int (*)(void*, char*, std::size_t);

    // Loading and registration are serialised together. Besides protecting the
    // cache, this is the only generally safe contract for arbitrary plugin
    // registration functions, which may mutate their own module statics.
    std::lock_guard lock(mu_);

    LoadedPlugin lp;
    if (auto it = modules_.find(so_path); it != modules_.end()) {
        lp = it->second;
    } else {
        // RTLD_LOCAL keeps plugin symbols out of the global namespace;
        // RTLD_NOW catches unresolved dependencies before registration.
        void* handle = ::dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            result.error = "dlopen failed: " + current_dlerror();
            result.failure = PluginLoadFailure::Dlopen;
            return result;
        }
        // Before registration there can be no escaping plugin closure, so an
        // incompatible or malformed module is still safe to close.
        auto reject_unregistered = [&](std::string msg, PluginLoadFailure failure) {
            ::dlclose(handle);
            result.error = std::move(msg);
            result.failure = failure;
            return result;
        };

        // Fingerprint, version, toolchain and manifest symbols are OPTIONAL: a
        // plugin built before the fingerprint gate falls back to the exact-hash
        // comparison, and one that predates the toolchain/manifest symbols is
        // refused by the fingerprint that introduced them.
        auto abi_fp_fn = dlsym_as<AbiFingerprintFn>(handle, "clink_plugin_abi_fingerprint");
        auto abi_version_fn = dlsym_as<AbiVersionFn>(handle, "clink_plugin_abi_version");
        auto toolchain_fn = dlsym_as<ToolchainFn>(handle, "clink_plugin_toolchain");
        auto manifest_fn = dlsym_as<ManifestFn>(handle, "clink_plugin_abi_manifest");
        auto abi_hash_fn = dlsym_as<AbiHashFn>(handle, "clink_plugin_abi_hash");
        if (abi_hash_fn == nullptr) {
            return reject_unregistered("plugin missing clink_plugin_abi_hash symbol",
                                       PluginLoadFailure::MissingSymbol);
        }
        auto triple_fn = dlsym_as<TripleFn>(handle, "clink_plugin_target_triple");
        if (triple_fn == nullptr) {
            return reject_unregistered("plugin missing clink_plugin_target_triple symbol",
                                       PluginLoadFailure::MissingSymbol);
        }
        auto metadata_fn = dlsym_as<MetadataFn>(handle, "clink_plugin_metadata");
        if (metadata_fn == nullptr) {
            return reject_unregistered("plugin missing clink_plugin_metadata symbol",
                                       PluginLoadFailure::MissingSymbol);
        }
        if (dlsym_as<RegisterFn>(handle, "clink_plugin_register") == nullptr) {
            return reject_unregistered("plugin missing clink_plugin_register symbol",
                                       PluginLoadFailure::MissingSymbol);
        }

        const char* plugin_abi = abi_hash_fn();
        const char* plugin_fp = abi_fp_fn != nullptr ? abi_fp_fn() : nullptr;
        const char* plugin_triple = triple_fn();
        const char* plugin_toolchain = toolchain_fn != nullptr ? toolchain_fn() : nullptr;
        const char* plugin_manifest = manifest_fn != nullptr ? manifest_fn() : nullptr;
        AbiCheckInput abi_in;
        abi_in.strict = strict_plugin_abi_enabled();
        abi_in.plugin_has_fingerprint = abi_fp_fn != nullptr;
        abi_in.plugin_fingerprint = plugin_fp != nullptr ? plugin_fp : "";
        abi_in.cluster_fingerprint = cluster_abi_fingerprint();
        abi_in.plugin_hash = plugin_abi != nullptr ? plugin_abi : "";
        abi_in.cluster_hash = cluster_abi_hash();
        abi_in.plugin_triple = plugin_triple != nullptr ? plugin_triple : "";
        abi_in.cluster_triple = cluster_target_triple();
        abi_in.plugin_has_toolchain = plugin_toolchain != nullptr;
        abi_in.plugin_toolchain = plugin_toolchain != nullptr ? plugin_toolchain : "";
        abi_in.cluster_toolchain = cluster_toolchain();
        abi_in.plugin_manifest = plugin_manifest != nullptr ? plugin_manifest : "";
        abi_in.cluster_manifest = cluster_abi_manifest();
        if (auto check = check_plugin_abi(abi_in); !check.error.empty()) {
            return reject_unregistered(std::move(check.error), check.failure);
        }

        const auto* meta = metadata_fn();
        if (meta == nullptr) {
            return reject_unregistered("plugin returned null metadata",
                                       PluginLoadFailure::NullMetadata);
        }

        lp.source_path = so_path;
        lp.name = meta->name != nullptr ? meta->name : "";
        lp.version = meta->version != nullptr ? meta->version : "";
        lp.abi_fingerprint = plugin_fp != nullptr ? plugin_fp : "";
        lp.abi_version = abi_version_fn != nullptr ? abi_version_fn() : 0;
        lp.abi_hash = plugin_abi != nullptr ? plugin_abi : "";
        lp.target_triple = plugin_triple != nullptr ? plugin_triple : "";
        lp.toolchain = plugin_toolchain != nullptr ? plugin_toolchain : "";
        // Intentionally never dlclose an admitted C++ module. The OS releases
        // it at process exit, and modules_ bounds the cost to one mapping per
        // source path rather than one per job/session.
        lp.module = std::shared_ptr<void>(handle, [](void*) {});
        lp.dl_handle = handle;
        modules_.emplace(so_path, lp);
    }

    auto register_fn = dlsym_as<RegisterFn>(lp.dl_handle, "clink_plugin_register");
    std::array<char, 1024> err_buf{};
    const int rc = register_fn(&registry, err_buf.data(), err_buf.size());
    if (rc != 0) {
        // Registration can fail after having installed some closures. Keep the
        // module mapped so destroying that partial registry remains safe.
        result.error = std::string{"plugin register hook failed (rc="} + std::to_string(rc) +
                       "): " + std::string{err_buf.data()};
        result.failure = PluginLoadFailure::RegisterFailed;
        return result;
    }

    result.ok = true;
    result.plugin = lp;
    return result;
}

const LoadedPlugin* PluginLoader::find(const std::string& so_path) const {
    std::lock_guard lock(mu_);
    auto it = loaded_.find(so_path);
    return it == loaded_.end() ? nullptr : &it->second;
}

PluginLoader& PluginLoader::default_instance() {
    static PluginLoader l;
    return l;
}

}  // namespace clink::cluster
