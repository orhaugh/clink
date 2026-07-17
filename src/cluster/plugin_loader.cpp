#include "clink/cluster/plugin_loader.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"  // SelectorRegistry
#include "clink/cluster/runner_helpers.hpp"     // SideOutputAttacherRegistry
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
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

bool strict_plugin_abi_enabled() noexcept {
    const char* v = std::getenv("CLINK_STRICT_PLUGIN_ABI");
    return v != nullptr && std::strcmp(v, "1") == 0;
}

std::string check_plugin_abi(const AbiCheckInput& in) {
    // Strict mode, or a legacy plugin that predates the fingerprint symbol:
    // fall back to the historic exact commit-hash gate.
    if (in.strict || !in.plugin_has_fingerprint) {
        if (in.plugin_hash != in.cluster_hash) {
            const char* why =
                in.strict ? "strict mode" : "legacy plugin (no ABI-fingerprint symbol)";
            return std::string{"plugin ABI hash mismatch ("} + why + "): plugin reports '" +
                   (in.plugin_hash.empty() ? "(none)" : in.plugin_hash) + "', cluster expects '" +
                   in.cluster_hash + "'";
        }
        return {};
    }
    // Default gate: compatible iff the structural ABI fingerprints match. The
    // fingerprint hashes the public headers + ABI options + manual ABI version,
    // so a difference means the plugin was built against an incompatible clink
    // ABI surface and must be rebuilt against this release.
    if (in.plugin_fingerprint != in.cluster_fingerprint) {
        return std::string{"plugin ABI fingerprint mismatch: plugin '"} +
               (in.plugin_fingerprint.empty() ? "(none)" : in.plugin_fingerprint) + "', cluster '" +
               in.cluster_fingerprint +
               "' (the clink ABI surface differs; rebuild the plugin against this release). "
               "plugin commit '" +
               (in.plugin_hash.empty() ? "(none)" : in.plugin_hash) + "', cluster commit '" +
               in.cluster_hash + "'";
    }
    return {};
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
    // No cache lookup here: each call dlopens fresh and runs
    // register_fn against `registry`. Callers that want path-level
    // idempotency against default-singletons should use load(); per-job
    // callers WANT a fresh register_fn run against THEIR bundle.

    // Make sure built-ins are present before we touch the registries.
    // The plugin's typed registrations may reference channel types
    // (e.g. "int64") that the built-ins own.
    ensure_built_ins_registered();

    // Each load_into MUST get a fresh module instance so the .so's
    // per-instance, call_once-gated registration (CLINK_REGISTER_JOB's
    // build_fn) re-runs into THIS caller's registry. dlopen() refcounts by
    // inode: opening the same path twice returns the same instance whose
    // call_once has already fired, so a second caller's registry (e.g. a
    // long-lived Coordinator planning a second job, or resubmitting the same
    // .so on a savepoint-driven upgrade) would get NO factories and plan_job
    // rejects with "no source factory registered". We sidestep that by
    // dlopening a unique per-load copy: a distinct inode is a distinct
    // instance with fresh static state - a fresh once_flag AND a fresh
    // inline-name counter, so build_fn re-mints the same deterministic op
    // names the submitter's graph_json references. The copy is unlinked
    // immediately after dlopen (the mapping stays valid); handles live for
    // the process lifetime (see LoadedPlugin), so the on-disk file is not
    // needed past the open.
    std::string instance_path;
    {
        static std::atomic<std::uint64_t> load_counter{0};
        const auto n = load_counter.fetch_add(1, std::memory_order_relaxed);
        std::filesystem::path unique{so_path};
        unique += ".inst." + std::to_string(static_cast<long>(::getpid())) + "." +
                  std::to_string(n) + ".so";
        std::error_code ec;
        std::filesystem::copy_file(
            so_path, unique, std::filesystem::copy_options::overwrite_existing, ec);
        if (ec) {
            result.error = "plugin instance copy failed: " + ec.message();
            return result;
        }
        instance_path = unique.string();
    }

    // RTLD_LOCAL keeps the plugin's symbols out of the global
    // namespace; RTLD_NOW resolves all relocations up front so we
    // catch missing dependencies at load rather than first call.
    void* handle = ::dlopen(instance_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    // Drop the on-disk copy now: the loaded mapping remains valid, and this
    // stops the plugin cache dir from accumulating one instance file per load.
    {
        std::error_code ec;
        std::filesystem::remove(instance_path, ec);
    }
    if (handle == nullptr) {
        result.error = "dlopen failed: " + current_dlerror();
        return result;
    }
    // Past this point any early return must dlclose the handle.
    auto fail = [&](std::string msg) {
        ::dlclose(handle);
        result.error = std::move(msg);
        result.ok = false;
        return result;
    };

    using AbiFingerprintFn = const char* (*)();
    using AbiVersionFn = int (*)();
    using AbiHashFn = const char* (*)();
    using TripleFn = const char* (*)();
    using MetadataFn = const ::clink::plugin::PluginMetadata* (*)();
    using RegisterFn = int (*)(void*, char*, std::size_t);

    // Fingerprint + version symbols are OPTIONAL: a plugin built before the
    // fingerprint gate lacks them, and check_plugin_abi falls back to the
    // exact-hash comparison.
    auto abi_fp_fn = dlsym_as<AbiFingerprintFn>(handle, "clink_plugin_abi_fingerprint");
    auto abi_version_fn = dlsym_as<AbiVersionFn>(handle, "clink_plugin_abi_version");
    auto abi_hash_fn = dlsym_as<AbiHashFn>(handle, "clink_plugin_abi_hash");
    if (abi_hash_fn == nullptr) {
        return fail("plugin missing clink_plugin_abi_hash symbol");
    }
    auto triple_fn = dlsym_as<TripleFn>(handle, "clink_plugin_target_triple");
    if (triple_fn == nullptr) {
        return fail("plugin missing clink_plugin_target_triple symbol");
    }
    auto metadata_fn = dlsym_as<MetadataFn>(handle, "clink_plugin_metadata");
    if (metadata_fn == nullptr) {
        return fail("plugin missing clink_plugin_metadata symbol");
    }
    auto register_fn = dlsym_as<RegisterFn>(handle, "clink_plugin_register");
    if (register_fn == nullptr) {
        return fail("plugin missing clink_plugin_register symbol");
    }

    const char* plugin_abi = abi_hash_fn();
    const char* plugin_fp = abi_fp_fn != nullptr ? abi_fp_fn() : nullptr;
    AbiCheckInput abi_in;
    abi_in.strict = strict_plugin_abi_enabled();
    abi_in.plugin_has_fingerprint = abi_fp_fn != nullptr;
    abi_in.plugin_fingerprint = plugin_fp != nullptr ? plugin_fp : "";
    abi_in.cluster_fingerprint = cluster_abi_fingerprint();
    abi_in.plugin_hash = plugin_abi != nullptr ? plugin_abi : "";
    abi_in.cluster_hash = cluster_abi_hash();
    if (auto err = check_plugin_abi(abi_in); !err.empty()) {
        return fail(err);
    }

    const char* plugin_triple = triple_fn();
    if (plugin_triple == nullptr || std::strcmp(plugin_triple, cluster_target_triple()) != 0) {
        return fail(std::string{"plugin target-triple mismatch: plugin reports '"} +
                    (plugin_triple == nullptr ? "(null)" : plugin_triple) + "', cluster expects '" +
                    cluster_target_triple() + "'");
    }

    const auto* meta = metadata_fn();
    if (meta == nullptr) {
        return fail("plugin returned null metadata");
    }

    std::array<char, 1024> err_buf{};
    const int rc = register_fn(&registry, err_buf.data(), err_buf.size());
    if (rc != 0) {
        return fail(std::string{"plugin register hook failed (rc="} + std::to_string(rc) +
                    "): " + std::string{err_buf.data()});
    }

    LoadedPlugin lp;
    lp.source_path = so_path;
    lp.name = meta->name != nullptr ? meta->name : "";
    lp.version = meta->version != nullptr ? meta->version : "";
    lp.abi_fingerprint = plugin_fp != nullptr ? plugin_fp : "";
    lp.abi_version = abi_version_fn != nullptr ? abi_version_fn() : 0;
    lp.abi_hash = plugin_abi;
    lp.target_triple = plugin_triple;
    lp.dl_handle = handle;
    result.ok = true;
    result.plugin = std::move(lp);
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
