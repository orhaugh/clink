#include "clink/cluster/plugin_loader.hpp"

#include <array>
#include <cstring>
#include <dlfcn.h>
#include <stdexcept>
#include <string>

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

const char* cluster_abi_hash() noexcept {
    return ::clink::plugin::kAbiHash;
}

const char* cluster_target_triple() noexcept {
    return CLINK_PLUGIN_TARGET_TRIPLE;
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

    // RTLD_LOCAL keeps the plugin's symbols out of the global
    // namespace; RTLD_NOW resolves all relocations up front so we
    // catch missing dependencies at load rather than first call.
    void* handle = ::dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL);
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

    using AbiHashFn = const char* (*)();
    using TripleFn = const char* (*)();
    using MetadataFn = const ::clink::plugin::PluginMetadata* (*)();
    using RegisterFn = int (*)(void*, char*, std::size_t);

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
    if (plugin_abi == nullptr || std::strcmp(plugin_abi, cluster_abi_hash()) != 0) {
        return fail(std::string{"plugin ABI hash mismatch: plugin reports '"} +
                    (plugin_abi == nullptr ? "(null)" : plugin_abi) + "', cluster expects '" +
                    cluster_abi_hash() + "'");
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
