// A deliberately incompatible plugin, for exercising the loader's refusal
// paths against a REAL dlopen'd module rather than synthetic strings. The
// handshake symbols are hand-written instead of using CLINK_DECLARE_PLUGIN so
// that exactly one identity value can be doctored per build variant while
// every other value stays genuine:
//
//   CLINK_MISMATCH_FINGERPRINT  bogus fingerprint (and a manifest that differs
//                               from the cluster's by one appended header, so
//                               the named-diff path has something to name)
//   CLINK_MISMATCH_TRIPLE       bogus target triple
//   CLINK_MISMATCH_TOOLCHAIN    bogus toolchain identity
//
// The register hook is a loadable no-op, but the gate must refuse before it
// ever runs; the tests assert exactly that.

#include <cstddef>
#include <string>

// plugin.hpp pulls in plugin_impl.hpp (the macros, CLINK_PLUGIN_TARGET_TRIPLE)
// and the generated abi_version.hpp / abi_surface.hpp constants; plugin_impl
// is not includable on its own.
#include "clink/plugin/plugin.hpp"

#if !defined(CLINK_MISMATCH_FINGERPRINT) && !defined(CLINK_MISMATCH_TRIPLE) && \
    !defined(CLINK_MISMATCH_TOOLCHAIN)
#error "build this fixture with exactly one CLINK_MISMATCH_* define"
#endif

extern "C" const char* clink_plugin_abi_fingerprint() {
#if defined(CLINK_MISMATCH_FINGERPRINT)
    return "deadbeef-not-the-cluster-fingerprint";
#else
    return ::clink::plugin::kAbiFingerprint;
#endif
}

extern "C" int clink_plugin_abi_version() {
    return ::clink::plugin::kAbiVersion;
}

extern "C" const char* clink_plugin_abi_hash() {
    return ::clink::plugin::kAbiHash;
}

extern "C" const char* clink_plugin_target_triple() {
#if defined(CLINK_MISMATCH_TRIPLE)
    return "vax-780";
#else
    return CLINK_PLUGIN_TARGET_TRIPLE;
#endif
}

extern "C" const char* clink_plugin_toolchain() {
#if defined(CLINK_MISMATCH_TOOLCHAIN)
    return "fortran-iv";
#else
    return ::clink::plugin::kToolchain;
#endif
}

extern "C" const char* clink_plugin_abi_manifest() {
#if defined(CLINK_MISMATCH_FINGERPRINT)
    // Differ from the cluster's manifest by one appended header so the
    // loader's named diff has a concrete header to report.
    static const std::string doctored = [] {
        std::string m = ::clink::plugin::kAbiSurfaceManifest;
        if (!m.empty() && m.back() != '\n') {
            m += '\n';
        }
        m += "include/clink/imaginary/added_by_fixture.hpp=0000\n";
        return m;
    }();
    return doctored.c_str();
#else
    return ::clink::plugin::kAbiSurfaceManifest;
#endif
}

extern "C" const ::clink::plugin::PluginMetadata* clink_plugin_metadata() {
    static const ::clink::plugin::PluginMetadata m{
        "mismatched-abi-plugin",
        "1.0.0",
        "fixture: one doctored identity value, everything else genuine",
        nullptr};
    return &m;
}

extern "C" int clink_plugin_register(void* /*registry_ptr*/,
                                     char* /*err_buf*/,
                                     std::size_t /*err_buf_size*/) {
    // Reaching this at all means a gate failed to refuse; return an error so
    // a buggy loader cannot mistake the fixture for a healthy plugin.
    return 99;
}
