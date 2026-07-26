#pragma once

// Which allocator this process is actually using.
//
// Exists so an opt-in build flag cannot quietly do nothing. CLINK_WITH_JEMALLOC only
// takes effect if jemalloc was found, linked, and is genuinely intercepting malloc, and
// none of that is visible from the outside - a build that reported success while leaving
// the system allocator in charge would look identical. clink_node logs this at startup
// and a test asserts it agrees with the build flag.
//
// The version string comes from jemalloc itself via mallctl rather than from a CMake
// variable, so it reports the library that is loaded rather than the one that was found
// at configure time.

#include <string>

#ifdef CLINK_HAS_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace clink {

// "jemalloc <version>" when jemalloc is linked and answering, "system" otherwise.
[[nodiscard]] inline std::string allocator_name() {
#ifdef CLINK_HAS_JEMALLOC
    const char* version = nullptr;
    std::size_t len = sizeof(version);
    if (mallctl("version", &version, &len, nullptr, 0) == 0 && version != nullptr) {
        return std::string{"jemalloc "} + version;
    }
    // Linked but not answering mallctl: report the discrepancy rather than claiming
    // jemalloc is in charge.
    return "jemalloc (version unavailable)";
#else
    return "system";
#endif
}

// True when this translation unit was compiled against a jemalloc-enabled build. Used by
// the test that pins the reported name to the build configuration.
[[nodiscard]] inline bool built_with_jemalloc() noexcept {
#ifdef CLINK_HAS_JEMALLOC
    return true;
#else
    return false;
#endif
}

}  // namespace clink
