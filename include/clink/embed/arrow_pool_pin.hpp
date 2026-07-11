#pragma once

// Bind this linkage unit's Arrow copy to the SYSTEM memory pool, once,
// before any Arrow allocation.
//
// WHY. Arrow's default pool is bundled mimalloc, and mimalloc misbehaves
// when a process carries a second Arrow copy with its own mimalloc
// instance. Two observed failure shapes, both in embedded-family
// processes (which link libarrow/libarrow_flight dylibs AND, via the
// pinned iceberg-cpp's statically bundled Arrow objects, a second copy):
//   - libclink loaded into a pyarrow process crashed inside mi_malloc
//     during schema export (the original clink_c.cpp finding);
//   - the Flight SQL DoGet handler wedged forever spinning in
//     mi_bitmap_clear_once_set (mimalloc's cross-thread page reclamation,
//     mi_free_try_collect_mt) while freeing an IPC buffer under load.
// Binding our copy to the system allocator removes mimalloc from its
// paths entirely.
//
// The env var is set only around the first default_memory_pool() call in
// OUR copy and then restored, so a host Arrow's allocator choice (e.g.
// pyarrow's) is untouched. An explicit ARROW_DEFAULT_MEMORY_POOL set by
// the user is always respected.

#include <cstdlib>
#include <mutex>

#include <arrow/memory_pool.h>

namespace clink::embed {

inline void pin_embedded_arrow_pool_once() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        const char* prior = std::getenv("ARROW_DEFAULT_MEMORY_POOL");
        if (prior == nullptr) {
            setenv("ARROW_DEFAULT_MEMORY_POOL", "system", /*overwrite=*/0);
        }
        (void)arrow::default_memory_pool();  // binds our copy's pool NOW
        if (prior == nullptr) {
            unsetenv("ARROW_DEFAULT_MEMORY_POOL");
        }
    });
}

}  // namespace clink::embed
