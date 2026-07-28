#include "clink/config/interned_name.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

// LeakSanitizer's intent annotation. Every allocation in this file is deliberately
// never freed - see the note on Table and the comments at each site - so without
// this LSan reports the interning table as leaked on any run that interns a name.
// Gated on ASAN BEING ACTIVE rather than on the header existing: Clang ships
// <sanitizer/lsan_interface.h> unconditionally and the symbol only comes with the
// ASan runtime, so an include-based guard links fine until it does not.
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define CLINK_ASAN_ACTIVE 1
#endif
#endif
#if defined(__SANITIZE_ADDRESS__)  // GCC spells it this way
#define CLINK_ASAN_ACTIVE 1
#endif
#if defined(CLINK_ASAN_ACTIVE)
#include <sanitizer/lsan_interface.h>
#define CLINK_LSAN_INTENTIONAL(p) __lsan_ignore_object(p)
#else
#define CLINK_LSAN_INTENTIONAL(p) ((void)(p))
#endif
#include <vector>

namespace clink::config {

namespace {

// FNV-1a over the name. Column names are short, so this is a couple of ns.
std::uint64_t hash_name(std::string_view v) noexcept {
    std::uint64_t h = 1469598103934665603ULL;
    for (const unsigned char c : v) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h;
}

// Open-addressed, insert-only, power-of-two table of published names.
//
// Slots are atomic pointers. A reader probes with acquire loads; a writer builds
// the std::string, then publishes it with a release store, so a reader that sees
// a non-null slot sees a fully constructed string. The table never shrinks and
// never rehashes below its capacity, so a pointer handed out stays valid for the
// life of the process and no reader can observe a moved entry.
//
// Growth takes the mutex, allocates a bigger slot array, and re-publishes every
// existing name into it. The OLD array is deliberately leaked rather than freed:
// a concurrent reader may still be probing it, and there is no cheap way to know
// when the last one has finished. The leak is bounded by the number of growths,
// which is bounded by the number of distinct schema column names.
struct Table {
    static constexpr std::size_t kInitialSlots = 1024;

    std::atomic<std::vector<std::atomic<const std::string*>>*> slots;
    std::mutex grow_mu;
    std::atomic<std::size_t> count{0};

    Table() : slots(new std::vector<std::atomic<const std::string*>>(kInitialSlots)) {
        // Lives for the process; see the struct comment.
        CLINK_LSAN_INTENTIONAL(slots.load(std::memory_order_relaxed));
    }
};

Table& table() {
    static Table t;
    return t;
}

// Probe `arr` for `v`. Returns the found string, or nullptr with `slot` set to
// the first empty slot index (where an insert would go).
const std::string* probe(std::vector<std::atomic<const std::string*>>& arr,
                         std::string_view v,
                         std::uint64_t h,
                         std::size_t* empty_slot) {
    const std::size_t mask = arr.size() - 1;
    std::size_t i = static_cast<std::size_t>(h) & mask;
    for (std::size_t step = 0; step < arr.size(); ++step) {
        const std::string* cur = arr[i].load(std::memory_order_acquire);
        if (cur == nullptr) {
            if (empty_slot != nullptr) {
                *empty_slot = i;
            }
            return nullptr;
        }
        if (*cur == v) {
            return cur;
        }
        i = (i + 1) & mask;
    }
    if (empty_slot != nullptr) {
        *empty_slot = arr.size();  // full
    }
    return nullptr;
}

}  // namespace

const std::string& InternedName::empty_() {
    static const std::string e;
    return e;
}

const std::string& InternedName::intern(std::string_view v) {
    if (v.empty()) {
        return empty_();
    }
    auto& t = table();
    const std::uint64_t h = hash_name(v);

    // Fast path: lock-free read of the published table.
    {
        auto* arr = t.slots.load(std::memory_order_acquire);
        if (const std::string* found = probe(*arr, v, h, nullptr)) {
            return *found;
        }
    }

    // Slow path: this name has not been published. Take the lock, re-probe (a
    // racing thread may have published it), then publish.
    const std::lock_guard<std::mutex> lock(t.grow_mu);
    auto* arr = t.slots.load(std::memory_order_acquire);
    std::size_t slot = arr->size();
    if (const std::string* found = probe(*arr, v, h, &slot)) {
        return *found;
    }

    // Keep the load factor under 1/2 so probe chains stay short, and grow before
    // publishing if this insert would breach it (or if the table is full).
    if (slot >= arr->size() || (t.count.load(std::memory_order_relaxed) + 1) * 2 >= arr->size()) {
        auto* bigger = new std::vector<std::atomic<const std::string*>>(arr->size() * 2);
        // Both this and the array it replaces outlive the process by design: a
        // lock-free reader may still be probing the old one.
        CLINK_LSAN_INTENTIONAL(bigger);
        for (auto& s : *arr) {
            const std::string* name = s.load(std::memory_order_acquire);
            if (name == nullptr) {
                continue;
            }
            std::size_t dest = 0;
            probe(*bigger, *name, hash_name(*name), &dest);
            (*bigger)[dest].store(name, std::memory_order_release);
        }
        // The old array is intentionally not deleted; readers may still hold it.
        t.slots.store(bigger, std::memory_order_release);
        arr = bigger;
        probe(*arr, v, h, &slot);
    }

    // Never freed, so the pointer stays valid for the life of the process.
    auto* owned = new std::string(v);
    CLINK_LSAN_INTENTIONAL(owned);
    (*arr)[slot].store(owned, std::memory_order_release);
    t.count.fetch_add(1, std::memory_order_relaxed);
    return *owned;
}

std::size_t InternedName::interned_count() noexcept {
    return table().count.load(std::memory_order_relaxed);
}

}  // namespace clink::config
