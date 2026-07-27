#pragma once

#include <atomic>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

// An interned column name: an 8-byte handle to a process-wide string that is
// never freed.
//
// WHY. A SQL Row stored its own copy of every column name. JsonObject is a
// sorted vector of pair<std::string, JsonValue>, and a std::string is 24 bytes
// even when the contents fit inline, so a six-column nexmark bid row spent 144
// of its 424 bytes on six names that are byte-identical in every row of the
// stream. That is per-record storage of something that belongs to the schema,
// and an operator pays it again for every row it RETAINS: nexmark q18 keeps one
// row per (bidder, auction) pair and reached 4.4 GB.
//
// Replacing the key with a pointer to one shared copy takes the pair from 56
// bytes to 40 - 96 bytes off every row of that shape - and makes an equality
// test a pointer compare.
//
// THREAD SAFETY, AND WHY THE TABLE IS SHAPED THIS WAY. Interning sits on the
// row-decode path, which runs on every worker thread at once. A mutex around
// the table would serialise all of them on a per-column-per-record basis, which
// is a worse problem than the one being fixed. So lookups are lock-free: the
// table is open-addressed with atomic slots and only ever grows, and a name is
// published with a release store after its string is fully constructed. The
// lock is taken only to INSERT a name not seen before - and column names come
// from declared schemas, so after the first few records every lookup is a
// lock-free read of a warm table.
//
// Names are never freed. That is safe because the set of names is bounded by the
// schemas a process declares; it is the same reason the table stays small enough
// to remain in cache. Do not intern unbounded user data with this.

namespace clink::config {

class InternedName {
public:
    InternedName() noexcept : s_(&empty_()) {}

    // EXPLICIT. Interning is a hash and a table probe, so it must be visible at
    // the call site: an implicit conversion would let it slip into a per-record
    // loop, trading the memory this type saves for CPU on the hot path - which is
    // exactly what happened on the first pass, at 4.7% of q0. It also keeps
    // `interned == some_std_string` unambiguous, since a std::string then has one
    // conversion (to string_view) rather than two.
    explicit InternedName(std::string_view v) : s_(&intern(v)) {}
    explicit InternedName(const std::string& v) : s_(&intern(v)) {}
    explicit InternedName(const char* v) : s_(&intern(std::string_view{v})) {}

    [[nodiscard]] const std::string& str() const noexcept { return *s_; }
    operator std::string_view() const noexcept { return *s_; }  // NOLINT(*-explicit-*)
    [[nodiscard]] const char* c_str() const noexcept { return s_->c_str(); }
    [[nodiscard]] std::size_t size() const noexcept { return s_->size(); }
    [[nodiscard]] bool empty() const noexcept { return s_->empty(); }

    // Interned, so identity is equality. Ordering compares content, because the
    // sorted-by-name iteration order the engine relies on for stable
    // serialisation must not depend on intern order.
    friend bool operator==(InternedName a, InternedName b) noexcept { return a.s_ == b.s_; }
    friend std::strong_ordering operator<=>(InternedName a, InternedName b) noexcept {
        if (a.s_ == b.s_) {
            return std::strong_ordering::equal;
        }
        return *a.s_ <=> *b.s_;
    }
    friend bool operator==(InternedName a, std::string_view b) noexcept { return *a.s_ == b; }
    friend std::strong_ordering operator<=>(InternedName a, std::string_view b) noexcept {
        return std::string_view{*a.s_} <=> b;
    }

    // Interning is exposed so hot paths can resolve a schema's names ONCE and
    // then build rows from the handles, skipping the table entirely per record.
    static const std::string& intern(std::string_view v);

    // Diagnostics for the tests: how many distinct names this process holds.
    static std::size_t interned_count() noexcept;

private:
    static const std::string& empty_();
    const std::string* s_;
};

static_assert(sizeof(InternedName) == sizeof(void*),
              "InternedName must stay pointer-sized; its whole purpose is to shrink the Row key");

}  // namespace clink::config
