#pragma once

// Per-ELEMENT retention for collection state.
//
// ListState / MapState in typed_state.hpp store a whole collection as one
// KeyedState value, so a TTL on them is necessarily per KEY: the entire
// collection for a key lives and dies as a unit, and touching any element
// refreshes all of them. That is the right shape for "keep a user's
// session while they are active", and the wrong shape for "expire each
// cart line an hour after it was added".
//
// These types are the second shape. Each element is its own backend entry
// with its own TTL stamp, so elements expire individually.
//
// The trade-off is real and is the whole reason both exist rather than one
// replacing the other:
//
//                       typed_state.hpp            this header
//   representation      one value per key          one entry per element
//   expiry granularity  per key                    per element
//   read one element    O(collection)              O(1)
//   read whole coll.    O(1) backend read          O(slot) scan
//   write one element   O(collection) read-mod-wr  O(1)
//   backend entries     one per key                one per element
//
// The O(slot) whole-collection read is the price. A slot scan walks every
// element of every key in the slot, so `entries(k)` on a slot holding a
// million elements is a million-element walk however few belong to `k`.
// Reach for these types when elements genuinely need independent lifetimes
// and are read individually; stay with typed_state.hpp otherwise.
//
// Everything else - stamping, expired-reads-as-absent, lazy purge,
// incremental cleanup, backend compaction - comes from KeyedState, because
// each element IS a KeyedState entry. That is deliberate: a second
// hand-rolled expiry mechanism would be one more place for the semantics
// to drift.

#include <algorithm>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

namespace detail {

// Separator between the user key and the element discriminator in a
// composite backend key. 0x1F is ASCII Unit Separator: it cannot appear in
// a UTF-8 text key, and a binary key that contains it is still handled
// correctly because the split takes the LAST occurrence (the element part
// is written by this header and never contains one).
inline constexpr char kElementSep = '\x1f';

inline std::string compose_key(std::string_view user_key, std::string_view element) {
    std::string out;
    out.reserve(user_key.size() + 1 + element.size());
    out.append(user_key);
    out.push_back(kElementSep);
    out.append(element);
    return out;
}

// Split at the LAST separator, so a user key containing one still round
// trips: this header only ever writes a separator-free element part.
inline bool split_key(std::string_view composite,
                      std::string_view& user_key,
                      std::string_view& element) {
    const auto pos = composite.rfind(kElementSep);
    if (pos == std::string_view::npos) {
        return false;
    }
    user_key = composite.substr(0, pos);
    element = composite.substr(pos + 1);
    return true;
}

}  // namespace detail

// A map MK -> MV per key K, with each ENTRY carrying its own deadline.
//
// Encoding the user key as bytes is the caller's job via `key_codec`; the
// map key likewise. Both are rendered to strings and joined, so the
// backend sees one entry per (K, MK) pair.
template <typename K, typename MK, typename MV>
class ExpiringMapState {
public:
    ExpiringMapState(StateBackend& backend,
                     OperatorId op,
                     std::string slot,
                     Codec<K> kc,
                     Codec<MK> mkc,
                     Codec<MV> mvc,
                     TtlConfig ttl)
        : state_(backend, op, std::move(slot), string_codec(), std::move(mvc), ttl),
          key_codec_(std::move(kc)),
          map_key_codec_(std::move(mkc)) {}

    void put(const K& k, const MK& mk, MV mv) { state_.put(composite_(k, mk), std::move(mv)); }

    [[nodiscard]] std::optional<MV> get(const K& k, const MK& mk) const {
        return state_.get(composite_(k, mk));
    }

    [[nodiscard]] bool contains(const K& k, const MK& mk) const { return get(k, mk).has_value(); }

    void remove(const K& k, const MK& mk) { state_.erase(composite_(k, mk)); }

    // Every live entry for `k`. O(slot), not O(entries-for-k) - see the
    // header note. Expired entries are filtered by KeyedState's scan, so
    // this never returns something a get() would refuse.
    [[nodiscard]] std::vector<std::pair<MK, MV>> entries(const K& k) const {
        const auto prefix = encode_(key_codec_, k) + detail::kElementSep;
        std::vector<std::pair<MK, MV>> out;
        state_.scan([&](const std::string& composite, const MV& value) {
            if (composite.size() <= prefix.size() ||
                composite.compare(0, prefix.size(), prefix) != 0) {
                return;
            }
            const std::string element = composite.substr(prefix.size());
            auto mk = map_key_codec_.decode(std::span<const std::byte>{
                reinterpret_cast<const std::byte*>(element.data()), element.size()});
            if (mk.has_value()) {
                out.emplace_back(std::move(*mk), value);
            }
        });
        return out;
    }

    // Remove every entry for `k`. Collects first: KeyedState forbids
    // mutation during a scan.
    void clear(const K& k) {
        const auto prefix = encode_(key_codec_, k) + detail::kElementSep;
        std::vector<std::string> doomed;
        state_.scan([&](const std::string& composite, const MV&) {
            if (composite.size() > prefix.size() &&
                composite.compare(0, prefix.size(), prefix) == 0) {
                doomed.push_back(composite);
            }
        });
        for (const auto& c : doomed) {
            state_.erase(c);
        }
    }

    void advance_watermark(std::int64_t watermark_ms) noexcept {
        state_.advance_watermark(watermark_ms);
    }
    std::size_t cleanup_batch(std::size_t budget = 256) { return state_.cleanup_batch(budget); }
    [[nodiscard]] const TtlStats& ttl_stats() const noexcept { return state_.ttl_stats(); }

private:
    template <typename C, typename V>
    static std::string encode_(const C& codec, const V& v) {
        const auto bytes = codec.encode(v);
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    [[nodiscard]] std::string composite_(const K& k, const MK& mk) const {
        return detail::compose_key(encode_(key_codec_, k), encode_(map_key_codec_, mk));
    }

    KeyedState<std::string, MV> state_;
    Codec<K> key_codec_;
    Codec<MK> map_key_codec_;
};

// A list of E per key K, with each ELEMENT carrying its own deadline.
//
// Elements are discriminated by a monotonically increasing sequence so
// insertion order is preserved and two equal values are distinct entries.
// The sequence is encoded big-endian so the backend's lexicographic key
// order matches insertion order, which is what makes get() return elements
// in the order they were added without sorting.
//
// The counter lives in memory, so after a restart it has to be recovered
// or the first append would reuse seq 0 and overwrite the oldest surviving
// element. next_seq_ therefore reads the high-water mark out of the stored
// keys on the first append for a key, lazily - a key that is never appended
// to after a restart pays nothing.
template <typename K, typename E>
class ExpiringListState {
public:
    ExpiringListState(StateBackend& backend,
                      OperatorId op,
                      std::string slot,
                      Codec<K> kc,
                      Codec<E> ec,
                      TtlConfig ttl)
        : state_(backend, op, std::move(slot), string_codec(), std::move(ec), ttl),
          key_codec_(std::move(kc)) {}

    void add(const K& k, E e) {
        const auto key = encode_key_(k);
        state_.put(detail::compose_key(key, seq_bytes_(next_seq_(k, key))), std::move(e));
    }

    void add_all(const K& k, const std::vector<E>& es) {
        for (const auto& e : es) {
            add(k, e);
        }
    }

    // Live elements for `k`, in insertion order. O(slot) - see the header.
    [[nodiscard]] std::vector<E> get(const K& k) const {
        const auto prefix = encode_key_(k) + detail::kElementSep;
        // Collected as (seq, value) then sorted: KeyedState::scan visits in
        // the backend's own order, which for a hash-based backend is not
        // key order, so relying on the big-endian encoding alone would make
        // ordering depend on the backend.
        std::vector<std::pair<std::string, E>> found;
        state_.scan([&](const std::string& composite, const E& value) {
            if (composite.size() > prefix.size() &&
                composite.compare(0, prefix.size(), prefix) == 0) {
                found.emplace_back(composite.substr(prefix.size()), value);
            }
        });
        std::sort(found.begin(), found.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        std::vector<E> out;
        out.reserve(found.size());
        for (auto& [_, v] : found) {
            out.push_back(std::move(v));
        }
        return out;
    }

    [[nodiscard]] bool empty(const K& k) const { return get(k).empty(); }

    void clear(const K& k) {
        const auto prefix = encode_key_(k) + detail::kElementSep;
        std::vector<std::string> doomed;
        state_.scan([&](const std::string& composite, const E&) {
            if (composite.size() > prefix.size() &&
                composite.compare(0, prefix.size(), prefix) == 0) {
                doomed.push_back(composite);
            }
        });
        for (const auto& c : doomed) {
            state_.erase(c);
        }
        seqs_.erase(encode_key_(k));
    }

    void advance_watermark(std::int64_t watermark_ms) noexcept {
        state_.advance_watermark(watermark_ms);
    }
    std::size_t cleanup_batch(std::size_t budget = 256) { return state_.cleanup_batch(budget); }
    [[nodiscard]] const TtlStats& ttl_stats() const noexcept { return state_.ttl_stats(); }

private:
    [[nodiscard]] std::string encode_key_(const K& k) const {
        const auto bytes = key_codec_.encode(k);
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    // Big-endian so lexicographic order matches numeric order.
    static std::string seq_bytes_(std::uint64_t seq) {
        std::string out(8, '\0');
        for (int i = 0; i < 8; ++i) {
            out[static_cast<std::size_t>(i)] = static_cast<char>((seq >> ((7 - i) * 8)) & 0xFFU);
        }
        return out;
    }

    // Next sequence for this key. On the first append after a restart the
    // high-water mark is recovered from what is actually stored, so a
    // restored list continues rather than overwriting its own tail.
    std::uint64_t next_seq_(const K& k, const std::string& encoded_key) {
        auto it = seqs_.find(encoded_key);
        if (it == seqs_.end()) {
            std::uint64_t high = 0;
            const auto prefix = encoded_key + detail::kElementSep;
            state_.scan([&](const std::string& composite, const E&) {
                if (composite.size() != prefix.size() + 8 ||
                    composite.compare(0, prefix.size(), prefix) != 0) {
                    return;
                }
                std::uint64_t seq = 0;
                for (std::size_t i = 0; i < 8; ++i) {
                    seq = (seq << 8) | static_cast<unsigned char>(composite[prefix.size() + i]);
                }
                high = std::max(high, seq + 1);
            });
            it = seqs_.emplace(encoded_key, high).first;
        }
        (void)k;
        return it->second++;
    }

    KeyedState<std::string, E> state_;
    Codec<K> key_codec_;
    // In-memory high-water sequence per encoded key; rebuilt lazily from
    // stored state on the first append after a restart.
    std::unordered_map<std::string, std::uint64_t> seqs_;
};

}  // namespace clink
