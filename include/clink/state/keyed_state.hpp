#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

// Optional time-to-live for a KeyedState slot. The  equivalent
// is StateTtlConfig - every entry is implicitly stamped on write and
// expires `ttl` after the last refreshing operation. v1 supports the
// two semantics  ships with:
//
//   refresh_on_write  : every put() resets the expiry. The default
//                       - common for things like "have I seen this
//                       user recently?" caches.
//   refresh_on_read   : get() also resets the expiry (LRU-ish), so
//                       active keys live forever and inactive keys
//                       fade. Set both fields true.
//
// Expired entries are not reported by get/scan and are lazy-purged
// on first observation. There's no background sweep in v1 - that
// would add a thread;  does it incrementally on access too.
struct TtlConfig {
    std::chrono::milliseconds ttl{0};  // 0 = disabled
    bool refresh_on_write{true};
    bool refresh_on_read{false};

    [[nodiscard]] bool enabled() const noexcept { return ttl.count() > 0; }
};

// KeyedState<K, V> is the typed view over a StateBackend that operators use.
// It owns codecs for K and V and a stable namespace prefix so multiple
// keyed-state slots inside the same operator don't collide.
//
// The class is non-owning relative to the backend: the backend lifetime is
// managed by the runtime (JobConfig::state_backend), not the operator.
template <typename K, typename V>
class KeyedState {
public:
    KeyedState(StateBackend& backend,
               OperatorId op,
               std::string slot_name,
               Codec<K> key_codec,
               Codec<V> value_codec)
        : backend_(&backend),
          op_(op),
          slot_name_(validate_slot_name_(std::move(slot_name))),
          key_codec_(std::move(key_codec)),
          value_codec_(std::move(value_codec)) {}

    // Same constructor with a TTL policy attached. Picking the no-TTL
    // ctor above is the historic shape; opting into TTL is per-slot.
    KeyedState(StateBackend& backend,
               OperatorId op,
               std::string slot_name,
               Codec<K> key_codec,
               Codec<V> value_codec,
               TtlConfig ttl)
        : backend_(&backend),
          op_(op),
          slot_name_(validate_slot_name_(std::move(slot_name))),
          key_codec_(std::move(key_codec)),
          value_codec_(std::move(value_codec)),
          ttl_(ttl) {}

    void put(const K& k, const V& v) {
        // Hot path: re-use thread_local scratch buffers so the per-
        // record put pays zero heap allocations after warm-up. The
        // backend's put() borrows the slices only for the duration of
        // the call, so handing it views into thread_local storage is
        // safe as long as we don't re-enter put on the same thread
        // before the backend returns (it doesn't).
        thread_local std::string key_scratch;
        thread_local std::vector<std::byte> value_scratch;
        encode_key_into(k, key_scratch);
        if (value_codec_.encode_into) {
            value_scratch.clear();  // append contract: encode_into appends to a cleared buffer
            value_codec_.encode_into(v, value_scratch);
        } else {
            value_scratch = value_codec_.encode(v);
        }
        if (!ttl_.enabled()) {
            const std::string_view value_view(reinterpret_cast<const char*>(value_scratch.data()),
                                              value_scratch.size());
            backend_->put(op_, key_scratch, value_view);
            return;
        }
        const std::string& key_str = key_scratch;
        const auto& v_bytes = value_scratch;
        // TTL layout: [8B expire-at-ms LE][user value bytes]. The
        // expire-at is "now + ttl" on every put; refresh_on_write
        // is the always-true behavior here. On get with
        // refresh_on_read, the value is re-put with a fresh expiry.
        const auto expire_at = now_ms_() + ttl_.ttl.count();
        std::vector<std::byte> stamped;
        stamped.reserve(8 + v_bytes.size());
        write_i64_le_(stamped, expire_at);
        stamped.insert(stamped.end(), v_bytes.begin(), v_bytes.end());
        const std::string_view value_view(reinterpret_cast<const char*>(stamped.data()),
                                          stamped.size());
        backend_->put(op_, key_str, value_view);
    }

    std::optional<V> get(const K& k) const {
        const std::string key_str = encode_key(k);
        auto v = backend_->get(op_, key_str);
        if (!v.has_value()) {
            return std::nullopt;
        }
        if (!ttl_.enabled()) {
            return value_codec_.decode(std::span<const std::byte>{v->data(), v->size()});
        }
        if (v->size() < 8) {
            return std::nullopt;  // truncated; treat as missing
        }
        const auto expire_at = read_i64_le_(v->data());
        if (expire_at <= now_ms_()) {
            // Lazy purge: a stale entry is observed once and erased
            // so subsequent gets short-circuit and snapshots shrink.
            backend_->erase(op_, key_str);
            return std::nullopt;
        }
        auto decoded =
            value_codec_.decode(std::span<const std::byte>{v->data() + 8, v->size() - 8});
        if (!decoded.has_value()) {
            return std::nullopt;
        }
        if (ttl_.refresh_on_read) {
            // Re-put with a refreshed expiry. The user value bytes
            // didn't change; only the leading expire_at advances.
            const auto new_expire_at = now_ms_() + ttl_.ttl.count();
            std::vector<std::byte> stamped(v->size());
            write_i64_le_at_(stamped.data(), new_expire_at);
            std::copy(v->data() + 8, v->data() + v->size(), stamped.data() + 8);
            const std::string_view value_view(reinterpret_cast<const char*>(stamped.data()),
                                              stamped.size());
            backend_->put(op_, key_str, value_view);
        }
        return decoded;
    }

    // Non-blocking twin of get(): a lazy Task yielding the same
    // optional<V> get() would, but driving the read through the backend's
    // async surface (get_async) so a remote/disaggregated backend can
    // suspend while the read is outstanding instead of blocking the runner
    // thread. Same TTL semantics as get(): expired entries return nullopt
    // and are lazy-purged, and a refresh_on_read entry is re-put with an
    // advanced expiry.
    //
    // Lifetime contract: the returned Task captures `this`, so the
    // KeyedState (and its backend) MUST outlive the Task. Operators hold
    // their KeyedState as a member, which satisfies this; do not call
    // get_async on a temporary view.
    //
    // Owned bytes across suspension: the key is taken BY VALUE so it is
    // copied into the coroutine frame (a `const K&` would be stored as a
    // dangling reference once the caller's argument, possibly a temporary,
    // is destroyed before the lazily-started body runs). The encoded key
    // also lives in the frame (encode_key returns a fresh owned string,
    // not the thread_local put() scratch), so a deferring backend may
    // retain the KeyView across the co_await. The TTL re-put / lazy-purge
    // writes are synchronous (no async write surface yet) and consume
    // frame-local buffers within the call, so no buffer crosses a
    // suspension into a deferred write today; when a put_async surface
    // lands, those writes must own their bytes the same way.
    async::Task<std::optional<V>> get_async(K k) const {
        const std::string key_str = encode_key(k);
        co_return decode_one_(key_str, co_await backend_->get_async(op_, key_str));
    }

    // Deadline-aware twin of get_async (ASYNC-12 consumer). `order_key` tags the
    // read with its urgency (lower = sooner; e.g. a deadline in ms): a deferring
    // backend carries the tag to its completion hand-back so that, when the
    // operator opted into deadline-aware resume, a poll's ready completions
    // resume most-urgent-first. order_key 0 is byte-identical to get_async(k).
    // Same lifetime + owned-bytes contract as get_async.
    async::Task<std::optional<V>> get_async(K k, std::uint64_t order_key) const {
        const std::string key_str = encode_key(k);
        co_return decode_one_(key_str, co_await backend_->get_async(op_, key_str, order_key));
    }

    // Batched non-blocking read (ASYNC-10): the typed twin of get_async over a
    // batch of keys. Encodes every key, issues ONE backend get_many_async (a
    // remote backend coalesces it into one batched fetch + single suspension),
    // then applies the same TTL decode / lazy-purge / refresh-on-read per
    // result. Returns one optional<V> per input key, positionally. Same
    // lifetime contract as get_async: this KeyedState must outlive the Task.
    async::Task<std::vector<std::optional<V>>> get_many_async(std::vector<K> ks) const {
        std::vector<std::string> key_strs;
        key_strs.reserve(ks.size());
        for (const auto& k : ks) {
            key_strs.push_back(encode_key(k));
        }
        auto raws = co_await backend_->get_many_async(op_, key_strs);
        std::vector<std::optional<V>> out;
        out.reserve(raws.size());
        for (std::size_t i = 0; i < raws.size() && i < key_strs.size(); ++i) {
            out.push_back(decode_one_(key_strs[i], std::move(raws[i])));
        }
        co_return out;
    }

    void erase(const K& k) { backend_->erase(op_, encode_key(k)); }

    // Visit every (K, V) currently in this slot. The slot's namespace prefix
    // is stripped before invoking the visitor. The visitor must not call
    // mutators on this KeyedState - buffer keys to delete and erase them
    // after iteration completes.
    using Visitor = std::function<void(const K& key, const V& value)>;
    void scan(const Visitor& visit) const {
        const std::string& slot = slot_name_;
        const auto now = now_ms_();
        const bool ttl_on = ttl_.enabled();
        backend_->scan(op_, [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            // Stored-key layout: [kg_byte][slot_name][|][user_key_bytes].
            // Strip the kg byte first, then check the slot namespace.
            if (k.size() < 1 + slot.size() + 1) {
                return;
            }
            if (k.compare(1, slot.size(), slot) != 0 || k[1 + slot.size()] != '|') {
                return;
            }
            const std::string_view user_key_view = k.substr(1 + slot.size() + 1);
            const auto k_bytes_ptr = reinterpret_cast<const std::byte*>(user_key_view.data());
            const auto* v_bytes_ptr = reinterpret_cast<const std::byte*>(v.data());
            std::size_t v_size = v.size();
            if (ttl_on) {
                if (v_size < 8) {
                    return;
                }
                const auto expire_at = read_i64_le_(v_bytes_ptr);
                if (expire_at <= now) {
                    return;  // expired; skip silently (purged on next get())
                }
                v_bytes_ptr += 8;
                v_size -= 8;
            }
            auto key_decoded =
                key_codec_.decode(std::span<const std::byte>{k_bytes_ptr, user_key_view.size()});
            auto value_decoded =
                value_codec_.decode(std::span<const std::byte>{v_bytes_ptr, v_size});
            if (key_decoded.has_value() && value_decoded.has_value()) {
                visit(*key_decoded, *value_decoded);
            }
        });
    }

    OperatorId operator_id() const noexcept { return op_; }
    const std::string& slot_name() const noexcept { return slot_name_; }
    const TtlConfig& ttl_config() const noexcept { return ttl_; }

private:
    // Apply TTL decode to one raw backend value: nullopt passthrough, no-TTL
    // decode, or TTL unwrap with lazy-purge (synchronous erase of an expired
    // entry) and refresh-on-read (synchronous re-put with an advanced expiry).
    // Synchronous (the TTL writes have no async surface yet) and runs on the
    // runner thread after the read resumes; key_str must stay owned by the
    // caller's frame for the (synchronous) erase/put. Shared by get_async and
    // get_many_async so both have byte-identical TTL semantics.
    std::optional<V> decode_one_(const std::string& key_str,
                                 std::optional<StateBackend::Value> v) const {
        if (!v.has_value()) {
            return std::nullopt;
        }
        if (!ttl_.enabled()) {
            return value_codec_.decode(std::span<const std::byte>{v->data(), v->size()});
        }
        if (v->size() < 8) {
            return std::nullopt;  // truncated; treat as missing
        }
        const auto expire_at = read_i64_le_(v->data());
        if (expire_at <= now_ms_()) {
            backend_->erase(op_, key_str);  // lazy purge
            return std::nullopt;
        }
        auto decoded =
            value_codec_.decode(std::span<const std::byte>{v->data() + 8, v->size() - 8});
        if (!decoded.has_value()) {
            return std::nullopt;
        }
        if (ttl_.refresh_on_read) {
            // Re-put with a refreshed expiry; only the leading expire_at
            // advances, user value bytes unchanged. `stamped` is a frame local
            // consumed by the synchronous put below.
            const auto new_expire_at = now_ms_() + ttl_.ttl.count();
            std::vector<std::byte> stamped(v->size());
            write_i64_le_at_(stamped.data(), new_expire_at);
            std::copy(v->data() + 8, v->data() + v->size(), stamped.data() + 8);
            const std::string_view value_view(reinterpret_cast<const char*>(stamped.data()),
                                              stamped.size());
            backend_->put(op_, key_str, value_view);
        }
        return decoded;
    }

    // Reject slot names that would break the stored-key layout or the
    // state-version pack format. '|' is the slot/user-key separator in the
    // stored key (see encode_key_into) - a slot name containing '|' would
    // make the key space non-prefix-free, so the slot-aware migrator's
    // prefix filter (state_migration_on_restore) could match a sibling
    // slot. '\n' is the StateVersionMap pack line separator. Mirrors the
    // same rejection in StateVersionMap::set so both ends of the
    // declare/encode pair agree.
    static std::string validate_slot_name_(std::string slot) {
        if (slot.find('|') != std::string::npos || slot.find('\n') != std::string::npos) {
            throw std::invalid_argument(
                "KeyedState: slot name must not contain '|' or '\\n' (reserved as the "
                "stored-key slot separator and the state-version pack delimiter)");
        }
        return slot;
    }

    // Stored-key layout: [kg_byte][slot_name][|][user_key_bytes].
    //
    // The leading kg byte is the FNV-1a-derived key group, computed over
    // the user key bytes only (not the slot prefix). It lets backends
    // filter by key-group range during restore() without having to know
    // anything about the user codec or slot names: a single byte compare
    // suffices. Same byte ordering across put/get/scan keeps the lookup
    // path branch-free.
    std::string encode_key(const K& k) const {
        std::string out;
        encode_key_into(k, out);
        return out;
    }

    void encode_key_into(const K& k, std::string& out) const {
        // Allocation-free key encode for the hot put/get/erase path:
        // callers (KeyedState::put) hand us a thread_local string and we
        // overwrite it in place. The key bytes go through encode_append into a
        // thread_local scratch buffer, so a key codec with encode_into (the
        // built-in int/pair/etc.) pays zero heap allocations after warm-up; a
        // codec without it falls back to encode() + a copy.
        thread_local std::vector<std::byte> key_bytes;
        key_bytes.clear();
        encode_append(key_codec_, k, key_bytes);
        const auto kg =
            key_group_for_key(std::span<const std::byte>{key_bytes.data(), key_bytes.size()});
        out.clear();
        out.reserve(1 + slot_name_.size() + 1 + key_bytes.size());
        out.push_back(static_cast<char>(kg & 0xFF));
        out.append(slot_name_);
        out.push_back('|');
        out.append(reinterpret_cast<const char*>(key_bytes.data()), key_bytes.size());
    }

    static std::int64_t now_ms_() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    static void write_i64_le_(std::vector<std::byte>& out, std::int64_t v) {
        const auto u = static_cast<std::uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
        }
    }
    static void write_i64_le_at_(std::byte* out, std::int64_t v) {
        const auto u = static_cast<std::uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            out[i] = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
        }
    }
    static std::int64_t read_i64_le_(const std::byte* p) {
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(p[i])) << (i * 8);
        }
        return static_cast<std::int64_t>(u);
    }
    static std::int64_t read_i64_le_(const char* p) {
        return read_i64_le_(reinterpret_cast<const std::byte*>(p));
    }

    StateBackend* backend_;
    OperatorId op_;
    std::string slot_name_;
    Codec<K> key_codec_;
    Codec<V> value_codec_;
    TtlConfig ttl_{};
};

}  // namespace clink
