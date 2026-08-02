#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
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

// Which clock a TTL is measured against.
//
// This distinction is not a nicety. A processing-time TTL is measured
// against the wall clock of the machine doing the processing, so on a
// backfill - a replay of six months of history through a job with a
// one-hour TTL - every entry is already older than the TTL the instant it
// is written, and the job silently produces nothing. Conversely a job that
// stalls for two hours expires state that, in the stream's own terms, is
// seconds old.
//
// Event time measures against the watermark, so retention means what a
// user means by it: "keep a key for an hour of DATA time". A replay then
// behaves identically to the original run, which is also what makes a
// TTL'd job deterministically replayable.
enum class TtlTimeDomain : std::uint8_t {
    ProcessingTime,  // wall clock on the processing machine
    EventTime,       // the operator's current watermark
};

// Optional time-to-live for a KeyedState slot. Every entry is stamped on
// write and expires `ttl` after the last refreshing operation.
//
//   refresh_on_write  : every put() resets the expiry. The default -
//                       common for "have I seen this user recently?".
//   refresh_on_read   : get() also resets the expiry (LRU-ish), so active
//                       keys live and inactive keys fade. Set both true.
//
// Semantics, stated so they can be relied on:
//
//   * An expired entry is never returned by get/get_many/scan, whether or
//     not it has been physically removed yet.
//   * Expiry is lazy on read AND incremental on cleanup (see
//     `cleanup_batch`), so memory is actually released rather than merely
//     hidden. Lazy-only expiry means a key that is written once and never
//     read again is retained for ever - which defeats the purpose.
//   * Snapshots carry the stamp, not a remaining duration, so a restore
//     resumes the same absolute expiry rather than silently extending
//     every entry's life by the length of the outage.
//   * Under EventTime, an entry whose stamp is in the future relative to
//     the current watermark is live. Before the FIRST watermark arrives
//     the domain has no time yet, and `expire_before_first_watermark`
//     decides whether that means "nothing expires" (the default, and the
//     safe reading) or "treat time as zero".
//   * Rescaling moves an entry to its new subtask with its stamp intact.
//     The stamp is absolute, so it survives the move unchanged.
//   * A late record targeting an expired key sees no state, exactly as if
//     the key had never existed. That is the documented behaviour, not an
//     accident: resurrecting expired state on a late arrival would make
//     retention unbounded again.
struct TtlConfig {
    std::chrono::milliseconds ttl{0};  // 0 = disabled
    bool refresh_on_write{true};
    bool refresh_on_read{false};
    TtlTimeDomain domain{TtlTimeDomain::ProcessingTime};
    // EventTime only. False (default): nothing expires until a watermark
    // has been seen, so a job that has not yet established event time
    // cannot mass-expire its state on the strength of a zero watermark.
    bool expire_before_first_watermark{false};

    [[nodiscard]] bool enabled() const noexcept { return ttl.count() > 0; }
};

// Counters for one TTL-enabled slot. Read by the operator's metrics
// reporting; the brief asks for live entries, expirations, cleanup lag and
// an estimate of state size, and these are the raw numbers behind them.
struct TtlStats {
    std::uint64_t expired_on_read{0};     // entries found dead and purged by a read
    std::uint64_t expired_in_cleanup{0};  // entries removed by incremental cleanup
    std::uint64_t live_entries{0};        // live entries at the last cleanup sweep
    std::uint64_t scanned_entries{0};     // entries visited at the last cleanup sweep
    std::uint64_t estimated_bytes{0};     // key+value bytes at the last cleanup sweep
    // Entries the last sweep did NOT reach because it hit its budget. The
    // cleanup lag: while this is non-zero, expired state is still resident.
    std::uint64_t unscanned_backlog{0};
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
        const auto expire_at = this->expire_at_();
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
        if (this->is_expired_(expire_at)) {
            ++this->stats_.expired_on_read;
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
            const auto new_expire_at = this->expire_at_();
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
        const auto now_opt = this->now_for_ttl_();
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
                if (now_opt.has_value() && expire_at <= *now_opt) {
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

    // --- event-time TTL ------------------------------------------------
    //
    // The operator feeds its current watermark in. KeyedState has no route
    // to the watermark of its own (it sees a backend, a codec pair and a
    // slot name), and threading a RuntimeContext in would couple state to
    // the runtime for one field. A setter the operator calls once per
    // watermark advance is cheaper and keeps the dependency out.
    //
    // Monotonic by construction: a watermark that went backwards would
    // resurrect state that had already expired, so a regression is ignored
    // rather than applied.
    void advance_watermark(std::int64_t watermark_ms) noexcept {
        if (!this->has_watermark_ || watermark_ms > this->watermark_ms_) {
            this->watermark_ms_ = watermark_ms;
            this->has_watermark_ = true;
        }
    }

    [[nodiscard]] bool has_watermark() const noexcept { return this->has_watermark_; }

    [[nodiscard]] const TtlStats& ttl_stats() const noexcept { return this->stats_; }

    // --- incremental cleanup -------------------------------------------
    //
    // Sweep up to `budget` entries in this slot and erase the expired
    // ones. Bounded on purpose: an unbounded sweep over a large keyspace
    // would stall the operator thread for as long as the scan takes, which
    // is a latency spike proportional to state size - exactly what a job
    // running a TTL is trying to avoid. Calling this once per checkpoint
    // (or every N records) walks the space over many small steps.
    //
    // Resumes from where the previous call stopped, so repeated calls make
    // progress through the whole slot rather than re-scanning the front of
    // it. Returns the number of entries erased.
    //
    // Without this, expiry is lazy only: an entry written once and never
    // read again is hidden from readers but never released, so a TTL that
    // was supposed to bound memory does not.
    std::size_t cleanup_batch(std::size_t budget = 256) {
        if (!ttl_.enabled() || budget == 0) {
            return 0;
        }
        const auto now = this->now_for_ttl_();
        if (!now.has_value()) {
            return 0;  // event time not yet established; nothing can be judged
        }
        // Hand the work to the backend when it can do it as part of
        // maintenance it already performs (an LSM compaction rewrites every
        // live SST anyway, so dropping expired entries there is free). The
        // scan below is the portable fallback for backends that cannot.
        if (backend_->supports_expiry_compaction()) {
            install_expiry_filter_once_();
            const auto reclaimed = backend_->compact_expired(op_);
            // nullopt means "reclaimed, count unknown" - RocksDB does not
            // report filter drops. Either way the backend has handled it and
            // a scan on top would be pure duplicated cost.
            stats_.expired_in_cleanup += reclaimed.value_or(0);
            return reclaimed.value_or(0);
        }

        std::vector<std::string> doomed;
        std::uint64_t scanned = 0;
        std::uint64_t live = 0;
        std::uint64_t bytes = 0;
        std::uint64_t skipped_before_cursor = 0;
        std::uint64_t backlog = 0;
        bool budget_hit = false;
        std::string last_visited;

        const std::string prefix = this->slot_prefix_();
        backend_->scan(op_, [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            const std::string key{k};
            if (key.find(prefix) == std::string::npos) {
                return;  // another slot in the same operator
            }
            if (budget_hit) {
                ++backlog;
                return;
            }
            // Resume point: skip everything at or before the previous
            // sweep's stopping key so successive calls advance.
            if (!this->cleanup_cursor_.empty() && key <= this->cleanup_cursor_) {
                ++skipped_before_cursor;
                return;
            }
            ++scanned;
            bytes += key.size() + v.size();
            if (v.size() >= 8) {
                const auto expire_at = read_i64_le_(v.data());
                if (expire_at <= *now) {
                    doomed.push_back(key);
                } else {
                    ++live;
                }
            } else {
                ++live;  // not TTL-stamped; leave it alone
            }
            last_visited = key;
            if (scanned >= budget) {
                budget_hit = true;
            }
        });

        for (const auto& key : doomed) {
            backend_->erase(op_, key);
        }
        // A sweep that reached the end restarts from the beginning next
        // time; one that stopped early resumes at its stopping key.
        this->cleanup_cursor_ = budget_hit ? last_visited : std::string{};

        this->stats_.expired_in_cleanup += doomed.size();
        this->stats_.scanned_entries = scanned;
        this->stats_.live_entries = live + skipped_before_cursor;
        this->stats_.estimated_bytes = bytes;
        this->stats_.unscanned_backlog = backlog;
        return doomed.size();
    }

    // Sweep the entire slot in one pass, ignoring the incremental budget.
    // For a test that needs a settled answer, or a snapshot path that
    // wants to avoid persisting known-dead entries. NOT for the hot path.
    std::size_t cleanup_all() {
        this->cleanup_cursor_.clear();
        std::size_t total = 0;
        // Bounded loop rather than while(true): each pass either erases
        // something or completes, and the cursor reset above guarantees
        // the first pass sees everything.
        for (;;) {
            const auto n = this->cleanup_batch(std::numeric_limits<std::size_t>::max());
            total += n;
            if (n == 0) {
                break;
            }
            this->cleanup_cursor_.clear();
        }
        return total;
    }

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
        if (this->is_expired_(expire_at)) {
            ++this->stats_.expired_on_read;
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
            const auto new_expire_at = this->expire_at_();
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

    // Give the backend a predicate it can consult during its own
    // compaction. Installed once per slot, lazily: a backend that never
    // compacts pays nothing, and a slot with no TTL installs nothing.
    //
    // The predicate runs on a BACKGROUND thread, so it must not touch this
    // object's mutable state. It reads the TTL stamp out of the value's
    // leading eight bytes and compares against a clock captured by value -
    // the event-time watermark at install, or the wall clock. That means an
    // event-time filter judges against a slightly stale watermark, which is
    // conservative in the safe direction: it keeps an entry a little longer
    // than strictly necessary, never drops a live one.
    void install_expiry_filter_once_() {
        if (filter_installed_ || !ttl_.enabled()) {
            return;
        }
        filter_installed_ = true;
        const bool event_time = ttl_.domain == TtlTimeDomain::EventTime;
        const auto wm = watermark_ms_;
        const bool have_wm = has_watermark_;
        const auto prefix = slot_prefix_();
        const auto my_op = op_;
        backend_->set_expiry_filter(
            [event_time, wm, have_wm, prefix, my_op](
                OperatorId op, StateBackend::KeyView key, StateBackend::ValueView value) {
                if (op != my_op) {
                    return false;
                }
                // Only this slot's keys: another slot in the same operator
                // may not be TTL-stamped at all, and reading its first
                // eight bytes as a deadline would drop live state.
                if (key.find(prefix) == std::string_view::npos) {
                    return false;
                }
                if (value.size() < 8) {
                    return false;  // not stamped; leave it alone
                }
                const auto expire_at =
                    read_i64_le_(reinterpret_cast<const std::byte*>(value.data()));
                if (event_time) {
                    return have_wm && expire_at <= wm;
                }
                const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
                return expire_at <= now;
            });
    }

    // The clock this slot's TTL is measured against.
    //
    // nullopt means "this domain has no time yet", which happens only for
    // EventTime before the first watermark. Every TTL decision treats that
    // as "nothing can be judged expired" rather than as time zero: a zero
    // watermark would make every stamped entry look expired and wipe the
    // slot the moment a job starts. `expire_before_first_watermark` opts
    // into the other reading for a job that genuinely wants it.
    [[nodiscard]] std::optional<std::int64_t> now_for_ttl_() const {
        if (ttl_.domain == TtlTimeDomain::ProcessingTime) {
            return now_ms_();
        }
        if (!has_watermark_) {
            return ttl_.expire_before_first_watermark ? std::optional<std::int64_t>{0}
                                                      : std::nullopt;
        }
        return watermark_ms_;
    }

    // The stamp to write for an entry created or refreshed now. Falls back
    // to the wall clock when event time is not yet established, so the
    // entry gets a sane absolute expiry rather than one relative to zero
    // (which would place it in 1970 and make it instantly dead).
    [[nodiscard]] std::int64_t expire_at_() const {
        const auto now = now_for_ttl_();
        const auto base =
            now.has_value()
                ? *now
                : (ttl_.domain == TtlTimeDomain::EventTime ? std::int64_t{0} : now_ms_());
        return base + ttl_.ttl.count();
    }

    // True when `expire_at` is in the past for this slot's domain. False
    // when the domain has no time yet - an entry cannot be proven dead
    // against a clock that has not started.
    [[nodiscard]] bool is_expired_(std::int64_t expire_at) const {
        const auto now = now_for_ttl_();
        return now.has_value() && expire_at <= *now;
    }

    // "<slot>|" - the marker that identifies a key as belonging to this
    // slot within the operator's keyspace (see encode_key_into: the layout
    // is <2B key group><slot name>'|'<user key>).
    [[nodiscard]] std::string slot_prefix_() const { return slot_name_ + "|"; }

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
    // Event-time TTL state. mutable because the read path is const and has
    // to record that it purged something.
    std::int64_t watermark_ms_{0};
    bool has_watermark_{false};
    mutable TtlStats stats_{};
    // Resume point for incremental cleanup, so successive bounded sweeps
    // walk the whole slot instead of re-scanning its front.
    std::string cleanup_cursor_;
    bool filter_installed_{false};
};

}  // namespace clink
