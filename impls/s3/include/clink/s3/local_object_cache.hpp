#pragma once

// LocalObjectCache - a bounded local-disk cache of large immutable checkpoint
// objects (DISAGG-5). It lets S3SnapshotStore::fetch download only the objects
// it does not already hold locally, so a same-host restart or an
// overlapping-key-range rescale re-uses SSTs already on disk instead of pulling
// the whole checkpoint from object storage again.
//
// Durability across restarts. The on-disk files ARE the cache; the in-RAM
// index is rebuilt from them on construction (scan dir_, recover each key,
// stat each size), so a fresh process / job over the same cache dir sees the
// objects a prior run left behind. This is the whole point: surviving a restart
// is what turns a download-avoidance cache into a restart-recovery accelerator.
// Because every instance over a shared dir rebuilds the same index and enforces
// the same byte budget, the budget is honoured across restarts (orphaned files
// from prior runs are reclaimed by eviction at startup, not leaked).
//
// Naming + namespace isolation. The cache key is (namespace, basename), where
// `namespace` is the cp-dir's PARENT path (bucket/prefix/<subtask>) and
// `basename` the object file name. Each entry is one flat file whose name is a
// LOSSLESS, reversible encoding of the key - enc(ns) + "|" + enc(basename),
// percent-encoding everything outside [A-Za-z0-9._-]. Lossless matters twice:
// (1) two distinct keys never collapse onto one on-disk file (a naive
// slash->'_' folding made "a/b_c" and "a/b/c" share a file and serve each
// other's bytes - silent state corruption); (2) the key is recoverable from the
// filename, which is what lets the constructor rebuild the index from disk.
//
// What is cached, and why it is safe. Only large, content-immutable files are
// cached: NNNNNN.sst and NNNNNN.blob. RocksDB never rewrites an SST/blob under
// a given number within a DB lineage, so the (number -> bytes) mapping is
// stable and the name identifies the bytes. MANIFEST-/OPTIONS-/CURRENT/LOG are
// NOT cached: MANIFEST is appended-to within a lineage (same number, growing
// bytes) so it is not immutable-by-name, and all four are tiny - re-fetching
// them every time is cheap and removes a whole class of stale-hit hazards.
//
// Residual stale-hit guard. A *different* DB lineage that reuses the same S3
// prefix (a fresh job over a recycled bucket/prefix - operator misuse, since it
// mixes two jobs' state) resets SST numbers from scratch, so a prior lineage's
// "000007.sst" can name different bytes. To stop a durable cache serving those
// stale bytes, get() takes the object's expected size (free from the S3 LIST)
// and treats a size mismatch as a miss (re-download + overwrite). That kills
// every different-size collision; the residual same-number/same-size/
// different-bytes case across lineages is closed fully by DISAGG-6's
// content-addressed (hash) manifest. Within one continuing lineage SST numbers
// are monotonic and never reused, so a same-job restart is always a correct hit.
//
// Eviction. Bounded by a byte budget. The policy is frequency-aware LRU: when
// over budget it evicts the entry with the lowest access frequency, breaking
// ties by the oldest access. A streaming scan (each object touched once, freq
// 1) therefore cannot evict a genuinely hot object (high freq) - it beats pure
// LRU on scan-heavy traces. An object larger than the whole budget is never
// cached (it would evict everything for itself).
//
// Thread-safety: all operations take an internal mutex, so the cache may be
// shared across threads / stores. Filesystem ops across instances sharing one
// dir race only benignly: a lost link/copy degrades to a re-download, never a
// wrong hit (the size guard + lossless naming hold regardless).

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <system_error>
#include <unordered_map>

#include "clink/core/sha256.hpp"
#include "clink/metrics/disagg_metrics.hpp"

namespace clink::s3 {

class LocalObjectCache {
public:
    // Sentinel for get(): "size unknown, skip the size guard" (the unit-test /
    // no-LIST path). The S3 store always passes the real object size.
    static constexpr std::uint64_t kUnknownSize = ~0ULL;

    LocalObjectCache(std::filesystem::path dir, std::uint64_t max_bytes)
        : dir_(std::move(dir)), max_bytes_(max_bytes) {
        std::error_code ec;
        std::filesystem::create_directories(dir_, ec);
        reload_from_disk_();
    }

    // Cacheable names: NNNNNN.sst / NNNNNN.blob (immutable-by-number within a
    // RocksDB lineage), and a 64-hex content hash (a DISAGG-6 content-addressed
    // object, immutable by construction - the name IS the digest of the bytes).
    // MANIFEST/OPTIONS are mutable-by-name or tiny; CURRENT/LOG/IDENTITY are
    // mutable - all re-fetched every time.
    static bool is_immutable(const std::string& basename) {
        return has_suffix_(basename, ".sst") || has_suffix_(basename, ".blob") ||
               Sha256::is_hex_digest(basename);
    }

    // Materialize a cached (namespace, basename) into `dest` (hard-link, falling
    // back to copy across filesystems). Returns true on a hit. A non-immutable
    // name always misses. `expected_size` (when not kUnknownSize) guards against
    // a stale cross-lineage object: a size mismatch is treated as a miss and the
    // stale entry is dropped so the caller's re-fetch repopulates it.
    bool get(const std::string& ns,
             const std::string& basename,
             const std::filesystem::path& dest,
             std::uint64_t expected_size = kUnknownSize) {
        if (!is_immutable(basename)) {
            return false;
        }
        std::lock_guard<std::mutex> lk(mu_);
        const auto it = entries_.find(key(ns, basename));
        if (it == entries_.end()) {
            record_miss_();
            return false;
        }
        if (expected_size != kUnknownSize && it->second.size != expected_size) {
            // Stale: a different lineage's object under the same name. Drop it so
            // the caller's re-download overwrites with the correct bytes.
            drop_(it);
            record_miss_();
            return false;
        }
        const std::filesystem::path cached = entry_path(it->second.ns, it->second.basename);
        std::error_code ec;
        if (!std::filesystem::exists(cached, ec)) {
            // Cache file vanished underneath us (manual cleanup / a sibling
            // instance evicted it); drop the entry.
            bytes_ -= it->second.size;
            entries_.erase(it);
            record_miss_();
            return false;
        }
        std::filesystem::remove(dest, ec);
        std::filesystem::create_hard_link(cached, dest, ec);
        if (ec) {
            std::error_code cp_ec;
            std::filesystem::copy_file(
                cached, dest, std::filesystem::copy_options::overwrite_existing, cp_ec);
            if (cp_ec) {
                record_miss_();
                return false;  // could not serve the cached copy; caller re-fetches
            }
        }
        it->second.freq += 1;
        it->second.last_seq = ++seq_;
        record_hit_();
        return true;
    }

    // Store `src` under (namespace, basename) for future hits. No-op for a
    // non-immutable name or an object larger than the whole budget. Evicts to
    // stay within the byte budget.
    void put(const std::string& ns, const std::string& basename, const std::filesystem::path& src) {
        if (!is_immutable(basename)) {
            return;
        }
        std::error_code ec;
        const std::uint64_t size = std::filesystem::file_size(src, ec);
        if (ec) {
            return;  // source unreadable; skip caching, not fatal
        }
        std::lock_guard<std::mutex> lk(mu_);
        if (size > max_bytes_) {
            return;  // would evict the entire cache for one object; don't bother
        }
        const std::string k = key(ns, basename);
        const std::filesystem::path cached = entry_path(ns, basename);
        std::error_code link_ec;
        std::filesystem::remove(cached, link_ec);
        std::filesystem::create_hard_link(src, cached, link_ec);
        if (link_ec) {
            std::error_code cp_ec;
            std::filesystem::copy_file(
                src, cached, std::filesystem::copy_options::overwrite_existing, cp_ec);
            if (cp_ec) {
                return;  // could not populate the cache; benign
            }
        }
        if (const auto it = entries_.find(k); it != entries_.end()) {
            bytes_ -= it->second.size;
            it->second.size = size;
            it->second.freq += 1;
            it->second.last_seq = ++seq_;
        } else {
            entries_.emplace(k, Entry{ns, basename, size, 1, ++seq_});
        }
        bytes_ += size;
        evict_to_budget_();
        clink::metrics::disagg::object_cache_entries_set(
            static_cast<std::int64_t>(entries_.size()));
    }

    [[nodiscard]] std::uint64_t hits() const {
        std::lock_guard<std::mutex> lk(mu_);
        return hits_;
    }
    [[nodiscard]] std::uint64_t misses() const {
        std::lock_guard<std::mutex> lk(mu_);
        return misses_;
    }
    [[nodiscard]] std::size_t entry_count() const {
        std::lock_guard<std::mutex> lk(mu_);
        return entries_.size();
    }
    [[nodiscard]] std::uint64_t bytes() const {
        std::lock_guard<std::mutex> lk(mu_);
        return bytes_;
    }
    [[nodiscard]] bool contains(const std::string& ns, const std::string& basename) const {
        std::lock_guard<std::mutex> lk(mu_);
        return entries_.find(key(ns, basename)) != entries_.end();
    }

private:
    struct Entry {
        std::string ns;
        std::string basename;
        std::uint64_t size{0};
        std::uint64_t freq{0};      // access count (frequency-aware eviction)
        std::uint64_t last_seq{0};  // recency tiebreak
    };

    // Bump the internal counter and mirror it to the disagg metrics registry
    // (OBS-3). Called under mu_; the registry has its own lock and is never
    // taken before mu_, so there is no lock-order hazard.
    void record_hit_() {
        ++hits_;
        clink::metrics::disagg::object_cache_hit();
    }
    void record_miss_() {
        ++misses_;
        clink::metrics::disagg::object_cache_miss();
    }

    static bool has_suffix_(const std::string& s, const std::string& suf) {
        // A non-empty stem is required (".sst" alone is not an SST).
        return s.size() > suf.size() && s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
    }

    // The in-RAM key uses the SAME lossless encoding as the on-disk name, so
    // the two derivations are byte-identical and the key is collision-free even
    // if ns/basename ever contained a literal '|' (RocksDB names never do; this
    // keeps the invariant total rather than input-dependent).
    static std::string key(const std::string& ns, const std::string& basename) {
        return enc(ns) + "|" + enc(basename);
    }

    // Percent-encode everything outside the filesystem-safe set so the result
    // is a single path component with no '/', and is losslessly reversible.
    static std::string enc(const std::string& s) {
        static const char* kHex = "0123456789ABCDEF";
        std::string out;
        out.reserve(s.size());
        for (char ch : s) {
            const auto c = static_cast<unsigned char>(ch);
            const bool safe = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                              (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_';
            if (safe) {
                out.push_back(static_cast<char>(c));
            } else {
                out.push_back('%');
                out.push_back(kHex[c >> 4]);
                out.push_back(kHex[c & 0x0F]);
            }
        }
        return out;
    }

    static int hexval_(char c) {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        return -1;
    }

    static std::string dec(const std::string& s) {
        std::string out;
        out.reserve(s.size());
        for (std::size_t i = 0; i < s.size(); ++i) {
            if (s[i] == '%' && i + 2 < s.size()) {
                const int hi = hexval_(s[i + 1]);
                const int lo = hexval_(s[i + 2]);
                if (hi >= 0 && lo >= 0) {
                    out.push_back(static_cast<char>((hi << 4) | lo));
                    i += 2;
                    continue;
                }
            }
            out.push_back(s[i]);
        }
        return out;
    }

    // One flat file per entry, named by the lossless encoding of the key. The
    // single literal '|' separates the two encoded halves (enc() never emits
    // '|'), so the key is recoverable for the on-disk reload.
    std::filesystem::path entry_path(const std::string& ns, const std::string& basename) const {
        return dir_ / key(ns, basename);  // on-disk name == the in-RAM key, by construction
    }

    // Rebuild the index from the files already in dir_ (a prior run's cache), so
    // a restart re-uses them and the byte budget is enforced across restarts.
    // Files whose name does not parse as enc(ns)|enc(basename) are foreign and
    // left untouched (the cache dir should be cache-exclusive).
    void reload_from_disk_() {
        std::error_code ec;
        for (const auto& de : std::filesystem::directory_iterator(dir_, ec)) {
            if (!de.is_regular_file(ec)) {
                continue;
            }
            const std::string name = de.path().filename().string();
            const auto bar = name.find('|');
            if (bar == std::string::npos) {
                continue;  // not one of ours
            }
            const std::string ns = dec(name.substr(0, bar));
            const std::string basename = dec(name.substr(bar + 1));
            if (!is_immutable(basename)) {
                continue;
            }
            const std::uint64_t size = std::filesystem::file_size(de.path(), ec);
            if (ec) {
                ec.clear();
                continue;
            }
            const std::string k = key(ns, basename);
            if (entries_.find(k) != entries_.end()) {
                continue;  // duplicate encoding; keep the first
            }
            entries_.emplace(k, Entry{ns, basename, size, 1, ++seq_});
            bytes_ += size;
        }
        evict_to_budget_();  // a prior run may have left more than the budget allows
    }

    void drop_(std::unordered_map<std::string, Entry>::iterator it) {
        std::error_code ec;
        std::filesystem::remove(entry_path(it->second.ns, it->second.basename), ec);
        bytes_ -= it->second.size;
        entries_.erase(it);
    }

    // Evict lowest-(freq, last_seq) entries until within budget. A hot (high
    // freq) object survives a scan of cold (freq 1) objects.
    void evict_to_budget_() {
        while (bytes_ > max_bytes_ && !entries_.empty()) {
            auto victim = entries_.begin();
            for (auto it = entries_.begin(); it != entries_.end(); ++it) {
                if (it->second.freq < victim->second.freq ||
                    (it->second.freq == victim->second.freq &&
                     it->second.last_seq < victim->second.last_seq)) {
                    victim = it;
                }
            }
            std::error_code ec;
            std::filesystem::remove(entry_path(victim->second.ns, victim->second.basename), ec);
            bytes_ -= victim->second.size;
            entries_.erase(victim);
        }
    }

    std::filesystem::path dir_;
    std::uint64_t max_bytes_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> entries_;
    std::uint64_t bytes_{0};
    std::uint64_t seq_{0};
    std::uint64_t hits_{0};
    std::uint64_t misses_{0};
};

}  // namespace clink::s3
