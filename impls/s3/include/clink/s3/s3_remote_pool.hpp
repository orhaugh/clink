#pragma once

// S3RemotePool - the production RemotePool behind RemoteReadBackend.
//
// Layout under <bucket>/<prefix>:
//   objects/<sha256-hex>      immutable, content-addressed value objects
//   manifests/cp-<id>         per-checkpoint manifest: (op, key) -> {hash, size}
//
// commit(id, base, changed, deleted) loads the base manifest, applies the
// delta (uploading only objects whose content is new - unchanged values share
// an object, so they are never re-uploaded), and writes the new manifest. A
// cold read loads the checkpoint's manifest once (cached) then GETs one value
// object by hash, so restore is lazy: a key is fetched only when first read.
//
// v1 bounds: purge drops a checkpoint's manifest but leaves its (shared) value
// objects; a refcount sweep over live manifests (as in S3CasSnapshotStore)
// reclaims orphans and is the follow-on.

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/filesystem/s3fs.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/core/sha256.hpp"
#include "clink/core/types.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::s3 {

class S3RemotePool final : public clink::RemotePool {
public:
    struct Options {
        std::string bucket;                            // required
        std::string prefix;                            // per-subtask key prefix, e.g. "job/0"
        std::optional<std::string> region;             // explicit region
        std::optional<std::string> endpoint_override;  // MinIO / LocalStack
        bool allow_anonymous{false};
    };

    explicit S3RemotePool(Options opts) : opts_(std::move(opts)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("S3RemotePool: bucket is required");
        }
    }

    void commit(CheckpointId id,
                CheckpointId base,
                const std::vector<RemotePoolEntry>& changed,
                const std::vector<RemotePoolKey>& deleted) override {
        auto fs = fs_();
        Manifest m;
        if (base.value() != 0) {
            // Inherit the base checkpoint. Absent == empty: a subtask that
            // committed nothing at `base` (no keyed state, or a source/sink
            // subtask) has no manifest there, which is not an error.
            m = load_manifest_or_empty_(*fs, base);
        }
        for (const auto& d : deleted) {
            m.erase({d.op.value(), d.key});
        }
        for (const auto& e : changed) {
            const std::string hash = sha256_hex_(e.value);
            const std::string obj_key = object_path_(hash);
            if (!object_present_(*fs, obj_key)) {
                write_object_(*fs, obj_key, to_string_(e.value));  // content-addressed: upload once
            }
            m[{e.op.value(), e.key}] = {hash, e.value.size()};
        }
        // Manifest LAST: only after every referenced object durably exists.
        write_object_(*fs, manifest_path_(id), encode_manifest_(m));
        std::lock_guard<std::mutex> lk(cache_mu_);
        cache_[id.value()] = std::move(m);
    }

    [[nodiscard]] std::optional<StateBackend::Value> read(CheckpointId id,
                                                          OperatorId op,
                                                          const std::string& key) const override {
        const Manifest& m = manifest_for_(id);
        auto it = m.find({op.value(), key});
        if (it == m.end()) {
            return std::nullopt;
        }
        auto fs = fs_();
        const std::string bytes = read_object_(*fs, object_path_(it->second.first));
        return StateBackend::Value{reinterpret_cast<const std::byte*>(bytes.data()),
                                   reinterpret_cast<const std::byte*>(bytes.data() + bytes.size())};
    }

    void purge(CheckpointId id) override {
        std::shared_ptr<arrow::fs::S3FileSystem> fs;
        try {
            fs = fs_();
        } catch (...) {
            return;  // cannot reach the store; leave everything (safe)
        }
        (void)fs->DeleteFile(manifest_path_(id));  // best-effort; objects GC deferred
        std::lock_guard<std::mutex> lk(cache_mu_);
        cache_.erase(id.value());
    }

private:
    // (op, key) -> (value-hash, value-size).
    using ManifestKey = std::pair<std::uint64_t, std::string>;
    using ManifestVal = std::pair<std::string, std::uint64_t>;
    using Manifest = std::map<ManifestKey, ManifestVal>;

    std::string base_prefix_() const {
        return opts_.prefix.empty() ? opts_.bucket : (opts_.bucket + "/" + opts_.prefix);
    }
    std::string object_path_(const std::string& hash) const {
        return base_prefix_() + "/objects/" + hash;
    }
    std::string manifest_path_(CheckpointId id) const {
        return base_prefix_() + "/manifests/cp-" + std::to_string(id.value());
    }

    static std::string to_string_(const StateBackend::Value& v) {
        return std::string(reinterpret_cast<const char*>(v.data()), v.size());
    }

    static std::string sha256_hex_(const StateBackend::Value& v) {
        clink::Sha256 h;
        h.update(v.data(), v.size());
        return clink::Sha256::to_hex(h.finalize());
    }

    // The manifest for `id`, loaded once and cached (cold reads hit it per key).
    const Manifest& manifest_for_(CheckpointId id) const {
        std::lock_guard<std::mutex> lk(cache_mu_);
        auto it = cache_.find(id.value());
        if (it == cache_.end()) {
            auto fs = fs_();
            it = cache_.emplace(id.value(), load_manifest_or_empty_(*fs, id)).first;
        }
        return it->second;
    }

    Manifest load_manifest_(arrow::fs::S3FileSystem& fs, CheckpointId id) const {
        return decode_manifest_(read_object_(fs, manifest_path_(id)));
    }

    // Load a manifest, treating an ABSENT manifest object as an empty one.
    // A checkpoint id with no manifest at this subtask's prefix means the
    // subtask committed no state there (empty keyed state, or a source/sink
    // subtask) - that is "no entries", not an error, matching the in-memory
    // pool double. A present-but-unreadable object still throws (real fault).
    Manifest load_manifest_or_empty_(arrow::fs::S3FileSystem& fs, CheckpointId id) const {
        const auto path = manifest_path_(id);
        if (!object_present_(fs, path)) {
            return Manifest{};
        }
        return decode_manifest_(read_object_(fs, path));
    }

    // --- binary manifest codec (length-prefixed; keys may be arbitrary bytes) ---

    static void put_u32_(std::string& o, std::uint32_t v) {
        for (int i = 0; i < 4; ++i)
            o.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
    static void put_u64_(std::string& o, std::uint64_t v) {
        for (int i = 0; i < 8; ++i)
            o.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
    static std::string encode_manifest_(const Manifest& m) {
        std::string o;
        put_u32_(o, static_cast<std::uint32_t>(m.size()));
        for (const auto& [k, v] : m) {
            put_u64_(o, k.first);  // op
            put_u32_(o, static_cast<std::uint32_t>(k.second.size()));
            o += k.second;  // key bytes
            put_u32_(o, static_cast<std::uint32_t>(v.first.size()));
            o += v.first;           // hash
            put_u64_(o, v.second);  // size
        }
        return o;
    }
    static Manifest decode_manifest_(const std::string& s) {
        Manifest m;
        std::size_t p = 0;
        auto need = [&](std::size_t n) {
            if (p + n > s.size())
                throw std::runtime_error("S3RemotePool: truncated manifest");
        };
        auto u32 = [&]() -> std::uint32_t {
            need(4);
            std::uint32_t v = 0;
            for (int i = 0; i < 4; ++i)
                v |= static_cast<std::uint32_t>(static_cast<unsigned char>(s[p + i])) << (i * 8);
            p += 4;
            return v;
        };
        auto u64 = [&]() -> std::uint64_t {
            need(8);
            std::uint64_t v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<std::uint64_t>(static_cast<unsigned char>(s[p + i])) << (i * 8);
            p += 8;
            return v;
        };
        auto bytes = [&](std::uint32_t n) -> std::string {
            need(n);
            std::string out = s.substr(p, n);
            p += n;
            return out;
        };
        const std::uint32_t count = u32();
        for (std::uint32_t i = 0; i < count; ++i) {
            const std::uint64_t op = u64();
            const std::string key = bytes(u32());
            const std::string hash = bytes(u32());
            const std::uint64_t size = u64();
            m[{op, key}] = {hash, size};
        }
        return m;
    }

    // --- S3 object I/O (mirrors S3CasSnapshotStore) ---

    static bool object_present_(arrow::fs::S3FileSystem& fs, const std::string& key) {
        auto info = fs.GetFileInfo(key);
        return info.ok() && info->type() == arrow::fs::FileType::File;
    }
    static void write_object_(arrow::fs::S3FileSystem& fs,
                              const std::string& key,
                              const std::string& bytes) {
        auto out_result = fs.OpenOutputStream(key);
        if (!out_result.ok()) {
            throw std::runtime_error("S3RemotePool::write OpenOutputStream(" + key +
                                     "): " + out_result.status().ToString());
        }
        auto out = *out_result;
        if (!bytes.empty()) {
            if (auto st = out->Write(bytes.data(), static_cast<std::int64_t>(bytes.size()));
                !st.ok()) {
                throw std::runtime_error("S3RemotePool::write Write(" + key +
                                         "): " + st.ToString());
            }
        }
        if (auto st = out->Close(); !st.ok()) {
            throw std::runtime_error("S3RemotePool::write Close(" + key + "): " + st.ToString());
        }
    }
    static std::string read_object_(arrow::fs::S3FileSystem& fs, const std::string& key) {
        auto file_result = fs.OpenInputFile(key);
        if (!file_result.ok()) {
            throw std::runtime_error("S3RemotePool::read OpenInputFile(" + key +
                                     "): " + file_result.status().ToString());
        }
        auto file = *file_result;
        auto size_result = file->GetSize();
        if (!size_result.ok()) {
            throw std::runtime_error("S3RemotePool::read GetSize(" + key +
                                     "): " + size_result.status().ToString());
        }
        const std::int64_t size = *size_result;
        std::string out(static_cast<std::size_t>(size), '\0');
        if (size > 0) {
            auto n = file->ReadAt(0, size, out.data());
            if (!n.ok()) {
                throw std::runtime_error("S3RemotePool::read ReadAt(" + key +
                                         "): " + n.status().ToString());
            }
        }
        return out;
    }

    std::shared_ptr<arrow::fs::S3FileSystem> fs_() const {
        std::lock_guard<std::mutex> lk(fs_mu_);
        if (fs_cached_) {
            return fs_cached_;
        }
        clink::detail::ensure_arrow_s3_initialised();
        auto s3_opts = arrow::fs::S3Options::Defaults();
        if (opts_.region) {
            s3_opts.region = *opts_.region;
        }
        if (opts_.endpoint_override) {
            s3_opts.endpoint_override = *opts_.endpoint_override;
            s3_opts.scheme = "http";
        }
        if (opts_.allow_anonymous) {
            s3_opts.ConfigureAnonymousCredentials();
        }
        auto fs_result = arrow::fs::S3FileSystem::Make(s3_opts);
        if (!fs_result.ok()) {
            throw std::runtime_error("S3RemotePool: S3FileSystem::Make failed: " +
                                     fs_result.status().ToString());
        }
        fs_cached_ = *fs_result;
        return fs_cached_;
    }

    Options opts_;
    mutable std::mutex fs_mu_;
    mutable std::shared_ptr<arrow::fs::S3FileSystem> fs_cached_;
    mutable std::mutex cache_mu_;
    mutable std::map<std::uint64_t, Manifest> cache_;
};

}  // namespace clink::s3
