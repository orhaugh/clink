#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/rocksdb_s3/install.hpp"
#include "clink/s3/s3_cas_snapshot_store.hpp"
#include "clink/s3/s3_materialization_store.hpp"
#include "clink/s3/s3_snapshot_store.hpp"
#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/rocksdb_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::rocksdb_s3 {
namespace {

namespace fs = std::filesystem;

std::pair<std::string, std::string> split_uri(const std::string& uri) {
    static constexpr std::string_view sep{"://"};
    const auto pos = uri.find(sep);
    if (pos == std::string::npos) {
        return {{}, uri};
    }
    return {uri.substr(0, pos), uri.substr(pos + sep.size())};
}

// Parsed remote-state URI: "<bucket>/<prefix...>[?local=<dir>&endpoint=<url>&
// region=<r>&anonymous=1]". The local dir is where the RocksDB working DB (and,
// for the changelog schemes, the small framing blob) live; the S3 fields target
// MinIO/localstack or an explicit region.
struct S3Cfg {
    std::string bucket;
    std::string prefix;
    std::string local;  // local working-dir root; empty -> temp default
    std::string endpoint;
    std::string region;
    bool anonymous{false};
    std::string cache;             // content-addressed object-cache dir (empty -> temp default)
    std::uint64_t cache_bytes{0};  // 0 -> cache disabled
    bool cas{false};  // content-addressed manifest store (DISAGG-6) vs the cp-dir store
};

S3Cfg parse_cfg(const std::string& base) {
    S3Cfg c;
    std::string path = base;
    std::string query;
    if (const auto q = base.find('?'); q != std::string::npos) {
        path = base.substr(0, q);
        query = base.substr(q + 1);
    }
    if (const auto slash = path.find('/'); slash != std::string::npos) {
        c.bucket = path.substr(0, slash);
        c.prefix = path.substr(slash + 1);
    } else {
        c.bucket = path;
    }
    while (!c.prefix.empty() && c.prefix.back() == '/') {
        c.prefix.pop_back();
    }
    for (std::size_t start = 0; start < query.size();) {
        const auto amp = query.find('&', start);
        const std::string kv =
            query.substr(start, amp == std::string::npos ? std::string::npos : amp - start);
        if (const auto eq = kv.find('='); eq != std::string::npos) {
            const std::string k = kv.substr(0, eq);
            const std::string v = kv.substr(eq + 1);
            if (k == "local") {
                c.local = v;
            } else if (k == "endpoint") {
                c.endpoint = v;
            } else if (k == "region") {
                c.region = v;
            } else if (k == "anonymous") {
                c.anonymous = (v == "1" || v == "true");
            } else if (k == "cas") {
                c.cas = (v == "1" || v == "true");
            } else if (k == "cache") {
                c.cache = v;
            } else if (k == "cache_bytes") {
                // Strict base-10 byte count: a non-empty all-digit token, else
                // disabled. (std::stoull would accept "10MB" as 10 and wrap a
                // leading '-' to a near-unbounded budget.)
                if (!v.empty() && v.find_first_not_of("0123456789") == std::string::npos) {
                    try {
                        c.cache_bytes = std::stoull(v);
                    } catch (...) {
                        c.cache_bytes = 0;  // overflow
                    }
                } else {
                    c.cache_bytes = 0;
                }
            }
        }
        if (amp == std::string::npos) {
            break;
        }
        start = amp + 1;
    }
    return c;
}

fs::path local_root(const S3Cfg& c) {
    return c.local.empty() ? (fs::temp_directory_path() / "clink-rocksdb-s3") : fs::path(c.local);
}

std::string subtask_prefix(const S3Cfg& c, std::uint32_t subtask) {
    return c.prefix.empty() ? std::to_string(subtask) : (c.prefix + "/" + std::to_string(subtask));
}

std::shared_ptr<clink::SnapshotStore> make_snapshot_store(const S3Cfg& c, std::uint32_t subtask) {
    if (c.cas) {
        // DISAGG-6: content-addressed manifest store (objects deduped by hash,
        // checkpoint cost O(changed objects), refcount-GC purge).
        clink::s3::S3CasSnapshotStore::Options o;
        o.bucket = c.bucket;
        o.prefix = subtask_prefix(c, subtask);
        o.subtask = subtask;
        if (!c.region.empty()) {
            o.region = c.region;
        }
        if (!c.endpoint.empty()) {
            o.endpoint_override = c.endpoint;
        }
        o.allow_anonymous = c.anonymous;
        if (c.cache_bytes > 0) {
            o.cache_dir = c.cache;
            o.cache_bytes = c.cache_bytes;
        }
        return std::make_shared<clink::s3::S3CasSnapshotStore>(std::move(o));
    }
    clink::s3::S3SnapshotStore::Options o;
    o.bucket = c.bucket;
    o.prefix = subtask_prefix(c, subtask);
    if (!c.region.empty()) {
        o.region = c.region;
    }
    if (!c.endpoint.empty()) {
        o.endpoint_override = c.endpoint;
    }
    o.allow_anonymous = c.anonymous;
    if (c.cache_bytes > 0) {
        o.cache_dir = c.cache;  // empty -> S3SnapshotStore's temp default
        o.cache_bytes = c.cache_bytes;
    }
    return std::make_shared<clink::s3::S3SnapshotStore>(std::move(o));
}

std::shared_ptr<clink::s3::S3MaterializationStore> make_mat_store(const S3Cfg& c,
                                                                  std::uint32_t subtask) {
    clink::s3::S3MaterializationStore::Options o;
    o.bucket = c.bucket;
    o.prefix = subtask_prefix(c, subtask) + "/mat";
    if (!c.region.empty()) {
        o.region = c.region;
    }
    if (!c.endpoint.empty()) {
        o.endpoint_override = c.endpoint;
    }
    o.allow_anonymous = c.anonymous;
    return std::make_shared<clink::s3::S3MaterializationStore>(std::move(o));
}

// Read N local framing blobs (changelog-<id>.snap, self-persisted by the
// changelog backend's set_snapshot_dir) under <root>/<src+i>/ and pack them.
// Mirrors build_changelog_file's restore; the materialization PAYLOADS they
// reference live in S3 and are fetched by the S3 mat store during restore.
std::vector<std::byte> read_local_framing_blobs(const fs::path& root,
                                                std::uint32_t src_first,
                                                std::uint32_t parent_count,
                                                std::uint64_t cp_id) {
    std::vector<std::vector<std::byte>> blobs;
    for (std::uint32_t i = 0; i < parent_count; ++i) {
        const fs::path blob =
            root / std::to_string(src_first + i) / ("changelog-" + std::to_string(cp_id) + ".snap");
        std::ifstream in(blob, std::ios::binary);
        if (!in) {
            throw std::runtime_error("changelog+s3 restore: framing blob not found: " +
                                     blob.string());
        }
        std::vector<std::byte> bytes;
        for (std::istreambuf_iterator<char> it{in}, end; it != end; ++it) {
            bytes.push_back(static_cast<std::byte>(*it));
        }
        blobs.push_back(std::move(bytes));
    }
    return clink::ChangelogStateBackend::frame_blobs(blobs);
}

// s3+rocksdb://: RocksDB on a local working dir; checkpoint dirs uploaded to S3
// via S3SnapshotStore. Restore builds the s3 dir handle(s) for the assigned
// parent subtask(s) and defers the download to restore() (no inline read).
BuiltStateBackend build_s3_rocksdb(const StateBackendSpec& spec) {
    const auto [scheme, base] = split_uri(spec.uri);
    (void)scheme;
    const auto cfg = parse_cfg(base);
    if (cfg.bucket.empty()) {
        throw std::runtime_error("s3+rocksdb scheme requires a bucket");
    }
    const fs::path subtask_local = local_root(cfg) / std::to_string(spec.subtask_idx);
    std::error_code ec;
    fs::create_directories(subtask_local, ec);  // RocksDB::Open mkdirs only the leaf

    clink::RocksDBStateBackend::Options ropts;
    ropts.path = subtask_local.string();
    ropts.create_if_missing = true;
    ropts.snapshot_store = make_snapshot_store(cfg, spec.subtask_idx);

    BuiltStateBackend out;
    out.backend = std::make_shared<clink::RocksDBStateBackend>(std::move(ropts));

    if (!spec.restore_uri.empty() && spec.restore_checkpoint_id != 0) {
        const auto [rscheme, rbase] = split_uri(spec.restore_uri);
        (void)rscheme;
        const auto rcfg = parse_cfg(rbase);
        const bool is_rescale =
            spec.restore_from_subtask_idx != std::numeric_limits<std::uint32_t>::max();
        const std::uint32_t src_first =
            is_rescale ? spec.restore_from_subtask_idx : spec.subtask_idx;
        const std::uint32_t parent_count =
            spec.restore_from_parent_count == 0 ? 1 : spec.restore_from_parent_count;
        // Each handle matches what that parent's store wrote and is what its
        // fetch_checkpoint_dir expects: for the cp-dir store
        // <bucket>/<prefix>/<parent>/cp-<id>; for the CAS store (cas=1) the
        // manifest key <bucket>/<prefix>/<parent>/manifests/cp-<id>.manifest.
        std::string joined;
        for (std::uint32_t i = 0; i < parent_count; ++i) {
            const std::string parent_base = rcfg.bucket + "/" + subtask_prefix(rcfg, src_first + i);
            const std::string id_str = std::to_string(spec.restore_checkpoint_id);
            const std::string h = cfg.cas ? (parent_base + "/manifests/cp-" + id_str + ".manifest")
                                          : (parent_base + "/cp-" + id_str);
            if (!joined.empty()) {
                joined.push_back('\n');
            }
            joined += h;
        }
        Snapshot snap;
        snap.checkpoint_id = CheckpointId{spec.restore_checkpoint_id};
        snap.bytes.assign(reinterpret_cast<const std::byte*>(joined.data()),
                          reinterpret_cast<const std::byte*>(joined.data() + joined.size()));
        out.restore_from = std::move(snap);
    }
    return out;
}

// changelog+s3[+rocksdb]://: changelog over an InMemory (or local RocksDB)
// inner; materialization payloads go to S3 via S3MaterializationStore. The
// small framing blob self-persists to the local working dir (the payload is
// the large, disaggregated part). Restore reads the local framing blob(s) and
// the changelog backend fetches the S3 payloads through the mat store.
BuiltStateBackend build_changelog_s3_inner(const StateBackendSpec& spec, bool rocksdb_inner) {
    const auto [scheme, base] = split_uri(spec.uri);
    (void)scheme;
    const auto cfg = parse_cfg(base);
    if (cfg.bucket.empty()) {
        throw std::runtime_error("changelog+s3 scheme requires a bucket");
    }
    const fs::path subtask_dir = local_root(cfg) / std::to_string(spec.subtask_idx);
    std::error_code ec;
    fs::create_directories(subtask_dir, ec);

    std::shared_ptr<StateBackend> inner;
    if (rocksdb_inner) {
        const fs::path inner_dir = subtask_dir / "inner";
        fs::create_directories(inner_dir, ec);
        clink::RocksDBStateBackend::Options io;
        io.path = inner_dir.string();
        io.create_if_missing = true;
        inner = std::make_shared<clink::RocksDBStateBackend>(std::move(io));
    } else {
        inner = std::make_shared<clink::InMemoryStateBackend>();
    }
    auto changelog = std::make_shared<clink::ChangelogStateBackend>(
        std::move(inner), make_mat_store(cfg, spec.subtask_idx));
    changelog->set_snapshot_dir(subtask_dir);  // framing blob self-persists locally

    BuiltStateBackend out;
    out.backend = changelog;

    if (!spec.restore_uri.empty() && spec.restore_checkpoint_id != 0) {
        const auto [rscheme, rbase] = split_uri(spec.restore_uri);
        (void)rscheme;
        const auto rcfg = parse_cfg(rbase);
        const bool is_rescale =
            spec.restore_from_subtask_idx != std::numeric_limits<std::uint32_t>::max();
        const std::uint32_t src_first =
            is_rescale ? spec.restore_from_subtask_idx : spec.subtask_idx;
        const std::uint32_t parent_count =
            spec.restore_from_parent_count == 0 ? 1 : spec.restore_from_parent_count;
        out.restore_from =
            Snapshot{CheckpointId{spec.restore_checkpoint_id},
                     read_local_framing_blobs(
                         local_root(rcfg), src_first, parent_count, spec.restore_checkpoint_id)};
    }
    return out;
}

BuiltStateBackend build_changelog_s3(const StateBackendSpec& spec) {
    return build_changelog_s3_inner(spec, /*rocksdb_inner=*/false);
}

BuiltStateBackend build_changelog_s3_rocksdb(const StateBackendSpec& spec) {
    return build_changelog_s3_inner(spec, /*rocksdb_inner=*/true);
}

}  // namespace

void install() {
    auto& f = clink::StateBackendFactory::default_instance();
    f.register_scheme("s3+rocksdb", &build_s3_rocksdb);
    f.register_scheme("changelog+s3+rocksdb", &build_changelog_s3_rocksdb);
    f.register_scheme("changelog+s3", &build_changelog_s3);
}

}  // namespace clink::rocksdb_s3
