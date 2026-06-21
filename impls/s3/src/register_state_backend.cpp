// Registers the remote-read:// state-backend scheme: an S3-backed,
// async-capable, disaggregated RemoteReadBackend.
//
//   remote-read://<bucket>/<prefix>[?endpoint=<url>&region=<r>&anonymous=1
//                                    &hot_max_bytes=<n>]
//
// State lives in S3 (content-addressed value objects + per-checkpoint
// manifests via S3RemotePool); cold reads defer to S3 through the async
// execution path; restore is lazy. Per-subtask key prefix is
// "<prefix>/<subtask_idx>". hot_max_bytes bounds the in-memory hot tier so
// working state genuinely exceeds RAM (0 / absent = unbounded hot tier).
//
// Restore covers same-parallelism failover, RESCALE (scale-up/down: the pool
// merges the parent subtasks' checkpoint, key-group-filters to this subtask's
// range, and relocates objects into its prefix via prepare_restore), and
// cross-location relocation (S3RemotePool::export_checkpoint copies a checkpoint
// to a new base, after which a job pointed there restores same-location).

#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/s3/install.hpp"
#include "clink/s3/s3_remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::s3 {
namespace {

std::pair<std::string, std::string> split_uri(const std::string& uri) {
    static constexpr std::string_view sep{"://"};
    const auto pos = uri.find(sep);
    if (pos == std::string::npos) {
        return {{}, uri};
    }
    return {uri.substr(0, pos), uri.substr(pos + sep.size())};
}

struct Cfg {
    std::string bucket;
    std::string prefix;
    std::string endpoint;
    std::string region;
    bool anonymous{false};
    std::size_t hot_max_bytes{0};  // 0 = unbounded hot tier (no eviction)
    // IO concurrency for the completion executor (ASYNC-9). The cold-read load
    // is a blocking S3 GET; one thread serializes every in-flight read, so the
    // default is well above 1. Raise it for high remote-read fan-out.
    std::size_t io_threads{clink::async::kDefaultIoThreads};
};

Cfg parse_cfg(const std::string& base) {
    Cfg c;
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
            if (k == "endpoint") {
                c.endpoint = v;
            } else if (k == "region") {
                c.region = v;
            } else if (k == "anonymous") {
                c.anonymous = (v == "1" || v == "true");
            } else if (k == "hot_max_bytes") {
                try {
                    c.hot_max_bytes = static_cast<std::size_t>(std::stoull(v));
                } catch (...) {
                    c.hot_max_bytes = 0;  // malformed -> unbounded (safe default)
                }
            } else if (k == "io_threads") {
                try {
                    c.io_threads = static_cast<std::size_t>(std::stoull(v));
                } catch (...) {
                    // malformed -> keep the default
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

std::string subtask_prefix(const Cfg& c, std::uint32_t subtask) {
    return c.prefix.empty() ? std::to_string(subtask) : (c.prefix + "/" + std::to_string(subtask));
}

clink::BuiltStateBackend build_remote_read(const clink::StateBackendSpec& spec) {
    const auto [scheme, base] = split_uri(spec.uri);
    (void)scheme;
    const auto cfg = parse_cfg(base);
    if (cfg.bucket.empty()) {
        throw std::runtime_error("remote-read scheme requires a bucket");
    }

    S3RemotePool::Options o;
    o.bucket = cfg.bucket;
    o.prefix = subtask_prefix(cfg, spec.subtask_idx);
    if (!cfg.region.empty()) {
        o.region = cfg.region;
    }
    if (!cfg.endpoint.empty()) {
        o.endpoint_override = cfg.endpoint;
    }
    o.allow_anonymous = cfg.anonymous;

    auto pool = std::make_shared<S3RemotePool>(o);

    // Rescale: this new subtask inherits the committed state of one parent
    // (scale-up) or a contiguous block of parents (scale-down), narrowed to its
    // assigned key-group range. Hand the pool the parent prefixes so restore()'s
    // prepare_restore can merge + key-group-filter + relocate their checkpoint
    // into this subtask's prefix. UINT32_MAX = "restore from own subtask" (the
    // same-parallelism path), which needs no merge.
    if (spec.restore_from_subtask_idx != std::numeric_limits<std::uint32_t>::max()) {
        const std::uint32_t first = spec.restore_from_subtask_idx;
        const std::uint32_t count =
            spec.restore_from_parent_count == 0 ? 1 : spec.restore_from_parent_count;
        std::vector<std::string> parents;
        parents.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            parents.push_back(subtask_prefix(cfg, first + i));
        }
        pool->set_restore_sources(std::move(parents));
    }

    clink::BuiltStateBackend out;
    out.backend =
        std::make_shared<clink::RemoteReadBackend>(pool, cfg.io_threads, cfg.hot_max_bytes);

    if (!spec.restore_uri.empty() && spec.restore_checkpoint_id != 0) {
        // The pool reads cp-<id> from this subtask's prefix (same-parallelism
        // failover) or, on rescale, materialises it from the parent prefixes via
        // prepare_restore. The Snapshot is just the marker id (the bytes live in
        // S3). LocalExecutor forwards this to backend->restore(snap, kg_filter).
        clink::Snapshot snap;
        snap.checkpoint_id = clink::CheckpointId{spec.restore_checkpoint_id};
        out.restore_from = std::move(snap);
    }
    return out;
}

}  // namespace

void install_state_backend() {
    clink::StateBackendFactory::default_instance().register_scheme("remote-read",
                                                                   &build_remote_read);
}

}  // namespace clink::s3
