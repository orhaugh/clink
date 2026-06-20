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
// v1 bounds: same-location, same-parallelism (failover) restore; cross-location
// savepoint relocation and rescale are follow-ons.

#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

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

    clink::BuiltStateBackend out;
    out.backend = std::make_shared<clink::RemoteReadBackend>(
        std::make_shared<S3RemotePool>(o), /*io_threads=*/1, cfg.hot_max_bytes);

    if (!spec.restore_uri.empty() && spec.restore_checkpoint_id != 0) {
        // v1: same-location, same-parallelism failover. The pool reads cp-<id>
        // from this subtask's prefix; the Snapshot is just the marker id (the
        // bytes live in S3). LocalExecutor forwards this to backend->restore().
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
