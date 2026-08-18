// Registers the "s3" coordination-store scheme: coordination records
// (checkpoint markers, commit receipts, HA manifests) on a bucket via
// S3CoordinationStore.
//
//   s3://<bucket>/<prefix>[?endpoint=<url>&region=<r>]
//
// Same host-registry rule as the state-backend scheme: this runs once at
// startup in the host process, and plugin .so's receive stores by data,
// never from their own RTLD_LOCAL copy of the registry.

#include <memory>
#include <string>
#include <string_view>

#include "clink/cluster/coordination_store.hpp"
#include "clink/s3/install.hpp"
#include "clink/s3/s3_coordination_store.hpp"

namespace clink::s3 {
namespace {

S3CoordinationStore::Options parse_root(const std::string& root_uri) {
    static constexpr std::string_view sep{"://"};
    auto base = root_uri;
    if (const auto pos = base.find(sep); pos != std::string::npos) {
        base = base.substr(pos + sep.size());
    }
    std::string query;
    if (const auto q = base.find('?'); q != std::string::npos) {
        query = base.substr(q + 1);
        base = base.substr(0, q);
    }
    S3CoordinationStore::Options o;
    if (const auto slash = base.find('/'); slash != std::string::npos) {
        o.bucket = base.substr(0, slash);
        o.prefix = base.substr(slash + 1);
    } else {
        o.bucket = base;
    }
    for (std::size_t start = 0; start < query.size();) {
        const auto amp = query.find('&', start);
        const std::string kv =
            query.substr(start, amp == std::string::npos ? std::string::npos : amp - start);
        if (const auto eq = kv.find('='); eq != std::string::npos) {
            const std::string k = kv.substr(0, eq);
            const std::string v = kv.substr(eq + 1);
            if (k == "endpoint") {
                o.endpoint = v;
            } else if (k == "region") {
                o.region = v;
            }
        }
        if (amp == std::string::npos) {
            break;
        }
        start = amp + 1;
    }
    return o;
}

}  // namespace

void install_coordination_store() {
    clink::cluster::register_coordination_store_scheme("s3", [](const std::string& root_uri) {
        return std::make_shared<S3CoordinationStore>(parse_root(root_uri));
    });
}

}  // namespace clink::s3
