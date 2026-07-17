// Multi-worker Queryable State tests. Validates that ClusterClient
// brute-iterates the given worker list and surfaces the first hit.
// Full coordinator-discovery integration (real coordinator + 2 registered workers +
// deployed job) is out of scope here; the direct-construction path
// covers the routing logic, and a unit test for the parser closes
// the discovery seam.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_HTTP

#include "clink/core/codec.hpp"
#include "clink/http/http_server.hpp"
#include "clink/queryable_state/bind.hpp"
#include "clink/queryable_state/cluster_client.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/queryable_state/server.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;
using namespace clink::queryable_state;

namespace {

// Owns a backend + slot + Registry + HttpServer for one simulated worker.
// Multiple instances stand in for a multi-worker deployment.
struct WorkerHarness {
    std::shared_ptr<InMemoryStateBackend> backend;
    std::shared_ptr<KeyedState<std::string, std::int64_t>> state;
    Registry registry;
    http::HttpServer server;
    std::uint16_t port{0};

    WorkerHarness(OperatorId op,
                  const std::string& slot,
                  const std::vector<std::pair<std::string, std::int64_t>>& seeds)
        : backend(std::make_shared<InMemoryStateBackend>()) {
        state = std::make_shared<KeyedState<std::string, std::int64_t>>(
            *backend, op, slot, string_codec(), int64_codec());
        for (const auto& [k, v] : seeds) {
            state->put(k, v);
        }
        bind_keyed_state(registry, slot, state, string_codec(), int64_codec());
        register_routes(server, registry);
        port = server.start("127.0.0.1", 0);
    }
    ~WorkerHarness() { server.stop(); }
};

}  // namespace

TEST(QueryableStateCluster, ClusterClientFindsKeyOnFirstWorker) {
    // Both workers host the slot "counts" but only worker-A has the key.
    WorkerHarness worker_a(OperatorId{1}, "counts", {{"alpha", 100}});
    WorkerHarness worker_b(OperatorId{1}, "counts", {{"beta", 200}});

    ClusterClient cc({
        {"127.0.0.1", worker_a.port},
        {"127.0.0.1", worker_b.port},
    });

    auto v = cc.get<std::string, std::int64_t>("counts", "alpha", string_codec(), int64_codec());
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 100);
}

TEST(QueryableStateCluster, ClusterClientFindsKeyOnSecondWorker) {
    // Same shape but the key lives on worker-B. ClusterClient should
    // iterate past worker-A (miss) and return worker-B's value.
    WorkerHarness worker_a(OperatorId{1}, "counts", {{"alpha", 100}});
    WorkerHarness worker_b(OperatorId{1}, "counts", {{"beta", 200}});

    ClusterClient cc({
        {"127.0.0.1", worker_a.port},
        {"127.0.0.1", worker_b.port},
    });

    auto v = cc.get<std::string, std::int64_t>("counts", "beta", string_codec(), int64_codec());
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 200);
}

TEST(QueryableStateCluster, ClusterClientReturnsNulloptWhenNoWorkerHasKey) {
    WorkerHarness worker_a(OperatorId{1}, "counts", {{"alpha", 100}});
    WorkerHarness worker_b(OperatorId{1}, "counts", {{"beta", 200}});

    ClusterClient cc({
        {"127.0.0.1", worker_a.port},
        {"127.0.0.1", worker_b.port},
    });

    auto v = cc.get<std::string, std::int64_t>("counts", "missing", string_codec(), int64_codec());
    EXPECT_FALSE(v.has_value());
}

TEST(QueryableStateCluster, ClusterClientWithEmptyWorkerListReturnsNullopt) {
    ClusterClient cc({});
    auto v = cc.get<std::string, std::int64_t>("counts", "alpha", string_codec(), int64_codec());
    EXPECT_FALSE(v.has_value());
}

#endif  // CLINK_HAS_HTTP
