// Multi-TM Queryable State tests. Validates that ClusterClient
// brute-iterates the given TM list and surfaces the first hit.
// Full JM-discovery integration (real JM + 2 registered TMs +
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

// Owns a backend + slot + Registry + HttpServer for one simulated TM.
// Multiple instances stand in for a multi-TM deployment.
struct TmHarness {
    std::shared_ptr<InMemoryStateBackend> backend;
    std::shared_ptr<KeyedState<std::string, std::int64_t>> state;
    Registry registry;
    http::HttpServer server;
    std::uint16_t port{0};

    TmHarness(OperatorId op,
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
    ~TmHarness() { server.stop(); }
};

}  // namespace

TEST(QueryableStateCluster, ClusterClientFindsKeyOnFirstTm) {
    // Both TMs host the slot "counts" but only TM-A has the key.
    TmHarness tm_a(OperatorId{1}, "counts", {{"alpha", 100}});
    TmHarness tm_b(OperatorId{1}, "counts", {{"beta", 200}});

    ClusterClient cc({
        {"127.0.0.1", tm_a.port},
        {"127.0.0.1", tm_b.port},
    });

    auto v = cc.get<std::string, std::int64_t>("counts", "alpha", string_codec(), int64_codec());
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 100);
}

TEST(QueryableStateCluster, ClusterClientFindsKeyOnSecondTm) {
    // Same shape but the key lives on TM-B. ClusterClient should
    // iterate past TM-A (miss) and return TM-B's value.
    TmHarness tm_a(OperatorId{1}, "counts", {{"alpha", 100}});
    TmHarness tm_b(OperatorId{1}, "counts", {{"beta", 200}});

    ClusterClient cc({
        {"127.0.0.1", tm_a.port},
        {"127.0.0.1", tm_b.port},
    });

    auto v = cc.get<std::string, std::int64_t>("counts", "beta", string_codec(), int64_codec());
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 200);
}

TEST(QueryableStateCluster, ClusterClientReturnsNulloptWhenNoTmHasKey) {
    TmHarness tm_a(OperatorId{1}, "counts", {{"alpha", 100}});
    TmHarness tm_b(OperatorId{1}, "counts", {{"beta", 200}});

    ClusterClient cc({
        {"127.0.0.1", tm_a.port},
        {"127.0.0.1", tm_b.port},
    });

    auto v = cc.get<std::string, std::int64_t>("counts", "missing", string_codec(), int64_codec());
    EXPECT_FALSE(v.has_value());
}

TEST(QueryableStateCluster, ClusterClientWithEmptyTmListReturnsNullopt) {
    ClusterClient cc({});
    auto v = cc.get<std::string, std::int64_t>("counts", "alpha", string_codec(), int64_codec());
    EXPECT_FALSE(v.has_value());
}

#endif  // CLINK_HAS_HTTP
