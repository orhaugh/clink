// Key-group-aware Queryable State routing tests.
//
// Two layers exercised:
//   1. JobManager::route_key_for_job - given a job, role, and key,
//      the JM resolves which subtask owns the key's key-group and
//      returns the TM HTTP target. Verifies the kg_range filter
//      (deploy_internal_ now fills these from per-role parallelism).
//   2. HTTP route + RoutedClient - register_jm_routes on an
//      HttpServer in front of the JM; RoutedClient against the JM
//      walks the full lookup -> TM-query path.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_HTTP

#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/task_manager.hpp"
#include "clink/core/codec.hpp"
#include "clink/http/http_server.hpp"
#include "clink/queryable_state/bind.hpp"
#include "clink/queryable_state/client.hpp"
#include "clink/queryable_state/cluster_client.hpp"
#include "clink/queryable_state/jm_routes.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/queryable_state/server.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace clink::queryable_state;
using namespace std::chrono_literals;

namespace {

// One simulated TM with: a queryable-state Registry + HttpServer on
// its own port, plus a TaskManager wired to advertise that port. The
// role handler blocks until cancelled so the test can observe the
// JM's routing state while tasks are "running".
struct TmHarness {
    InMemoryStateBackend backend;
    std::shared_ptr<KeyedState<std::string, std::int64_t>> state;
    Registry registry;
    http::HttpServer http_server;
    std::uint16_t http_port{0};
    std::unique_ptr<TaskManager> tm;

    TmHarness(const std::string& tm_id, std::uint32_t subtask_idx = 0) {
        state = std::make_shared<KeyedState<std::string, std::int64_t>>(
            backend, OperatorId{1}, "counts", string_codec(), int64_codec());
        // Bind both bare and subtask-scoped variants so this harness
        // can back tests that use Client (bare slot) AND tests that
        // use RoutedClient (subtask-scoped via the JM's /route).
        bind_keyed_state(registry, "counts", state, string_codec(), int64_codec());
        bind_keyed_state_for_subtask(
            registry, "worker", subtask_idx, "counts", state, string_codec(), int64_codec());
        register_routes(http_server, registry);
        http_port = http_server.start("127.0.0.1", 0);

        TaskManager::Config cfg;
        cfg.http_port = http_port;
        tm = std::make_unique<TaskManager>(tm_id, "127.0.0.1", cfg);
        tm->register_role("worker", [this](const DeploymentTask&) {
            while (!tm->was_cancelled()) {
                std::this_thread::sleep_for(10ms);
            }
        });
    }

    void connect(std::uint16_t jm_port) { tm->connect_to_jm("127.0.0.1", jm_port); }

    void shutdown() {
        if (tm) {
            tm->stop();
        }
        http_server.stop();
    }
};

JobPlan two_subtask_plan() {
    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-a",
        .role = "worker",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    plan.tasks.push_back(PlannedTask{
        .tm_id = "tm-b",
        .role = "worker",
        .subtask_idx = 1,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    return plan;
}

}  // namespace

TEST(QueryableStateRouting, JmRoutesKeysAcrossTwoSubtasksByKeyGroup) {
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    TmHarness tm_a("tm-a");
    TmHarness tm_b("tm-b");
    tm_a.connect(jm_port);
    tm_b.connect(jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));
    jm.deploy(two_subtask_plan());

    // Probe a slate of keys: routing must hit both subtasks, and
    // every probed key must resolve. With p=2 contiguous ranges,
    // subtask 0 owns kg [0, 64) and subtask 1 owns [64, 128); the
    // TMs map subtask_idx -> http_port via their advertised ports.
    std::unordered_map<std::uint32_t, int> subtask_hit_count;
    std::unordered_map<std::uint16_t, int> port_hit_count;
    for (int i = 0; i < 256; ++i) {
        const std::string k = "key-" + std::to_string(i);
        const auto bytes = string_codec().encode(k);
        auto target = jm.route_key_for_job(
            1, "worker", std::span<const std::byte>{bytes.data(), bytes.size()});
        ASSERT_TRUE(target.has_value()) << "key " << k << " did not route";
        ++subtask_hit_count[target->subtask_idx];
        ++port_hit_count[target->port];
    }
    EXPECT_GT(subtask_hit_count[0], 0);
    EXPECT_GT(subtask_hit_count[1], 0);
    EXPECT_EQ(subtask_hit_count[0] + subtask_hit_count[1], 256);
    // Both TMs' advertised HTTP ports show up in the routing.
    EXPECT_GT(port_hit_count[tm_a.http_port], 0);
    EXPECT_GT(port_hit_count[tm_b.http_port], 0);

    // Routing for an unknown role yields nullopt.
    {
        const auto bytes = string_codec().encode(std::string{"any"});
        EXPECT_FALSE(jm.route_key_for_job(1,
                                          "no-such-role",
                                          std::span<const std::byte>{bytes.data(), bytes.size()})
                         .has_value());
    }
    // Routing for an unknown job yields nullopt.
    {
        const auto bytes = string_codec().encode(std::string{"any"});
        EXPECT_FALSE(jm.route_key_for_job(
                           9999, "worker", std::span<const std::byte>{bytes.data(), bytes.size()})
                         .has_value());
    }

    jm.cancel_job(1);
    ASSERT_TRUE(jm.await_completion(2s));
    tm_a.shutdown();
    tm_b.shutdown();
    jm.stop();
}

TEST(QueryableStateRouting, HttpRouteEndpointReturnsRoutingTarget) {
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    TmHarness tm_a("tm-a");
    TmHarness tm_b("tm-b");
    tm_a.connect(jm_port);
    tm_b.connect(jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));
    jm.deploy(two_subtask_plan());

    http::HttpServer jm_http;
    register_jm_routes(jm_http, jm);
    const std::uint16_t jm_http_port = jm_http.start("127.0.0.1", 0);

    // Round-trip: ask the JM-method what the answer should be, then
    // request the same via HTTP and assert the response carries the
    // host/port/subtask_idx values.
    {
        const auto bytes = string_codec().encode(std::string{"alpha"});
        const auto expected = jm.route_key_for_job(
            1, "worker", std::span<const std::byte>{bytes.data(), bytes.size()});
        ASSERT_TRUE(expected.has_value());

        http::HttpClient http("127.0.0.1", jm_http_port);
        const auto hex = queryable_state::detail::hex_encode(
            std::span<const std::byte>{bytes.data(), bytes.size()});
        auto resp = http.get("/api/v1/queryable_state/job/1/op/worker/route?key=" + hex);
        ASSERT_EQ(resp.status, 200) << resp.body;
        EXPECT_NE(resp.body.find("\"port\":" + std::to_string(expected->port)), std::string::npos);
        EXPECT_NE(resp.body.find("\"subtask_idx\":" + std::to_string(expected->subtask_idx)),
                  std::string::npos);
    }
    // Unknown role -> 404.
    {
        const auto bytes = string_codec().encode(std::string{"alpha"});
        const auto hex = queryable_state::detail::hex_encode(
            std::span<const std::byte>{bytes.data(), bytes.size()});
        http::HttpClient http("127.0.0.1", jm_http_port);
        auto resp = http.get("/api/v1/queryable_state/job/1/op/missing/route?key=" + hex);
        EXPECT_EQ(resp.status, 404);
    }
    // Malformed hex -> 400.
    {
        http::HttpClient http("127.0.0.1", jm_http_port);
        auto resp = http.get("/api/v1/queryable_state/job/1/op/worker/route?key=zzzz");
        EXPECT_EQ(resp.status, 400);
    }

    jm_http.stop();
    jm.cancel_job(1);
    ASSERT_TRUE(jm.await_completion(2s));
    tm_a.shutdown();
    tm_b.shutdown();
    jm.stop();
}

TEST(QueryableStateRouting, SubtaskScopedSlotsDontCollideOnSameTm) {
    // Two subtasks of the same op on the same Registry: bind each
    // under (worker, subtask_idx, "counts") with DIFFERENT contents.
    // Verify that Client::get(role, subtask_idx, slot, key) returns
    // the right value for each subtask - proving subtask-scoping
    // namespaces the slot correctly.
    InMemoryStateBackend backend_a;
    InMemoryStateBackend backend_b;
    auto state_a = std::make_shared<KeyedState<std::string, std::int64_t>>(
        backend_a, OperatorId{1}, "counts", string_codec(), int64_codec());
    auto state_b = std::make_shared<KeyedState<std::string, std::int64_t>>(
        backend_b, OperatorId{1}, "counts", string_codec(), int64_codec());
    state_a->put("alpha", 100);
    state_b->put("alpha", 999);

    Registry reg;
    bind_keyed_state_for_subtask(
        reg, "worker", 0, "counts", state_a, string_codec(), int64_codec());
    bind_keyed_state_for_subtask(
        reg, "worker", 1, "counts", state_b, string_codec(), int64_codec());
    http::HttpServer server;
    register_routes(server, reg);
    const std::uint16_t port = server.start("127.0.0.1", 0);

    Client c("127.0.0.1", port);
    auto a0 = c.get<std::string, std::int64_t>(
        "worker", 0, "counts", "alpha", string_codec(), int64_codec());
    auto a1 = c.get<std::string, std::int64_t>(
        "worker", 1, "counts", "alpha", string_codec(), int64_codec());
    ASSERT_TRUE(a0.has_value());
    ASSERT_TRUE(a1.has_value());
    EXPECT_EQ(*a0, 100);
    EXPECT_EQ(*a1, 999);

    server.stop();
}

TEST(QueryableStateRouting, RoutedClientCachesRoutesPerKeyGroup) {
    // Two subtasks of "worker", two TMs. RoutedClient queries the
    // same key 5 times - the JM /route endpoint should be hit only
    // ONCE (the first time); subsequent lookups use the cached
    // (kg → TmTarget) entry and skip straight to the TM.
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    TmHarness tm_a("tm-a", /*subtask_idx=*/0);
    TmHarness tm_b("tm-b", /*subtask_idx=*/1);
    tm_a.connect(jm_port);
    tm_b.connect(jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));
    jm.deploy(two_subtask_plan());

    http::HttpServer jm_http;
    register_jm_routes(jm_http, jm);
    const std::uint16_t jm_http_port = jm_http.start("127.0.0.1", 0);

    // Seed the same key on whichever subtask owns its kg.
    const std::string k = "alpha";
    const auto bytes = string_codec().encode(k);
    const auto kg = key_group_for_key(std::span<const std::byte>{bytes.data(), bytes.size()});
    if (kg < 64) {
        tm_a.state->put(k, 42);
    } else {
        tm_b.state->put(k, 42);
    }

    RoutedClient rc("127.0.0.1", jm_http_port, JobId{1}, "worker");
    for (int i = 0; i < 5; ++i) {
        auto v = rc.get<std::string, std::int64_t>("counts", k, string_codec(), int64_codec());
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, 42);
    }
    // First get: 1 JM hit. Cached entry serves the next 4.
    EXPECT_EQ(rc.jm_route_requests(), 1u);

    jm_http.stop();
    jm.cancel_job(1);
    ASSERT_TRUE(jm.await_completion(2s));
    tm_a.shutdown();
    tm_b.shutdown();
    jm.stop();
}

TEST(QueryableStateRouting, RoutedClientEvictsCacheOn404FromTm) {
    // When the TM returns 404 (could be a normal miss or stale
    // routing post-rescale), the cache entry is evicted and the
    // next lookup hits the JM again. Verified by querying a key
    // that doesn't exist and confirming JM hit count grows.
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    TmHarness tm_a("tm-a", /*subtask_idx=*/0);
    TmHarness tm_b("tm-b", /*subtask_idx=*/1);
    tm_a.connect(jm_port);
    tm_b.connect(jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));
    jm.deploy(two_subtask_plan());

    http::HttpServer jm_http;
    register_jm_routes(jm_http, jm);
    const std::uint16_t jm_http_port = jm_http.start("127.0.0.1", 0);

    RoutedClient rc("127.0.0.1", jm_http_port, JobId{1}, "worker");
    // Same missing key, queried 3 times. Each TM 404 evicts the
    // cache, so each get() hits the JM.
    for (int i = 0; i < 3; ++i) {
        auto v =
            rc.get<std::string, std::int64_t>("counts", "missing", string_codec(), int64_codec());
        EXPECT_FALSE(v.has_value());
    }
    EXPECT_EQ(rc.jm_route_requests(), 3u);

    jm_http.stop();
    jm.cancel_job(1);
    ASSERT_TRUE(jm.await_completion(2s));
    tm_a.shutdown();
    tm_b.shutdown();
    jm.stop();
}

TEST(QueryableStateRouting, RoutedClientChecksTopologyVersionAfterInterval) {
    // RoutedClient polls topology_version on the configured interval.
    // After the interval expires, the next get() hits the version
    // endpoint. Unchanged version leaves the cache intact (route
    // count stays at 1 for the cached key).
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    TmHarness tm_a("tm-a", /*subtask_idx=*/0);
    TmHarness tm_b("tm-b", /*subtask_idx=*/1);
    tm_a.connect(jm_port);
    tm_b.connect(jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));
    jm.deploy(two_subtask_plan());

    http::HttpServer jm_http;
    register_jm_routes(jm_http, jm);
    const std::uint16_t jm_http_port = jm_http.start("127.0.0.1", 0);

    const std::string k = "alpha";
    const auto bytes = string_codec().encode(k);
    const auto kg = key_group_for_key(std::span<const std::byte>{bytes.data(), bytes.size()});
    if (kg < 64) {
        tm_a.state->put(k, 42);
    } else {
        tm_b.state->put(k, 42);
    }

    RoutedClient rc("127.0.0.1", jm_http_port, JobId{1}, "worker");
    rc.set_topology_version_check_interval(50ms);

    // First call → 1 route hit + 1 topology_version hit (initial).
    ASSERT_TRUE((rc.get<std::string, std::int64_t>("counts", k, string_codec(), int64_codec())));
    const auto routes_after_first = rc.jm_route_requests();
    const auto version_checks_after_first = rc.jm_topology_version_requests();
    EXPECT_EQ(routes_after_first, 1u);

    // Within interval: no additional version check.
    ASSERT_TRUE((rc.get<std::string, std::int64_t>("counts", k, string_codec(), int64_codec())));
    EXPECT_EQ(rc.jm_route_requests(), 1u);
    EXPECT_EQ(rc.jm_topology_version_requests(), version_checks_after_first);

    // Past interval: version is re-checked. Same value, cache stays.
    std::this_thread::sleep_for(80ms);
    ASSERT_TRUE((rc.get<std::string, std::int64_t>("counts", k, string_codec(), int64_codec())));
    EXPECT_EQ(rc.jm_route_requests(), 1u) << "cache should still serve after a no-op check";
    EXPECT_GT(rc.jm_topology_version_requests(), version_checks_after_first);

    jm_http.stop();
    jm.cancel_job(1);
    ASSERT_TRUE(jm.await_completion(2s));
    tm_a.shutdown();
    tm_b.shutdown();
    jm.stop();
}

TEST(QueryableStateRouting, TopologyVersionEndpointReturnsCurrentValue) {
    // Direct HTTP test of the /topology_version endpoint. Returns 0
    // for unknown jobs; returns 1 after initial deploy.
    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    TmHarness tm_a("tm-a", /*subtask_idx=*/0);
    TmHarness tm_b("tm-b", /*subtask_idx=*/1);
    tm_a.connect(jm_port);
    tm_b.connect(jm_port);
    ASSERT_TRUE(jm.await_registrations(2s));

    http::HttpServer jm_http;
    register_jm_routes(jm_http, jm);
    const std::uint16_t jm_http_port = jm_http.start("127.0.0.1", 0);

    // Unknown job → version 0.
    {
        http::HttpClient http("127.0.0.1", jm_http_port);
        auto resp = http.get("/api/v1/queryable_state/job/9999/topology_version");
        ASSERT_EQ(resp.status, 200);
        EXPECT_NE(resp.body.find("\"version\":0"), std::string::npos) << resp.body;
    }

    jm.deploy(two_subtask_plan());

    // Just-deployed job → version 1.
    {
        http::HttpClient http("127.0.0.1", jm_http_port);
        auto resp = http.get("/api/v1/queryable_state/job/1/topology_version");
        ASSERT_EQ(resp.status, 200);
        EXPECT_NE(resp.body.find("\"version\":1"), std::string::npos) << resp.body;
    }

    jm_http.stop();
    jm.cancel_job(1);
    ASSERT_TRUE(jm.await_completion(2s));
    tm_a.shutdown();
    tm_b.shutdown();
    jm.stop();
}

TEST(QueryableStateRouting, RoutedClientHopsThroughJmAndFetchesValueFromCorrectTm) {
    // Full end-to-end through RoutedClient. Each TM's slot is seeded
    // with the keys whose key-group routes to that subtask. The
    // routed client computes nothing itself - it asks the JM, then
    // queries the routed TM and pulls back the value.

    JobManager jm;
    const std::uint16_t jm_port = jm.start();
    jm.expect_tms({"tm-a", "tm-b"});

    // tm-a hosts worker subtask 0; tm-b hosts worker subtask 1.
    // Bind the queryable slot under both bare and subtask-scoped
    // names so RoutedClient's per-subtask lookup hits the matching
    // registration on each TM.
    TmHarness tm_a("tm-a", /*subtask_idx=*/0);
    TmHarness tm_b("tm-b", /*subtask_idx=*/1);
    tm_a.connect(jm_port);
    tm_b.connect(jm_port);

    ASSERT_TRUE(jm.await_registrations(2s));
    jm.deploy(two_subtask_plan());

    http::HttpServer jm_http;
    register_jm_routes(jm_http, jm);
    const std::uint16_t jm_http_port = jm_http.start("127.0.0.1", 0);

    // Seed each TM with keys belonging to its kg-range.
    std::vector<std::pair<std::string, std::int64_t>> seed_a, seed_b;
    for (int i = 0; i < 32; ++i) {
        const std::string k = "k-" + std::to_string(i);
        const auto bytes = string_codec().encode(k);
        const auto kg = key_group_for_key(std::span<const std::byte>{bytes.data(), bytes.size()});
        const std::int64_t v = 1000 + i;
        if (kg < 64) {
            seed_a.emplace_back(k, v);
        } else {
            seed_b.emplace_back(k, v);
        }
    }
    for (const auto& [k, v] : seed_a) {
        tm_a.state->put(k, v);
    }
    for (const auto& [k, v] : seed_b) {
        tm_b.state->put(k, v);
    }

    RoutedClient rc("127.0.0.1", jm_http_port, JobId{1}, "worker");
    for (const auto& [k, v] : seed_a) {
        auto got = rc.get<std::string, std::int64_t>("counts", k, string_codec(), int64_codec());
        ASSERT_TRUE(got.has_value()) << "miss for " << k;
        EXPECT_EQ(*got, v) << "wrong value for " << k;
    }
    for (const auto& [k, v] : seed_b) {
        auto got = rc.get<std::string, std::int64_t>("counts", k, string_codec(), int64_codec());
        ASSERT_TRUE(got.has_value()) << "miss for " << k;
        EXPECT_EQ(*got, v) << "wrong value for " << k;
    }
    // A key absent from all TMs returns nullopt (the routed TM
    // responds 404).
    {
        auto got = rc.get<std::string, std::int64_t>(
            "counts", "no-such-key", string_codec(), int64_codec());
        EXPECT_FALSE(got.has_value());
    }

    jm_http.stop();
    jm.cancel_job(1);
    ASSERT_TRUE(jm.await_completion(2s));
    tm_a.shutdown();
    tm_b.shutdown();
    jm.stop();
}

#endif  // CLINK_HAS_HTTP
