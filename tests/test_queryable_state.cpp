// Queryable State - end-to-end tests of the registry + HTTP route +
// client triple. Each test stands up an HttpServer on an OS-picked
// port, registers routes against a populated registry, and queries
// through a fresh Client.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_HTTP

#include "clink/core/codec.hpp"
#include "clink/http/http_server.hpp"
#include "clink/queryable_state/bind.hpp"
#include "clink/queryable_state/client.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/queryable_state/server.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;
using namespace clink::queryable_state;
using namespace std::chrono_literals;

namespace {

// Helper that owns a backend + KeyedState + Registry + HttpServer and
// hands back a Client pointed at the bound port. Constructor populates
// the backend with the supplied entries.
struct Harness {
    std::shared_ptr<InMemoryStateBackend> backend;
    std::shared_ptr<KeyedState<std::string, std::int64_t>> state;
    Registry registry;
    http::HttpServer server;
    std::uint16_t port{0};

    Harness(const std::vector<std::pair<std::string, std::int64_t>>& seeds,
            const std::string& slot,
            bool treat_missing_as_404 = true)
        : backend(std::make_shared<InMemoryStateBackend>()) {
        state = std::make_shared<KeyedState<std::string, std::int64_t>>(
            *backend, OperatorId{1}, slot, string_codec(), int64_codec());
        for (const auto& [k, v] : seeds) {
            state->put(k, v);
        }
        bind_keyed_state(registry, slot, state, string_codec(), int64_codec());
        register_routes(server, registry, treat_missing_as_404);
        port = server.start("127.0.0.1", 0);
    }
    ~Harness() { server.stop(); }
};

}  // namespace

TEST(QueryableState, GetReturnsValueForRegisteredKey) {
    Harness h({{"alpha", 1}, {"beta", 22}, {"gamma", 333}}, "counts");

    Client client("127.0.0.1", h.port);
    auto v1 =
        client.get<std::string, std::int64_t>("counts", "alpha", string_codec(), int64_codec());
    auto v2 =
        client.get<std::string, std::int64_t>("counts", "beta", string_codec(), int64_codec());
    auto v3 =
        client.get<std::string, std::int64_t>("counts", "gamma", string_codec(), int64_codec());
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v2.has_value());
    ASSERT_TRUE(v3.has_value());
    EXPECT_EQ(*v1, 1);
    EXPECT_EQ(*v2, 22);
    EXPECT_EQ(*v3, 333);
}

TEST(QueryableState, MissingKeyReturns404AsNullopt) {
    Harness h({{"alpha", 1}}, "counts");

    Client client("127.0.0.1", h.port);
    auto v =
        client.get<std::string, std::int64_t>("counts", "missing", string_codec(), int64_codec());
    EXPECT_FALSE(v.has_value());
}

TEST(QueryableState, UnknownSlotReturns404AsNullopt) {
    Harness h({{"alpha", 1}}, "counts");

    Client client("127.0.0.1", h.port);
    // The slot "other" isn't registered - the server returns 404 and
    // the client surfaces it as nullopt.
    auto v = client.get<std::string, std::int64_t>("other", "alpha", string_codec(), int64_codec());
    EXPECT_FALSE(v.has_value());
}

TEST(QueryableState, SlotListingEnumeratesRegisteredSlots) {
    Harness h({{"alpha", 1}}, "counts");
    // Add a second slot via a fresh KeyedState + bind so the listing
    // endpoint has two entries.
    auto state2 = std::make_shared<KeyedState<std::string, std::int64_t>>(
        *h.backend, OperatorId{1}, "labels", string_codec(), int64_codec());
    state2->put("x", 1);
    bind_keyed_state(h.registry, "labels", state2, string_codec(), int64_codec());

    http::HttpClient http("127.0.0.1", h.port);
    auto resp = http.get("/api/v1/queryable_state");
    ASSERT_EQ(resp.status, 200);
    // Body contains both slot names somewhere in the JSON array.
    EXPECT_NE(resp.body.find("\"counts\""), std::string::npos) << resp.body;
    EXPECT_NE(resp.body.find("\"labels\""), std::string::npos) << resp.body;
}

TEST(QueryableState, UnregisterRemovesSlot) {
    Harness h({{"alpha", 1}}, "counts");
    Client client("127.0.0.1", h.port);

    auto before =
        client.get<std::string, std::int64_t>("counts", "alpha", string_codec(), int64_codec());
    ASSERT_TRUE(before.has_value());

    h.registry.unregister_slot("counts");

    auto after =
        client.get<std::string, std::int64_t>("counts", "alpha", string_codec(), int64_codec());
    EXPECT_FALSE(after.has_value()) << "unregistered slot should look like missing";
}

#endif  // CLINK_HAS_HTTP
