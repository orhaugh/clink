// Token auth on the HTTP control plane (roadmap F6). A server started with a
// token (CLINK_AUTH_TOKEN in production) answers 401 to any request without a
// matching `Authorization: Bearer <token>`, and serves the handler otherwise;
// with no token configured, auth is off (backward compatible).

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "clink/http/http_client.hpp"
#include "clink/http/http_server.hpp"

using namespace clink::http;

namespace {

HttpResponse pong(const HttpRequest&) {
    HttpResponse r;
    r.status = 200;
    r.content_type = "text/plain";
    r.body = "pong";
    return r;
}

}  // namespace

TEST(HttpAuth, TokenGateRejectsUnauthenticatedAndAllowsBearer) {
    HttpServer server;
    server.get("/ping", pong);
    server.set_auth_token("s3cret");
    const std::uint16_t port = server.start("127.0.0.1", 0);

    // No Authorization header -> 401 before the handler runs.
    {
        HttpClient c("127.0.0.1", port);
        const auto r = c.get("/ping");
        EXPECT_EQ(r.status, 401) << r.body;
        EXPECT_NE(r.body, "pong");
    }
    // Wrong token -> 401.
    {
        HttpClient c("127.0.0.1", port);
        c.set_bearer_token("wrong");
        const auto r = c.get("/ping");
        EXPECT_EQ(r.status, 401) << r.body;
    }
    // Correct token -> the handler runs.
    {
        HttpClient c("127.0.0.1", port);
        c.set_bearer_token("s3cret");
        const auto r = c.get("/ping");
        EXPECT_EQ(r.status, 200) << r.body;
        EXPECT_EQ(r.body, "pong");
    }

    server.stop();
}

TEST(HttpAuth, NoTokenConfiguredLeavesAuthOff) {
    HttpServer server;
    server.get("/ping", pong);
    // No set_auth_token: unauthenticated requests are served (trusted-network
    // default, unchanged behaviour).
    const std::uint16_t port = server.start("127.0.0.1", 0);

    HttpClient c("127.0.0.1", port);
    const auto r = c.get("/ping");
    EXPECT_EQ(r.status, 200) << r.body;
    EXPECT_EQ(r.body, "pong");

    server.stop();
}

// The start/stop race that wedged a 0.05s test for its full 180s ctest
// bound in CI (watch item 64): httplib's Server::stop() is a NO-OP until
// the listen thread has set is_running_, so a stop() outrunning a
// slow-starting listen thread was silently lost and the join hung on an
// accept() nobody would ever wake. stop() now waits for the listen loop
// to be ready before stopping it, which makes this hammer deterministic:
// every cycle must complete, including the ones where stop() wins the
// old race (the first iterations, before the thread scheduler warms up,
// are exactly the hostile interleaving). Pre-fix this wedges within a
// handful of cycles on a loaded machine; the ctest timeout is the
// failure detector.
TEST(HttpLifecycle, ImmediateStopAfterStartNeverWedges) {
    for (int i = 0; i < 200; ++i) {
        HttpServer server;
        server.get("/ping", pong);
        (void)server.start("127.0.0.1", 0);
        server.stop();  // as close to start() as the code can express
    }
}
