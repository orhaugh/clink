// A standby Coordinator forwards its HTTP API to the leader, so a client that
// reaches either Coordinator gets the leader's answer. These pin the pieces:
// forward_request carries the request on as it was received, the server's
// forwarder answers after auth and before the handler, and a standby answers
// what is about itself, forwards the rest, and refuses rather than forward a
// request twice or to itself.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/ha_coordinator.hpp"
#include "clink/cluster/standby_http.hpp"
#include "clink/http/forward.hpp"
#include "clink/http/http_client.hpp"
#include "clink/http/http_server.hpp"
#include "clink/http/json_writer.hpp"

using namespace std::chrono_literals;
using clink::http::HttpRequest;
using clink::http::HttpResponse;

namespace {

// Answers with what it was asked: method, path, query `x`, the headers a
// forward carries, the body or the uploaded part, in a content type of its own.
HttpResponse echo(const HttpRequest& req) {
    clink::http::JsonWriter w;
    w.begin_object();
    w.kv("method", req.method);
    w.kv("path", req.path);
    const auto field = [&](const auto& map, const char* key) {
        const auto it = map.find(key);
        return it == map.end() ? std::string{"-"} : it->second;
    };
    w.kv("x", field(req.query, "x"));
    w.kv("authorization", field(req.headers, "authorization"));
    w.kv("forwarded", field(req.headers, "x-clink-forwarded"));
    w.kv("content_type", field(req.headers, "content-type"));
    w.kv("body", req.body);
    const auto part = req.files.find("job_so");
    w.kv("part", part == req.files.end() ? std::string{"-"} : part->second.content);
    w.end_object();
    HttpResponse r;
    r.content_type = "application/vnd.test+json";
    r.headers["Retry-After"] = "7";
    r.body = w.str();
    return r;
}

struct Echo {
    clink::http::HttpServer server;
    std::uint16_t port{0};
    Echo() {
        server.get("/api/v1/echo", echo);
        server.post("/api/v1/echo", echo);
        server.post("/api/v1/jobs", echo);
        port = server.start("127.0.0.1", 0);
    }
};

HttpRequest request(std::string method, std::string path) {
    HttpRequest r;
    r.method = std::move(method);
    r.path = std::move(path);
    return r;
}

std::filesystem::path fresh_ha_dir() {
    static int counter = 0;
    auto dir =
        std::filesystem::temp_directory_path() /
        ("clink_standby_http_" + std::to_string(::getpid()) + "_" + std::to_string(++counter));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

bool eventually(const std::function<bool()>& pred) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

}  // namespace

TEST(HttpForward, ARequestReachesTheOtherNodeAsThisNodeReceivedIt) {
    Echo leader;
    auto get = request("GET", "/api/v1/echo");
    get.query["x"] = "a b&c";
    get.headers["authorization"] = "Bearer t0k";
    get.headers["cookie"] = "not forwarded";
    const auto got = clink::http::forward_request(get, "127.0.0.1", leader.port, 5s);
    EXPECT_EQ(got.status, 200);
    EXPECT_EQ(got.content_type, "application/vnd.test+json");
    EXPECT_EQ(got.headers.at("Retry-After"), "7");
    EXPECT_NE(got.body.find(R"("x":"a b&c")"), std::string::npos) << got.body;
    EXPECT_NE(got.body.find(R"("authorization":"Bearer t0k")"), std::string::npos) << got.body;
    EXPECT_NE(got.body.find(R"("forwarded":"1")"), std::string::npos) << got.body;

    auto post = request("POST", "/api/v1/echo");
    post.headers["content-type"] = "text/plain";
    post.body = "INSERT INTO t SELECT 1";
    const auto posted = clink::http::forward_request(post, "127.0.0.1", leader.port, 5s);
    EXPECT_NE(posted.body.find(R"("body":"INSERT INTO t SELECT 1")"), std::string::npos)
        << posted.body;
    EXPECT_NE(posted.body.find(R"("content_type":"text/plain")"), std::string::npos) << posted.body;

    auto upload = request("POST", "/api/v1/jobs");
    upload.files["job_so"] = {"job.so", "application/octet-stream", "ELF..."};
    const auto uploaded = clink::http::forward_request(upload, "127.0.0.1", leader.port, 5s);
    EXPECT_NE(uploaded.body.find(R"("part":"ELF...")"), std::string::npos) << uploaded.body;
}

TEST(HttpForward, ANodeThatDoesNotAnswerIsA502NamingIt) {
    std::uint16_t closed = 0;
    {
        Echo gone;
        closed = gone.port;
    }
    const auto got =
        clink::http::forward_request(request("GET", "/api/v1/jobs"), "127.0.0.1", closed, 2s);
    EXPECT_EQ(got.status, 502);
    EXPECT_NE(got.body.find("127.0.0.1:" + std::to_string(closed) + " did not answer"),
              std::string::npos)
        << got.body;
}

TEST(HttpServerForwarder, TheForwarderAnswersAfterAuthAndBeforeTheHandler) {
    clink::http::HttpServer server;
    server.get("/api/v1/mine", [](const HttpRequest&) {
        HttpResponse r;
        r.body = "handler";
        return r;
    });
    server.get("/api/v1/theirs", [](const HttpRequest&) {
        HttpResponse r;
        r.body = "handler";
        return r;
    });
    std::atomic<int> consulted{0};
    server.set_forwarder([&](const HttpRequest& req) -> std::optional<HttpResponse> {
        ++consulted;
        if (req.path != "/api/v1/theirs") {
            return std::nullopt;
        }
        HttpResponse r;
        r.body = "forwarded";
        return r;
    });
    server.set_auth_token("t0k");
    const auto port = server.start("127.0.0.1", 0);

    clink::http::HttpClient anonymous("127.0.0.1", port);
    EXPECT_EQ(anonymous.get("/api/v1/theirs").status, 401);
    EXPECT_EQ(consulted, 0) << "a request auth refuses never reaches the forwarder";

    clink::http::HttpClient client("127.0.0.1", port);
    client.set_bearer_token("t0k");
    EXPECT_EQ(client.get("/api/v1/theirs").body, "forwarded");
    EXPECT_EQ(client.get("/api/v1/mine").body, "handler");
    EXPECT_EQ(consulted, 2);
}

TEST(StandbyHttp, ACoordinatorAnswersRequestsAboutItself) {
    using clink::cluster::answered_by_this_node;
    for (const char* path :
         {"/livez", "/readyz", "/api/v1/health", "/metrics", "/", "/assets/x.js"}) {
        EXPECT_TRUE(answered_by_this_node("GET", path)) << path;
    }
    for (const char* path : {"/api/v1/jobs", "/api/v1/cluster", "/api/v1/jobs/3/checkpoints"}) {
        EXPECT_FALSE(answered_by_this_node("GET", path)) << path;
    }
    EXPECT_FALSE(answered_by_this_node("POST", "/api/v1/jobs/spec"));
    EXPECT_FALSE(answered_by_this_node("POST", "/livez"));
}

TEST(StandbyHttp, AStandbyForwardsTheApiToTheLeaderAndAnswersWhatIsAboutItself) {
    Echo leader_http;
    const auto dir = fresh_ha_dir();
    auto leader = clink::cluster::make_file_ha_coordinator(
        dir.string(), {"127.0.0.1", 6123, 0, 0, leader_http.port}, 20ms);
    leader->start();
    ASSERT_TRUE(eventually([&] { return leader->current_leader_endpoint().has_value(); }));
    // The standby is never started: a second coordinator in this process would
    // take the lock as well (POSIX record locks are per process), and a standby
    // answers from the record alone.
    auto standby =
        clink::cluster::make_file_ha_coordinator(dir.string(), {"127.0.0.1", 6124, 0, 0, 9}, 20ms);

    const auto api = request("GET", "/api/v1/echo");
    const auto forwarded = clink::cluster::answer_as_standby(*standby, "127.0.0.1", 9, api, 5s);
    ASSERT_TRUE(forwarded.has_value());
    EXPECT_EQ(forwarded->status, 200);
    EXPECT_NE(forwarded->body.find(R"("forwarded":"1")"), std::string::npos) << forwarded->body;
    EXPECT_FALSE(clink::cluster::answer_as_standby(
                     *standby, "127.0.0.1", 9, request("GET", "/api/v1/health"), 5s)
                     .has_value());
    EXPECT_FALSE(clink::cluster::answer_as_standby(*leader, "127.0.0.1", leader_http.port, api, 5s)
                     .has_value())
        << "the leader answers its API itself";

    auto twice = api;
    twice.headers["x-clink-forwarded"] = "1";
    const auto refused = clink::cluster::answer_as_standby(*standby, "127.0.0.1", 9, twice, 5s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->status, 503);
    EXPECT_NE(refused->body.find("forwarded to it as the leader"), std::string::npos)
        << refused->body;

    const auto standby_ready =
        clink::cluster::coordinator_readiness(standby.get(), false, true, "127.0.0.1", 9);
    EXPECT_EQ(standby_ready.status, 200);
    EXPECT_EQ(standby_ready.body, "standby\n");
    EXPECT_EQ(clink::cluster::coordinator_readiness(leader.get(), true, true, "127.0.0.1", 1).body,
              "leader\n");
    EXPECT_EQ(
        clink::cluster::coordinator_readiness(leader.get(), false, true, "127.0.0.1", 1).status,
        503)
        << "a leader is not ready before its control port is bound";
    const auto recovering =
        clink::cluster::coordinator_readiness(leader.get(), true, false, "127.0.0.1", 1);
    EXPECT_EQ(recovering.status, 503) << "a leader is not ready before it has recovered its jobs";
    EXPECT_EQ(recovering.body, "recovering\n");
    EXPECT_EQ(clink::cluster::coordinator_readiness(nullptr, true, true, "127.0.0.1", 1).body,
              "ok\n");
    leader->stop();
}

// A leader that has just taken over refuses the job API until it has recovered
// its predecessor's jobs, and answers everything else, so a client is told to
// wait rather than shown a list without them.
TEST(StandbyHttp, ALeaderStillRecoveringRefusesTheJobApiAndNothingElse) {
    using clink::cluster::answer_while_recovering;
    for (const auto& [method, path] :
         std::vector<std::pair<std::string, std::string>>{{"GET", "/api/v1/jobs"},
                                                          {"GET", "/api/v1/jobs/3"},
                                                          {"POST", "/api/v1/jobs/sql"},
                                                          {"POST", "/api/v1/jobs/3/savepoint"}}) {
        const auto refused = answer_while_recovering(request(method, path));
        ASSERT_TRUE(refused.has_value()) << method << " " << path;
        EXPECT_EQ(refused->status, 503);
        EXPECT_EQ(refused->headers.at("Retry-After"), "2");
        EXPECT_NE(refused->body.find("recovering"), std::string::npos) << refused->body;
    }
    for (const char* path :
         {"/api/v1/workers", "/api/v1/cluster", "/api/v1/jobsx", "/readyz", "/livez", "/"}) {
        EXPECT_FALSE(answer_while_recovering(request("GET", path)).has_value()) << path;
    }
}

TEST(StandbyHttp, AStandbyWithNoLeaderToForwardToRefusesAndIsNotReady) {
    const auto dir = fresh_ha_dir();
    const auto write_record = [&](const std::string& body) {
        std::ofstream(dir / "active-leader.json") << body;
    };
    auto standby = clink::cluster::make_file_ha_coordinator(dir.string(), {}, 20ms);
    const auto api = request("GET", "/api/v1/jobs");

    // A leader of an earlier release, which records no HTTP port.
    write_record(R"({"host":"10.0.0.7","port":6123,"epoch":3,"updated_unix_ms":1})");
    auto refused = clink::cluster::answer_as_standby(*standby, "10.0.0.8", 8081, api, 2s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->status, 503);
    EXPECT_EQ(refused->headers.at("Retry-After"), "1");
    EXPECT_EQ(
        clink::cluster::coordinator_readiness(standby.get(), false, true, "10.0.0.8", 8081).body,
        "no leader\n");

    // A record this node left behind when it led before.
    write_record(
        R"({"host":"10.0.0.8","port":6123,"epoch":4,"updated_unix_ms":1,"http_port":8081})");
    refused = clink::cluster::answer_as_standby(*standby, "10.0.0.8", 8081, api, 2s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->status, 503);
    EXPECT_EQ(
        clink::cluster::coordinator_readiness(standby.get(), false, true, "10.0.0.8", 8081).status,
        503);
}
