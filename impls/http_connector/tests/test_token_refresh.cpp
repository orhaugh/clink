// Bearer-token refresh: HttpRequest::Options.auth_token_provider is consulted per request, and
// make_file_token_provider re-reads the token file when it changes. Verified end-to-end against
// an in-process httplib server that echoes back the Authorization header it received, so a
// rotated token file is observed on the wire without rebuilding the client.

#include <chrono>
#include <filesystem>
#include <fstream>
#include <httplib.h>
#include <mutex>
#include <random>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/token_source.hpp"

using clink::http_connector::HttpRequest;
using clink::http_connector::make_file_token_provider;

namespace {

class AuthEchoServer {
public:
    AuthEchoServer() {
        auto record = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                last_auth_ = req.get_header_value("Authorization");
            }
            res.set_content("{}", "application/json");
        };
        svr_.Get("/x", record);
        svr_.Post("/x", record);
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~AuthEchoServer() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    std::string base_url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    std::string last_auth() {
        std::lock_guard<std::mutex> lk(mu_);
        return last_auth_;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::string last_auth_;
};

std::filesystem::path temp_token_file() {
    static std::mt19937_64 rng{std::random_device{}()};
    return std::filesystem::temp_directory_path() /
           ("clink_token_" + std::to_string(rng()) + ".txt");
}

void write_file(const std::filesystem::path& p, const std::string& content) {
    std::ofstream f(p, std::ios::trunc);
    f << content;
}

}  // namespace

TEST(FileTokenProvider, ReadsTrimsAndRefreshesOnChange) {
    const auto path = temp_token_file();
    write_file(path, "tok-1\n");  // trailing newline is trimmed
    auto provider = make_file_token_provider(path.string());
    EXPECT_EQ(provider(), "tok-1");
    EXPECT_EQ(provider(), "tok-1");  // cached (no change)

    write_file(path, "  tok-2\t");
    // Force a distinct mtime so the change is detected deterministically.
    std::filesystem::last_write_time(
        path, std::filesystem::last_write_time(path) + std::chrono::seconds(2));
    EXPECT_EQ(provider(), "  tok-2");  // leading space kept, trailing whitespace trimmed
    std::filesystem::remove(path);
}

TEST(FileTokenProvider, EmptyWhenFileMissing) {
    auto provider = make_file_token_provider("/no/such/clink/token/file");
    EXPECT_EQ(provider(), "");
}

TEST(HttpRequestAuthProvider, SendsRefreshedBearerTokenPerRequest) {
    AuthEchoServer srv;
    const auto path = temp_token_file();
    write_file(path, "tok-1");

    HttpRequest::Options o;
    o.base_url = srv.base_url();
    o.auth_token_provider = make_file_token_provider(path.string());
    HttpRequest req(o);

    (void)req.get("/x");
    EXPECT_EQ(srv.last_auth(), "Bearer tok-1");
    (void)req.post("/x", "{}", "application/json");
    EXPECT_EQ(srv.last_auth(), "Bearer tok-1");

    // Rotate the token file; the SAME client must pick it up on the next request.
    write_file(path, "tok-2");
    std::filesystem::last_write_time(
        path, std::filesystem::last_write_time(path) + std::chrono::seconds(2));
    (void)req.get("/x");
    EXPECT_EQ(srv.last_auth(), "Bearer tok-2");
    std::filesystem::remove(path);
}

TEST(HttpRequestAuthProvider, NoProviderSendsNoAuthHeader) {
    AuthEchoServer srv;
    HttpRequest::Options o;
    o.base_url = srv.base_url();
    HttpRequest req(o);
    (void)req.get("/x");
    EXPECT_EQ(srv.last_auth(), "");  // no Authorization header sent
}
