// serve_static_file: the same-origin console serving behind
// clink_node --http-static-dir. The rules under test are the ones a wrong
// implementation would get dangerously wrong: nothing outside the root is
// ever served (dot-dot segments, symlinks pointing out), SPA deep links
// fall back to index.html, and a missing asset with an extension is a real
// 404 rather than HTML served to a script tag.

#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/http/static_files.hpp"

namespace {

namespace fs = std::filesystem;
using clink::http::serve_static_file;

class StaticFiles : public ::testing::Test {
protected:
    void SetUp() override {
        // Unique per test: ctest runs each test as its own process in
        // parallel, so a shared fixed directory gets set up and torn down
        // concurrently by sibling tests. PID + test name keeps every
        // instance disjoint; the secret lives inside base_ (outside the
        // served root) so cleanup stays one remove_all.
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        base_ = fs::temp_directory_path() /
                ("clink_static_" + std::to_string(::getpid()) + "_" + info->name());
        root_ = base_ / "webroot";
        fs::remove_all(base_);
        fs::create_directories(root_ / "assets");
        write(root_ / "index.html", "<html>console</html>");
        write(root_ / "assets" / "app.js", "console.log(1)");
        write(root_ / "assets" / "app.css", "body{}");
        // A secret OUTSIDE the served root, for the escape tests.
        write(base_ / "clink_static_secret.txt", "secret");
        canonical_root_ = fs::weakly_canonical(root_);
    }

    void TearDown() override { fs::remove_all(base_); }

    static void write(const fs::path& p, const std::string& body) {
        std::ofstream out(p, std::ios::trunc | std::ios::binary);
        out << body;
    }

    fs::path base_;
    fs::path root_;
    fs::path canonical_root_;
};

TEST_F(StaticFiles, ServesFilesWithExtensionDerivedTypes) {
    auto resp = serve_static_file(canonical_root_, "assets/app.js");
    EXPECT_EQ(resp.status, 200);
    EXPECT_EQ(resp.body, "console.log(1)");
    EXPECT_EQ(resp.content_type, "text/javascript; charset=utf-8");

    resp = serve_static_file(canonical_root_, "assets/app.css");
    EXPECT_EQ(resp.status, 200);
    EXPECT_EQ(resp.content_type, "text/css; charset=utf-8");
}

TEST_F(StaticFiles, RootAndSpaRoutesFallBackToIndex) {
    // "/", a client-side route, and a nested one - all land on the SPA.
    for (const std::string tail : {"", "workers", "jobs/7"}) {
        auto resp = serve_static_file(canonical_root_, tail);
        EXPECT_EQ(resp.status, 200) << tail;
        EXPECT_EQ(resp.body, "<html>console</html>") << tail;
        EXPECT_EQ(resp.content_type, "text/html; charset=utf-8") << tail;
    }
}

TEST_F(StaticFiles, MissingAssetWithExtensionIs404NotIndex) {
    // A script tag must get a 404, never HTML.
    auto resp = serve_static_file(canonical_root_, "assets/gone.js");
    EXPECT_EQ(resp.status, 404);
    EXPECT_NE(resp.body, "<html>console</html>");
}

TEST_F(StaticFiles, DotDotSegmentsNeverEscapeTheRoot) {
    for (const std::string tail : {"../clink_static_secret.txt",
                                   "assets/../../clink_static_secret.txt",
                                   "..%2Fclink_static_secret.txt"}) {
        auto resp = serve_static_file(canonical_root_, tail);
        EXPECT_EQ(resp.status, 404) << tail;
        EXPECT_EQ(resp.body.find("secret"), std::string::npos) << tail;
    }
}

TEST_F(StaticFiles, SymlinkPointingOutsideTheRootIsRefused) {
    std::error_code ec;
    fs::create_symlink(base_ / "clink_static_secret.txt", root_ / "leak.txt", ec);
    if (ec) {
        GTEST_SKIP() << "filesystem does not permit symlinks here";
    }
    const auto resp = serve_static_file(canonical_root_, "leak.txt");
    EXPECT_EQ(resp.status, 404);
    EXPECT_EQ(resp.body.find("secret"), std::string::npos);
}

TEST_F(StaticFiles, LeadingSlashesAreTolerated) {
    const auto resp = serve_static_file(canonical_root_, "/assets/app.js");
    EXPECT_EQ(resp.status, 200);
    EXPECT_EQ(resp.body, "console.log(1)");
}

}  // namespace

// A packaged clink ships the console beside its binaries, and the coordinator
// serves it without being told where. The lookup is relative to the binary,
// so an SDK unpacked anywhere, or the image's /usr/local, finds its own.
TEST_F(StaticFiles, TheConsoleBesideTheBinaryIsFoundFromItsExecutable) {
    const auto prefix = base_ / "prefix";
    fs::create_directories(prefix / "bin");
    fs::create_directories(prefix / "share" / "clink" / "console");
    write(prefix / "share" / "clink" / "console" / "index.html", "<html>console</html>");
    EXPECT_EQ(clink::http::console_beside(prefix / "bin" / "clink_node"),
              prefix / "share" / "clink" / "console");

    // A build tree has no share/ beside its binaries, and an empty bundle
    // directory is no console: both mean the JSON signpost.
    EXPECT_TRUE(clink::http::console_beside(base_ / "build" / "clink_node").empty());
    fs::remove(prefix / "share" / "clink" / "console" / "index.html");
    EXPECT_TRUE(clink::http::console_beside(prefix / "bin" / "clink_node").empty());
    EXPECT_TRUE(clink::http::console_beside({}).empty());
}

// The requests a node that requires a token serves without one: a console's
// own files. Everything under the API and the metrics endpoint still needs it.
TEST(StaticFilesAuth, OnlyRequestsForTheConsolesFilesAreExempt) {
    using clink::http::is_console_file_request;
    EXPECT_TRUE(is_console_file_request("GET", "/"));
    EXPECT_TRUE(is_console_file_request("GET", "/jobs/7"));
    EXPECT_TRUE(is_console_file_request("GET", "/assets/index-abc.js"));
    EXPECT_TRUE(is_console_file_request("HEAD", "/index.html"));
    EXPECT_FALSE(is_console_file_request("GET", "/api/v1/jobs"));
    EXPECT_FALSE(is_console_file_request("GET", "/api"));
    EXPECT_FALSE(is_console_file_request("GET", "/api/v1/events"));
    EXPECT_FALSE(is_console_file_request("GET", "/metrics"));
    EXPECT_FALSE(is_console_file_request("POST", "/"));
    EXPECT_FALSE(is_console_file_request("POST", "/api/v1/jobs/1/cancel"));
}
