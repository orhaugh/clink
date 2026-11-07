// A SQL table over a string-channel source whose JSON decoding meets a record it
// cannot read. An embedded `clink run` reads a bounded, resource-mode http_poll
// table whose endpoint serves a JSON number between two objects; the number is
// valid JSON and no object, so the JSON bridge refuses it. It used to become a row
// of nulls in the output. Under on_decode_error='drop', the default, the run
// finishes with the two rows, and the dead-letter queue's report of the number is
// in its log; under 'fail' it stops with the bridge's error naming the table and
// why, having written nothing.
// Needs the clink binary; the endpoint is served by the test.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <httplib.h>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;

std::filesystem::path sjd_cli_binary() {
#ifdef CLINK_CLI_BINARY
    return std::filesystem::path{CLINK_CLI_BINARY};
#else
    return {};
#endif
}

class SqlJsonDecode : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(sjd_cli_binary())) {
            GTEST_SKIP() << "the clink binary is not built";
        }
        root_ = std::filesystem::temp_directory_path() /
                ("clink-sql-json-decode-" + std::to_string(::getpid()) + "-" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_);
        svr_.Get("/resource", [](const httplib::Request&, httplib::Response& res) {
            res.set_content(R"([{"id":1,"v":"a"},7,{"id":2,"v":"b"}])", "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    void TearDown() override {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
        if (!HasFailure()) {
            std::error_code ec;
            std::filesystem::remove_all(root_, ec);
        }
    }

    // Runs the pipeline with `extra` added to the source table's options; returns
    // the exit code (nullopt when it did not end), the run's log and the output.
    struct Run {
        std::optional<int> code;
        std::string log;
        std::vector<std::string> lines;
    };
    Run run_with(const std::string& extra) {
        const auto out_file = root_ / "out.jsonl";
        const auto script = root_ / "pipeline.sql";
        std::ofstream(script)
            << "CREATE TABLE feed (id BIGINT, v STRING) WITH (connector='http_poll', "
               "format='json', url='http://127.0.0.1:"
            << port_ << "', path='/resource', bounded='true', poll_interval_ms='0'" << extra
            << ");\n"
               "CREATE TABLE out (id BIGINT, v STRING) WITH (connector='filesystem', "
               "path='"
            << out_file.string()
            << "', format='json');\n"
               "INSERT INTO out SELECT id, v FROM feed;\n";
        clink::itest::Process proc;
        Run r;
        if (!proc.spawn("clink-run",
                        sjd_cli_binary(),
                        {sjd_cli_binary().string(), "run", script.string()},
                        root_)) {
            r.log = "clink run did not start";
            return r;
        }
        r.code = proc.await_exit(120s);
        if (!r.code.has_value()) {
            proc.kill_hard();
        }
        r.log = proc.read_log();
        std::ifstream in(out_file);
        for (std::string line; std::getline(in, line);) {
            if (!line.empty()) {
                r.lines.push_back(line);
            }
        }
        return r;
    }

    std::filesystem::path root_;
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
};

TEST_F(SqlJsonDecode, UnderFailARecordThatIsNotAnObjectStopsTheRun) {
    const auto r = run_with(", on_decode_error='fail'");
    ASSERT_TRUE(r.code.has_value()) << "the run did not end\n" << r.log;
    EXPECT_NE(*r.code, 0) << r.log;
    EXPECT_NE(r.log.find("json_string_to_row: a record of table 'feed'"), std::string::npos)
        << r.log;
    EXPECT_NE(r.log.find("it is a number, not an object"), std::string::npos) << r.log;
    EXPECT_NE(r.log.find("on_decode_error='drop'"), std::string::npos) << r.log;
    for (const auto& line : r.lines) {
        EXPECT_EQ(line.find("null"), std::string::npos) << "a row of nulls was written: " << line;
    }
    std::printf("[sql-json-decode] cell=fail exit=%d rows=%zu\n", *r.code, r.lines.size());
    std::fflush(stdout);
}

TEST_F(SqlJsonDecode, ByDefaultTheRunSkipsTheRecordAndReportsIt) {
    const auto r = run_with("");
    ASSERT_TRUE(r.code.has_value()) << "the run did not end\n" << r.log;
    ASSERT_EQ(*r.code, 0) << r.log;
    EXPECT_EQ(r.lines, (std::vector<std::string>{R"({"id":1,"v":"a"})", R"({"id":2,"v":"b"})"}));
    // The dead-letter queue's report: the default queue logs each bad record.
    EXPECT_NE(r.log.find("dlq:http_poll_source"), std::string::npos) << r.log;
    EXPECT_NE(r.log.find("table 'feed'"), std::string::npos) << r.log;
    EXPECT_NE(r.log.find("it is a number, not an object"), std::string::npos) << r.log;
    std::printf("[sql-json-decode] cell=drop exit=%d rows=%zu\n", *r.code, r.lines.size());
    std::fflush(stdout);
}

}  // namespace
