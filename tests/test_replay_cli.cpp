// End-to-end determinism gate for `clink replay` (the incident-replay
// loop): run a windowed SQL job with the flight recorder armed, then
// replay the tumbling-window operator's captured epoch through the CLI
// and require (a) the emissions to equal the live job's sink output and
// (b) --verify to pass (two runs byte-identical). This is the
// executable form of docs/internals/replay-determinism.md.

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/sql/replay.hpp"

namespace fs = std::filesystem;

namespace {

struct CmdResult {
    int exit_code{-1};
    std::string output;  // stdout + stderr
};

CmdResult run_cmd(const std::string& cmd) {
    CmdResult r;
    FILE* pipe = popen((cmd + " 2>&1").c_str(), "r");
    if (pipe == nullptr) {
        return r;
    }
    std::array<char, 4096> buf{};
    while (fgets(buf.data(), buf.size(), pipe) != nullptr) {
        r.output += buf.data();
    }
    const int status = pclose(pipe);
    r.exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    return r;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

TEST(ReplayCli, WindowedEpochReplaysTheLiveEmissionsAndVerifiesDeterministic) {
    const std::string cli = CLINK_CLI_BINARY;
    const auto dir = fs::temp_directory_path() / "clink_replay_cli_e2e";
    fs::remove_all(dir);
    fs::create_directories(dir);

    // Input spanning two event-time windows for two keys.
    {
        std::ofstream in(dir / "in.jsonl");
        in << R"({"usr":"alice","ts":100,"amount":10})" << "\n"
           << R"({"usr":"alice","ts":400,"amount":5})" << "\n"
           << R"({"usr":"bob","ts":700,"amount":7})" << "\n"
           << R"({"usr":"alice","ts":1200,"amount":100})" << "\n"
           << R"({"usr":"bob","ts":1500,"amount":50})" << "\n";
    }
    {
        std::ofstream sql(dir / "job.sql");
        sql << "CREATE TABLE evt (usr TEXT, ts BIGINT, amount BIGINT) WITH (connector='file', "
               "format='json', path='"
            << (dir / "in.jsonl").string() << "', event_time_column='ts');\n"
            << "CREATE TABLE totals (usr TEXT, total BIGINT) WITH (connector='file', "
               "format='json', path='"
            << (dir / "out.jsonl").string() << "');\n"
            << "INSERT INTO totals SELECT usr, SUM(amount) AS total FROM evt "
               "GROUP BY TUMBLE(ts, 1000), usr;\n";
    }

    // Live run with the flight recorder armed.
    const auto run = run_cmd(cli + " run " + (dir / "job.sql").string() + " --checkpoint-dir=" +
                             (dir / "ckpt").string() + " --checkpoint-interval-ms=200" +
                             " --capture-dir=" + (dir / "capture").string());
    ASSERT_EQ(run.exit_code, 0) << run.output;
    const auto live_out = read_file(dir / "out.jsonl");
    ASSERT_FALSE(live_out.empty());

    // Find the tumbling-window operator's capture by its op.json type.
    std::string window_op;
    for (const auto& entry : fs::directory_iterator(dir / "capture")) {
        const auto op_json = entry.path() / "subtask-1" / "op.json";
        if (!fs::exists(op_json)) {
            continue;
        }
        auto js = clink::config::parse(read_file(op_json));
        if (js.is_object() && js.at("op_type").is_string() &&
            js.at("op_type").as_string() == "tumbling_window_row") {
            window_op = entry.path().filename().string().substr(3);  // strip "op-"
        }
    }
    ASSERT_FALSE(window_op.empty()) << "no tumbling_window_row capture found";

    const std::string replay_base = cli + " replay --capture-dir=" + (dir / "capture").string() +
                                    " --checkpoint-dir=" + (dir / "ckpt").string() +
                                    " --op=" + window_op + " --epoch=1";

    // The replayed emissions must be exactly the live job's output rows
    // (same rows, same order - the sink wrote what this operator emitted).
    const auto replay = run_cmd(replay_base + " --max-rows=0");
    ASSERT_EQ(replay.exit_code, 0) << replay.output;
    std::vector<std::string> replay_rows;
    {
        std::istringstream is(replay.output);
        std::string line;
        bool in_body = false;
        while (std::getline(is, line)) {
            if (line == "---") {
                in_body = !in_body;
                continue;
            }
            if (in_body && !line.empty() && line.front() == '{') {
                replay_rows.push_back(line);
            }
        }
    }
    std::vector<std::string> live_rows;
    {
        std::istringstream is(live_out);
        std::string line;
        while (std::getline(is, line)) {
            if (!line.empty()) {
                live_rows.push_back(line);
            }
        }
    }
    EXPECT_EQ(replay_rows, live_rows)
        << "replayed emissions differ from the live sink output\nreplay:\n"
        << replay.output << "\nlive:\n"
        << live_out;

    // And the determinism gate: two runs byte-identical.
    const auto verify = run_cmd(replay_base + " --verify");
    EXPECT_EQ(verify.exit_code, 0) << verify.output;
    EXPECT_NE(verify.output.find("deterministic:"), std::string::npos) << verify.output;

    // Whole-job sweep (no --op): every captured operator replays and
    // verifies deterministic in one command.
    const auto whole =
        run_cmd(cli + " replay --capture-dir=" + (dir / "capture").string() +
                " --checkpoint-dir=" + (dir / "ckpt").string() + " --epoch=1 --verify");
    EXPECT_EQ(whole.exit_code, 0) << whole.output;
    EXPECT_NE(whole.output.find("skipped 0"), std::string::npos) << whole.output;
    EXPECT_NE(whole.output.find("every replayed operator byte-identical"), std::string::npos)
        << whole.output;

    // Cross-version A/B plumbing: two --out dumps diff identical (exit 0);
    // a doctored dump diffs different (exit 1) with the emission located.
    const auto dump_a = run_cmd(replay_base + " --out=" + (dir / "a.ndjson").string());
    const auto dump_b = run_cmd(replay_base + " --out=" + (dir / "b.ndjson").string());
    ASSERT_EQ(dump_a.exit_code, 0) << dump_a.output;
    ASSERT_EQ(dump_b.exit_code, 0) << dump_b.output;
    const auto same = run_cmd(cli + " replay-diff " + (dir / "a.ndjson").string() + " " +
                              (dir / "b.ndjson").string());
    EXPECT_EQ(same.exit_code, 0) << same.output;
    EXPECT_NE(same.output.find("identical:"), std::string::npos) << same.output;
    {
        auto doctored = read_file(dir / "a.ndjson");
        const auto pos = doctored.find(":15");  // alice's first-window total
        ASSERT_NE(pos, std::string::npos) << doctored;
        doctored.replace(pos, 3, ":16");
        std::ofstream out(dir / "c.ndjson", std::ios::binary);
        out << doctored;
    }
    const auto diff = run_cmd(cli + " replay-diff " + (dir / "a.ndjson").string() + " " +
                              (dir / "c.ndjson").string());
    EXPECT_EQ(diff.exit_code, 1) << diff.output;
    EXPECT_NE(diff.output.find("differing emission"), std::string::npos) << diff.output;

    // Incident -> regression test: emit a self-contained bundle, then run
    // it through the library exactly as the generated gtest does. The
    // bundle must pass against its own golden, the artefacts must all
    // exist, and a doctored golden must fail with the divergence located.
    const auto bundle = dir / "bundle";
    const auto emit = run_cmd(replay_base + " --emit-test=" + bundle.string());
    ASSERT_EQ(emit.exit_code, 0) << emit.output;
    EXPECT_TRUE(fs::exists(bundle / "bundle.json"));
    EXPECT_TRUE(fs::exists(bundle / "golden.ndjson"));
    EXPECT_TRUE(fs::exists(bundle / "replay_regression_test.cpp"));
    EXPECT_EQ(clink::sql::run_replay_regression(bundle.string()), "");
    {
        auto golden = read_file(bundle / "golden.ndjson");
        const auto pos = golden.find(":15");
        ASSERT_NE(pos, std::string::npos) << golden;
        golden.replace(pos, 3, ":99");
        std::ofstream out(bundle / "golden.ndjson", std::ios::binary | std::ios::trunc);
        out << golden;
    }
    const auto regression_error = clink::sql::run_replay_regression(bundle.string());
    EXPECT_NE(regression_error.find("first divergence"), std::string::npos) << regression_error;

    fs::remove_all(dir);
}
