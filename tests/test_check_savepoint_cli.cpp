// End-to-end test for `clink check-savepoint`. Writes a real
// InMemoryStateBackend snapshot (with a non-empty StateVersionMap) to
// a temp file, invokes the built CLI binary on it, and asserts the
// printed output contains the expected operator/state/version
// triplets. Mirrors the test_sql_cli.cpp pattern - subprocess capture
// via std::system + temp files, unique per-test PID+counter tag so
// parallel ctest doesn't race.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/core/types.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/schema_version.hpp"

namespace {

const char* binary_path() {
#ifdef CLINK_CLI_BINARY
    return CLINK_CLI_BINARY;
#else
    return "clink";
#endif
}

const char* schema_evo_job_path() {
#ifdef CLINK_SCHEMA_EVO_JOB_PATH
    return CLINK_SCHEMA_EVO_JOB_PATH;
#else
    return nullptr;
#endif
}

struct RunResult {
    int exit_code;
    std::string stdout_text;
    std::string stderr_text;
};

RunResult run_cli(const std::string& args) {
    namespace fs = std::filesystem;
    static std::atomic<std::uint64_t> counter{0};
    auto tag = std::to_string(getpid()) + "_" + std::to_string(counter.fetch_add(1));
    auto out_path = fs::temp_directory_path() / ("check_sp_stdout_" + tag + ".txt");
    auto err_path = fs::temp_directory_path() / ("check_sp_stderr_" + tag + ".txt");
    fs::remove(out_path);
    fs::remove(err_path);
    std::string cmd = std::string{binary_path()} + " " + args + " > '" + out_path.string() +
                      "' 2> '" + err_path.string() + "'";
    int rc = std::system(cmd.c_str());
    auto read = [](const fs::path& p) {
        std::ifstream in(p);
        std::ostringstream s;
        s << in.rdbuf();
        return s.str();
    };
    auto out = read(out_path);
    auto err = read(err_path);
    fs::remove(out_path);
    fs::remove(err_path);
    return RunResult{WEXITSTATUS(rc), std::move(out), std::move(err)};
}

std::filesystem::path write_savepoint_file(const clink::Snapshot& snap, const std::string& tag) {
    namespace fs = std::filesystem;
    auto path = fs::temp_directory_path() / ("check_sp_input_" + tag + ".snap");
    std::ofstream f(path, std::ios::binary);
    if (!snap.bytes.empty()) {
        f.write(reinterpret_cast<const char*>(snap.bytes.data()),
                static_cast<std::streamsize>(snap.bytes.size()));
    }
    return path;
}

}  // namespace

TEST(CheckSavepointCli, PrintsStampedVersions) {
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(clink::OperatorId{1}, "k", std::string_view{"v"});
    backend->put(clink::OperatorId{2}, "k", std::string_view{"v"});

    clink::StateVersionMap versions;
    versions.set(clink::OperatorId{1}, "CounterState", 2);
    versions.set(clink::OperatorId{2}, "WindowAgg", 4);
    backend->set_state_versions(versions);

    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto path = write_savepoint_file(snap, "v" + std::to_string(getpid()));

    auto result = run_cli("check-savepoint --file=" + path.string());
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;

    // Should report 2 entries and 2 versioned ops.
    EXPECT_NE(result.stdout_text.find("entries=2"), std::string::npos)
        << "stdout: " << result.stdout_text;
    EXPECT_NE(result.stdout_text.find("versioned_ops=2"), std::string::npos)
        << "stdout: " << result.stdout_text;
    EXPECT_NE(result.stdout_text.find("CounterState"), std::string::npos);
    EXPECT_NE(result.stdout_text.find("WindowAgg"), std::string::npos);

    std::filesystem::remove(path);
}

TEST(CheckSavepointCli, ReportsNoStampsWhenUnversioned) {
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(clink::OperatorId{7}, "k", std::string_view{"v"});

    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto path = write_savepoint_file(snap, "u" + std::to_string(getpid()));

    auto result = run_cli("check-savepoint --file=" + path.string());
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    EXPECT_NE(result.stdout_text.find("versioned_ops=0"), std::string::npos)
        << "stdout: " << result.stdout_text;
    EXPECT_NE(result.stdout_text.find("no state version stamps recorded"), std::string::npos);

    std::filesystem::remove(path);
}

TEST(CheckSavepointCli, FailsOnMissingFile) {
    auto result = run_cli("check-savepoint --file=/nonexistent/path/savepoint.snap");
    EXPECT_NE(result.exit_code, 0);
    EXPECT_NE(result.stderr_text.find("cannot open file"), std::string::npos)
        << "stderr: " << result.stderr_text;
}

TEST(CheckSavepointCli, FailsOnMissingFlag) {
    auto result = run_cli("check-savepoint");
    EXPECT_NE(result.exit_code, 0);
}

TEST(CheckSavepointCli, QuietSuppressesStdout) {
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    backend->put(clink::OperatorId{1}, "k", std::string_view{"v"});
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto path = write_savepoint_file(snap, "q" + std::to_string(getpid()));

    auto result = run_cli("check-savepoint --quiet --file=" + path.string());
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    EXPECT_TRUE(result.stdout_text.empty()) << "stdout was: " << result.stdout_text;

    std::filesystem::remove(path);
}

// --expected runs the .so-side pre-deploy compatibility check. The
// schema_evo_test_job fixture expects "counter" v3 with a 1->2->3
// migration chain registered, keyed on operator_id_from_uid("counter-op").

TEST(CheckSavepointCli, ExpectedReportsCompatibleSavepoint) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    const auto op = clink::operator_id_from_uid("counter-op");
    backend->put(op, "k", std::string_view{"v"});
    clink::StateVersionMap versions;
    versions.set(op, "counter", 1);  // v1 -> reaches expected v3 via chain
    backend->set_state_versions(versions);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto path = write_savepoint_file(snap, "compat" + std::to_string(getpid()));

    auto result =
        run_cli("check-savepoint --file=" + path.string() + " --expected=" + schema_evo_job_path());
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    EXPECT_NE(result.stdout_text.find("compatible"), std::string::npos)
        << "stdout: " << result.stdout_text;

    std::filesystem::remove(path);
}

TEST(CheckSavepointCli, ExpectedRejectsIncompatibleSavepoint) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    const auto op = clink::operator_id_from_uid("counter-op");
    backend->put(op, "k", std::string_view{"v"});
    clink::StateVersionMap versions;
    versions.set(op, "counter", 5);  // v5 -> no downgrade path to v3
    backend->set_state_versions(versions);
    auto snap = backend->snapshot(clink::CheckpointId{1});
    auto path = write_savepoint_file(snap, "incompat" + std::to_string(getpid()));

    auto result =
        run_cli("check-savepoint --file=" + path.string() + " --expected=" + schema_evo_job_path());
    EXPECT_EQ(result.exit_code, 3) << "stderr: " << result.stderr_text;
    EXPECT_NE(result.stderr_text.find("INCOMPATIBLE"), std::string::npos)
        << "stderr: " << result.stderr_text;
    EXPECT_NE(result.stderr_text.find("counter"), std::string::npos);

    std::filesystem::remove(path);
}
