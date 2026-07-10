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

// ----- state-export --dir/--id (multi-subtask merge) -----

// Two subtask snapshot files under one checkpoint root export as ONE
// merged stream: the keyed union is exact (disjoint key groups), the
// duplicated operator-state offset row resolves to the greater value
// (the scale-down restore collision policy), and the exported bytes
// restore cleanly into the format's reference reader.
TEST(StateExportCli, DirFormMergesSubtaskFiles) {
    namespace fs = std::filesystem;
    const auto tag = std::to_string(getpid());
    const auto root = fs::temp_directory_path() / ("state_export_dir_" + tag);
    fs::remove_all(root);
    fs::create_directories(root / "0");
    fs::create_directories(root / "1");

    auto write_subtask = [&](int subtask, const std::string& key, std::int64_t offset) {
        clink::InMemoryStateBackend b;
        const std::string keyed = std::string{"\x05"} + "slot|" + key;
        b.put(clink::OperatorId{1}, std::string_view{keyed}, std::string_view{"v-" + key});
        // The same operator-state key in both subtasks, different offsets.
        const std::string op_key = std::string{"\xFF"} + "offsets|p0";
        std::string off(8, '\0');
        for (int i = 0; i < 8; ++i) {
            off[static_cast<std::size_t>(i)] = static_cast<char>((offset >> (i * 8)) & 0xFF);
        }
        b.put(clink::OperatorId{2}, std::string_view{op_key}, std::string_view{off});
        auto snap = b.snapshot(clink::CheckpointId{5});
        std::ofstream f(root / std::to_string(subtask) / "checkpoint-5.snap", std::ios::binary);
        f.write(reinterpret_cast<const char*>(snap.bytes.data()),
                static_cast<std::streamsize>(snap.bytes.size()));
    };
    write_subtask(0, "alpha", 100);
    write_subtask(1, "beta", 700);

    const auto out = fs::temp_directory_path() / ("state_export_dir_out_" + tag + ".arrows");
    fs::remove(out);
    auto result = run_cli("state-export --dir=" + root.string() + " --id=5 --out=" + out.string() +
                          " --format=arrow");
    ASSERT_EQ(result.exit_code, 0) << result.stderr_text;
    EXPECT_NE(result.stdout_text.find("2 subtask files"), std::string::npos) << result.stdout_text;

    // Restore the export and verify the merged view.
    std::ifstream in(out, std::ios::binary);
    std::vector<char> raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    clink::Snapshot snap;
    snap.bytes.resize(raw.size());
    std::memcpy(snap.bytes.data(), raw.data(), raw.size());
    clink::InMemoryStateBackend merged;
    merged.restore(snap);

    std::size_t keyed_rows = 0;
    merged.scan(clink::OperatorId{1}, [&](std::string_view, std::string_view) { ++keyed_rows; });
    EXPECT_EQ(keyed_rows, 2u);  // alpha + beta, both subtasks' keys
    // The colliding offset row kept the GREATER value (700).
    const std::string op_key = std::string{"\xFF"} + "offsets|p0";
    auto v = merged.get(clink::OperatorId{2}, std::string_view{op_key});
    ASSERT_TRUE(v.has_value());
    std::int64_t got = 0;
    for (int i = 0; i < 8; ++i) {
        got |= static_cast<std::int64_t>(
                   std::to_integer<std::uint8_t>((*v)[static_cast<std::size_t>(i)]))
               << (i * 8);
    }
    EXPECT_EQ(got, 700);

    // The parquet dir-form works too (summary only; content pinned elsewhere).
    const auto pq = fs::temp_directory_path() / ("state_export_dir_out_" + tag + ".parquet");
    fs::remove(pq);
    auto result2 = run_cli("state-export --dir=" + root.string() + " --id=5 --out=" + pq.string());
    ASSERT_EQ(result2.exit_code, 0) << result2.stderr_text;
    EXPECT_TRUE(fs::exists(pq));

    // Mutually exclusive input forms are rejected.
    auto bad = run_cli("state-export --from=x --dir=y --id=1 --out=z");
    EXPECT_EQ(bad.exit_code, 2);

    fs::remove_all(root);
    fs::remove(out);
    fs::remove(pq);
}

// ----- state-query (SQL over a snapshot) -----

// A savepoint's keyed state is queryable with SQL through the embedded
// engine: filters see rendered keys and int64 value readings, and a
// GROUP BY (a retracting plan) prints its final netted row. Skips on
// builds whose CLI lacks the SQL frontend.
TEST(StateQueryCli, FiltersAndAggregatesOverSavepoint) {
    namespace fs = std::filesystem;
    clink::InMemoryStateBackend backend;
    auto put_count = [&](const std::string& key, std::int64_t v) {
        const std::string keyed = std::string{"\x05"} + "counts|" + key;
        std::string val(8, '\0');
        for (int i = 0; i < 8; ++i) {
            val[static_cast<std::size_t>(i)] = static_cast<char>((v >> (i * 8)) & 0xFF);
        }
        backend.put(clink::OperatorId{7}, std::string_view{keyed}, std::string_view{val});
    };
    put_count("alpha", 1);
    put_count("beta", 22);
    put_count("gamma", 333);
    auto snap = backend.snapshot(clink::CheckpointId{1});
    auto path = write_savepoint_file(snap, "query" + std::to_string(getpid()));

    auto probe = run_cli("state-query --help");
    if (probe.stderr_text.find("requires a build") != std::string::npos) {
        GTEST_SKIP() << "CLI built without the SQL frontend";
    }

    auto filtered = run_cli("state-query --from=" + path.string() +
                            " '--sql=SELECT user_key, value_int FROM state "
                            "WHERE slot='\\''counts'\\'' AND value_int > 10'");
    ASSERT_EQ(filtered.exit_code, 0) << filtered.stderr_text;
    EXPECT_NE(filtered.stdout_text.find("\"user_key\":\"beta\",\"value_int\":22"),
              std::string::npos)
        << filtered.stdout_text;
    EXPECT_NE(filtered.stdout_text.find("\"user_key\":\"gamma\",\"value_int\":333"),
              std::string::npos);
    EXPECT_EQ(filtered.stdout_text.find("alpha"), std::string::npos);

    auto agg = run_cli("state-query --from=" + path.string() +
                       " '--sql=SELECT slot, count(*) AS n FROM state GROUP BY slot'");
    ASSERT_EQ(agg.exit_code, 0) << agg.stderr_text;
    EXPECT_NE(agg.stdout_text.find("\"n\":3,\"slot\":\"counts\""), std::string::npos)
        << agg.stdout_text;

    fs::remove(path);
}
