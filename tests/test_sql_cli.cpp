#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <signal.h>
#include <spawn.h>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include "clink/config/json.hpp"

extern char** environ;

// End-to-end test for the clink_submit_sql binary. Writes a real .sql
// file, invokes the compiler, captures stdout, asserts the produced
// JobGraphSpec JSON matches the expected shape.
//
// This SCOPES OUT actual cluster execution of the produced spec -
// that needs an HTTP / wire submission path the coordinator doesn't expose yet
// for JSON specs. The clink::sql tests verify the compile path; this
// test verifies the BINARY assembles the same artifact when invoked
// as users will invoke it.

namespace {

const char* binary_path() {
#ifdef CLINK_SUBMIT_SQL_BINARY
    return CLINK_SUBMIT_SQL_BINARY;
#else
    return "clink_submit_sql";  // PATH fallback for ad-hoc runs
#endif
}

struct RunResult {
    int exit_code;
    std::string stdout_text;
    std::string stderr_text;
};

// Unique per-call capture paths so two SqlCli tests running in
// parallel (ctest -j) don't race on the same stdout/stderr files.
// PID covers process-level isolation; the counter handles the
// repeated calls within a single test.
RunResult run_compiler(const std::string& cmd_args) {
    namespace fs = std::filesystem;
    static std::atomic<std::uint64_t> counter{0};
    auto tag = std::to_string(getpid()) + "_" + std::to_string(counter.fetch_add(1));
    auto stdout_path = fs::temp_directory_path() / ("clink_sql_cli_stdout_" + tag + ".txt");
    auto stderr_path = fs::temp_directory_path() / ("clink_sql_cli_stderr_" + tag + ".txt");
    fs::remove(stdout_path);
    fs::remove(stderr_path);
    std::string full = std::string{binary_path()} + " " + cmd_args + " > '" + stdout_path.string() +
                       "' 2> '" + stderr_path.string() + "'";
    int rc = std::system(full.c_str());
    auto read = [](const fs::path& p) {
        std::ifstream in(p);
        std::ostringstream s;
        s << in.rdbuf();
        return s.str();
    };
    auto out = read(stdout_path);
    auto err = read(stderr_path);
    fs::remove(stdout_path);
    fs::remove(stderr_path);
    return RunResult{WEXITSTATUS(rc), std::move(out), std::move(err)};
}

}  // namespace

TEST(SqlCli, CompilesSimpleFileToFilePipeline) {
    namespace fs = std::filesystem;
    auto sql_path = fs::temp_directory_path() / "clink_sql_cli_input.sql";
    {
        std::ofstream out(sql_path);
        out << "CREATE TABLE src_t (line TEXT) "
            << "WITH (connector='file', path='/tmp/sql_cli_in.txt');\n"
            << "CREATE TABLE dst_t (line TEXT) "
            << "WITH (connector='file', path='/tmp/sql_cli_out.txt');\n"
            << "INSERT INTO dst_t SELECT line FROM src_t;\n";
    }

    auto result = run_compiler("--file " + sql_path.string());
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    EXPECT_TRUE(result.stderr_text.empty()) << "unexpected stderr: " << result.stderr_text;

    auto json = clink::config::parse(result.stdout_text);
    ASSERT_TRUE(json.is_object());
    ASSERT_TRUE(json.contains("ops"));
    const auto& ops = json.at("ops").as_array();
    ASSERT_EQ(ops.size(), 3u);

    bool saw_source = false;
    bool saw_proj = false;
    bool saw_sink = false;
    for (const auto& op : ops) {
        const auto& kind = op.at("type").as_string();
        if (kind == "file_text_source") {
            saw_source = true;
            EXPECT_EQ(op.at("params").at("path").as_string(), "/tmp/sql_cli_in.txt");
        } else if (kind == "identity_string") {
            saw_proj = true;
        } else if (kind == "file_text_sink") {
            saw_sink = true;
            EXPECT_EQ(op.at("params").at("path").as_string(), "/tmp/sql_cli_out.txt");
        }
    }
    EXPECT_TRUE(saw_source);
    EXPECT_TRUE(saw_proj);
    EXPECT_TRUE(saw_sink);
}

TEST(SqlCli, ExplainPrintsLogicalPlanTree) {
    auto result = run_compiler(
        "--explain -e \"CREATE TABLE s (line TEXT) WITH (connector='file', path='/x'); "
        "CREATE TABLE d (line TEXT) WITH (connector='file', path='/y'); "
        "INSERT INTO d SELECT line FROM s\"");
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    EXPECT_NE(result.stdout_text.find("Sink"), std::string::npos);
    EXPECT_NE(result.stdout_text.find("Project"), std::string::npos);
    EXPECT_NE(result.stdout_text.find("Scan"), std::string::npos);
    EXPECT_NE(result.stdout_text.find("VARCHAR"), std::string::npos);
}

// EXPLAIN annotates every node with its estimated output rows (the numbers
// the cost-based join reorderer compares); a scan with no declared statistics
// is flagged so the default placeholder is not mistaken for a measurement.
TEST(SqlCli, ExplainAnnotatesCardinalities) {
    auto result = run_compiler(
        "--explain -e \"CREATE TABLE s (line TEXT) WITH (connector='file', path='/x'); "
        "CREATE TABLE d (line TEXT) WITH (connector='file', path='/y'); "
        "INSERT INTO d SELECT line FROM s\"");
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    EXPECT_NE(result.stdout_text.find("(rows="), std::string::npos) << result.stdout_text;
    EXPECT_NE(result.stdout_text.find("no declared stats"), std::string::npos)
        << result.stdout_text;
}

// With declared statistics the scan shows the declared cardinality (no flag)
// and a WHERE shows the selectivity-reduced estimate on the Filter node.
TEST(SqlCli, ExplainShowsDeclaredStatsEstimates) {
    auto result = run_compiler(
        "--explain -e \"CREATE TABLE ev (k BIGINT, v BIGINT) WITH (connector='file', "
        "format='json', path='/x', row_count='1000', ndv_k='10'); "
        "CREATE TABLE out_t (k BIGINT, v BIGINT) WITH (connector='file', format='json', "
        "path='/y'); "
        "INSERT INTO out_t SELECT k, v FROM ev WHERE k = 3\"");
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    // Scan: the declared 1000 rows, unflagged. Filter: k = 3 with NDV(k)=10
    // estimates 1000/10 = 100 rows.
    EXPECT_NE(result.stdout_text.find("(rows=1000)"), std::string::npos) << result.stdout_text;
    EXPECT_NE(result.stdout_text.find("(rows=100)"), std::string::npos) << result.stdout_text;
    EXPECT_EQ(result.stdout_text.find("no declared stats"), std::string::npos)
        << result.stdout_text;
}

TEST(SqlCli, ParseErrorExitsNonZeroWithDiagnostic) {
    auto result = run_compiler("-e \"SELECT FROM WHERE\"");
    EXPECT_NE(result.exit_code, 0);
    EXPECT_NE(result.stderr_text.find("parse error"), std::string::npos);
}

TEST(SqlCli, TranslationErrorExitsNonZeroWithDiagnostic) {
    // Channel mismatch: source is Row (format='json'), sink is String.
    // Both endpoints are required to use the same channel.
    auto result = run_compiler(
        "-e \"CREATE TABLE r_in (a BIGINT, b TEXT) WITH (connector='file', format='json', "
        "path='/x'); "
        "CREATE TABLE s_out (line TEXT) WITH (connector='file', path='/y'); "
        "INSERT INTO s_out SELECT b FROM r_in\"");
    EXPECT_NE(result.exit_code, 0);
    EXPECT_NE(result.stderr_text.find("compile error"), std::string::npos);
    EXPECT_NE(result.stderr_text.find("channel"), std::string::npos);
}

TEST(SqlCli, MissingArgsShowsUsage) {
    auto result = run_compiler("");
    EXPECT_NE(result.exit_code, 0);
    EXPECT_NE(result.stderr_text.find("Usage"), std::string::npos);
}

TEST(SqlCli, WhereClauseEmitsFilterStringPredicateOp) {
    auto result = run_compiler(
        "-e \"CREATE TABLE k_in (msg TEXT) WITH (connector='kafka', topic='clicks', "
        "bootstrap='localhost:9092'); "
        "CREATE TABLE f_out (msg TEXT) WITH (connector='file', path='/tmp/out'); "
        "INSERT INTO f_out SELECT msg FROM k_in WHERE msg = 'hello'\"");
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;

    auto json = clink::config::parse(result.stdout_text);
    const auto& ops = json.at("ops").as_array();
    bool saw_filter = false;
    for (const auto& op : ops) {
        if (op.at("type").as_string() == "filter_string_predicate") {
            saw_filter = true;
            const auto& pred = op.at("params").at("predicate").as_string();
            EXPECT_NE(pred.find("\"op\":\"eq\""), std::string::npos);
            EXPECT_NE(pred.find("\"literal\":\"hello\""), std::string::npos);
        }
    }
    EXPECT_TRUE(saw_filter) << "filter_string_predicate not in spec: " << result.stdout_text;
}

TEST(SqlCli, ShowTablesPrintsCatalogContents) {
    namespace fs = std::filesystem;
    auto cat_dir = fs::temp_directory_path() /
                   ("clink_sql_cli_show_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::remove_all(cat_dir);

    // First invocation: define + immediately list. Both statements
    // run in one CLI call so we don't test persistence here.
    auto result =
        run_compiler("--catalog-dir " + cat_dir.string() +
                     " -e \"CREATE TABLE alpha (a TEXT) WITH (connector='file', path='/x'); "
                     "CREATE TABLE beta (b TEXT) WITH (connector='file', path='/y'); "
                     "SHOW TABLES\"");
    EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_text;
    EXPECT_NE(result.stdout_text.find("alpha"), std::string::npos);
    EXPECT_NE(result.stdout_text.find("beta"), std::string::npos);

    fs::remove_all(cat_dir);
}

TEST(SqlCli, DropTableRemovesEntryAndFile) {
    namespace fs = std::filesystem;
    auto cat_dir = fs::temp_directory_path() /
                   ("clink_sql_cli_drop_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::remove_all(cat_dir);

    auto r1 =
        run_compiler("--catalog-dir " + cat_dir.string() +
                     " -e \"CREATE TABLE doomed (a TEXT) WITH (connector='file', path='/x')\"");
    EXPECT_EQ(r1.exit_code, 0);
    EXPECT_TRUE(fs::exists(cat_dir / "doomed.json"));

    auto r2 = run_compiler("--catalog-dir " + cat_dir.string() + " -e \"DROP TABLE doomed\"");
    EXPECT_EQ(r2.exit_code, 0) << "stderr: " << r2.stderr_text;
    EXPECT_FALSE(fs::exists(cat_dir / "doomed.json"));

    // DROP TABLE without IF EXISTS on missing table fails;
    // with IF EXISTS succeeds.
    auto r3 = run_compiler("--catalog-dir " + cat_dir.string() + " -e \"DROP TABLE doomed\"");
    EXPECT_NE(r3.exit_code, 0);

    auto r4 =
        run_compiler("--catalog-dir " + cat_dir.string() + " -e \"DROP TABLE IF EXISTS doomed\"");
    EXPECT_EQ(r4.exit_code, 0) << "stderr: " << r4.stderr_text;

    fs::remove_all(cat_dir);
}

// DROP MATERIALIZED VIEW enforces object-kind matching end-to-end (the driver
// maps Catalog::drop_object's KindMismatch to a non-zero exit + diagnostic): it
// must refuse a plain table, leave it intact, and DROP TABLE then still works.
// IF EXISTS on a genuinely-absent name is silent. (The successful matview drop
// is covered by SqlCatalog.DropObjectEnforcesObjectKind without a cluster.)
TEST(SqlCli, DropMaterializedViewRejectsPlainTable) {
    namespace fs = std::filesystem;
    auto cat_dir = fs::temp_directory_path() /
                   ("clink_sql_cli_dropmv_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::remove_all(cat_dir);

    auto r1 = run_compiler("--catalog-dir " + cat_dir.string() +
                           " -e \"CREATE TABLE t (a TEXT) WITH (connector='file', path='/x')\"");
    EXPECT_EQ(r1.exit_code, 0) << "stderr: " << r1.stderr_text;

    // DROP MATERIALIZED VIEW on a plain table is rejected; the table survives.
    auto r2 =
        run_compiler("--catalog-dir " + cat_dir.string() + " -e \"DROP MATERIALIZED VIEW t\"");
    EXPECT_NE(r2.exit_code, 0);
    EXPECT_NE(r2.stderr_text.find("not a materialized view"), std::string::npos)
        << "stderr: " << r2.stderr_text;
    EXPECT_TRUE(fs::exists(cat_dir / "t.json")) << "the table must not have been dropped";

    // IF EXISTS on an absent materialized view is silent.
    auto r3 = run_compiler("--catalog-dir " + cat_dir.string() +
                           " -e \"DROP MATERIALIZED VIEW IF EXISTS nope\"");
    EXPECT_EQ(r3.exit_code, 0) << "stderr: " << r3.stderr_text;

    // The correct kind still drops the table.
    auto r4 = run_compiler("--catalog-dir " + cat_dir.string() + " -e \"DROP TABLE t\"");
    EXPECT_EQ(r4.exit_code, 0) << "stderr: " << r4.stderr_text;
    EXPECT_FALSE(fs::exists(cat_dir / "t.json"));

    fs::remove_all(cat_dir);
}

// CREATE VIEW + DROP VIEW through the driver, in a single invocation (logical
// views are session-scoped, not persisted). Also checks the object-kind guards:
// DROP TABLE refuses a view and DROP VIEW refuses a table.
TEST(SqlCli, CreateViewAndDropViewEndToEnd) {
    const std::string mk =
        "CREATE TABLE t (a BIGINT) WITH (connector='file', format='json', path='/x'); ";

    // Create a table, a view over it, then drop the view - all succeed.
    auto r1 = run_compiler("-e \"" + mk + "CREATE VIEW v AS SELECT a FROM t; DROP VIEW v\"");
    EXPECT_EQ(r1.exit_code, 0) << "stderr: " << r1.stderr_text;

    // DROP TABLE on a view is rejected.
    auto r2 = run_compiler("-e \"" + mk + "CREATE VIEW v AS SELECT a FROM t; DROP TABLE v\"");
    EXPECT_NE(r2.exit_code, 0);
    EXPECT_NE(r2.stderr_text.find("not a table"), std::string::npos) << r2.stderr_text;

    // DROP VIEW on a table is rejected.
    auto r3 = run_compiler("-e \"" + mk + "DROP VIEW t\"");
    EXPECT_NE(r3.exit_code, 0);
    EXPECT_NE(r3.stderr_text.find("not a view"), std::string::npos) << r3.stderr_text;
}

// ALTER TABLE mutates the persisted catalog: a later invocation loads the table,
// alters it, and re-persists, so the change survives across processes.
TEST(SqlCli, AlterTablePersistsAcrossInvocations) {
    namespace fs = std::filesystem;
    auto cat_dir = fs::temp_directory_path() /
                   ("clink_sql_cli_alter_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::remove_all(cat_dir);

    auto r1 = run_compiler("--catalog-dir " + cat_dir.string() +
                           " -e \"CREATE TABLE t (a BIGINT) WITH (connector='file', path='/x')\"");
    EXPECT_EQ(r1.exit_code, 0) << r1.stderr_text;

    // A separate invocation loads the persisted table and adds a column.
    auto r2 = run_compiler("--catalog-dir " + cat_dir.string() +
                           " -e \"ALTER TABLE t ADD COLUMN bcol TEXT\"");
    EXPECT_EQ(r2.exit_code, 0) << r2.stderr_text;

    // The re-persisted catalog JSON now carries the new column.
    std::ifstream in(cat_dir / "t.json");
    std::ostringstream ss;
    ss << in.rdbuf();
    EXPECT_NE(ss.str().find("bcol"), std::string::npos)
        << "altered column not persisted: " << ss.str();

    fs::remove_all(cat_dir);
}

// ALTER TABLE RENAME TO re-keys the persisted catalog: a later invocation loads
// the table, renames it, and the JSON file moves from the old name to the new.
TEST(SqlCli, RenameTablePersistsAcrossInvocations) {
    namespace fs = std::filesystem;
    auto cat_dir = fs::temp_directory_path() /
                   ("clink_sql_cli_rename_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::remove_all(cat_dir);

    auto r1 = run_compiler("--catalog-dir " + cat_dir.string() +
                           " -e \"CREATE TABLE t (a BIGINT) WITH (connector='file', path='/x')\"");
    EXPECT_EQ(r1.exit_code, 0) << r1.stderr_text;
    EXPECT_TRUE(fs::exists(cat_dir / "t.json"));

    auto r2 =
        run_compiler("--catalog-dir " + cat_dir.string() + " -e \"ALTER TABLE t RENAME TO t2\"");
    EXPECT_EQ(r2.exit_code, 0) << r2.stderr_text;
    EXPECT_FALSE(fs::exists(cat_dir / "t.json")) << "old table file should be removed";
    EXPECT_TRUE(fs::exists(cat_dir / "t2.json")) << "renamed table file should exist";

    fs::remove_all(cat_dir);
}

// ---------------------------------------------------------------------------
// HTTP submission integration test. Skips when clink_node isn't available.
// ---------------------------------------------------------------------------

#ifdef CLINK_NODE_BINARY

namespace {

namespace integration {

namespace fs = std::filesystem;

const char* node_binary_path() {
    return CLINK_NODE_BINARY;
}

// Bind+release a free OS-assigned port for cluster member to claim.
// Racy but good enough for local tests.
std::uint16_t probe_free_port() {
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    int yes = 1;
    ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    ::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(s, reinterpret_cast<sockaddr*>(&addr), &len);
    ::close(s);
    return ntohs(addr.sin_port);
}

bool await_http_ready(std::uint16_t port, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        std::string cmd =
            "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:" + std::to_string(port) +
            "/api/v1/health";
        FILE* p = ::popen(cmd.c_str(), "r");
        if (p != nullptr) {
            char buf[8] = {0};
            std::fread(buf, 1, sizeof(buf) - 1, p);
            ::pclose(p);
            if (std::string(buf) == "200")
                return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

pid_t spawn(std::vector<std::string> args) {
    std::vector<char*> raw;
    raw.reserve(args.size() + 1);
    for (auto& s : args)
        raw.push_back(const_cast<char*>(s.c_str()));
    raw.push_back(nullptr);
    pid_t pid = -1;
    int rc = ::posix_spawn(&pid, args[0].c_str(), nullptr, nullptr, raw.data(), ::environ);
    return rc == 0 ? pid : -1;
}

void kill_quietly(pid_t pid) {
    if (pid > 0) {
        ::kill(pid, SIGKILL);
        int s = 0;
        ::waitpid(pid, &s, 0);
    }
}

struct Cluster {
    pid_t coordinator_pid{-1};
    pid_t worker_pid{-1};
    std::uint16_t coordinator_http_port{0};
    std::uint16_t coordinator_control_port{0};
    ~Cluster() {
        kill_quietly(worker_pid);
        kill_quietly(coordinator_pid);
    }
};

bool start_cluster(Cluster& c) {
    c.coordinator_control_port = probe_free_port();
    c.coordinator_http_port = probe_free_port();
    auto worker_http_port = probe_free_port();
    c.coordinator_pid = spawn({node_binary_path(),
                               "--role=coordinator",
                               "--port=" + std::to_string(c.coordinator_control_port),
                               "--http-port=" + std::to_string(c.coordinator_http_port),
                               "--http-bind=127.0.0.1"});
    if (c.coordinator_pid <= 0 ||
        !await_http_ready(c.coordinator_http_port, std::chrono::seconds(3)))
        return false;
    c.worker_pid = spawn({node_binary_path(),
                          "--role=worker",
                          "--id=worker-sql-1",
                          "--coordinator-host=127.0.0.1",
                          "--coordinator-port=" + std::to_string(c.coordinator_control_port),
                          "--http-port=" + std::to_string(worker_http_port),
                          "--http-bind=127.0.0.1"});
    if (c.worker_pid <= 0 || !await_http_ready(worker_http_port, std::chrono::seconds(3)))
        return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(400));  // worker register settle
    return true;
}

}  // namespace integration

}  // namespace

TEST(SqlCli, HttpSubmitDeliversSpecToCoordinator) {
    integration::Cluster c;
    if (!integration::start_cluster(c)) {
        GTEST_SKIP() << "cluster startup failed";
    }

    // Use unique paths so reruns don't conflict.
    auto in_path =
        std::filesystem::temp_directory_path() /
        ("clink_sql_http_in_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".txt");
    auto out_path =
        std::filesystem::temp_directory_path() /
        ("clink_sql_http_out_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".txt");
    {
        std::ofstream in(in_path);
        in << "hello\nworld\nclink-sql\n";
    }

    auto sql = std::string("CREATE TABLE src_t (line TEXT) WITH (connector='file', path='") +
               in_path.string() +
               "'); "
               "CREATE TABLE dst_t (line TEXT) WITH (connector='file', path='" +
               out_path.string() +
               "'); "
               "INSERT INTO dst_t SELECT line FROM src_t";

    auto result = run_compiler("--coordinator-host 127.0.0.1 --coordinator-port " +
                               std::to_string(c.coordinator_http_port) +
                               " --name sql-http-test -e \"" + sql + "\"");

    EXPECT_EQ(result.exit_code, 0)
        << "stderr: " << result.stderr_text << "\nstdout: " << result.stdout_text;
    EXPECT_NE(result.stdout_text.find("\"ok\":true"), std::string::npos)
        << "stdout: " << result.stdout_text;
    EXPECT_NE(result.stdout_text.find("\"job_id\""), std::string::npos);
}

#endif  // CLINK_NODE_BINARY

TEST(SqlCli, CatalogDirSurvivesAcrossInvocations) {
    namespace fs = std::filesystem;
    auto cat_dir = fs::temp_directory_path() /
                   ("clink_sql_cli_cat_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::remove_all(cat_dir);

    // First invocation: declare two tables only. No INSERT.
    auto r1 = run_compiler(
        "--catalog-dir " + cat_dir.string() +
        " -e \"CREATE TABLE src_t (line TEXT) WITH (connector='file', path='/tmp/in'); "
        "CREATE TABLE dst_t (line TEXT) WITH (connector='file', path='/tmp/out')\"");
    EXPECT_EQ(r1.exit_code, 0) << "stderr: " << r1.stderr_text;
    EXPECT_TRUE(fs::exists(cat_dir / "src_t.json"));
    EXPECT_TRUE(fs::exists(cat_dir / "dst_t.json"));

    // Second invocation: reference the previously-declared tables.
    // Without persistence this would fail "table not found".
    auto r2 = run_compiler("--catalog-dir " + cat_dir.string() +
                           " -e \"INSERT INTO dst_t SELECT line FROM src_t\"");
    EXPECT_EQ(r2.exit_code, 0) << "stderr: " << r2.stderr_text;
    EXPECT_NE(r2.stdout_text.find("file_text_source"), std::string::npos);
    EXPECT_NE(r2.stdout_text.find("file_text_sink"), std::string::npos);

    fs::remove_all(cat_dir);
}
