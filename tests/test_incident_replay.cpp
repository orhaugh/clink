// The replay checks of an incident bundle (design record 014) on a real
// captured run: `clink run` executes a windowed SQL job with the flight
// recorder and checkpointing armed, incident collection gathers it, and each
// test mutates a copy of the bundle to show the check catching what it exists
// to catch. The job graph the checks read comes from planning the same script,
// as a coordinator's job manifest would record it.

#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <unistd.h>
#include <variant>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/incident_replay.hpp"
#include "clink/sql/replay.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/script_runner.hpp"

namespace fs = std::filesystem;
using clink::config::JsonValue;
using clink::incident::Check;
using clink::incident::Verdict;

namespace {

std::string slurp_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream s;
    s << in.rdbuf();
    return s.str();
}

void spill_file(const fs::path& p, const std::string& body) {
    fs::create_directories(p.parent_path());
    std::ofstream(p, std::ios::binary | std::ios::trunc) << body;
}

int run_command(const std::string& cmd, std::string* output = nullptr) {
    FILE* pipe = ::popen((cmd + " 2>&1").c_str(), "r");
    if (pipe == nullptr) {
        return -1;
    }
    std::array<char, 4096> buf{};
    std::string out;
    while (std::fgets(buf.data(), buf.size(), pipe) != nullptr) {
        out += buf.data();
    }
    const int status = ::pclose(pipe);
    if (output != nullptr) {
        *output = out;
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

const Check* find_check(const std::vector<Check>& checks, const std::string& id) {
    for (const auto& c : checks) {
        if (c.id == id) {
            return &c;
        }
    }
    return nullptr;
}

std::string describe(const Check* c) {
    if (c == nullptr) {
        return "(no such check)";
    }
    std::string s = std::string{clink::incident::to_string(c->verdict)} + ": " + c->reason;
    for (std::size_t i = 0; i < c->counts.size(); ++i) {
        s += (i == 0 ? "\n  counts: " : ", ") + c->counts[i].first + " " +
             std::to_string(c->counts[i].second);
    }
    for (const auto& d : c->details) {
        s += "\n  " + d;
    }
    return s;
}

// One of a check's selection counts; fails the test when it has none.
std::uint64_t count_of(const Check* c, const std::string& name) {
    if (c != nullptr) {
        for (const auto& [n, value] : c->counts) {
            if (n == name) {
                return value;
            }
        }
    }
    ADD_FAILURE() << "no count '" << name << "' in " << describe(c);
    return 0;
}

// A check of a JSON report `clink incident-verify --report` wrote.
JsonValue report_check(const fs::path& report, const std::string& id) {
    const auto json = clink::config::parse(slurp_file(report));
    for (const auto& c : json.at("checks").as_array()) {
        if (c.string_or("id", "") == id) {
            return c;
        }
    }
    ADD_FAILURE() << "no " << id << " in " << report;
    return JsonValue{clink::config::JsonObject{}};
}

// The input: five users, events 50 ms apart, so a one-second window holds
// twenty events across them.
struct Event {
    std::string usr;
    std::int64_t ts;
    std::int64_t amount;
};

std::vector<Event> input_events() {
    std::vector<Event> out;
    for (std::int64_t i = 0; i < 600; ++i) {
        out.push_back(Event{"u" + std::to_string(i % 5), i * 50, (i * 13) % 41});
    }
    return out;
}

// What the window operator must emit, computed here from the input alone.
std::vector<std::string> expected_window_rows() {
    std::map<std::pair<std::string, std::int64_t>, std::int64_t> totals;
    for (const auto& e : input_events()) {
        if (e.amount > 2) {
            totals[{e.usr, e.ts / 1000 * 1000}] += e.amount;
        }
    }
    std::vector<std::string> rows;
    for (const auto& [key, total] : totals) {
        rows.push_back("{\"total\":" + std::to_string(total) + ",\"usr\":\"" + key.first +
                       "\",\"window_end\":" + std::to_string(key.second + 1000) +
                       ",\"window_start\":" + std::to_string(key.second) + "}");
    }
    return rows;
}

class IncidentReplayTest : public ::testing::Test {
protected:
    static inline fs::path root_;
    static inline fs::path base_;       // the bundle collected from the full run
    static inline fs::path truncated_;  // the bundle of a run whose captures were capped
    static inline std::string setup_error_;

    static std::string ddl(const fs::path& dir) {
        return "CREATE TABLE evt (usr TEXT, ts BIGINT, amount BIGINT) WITH (connector='file', "
               "format='json', path='" +
               (dir / "in.jsonl").string() +
               "', event_time_column='ts');\n"
               "CREATE TABLE totals (usr TEXT, total BIGINT) WITH (connector='file', "
               "format='json', path='" +
               (dir / "out.jsonl").string() + "');\n";
    }

    static std::string insert() {
        return "INSERT INTO totals SELECT usr, SUM(amount) AS total FROM evt WHERE amount > 2 "
               "GROUP BY TUMBLE(ts, 1000), usr;\n";
    }

    // Run the job captured in `dir`, write the job manifest a coordinator
    // would hold, and collect the bundle at `out`.
    static void run_and_collect(const fs::path& dir,
                                const fs::path& out,
                                const std::string& extra_flags) {
        fs::create_directories(dir);
        {
            std::ofstream in(dir / "in.jsonl");
            for (const auto& e : input_events()) {
                in << "{\"usr\":\"" << e.usr << "\",\"ts\":" << e.ts << ",\"amount\":" << e.amount
                   << "}\n";
            }
        }
        spill_file(dir / "job.sql", ddl(dir) + insert());
        std::string output;
        const auto rc = run_command(
            std::string{CLINK_CLI_BINARY} + " run " + (dir / "job.sql").string() +
                " --checkpoint-dir=" + (dir / "ckpt").string() +
                " --checkpoint-interval-ms=200 --capture-dir=" + (dir / "capture").string() + " " +
                extra_flags,
            &output);
        if (rc != 0) {
            throw std::runtime_error("clink run failed: " + output);
        }
        clink::sql::Catalog catalog;
        clink::sql::ScriptRunOptions opts;
        opts.parallelism = 1;
        std::ostringstream sink_out;
        std::ostringstream sink_err;
        std::vector<clink::cluster::JobGraphSpec> specs;
        const auto submit = [&specs](const clink::cluster::JobGraphSpec& spec, const std::string&) {
            specs.push_back(spec);
            return 0;
        };
        if (clink::sql::run_script(ddl(dir) + insert(),
                                   catalog,
                                   opts,
                                   clink::sql::ScriptIO{&sink_out, &sink_err},
                                   submit) != 0 ||
            specs.size() != 1) {
            throw std::runtime_error("the script did not plan: " + sink_err.str());
        }
        clink::config::JsonObject manifest;
        manifest["graph_json"] = JsonValue{specs.front().to_json()};
        manifest["requires_commit_confirmation"] = JsonValue{false};
        manifest["checkpoint_dir"] = JsonValue{(dir / "ckpt").string()};
        spill_file(dir / "ha" / "jobs" / "1" / "manifest.json",
                   JsonValue{std::move(manifest)}.serialize(2));
        clink::incident::CollectOptions o;
        o.checkpoint_dir = (dir / "ckpt").string();
        o.job = 1;
        o.ha_dir = (dir / "ha").string();
        o.capture_dir = (dir / "capture").string();
        o.include_state = true;
        o.include_captures = true;
        o.out = out.string();
        (void)clink::incident::collect(o);
    }

    static void SetUpTestSuite() {
        root_ = fs::temp_directory_path() / ("clink_incident_replay_" + std::to_string(::getpid()));
        fs::remove_all(root_);
        base_ = root_ / "bundle";
        truncated_ = root_ / "bundle-truncated";
        try {
            run_and_collect(root_ / "run", base_, "");
            run_and_collect(root_ / "run-capped", truncated_, "--capture-records=20");
        } catch (const std::exception& e) {
            setup_error_ = e.what();
        }
    }

    static void TearDownTestSuite() {
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    void SetUp() override {
        ASSERT_TRUE(setup_error_.empty()) << setup_error_;
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        work_ = root_ / info->name();
        fs::remove_all(work_);
        fs::copy(base_, work_, fs::copy_options::recursive);
    }

    fs::path work_;

    [[nodiscard]] clink::incident::Bundle bundle() const {
        return clink::incident::read_bundle(work_);
    }

    // The captured operator of `op_type`, by the op.json the bundle holds.
    static std::uint64_t op_of(const fs::path& bundle_root, const std::string& op_type) {
        for (const auto& e : fs::directory_iterator(bundle_root / "capture")) {
            for (const auto& sub : fs::directory_iterator(e.path())) {
                const auto spec = clink::config::parse(slurp_file(sub.path() / "op.json"));
                if (spec.string_or("op_type", "") == op_type) {
                    return std::stoull(e.path().filename().string().substr(3));
                }
            }
        }
        ADD_FAILURE() << "no capture of " << op_type;
        return 0;
    }

    static fs::path epoch_file(const fs::path& bundle_root, std::uint64_t op, std::uint64_t epoch) {
        for (const auto& sub :
             fs::directory_iterator(bundle_root / "capture" / ("op-" + std::to_string(op)))) {
            const auto f = sub.path() / ("epoch-" + std::to_string(epoch) + ".cap");
            if (fs::exists(f)) {
                return f;
            }
        }
        return {};
    }

    void edit_manifest(const std::function<void(JsonValue&)>& f) const {
        auto m = clink::config::parse(slurp_file(work_ / "manifest.json"));
        f(m);
        spill_file(work_ / "manifest.json", m.serialize(2));
    }

    // Record a changed or added file's size and digest in the manifest, as a
    // collection of evidence that was already so would have.
    void relist(const fs::path& file, const std::string& role) const {
        const auto rel = fs::relative(file, work_).generic_string();
        edit_manifest([&](JsonValue& m) {
            bool found = false;
            for (auto& a : m.as_object()["artifacts"].as_array()) {
                if (a.string_or("path", "") == rel) {
                    a.as_object()["bytes"] =
                        JsonValue{static_cast<std::int64_t>(fs::file_size(file))};
                    a.as_object()["sha256"] = JsonValue{clink::incident::sha256_file(file)};
                    found = true;
                }
            }
            if (!found) {
                clink::config::JsonObject a;
                a["path"] = JsonValue{rel};
                a["role"] = JsonValue{role};
                a["bytes"] = JsonValue{static_cast<std::int64_t>(fs::file_size(file))};
                a["sha256"] = JsonValue{clink::incident::sha256_file(file)};
                m.as_object()["artifacts"].as_array().emplace_back(std::move(a));
            }
        });
    }

    // The window operator's epoch 1 copied as its epoch 2, which starts from
    // checkpoint 1, as a longer run would have captured it. Returns the op.
    std::uint64_t add_second_epoch() const {
        const auto window = op_of(work_, "tumbling_window_row");
        const auto first = epoch_file(work_, window, 1);
        const auto second = first.parent_path() / "epoch-2.cap";
        fs::copy_file(first, second);
        relist(second, "capture_epoch");
        const auto rel = fs::relative(second, work_).generic_string();
        edit_manifest([&](JsonValue& m) {
            clink::config::JsonObject cap;
            cap["op"] = JsonValue{std::to_string(window)};
            cap["subtask"] = JsonValue{static_cast<std::int64_t>(
                std::stoull(first.parent_path().filename().string().substr(8)))};
            cap["epoch"] = JsonValue{static_cast<std::int64_t>(2)};
            cap["path"] = JsonValue{rel};
            cap["truncated"] = JsonValue{false};
            m.as_object()["captures"].as_array().emplace_back(std::move(cap));
        });
        return window;
    }

    // What production handed the window operator's consumer in epoch 1, with
    // its first row's total changed: the window's replay no longer agrees.
    void doctor_the_windows_output() const {
        const auto file = epoch_file(work_, op_of(work_, "row_bind_columns"), 1);
        rewrite_capture(file, [](clink::sql::Row& row) {
            auto obj = clink::sql::to_json_object(row.values);
            obj["total"] = JsonValue{static_cast<std::int64_t>(999999)};
            row.values = clink::sql::row_columns_from_json(std::move(obj));
        });
        relist(file, "capture_epoch");
    }

    // Where an operator's epoch falls in the selection of every captured
    // epoch, which is replayed in the bundle's order: by operator id, then
    // subtask, then epoch.
    [[nodiscard]] std::size_t position_of(std::uint64_t op, std::uint64_t epoch) const {
        const auto b = bundle();
        for (std::size_t i = 0; i < b.captures.size(); ++i) {
            if (b.captures[i].op == op && b.captures[i].epoch == epoch) {
                return i;
            }
        }
        ADD_FAILURE() << "no capture of op " << op << " epoch " << epoch;
        return 0;
    }

    // `clink incident-verify <work> --profile=replay <flags>`, its JSON report
    // at `report`; returns the exit code.
    int verify_cli(const std::string& flags, const fs::path& report) const {
        std::string output;
        const auto rc =
            run_command(std::string{CLINK_CLI_BINARY} + " incident-verify " + work_.string() +
                            " --profile=replay " + flags + " --report=" + report.string(),
                        &output);
        if (rc != 0 && rc != 1 && rc != 3) {
            ADD_FAILURE() << "exit " << rc << ": " << output;
        }
        return rc;
    }

    // Rewrite a capture's records through `change`, keeping its header.
    static void rewrite_capture(const fs::path& file,
                                const std::function<void(clink::sql::Row&)>& change) {
        const auto text = slurp_file(file);
        auto parsed = clink::capture::read_capture_events(std::as_bytes(std::span{text}),
                                                          clink::sql::row_json_codec());
        ASSERT_TRUE(parsed.has_value());
        bool changed = false;
        for (auto& ev : parsed->second) {
            if (auto* rec = std::get_if<clink::Record<clink::sql::Row>>(&ev); rec && !changed) {
                change(rec->value());
                changed = true;
            }
        }
        const auto bytes = clink::capture::encode_capture_file(
            parsed->first, parsed->second, clink::sql::row_json_codec());
        std::string out(bytes.size(), '\0');
        for (std::size_t i = 0; i < bytes.size(); ++i) {
            out[i] = static_cast<char>(bytes[i]);
        }
        spill_file(file, out);
    }
};

TEST_F(IncidentReplayTest, ARealRunReplaysDeterministicallyAndAgreesWithProduction) {
    // Every captured epoch of the job replays identically. Every one whose
    // consumer is captured agrees with production; the operator before the
    // sink cannot be compared, by its topology, so the comparison does not
    // establish the whole job's selection: unsupported, never passed.
    const auto checks = clink::sql::incident_replay_checks(bundle(), {});
    const auto* det = find_check(checks, "replay.determinism");
    const auto* rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(det->verdict, Verdict::Passed) << describe(det);
    EXPECT_EQ(count_of(det, "passed"), bundle().captures.size()) << describe(det);
    EXPECT_EQ(rec->verdict, Verdict::Unsupported) << describe(rec);
    EXPECT_EQ(count_of(rec, "unsupported"), 1u) << describe(rec);
    EXPECT_EQ(count_of(rec, "passed") + 1, count_of(rec, "selected")) << describe(rec);
    EXPECT_EQ(count_of(rec, "omitted"), 0u) << describe(rec);
    EXPECT_EQ(find_check(checks, "replay.expected_output"), nullptr) << "no expectation was given";
    EXPECT_NE(describe(rec).find("is a sink"), std::string::npos)
        << "the operator before the sink cannot be compared, by its topology: " << describe(rec);
    // The window operator's own epoch is among those compared with what
    // production handed its consumer, and selected alone it passes.
    clink::sql::IncidentReplayOptions o;
    o.op = op_of(work_, "tumbling_window_row");
    const auto window = clink::sql::incident_replay_checks(bundle(), o);
    const auto* compared = find_check(window, "replay.recorded_output");
    EXPECT_EQ(compared->verdict, Verdict::Passed) << describe(compared);
    EXPECT_EQ(compared->evidence.size(), 1u) << describe(compared);
}

// Production's window state was built by its own history and the replay's is
// restored from a snapshot, so an operator that fires several keys at one
// watermark may visit them in another order. That passes, and is said; one
// key's own rows in another order does not.
TEST_F(IncidentReplayTest, RowsOfDifferentKeysMayComeInAnotherOrderButOneKeysMayNot) {
    const auto window = op_of(work_, "tumbling_window_row");
    const auto file = epoch_file(work_, op_of(work_, "row_bind_columns"), 1);
    const auto original = slurp_file(file);
    // Swap the first two neighbouring records whose keys (usr) differ, which
    // leaves each key's own rows in order, or, for the second case, two
    // neighbouring records of one user.
    const auto swap_first = [&](bool same_key) {
        spill_file(file, original);
        auto parsed = clink::capture::read_capture_events(std::as_bytes(std::span{original}),
                                                          clink::sql::row_json_codec());
        ASSERT_TRUE(parsed.has_value());
        std::vector<std::size_t> data;
        for (std::size_t i = 0; i < parsed->second.size(); ++i) {
            if (std::holds_alternative<clink::Record<clink::sql::Row>>(parsed->second[i])) {
                data.push_back(i);
            }
        }
        const auto usr = [&](std::size_t i) {
            return clink::sql::to_json_object(
                       std::get<clink::Record<clink::sql::Row>>(parsed->second[i]).value().values)
                .at("usr")
                .as_string();
        };
        for (std::size_t x = 0; x + 1 < data.size(); ++x) {
            const auto y = x + 1;
            if ((usr(data[x]) == usr(data[y])) == same_key) {
                std::swap(parsed->second[data[x]], parsed->second[data[y]]);
                const auto bytes = clink::capture::encode_capture_file(
                    parsed->first, parsed->second, clink::sql::row_json_codec());
                std::string out(bytes.size(), '\0');
                for (std::size_t i = 0; i < bytes.size(); ++i) {
                    out[i] = static_cast<char>(bytes[i]);
                }
                spill_file(file, out);
                return;
            }
        }
        FAIL() << "no two records to swap";
    };
    clink::sql::IncidentReplayOptions o;
    o.op = window;

    swap_first(/*same_key=*/false);
    relist(file, "capture_epoch");
    auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(rec->verdict, Verdict::Passed) << describe(rec);
    EXPECT_NE(describe(rec).find("another order"), std::string::npos) << describe(rec);

    swap_first(/*same_key=*/true);
    relist(file, "capture_epoch");
    checks = clink::sql::incident_replay_checks(bundle(), o);
    rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(rec->verdict, Verdict::Failed) << describe(rec);
    EXPECT_NE(describe(rec).find("a key's own rows"), std::string::npos) << describe(rec);
}

TEST_F(IncidentReplayTest, ProductionOutputThatDiffersFailsWhileTheReplayStaysDeterministic) {
    // What production handed the window's consumer, changed: the replay still
    // agrees with itself, and no longer with production.
    doctor_the_windows_output();
    clink::sql::IncidentReplayOptions o;
    o.op = op_of(work_, "tumbling_window_row");
    const auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(checks, "replay.determinism");
    const auto* rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(det->verdict, Verdict::Passed) << describe(det);
    EXPECT_EQ(rec->verdict, Verdict::Failed) << describe(rec);
    EXPECT_NE(describe(rec).find("999999"), std::string::npos) << describe(rec);
}

TEST_F(IncidentReplayTest, ATruncatedCaptureReplaysItsPrefixAndCannotStandForTheEpoch) {
    fs::remove_all(work_);
    fs::copy(truncated_, work_, fs::copy_options::recursive);
    clink::sql::IncidentReplayOptions o;
    o.op = op_of(work_, "filter_row_predicate");
    const auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(checks, "replay.determinism");
    const auto* rec = find_check(checks, "replay.recorded_output");
    // The stored prefix replays identically twice, and the records past the
    // cap were never replayed: the epoch is not established.
    EXPECT_EQ(det->verdict, Verdict::Unavailable) << describe(det);
    EXPECT_NE(describe(det).find("identical twice"), std::string::npos) << describe(det);
    EXPECT_NE(describe(det).find("truncated"), std::string::npos) << describe(det);
    EXPECT_EQ(rec->verdict, Verdict::Unavailable) << describe(rec);
    EXPECT_NE(describe(rec).find("truncated"), std::string::npos) << describe(rec);
}

TEST_F(IncidentReplayTest, AnOperatorThisBuildCannotRebuildIsUnsupported) {
    const auto window = op_of(work_, "tumbling_window_row");
    const auto spec_file = epoch_file(work_, window, 1).parent_path() / "op.json";
    auto spec = clink::config::parse(slurp_file(spec_file));
    spec.as_object()["op_type"] = JsonValue{"window_this_build_never_registered_row"};
    spill_file(spec_file, spec.serialize(0));
    relist(spec_file, "capture_spec");
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    const auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unsupported) << describe(det);
}

TEST_F(IncidentReplayTest, ALookupOperatorIsNeverReplayedSoNothingIsContacted) {
    // A listener standing in for the lookup's service: verification must not
    // connect to it.
    const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof addr), 0);
    ASSERT_EQ(::listen(listener, 8), 0);
    socklen_t len = sizeof addr;
    ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len);
    const auto port = std::to_string(ntohs(addr.sin_port));

    const auto window = op_of(work_, "tumbling_window_row");
    const auto spec_file = epoch_file(work_, window, 1).parent_path() / "op.json";
    clink::config::JsonObject params;
    params["function_name"] = JsonValue{"enrich"};
    params["url"] = JsonValue{"http://127.0.0.1:" + port};
    clink::config::JsonObject spec;
    spec["op_type"] = JsonValue{"async_lookup_join_row"};
    spec["in_channel"] = JsonValue{"row"};
    spec["out_channel"] = JsonValue{"row"};
    spec["uid"] = JsonValue{""};
    spec["params"] = JsonValue{std::move(params)};
    spill_file(spec_file, JsonValue{std::move(spec)}.serialize(0));
    relist(spec_file, "capture_spec");
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    const auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unsupported) << describe(det);
    EXPECT_NE(describe(det).find("external system"), std::string::npos) << describe(det);
    EXPECT_THROW((void)clink::sql::incident_regress(
                     bundle(), window, 1, std::nullopt, (work_ / "regression").string()),
                 std::runtime_error);

    timeval none{};
    fd_set ready;
    FD_ZERO(&ready);
    FD_SET(listener, &ready);
    EXPECT_EQ(::select(listener + 1, &ready, nullptr, nullptr, &none), 0)
        << "verification connected to the lookup's service";
    ::close(listener);
}

TEST_F(IncidentReplayTest, AnEpochWithoutItsStartingStateIsUnavailable) {
    // A second epoch of the window operator, starting from checkpoint 1, as a
    // longer run would have captured. With checkpoint 1's state it replays;
    // without it, it is unavailable, not failed.
    const auto window = add_second_epoch();
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    o.epoch = 2;
    auto checks = clink::sql::incident_replay_checks(bundle(), o);
    EXPECT_EQ(find_check(checks, "replay.determinism")->verdict, Verdict::Passed)
        << describe(find_check(checks, "replay.determinism"));

    // Checkpoint 1's snapshots, gone from the bundle and its manifest.
    edit_manifest([&](JsonValue& m) {
        auto& arts = m.as_object()["artifacts"].as_array();
        std::erase_if(arts, [&](const JsonValue& a) {
            const auto p = a.string_or("path", "");
            if (p.ends_with("checkpoint-1.snap")) {
                fs::remove(work_ / p);
                return true;
            }
            return false;
        });
    });
    checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unavailable) << describe(det);
}

TEST_F(IncidentReplayTest, TheExpectationComparesRowsAndNamesTheDifference) {
    clink::sql::IncidentReplayOptions o;
    o.op = op_of(work_, "tumbling_window_row");
    o.expected_path = (work_ / "expected.ndjson").string();
    auto rows = expected_window_rows();
    {
        std::ofstream out(o.expected_path);
        for (auto it = rows.rbegin(); it != rows.rend(); ++it) {  // order is not compared
            out << *it << "\n";
        }
    }
    auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* exp = find_check(checks, "replay.expected_output");
    ASSERT_NE(exp, nullptr);
    EXPECT_EQ(exp->verdict, Verdict::Passed) << describe(exp);

    rows.front().replace(rows.front().find("\"total\":"), 8, "\"total\":1");
    {
        std::ofstream out(o.expected_path, std::ios::trunc);
        for (const auto& r : rows) {
            out << r << "\n";
        }
    }
    checks = clink::sql::incident_replay_checks(bundle(), o);
    exp = find_check(checks, "replay.expected_output");
    EXPECT_EQ(exp->verdict, Verdict::Failed) << describe(exp);
    EXPECT_NE(describe(exp).find("only expected"), std::string::npos) << describe(exp);
    EXPECT_NE(describe(exp).find("only in the replay"), std::string::npos) << describe(exp);
}

TEST_F(IncidentReplayTest, AReplayedEpochBecomesARegressionBundleThatPasses) {
    const auto window = op_of(work_, "tumbling_window_row");
    const auto out = work_ / "regression";
    const auto n = clink::sql::incident_regress(bundle(), window, 1, std::nullopt, out.string());
    EXPECT_GT(n, 0u);
    EXPECT_TRUE(fs::exists(out / "replay_regression_test.cpp"));
    EXPECT_EQ(clink::sql::run_replay_regression(out.string()), "");
    std::string output;
    EXPECT_EQ(
        run_command(std::string{CLINK_CLI_BINARY} + " replay --run-test=" + out.string(), &output),
        0)
        << output;
    // A golden that no longer matches is located, from the command line too.
    auto golden = slurp_file(out / "golden.ndjson");
    golden.replace(golden.find("\"total\":"), 8, "\"total\":7");
    spill_file(out / "golden.ndjson", golden);
    EXPECT_NE(clink::sql::run_replay_regression(out.string()), "");
    EXPECT_EQ(
        run_command(std::string{CLINK_CLI_BINARY} + " replay --run-test=" + out.string(), &output),
        1)
        << output;
}

TEST_F(IncidentReplayTest, ACaptureCutShortIsUnavailableRatherThanReplayedAsTheEpoch) {
    const auto window = op_of(work_, "tumbling_window_row");
    const auto file = epoch_file(work_, window, 1);
    const auto bytes = slurp_file(file);
    spill_file(file, bytes.substr(0, bytes.size() * 2 / 3));
    relist(file, "capture_epoch");
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    o.expected_path = (work_ / "expected.ndjson").string();
    {
        std::ofstream out(o.expected_path);
        for (const auto& row : expected_window_rows()) {
            out << row << "\n";
        }
    }
    const auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unavailable) << describe(det);
    EXPECT_NE(describe(det).find("cut short"), std::string::npos) << describe(det);
    EXPECT_EQ(find_check(checks, "replay.recorded_output")->verdict, Verdict::Unavailable);
    EXPECT_EQ(find_check(checks, "replay.expected_output")->verdict, Verdict::Unavailable)
        << describe(find_check(checks, "replay.expected_output"));
}

TEST_F(IncidentReplayTest, AnExpectationOverATruncatedEpochIsUnavailable) {
    fs::remove_all(work_);
    fs::copy(truncated_, work_, fs::copy_options::recursive);
    clink::sql::IncidentReplayOptions o;
    o.op = op_of(work_, "tumbling_window_row");
    o.expected_path = (work_ / "expected.ndjson").string();
    {
        std::ofstream out(o.expected_path);
        for (const auto& row : expected_window_rows()) {
            out << row << "\n";
        }
    }
    const auto exp_checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* exp = find_check(exp_checks, "replay.expected_output");
    ASSERT_NE(exp, nullptr);
    EXPECT_EQ(exp->verdict, Verdict::Unavailable) << describe(exp);
    // Nor is a part of an epoch frozen as a regression test of it, and nothing
    // is left behind.
    const auto out = work_ / "regression";
    EXPECT_THROW((void)clink::sql::incident_regress(bundle(), *o.op, 1, std::nullopt, out.string()),
                 std::runtime_error);
    EXPECT_FALSE(fs::exists(out));
}

TEST_F(IncidentReplayTest, AStartingSnapshotOutsideTheBundleIsNeverRead) {
    const auto window = add_second_epoch();
    // A copy of the state outside the bundle, which checkpoint 1's marker now
    // names, as a marker would name state the bundle did not collect.
    const auto outside = root_ / (std::string{"outside-"} +
                                  ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(outside);
    fs::copy(work_ / "checkpoint" / "_state", outside, fs::copy_options::recursive);
    const auto marker = work_ / "checkpoint" / "_jobs" / "1" / "COMPLETED-1";
    auto body = slurp_file(marker);
    const auto at = body.find("state=");
    ASSERT_NE(at, std::string::npos) << body;
    body = body.substr(0, at) + "state=" + (outside / "1").string() + "\n";
    spill_file(marker, body);
    relist(marker, "completed_marker");
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    o.epoch = 2;
    const auto det_checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(det_checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unavailable) << describe(det);
    EXPECT_NE(describe(det).find("not in the bundle"), std::string::npos) << describe(det);
}

TEST_F(IncidentReplayTest, AnEpochThatBeginsOnARescaleIsUnsupported) {
    const auto window = add_second_epoch();
    // The graph was rescaled after checkpoint 1: epoch 2 starts from the old
    // layout's state, which the restore re-divided across the new subtasks.
    edit_manifest([](JsonValue& m) {
        m.as_object()["job"].as_object()["rescaled"] = JsonValue{true};
        m.as_object()["job"].as_object()["prior_graph_through"] =
            JsonValue{static_cast<std::int64_t>(1)};
    });
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    o.epoch = 2;
    const auto det_checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* det = find_check(det_checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unsupported) << describe(det);
    EXPECT_NE(describe(det).find("rescaled"), std::string::npos) << describe(det);
}

TEST_F(IncidentReplayTest, TheCommandTakesEvery64BitOperatorIdAndRefusesAMalformedBundleCleanly) {
    const std::string cli = CLINK_CLI_BINARY;
    const auto verify = cli + " incident-verify " + work_.string() + " --profile=replay";
    std::string output;
    // The largest id parses; it selects nothing, so the replays are partial.
    EXPECT_EQ(run_command(verify + " --op=18446744073709551615 --max-replays=5", &output), 3)
        << output;
    EXPECT_EQ(run_command(verify + " --op=18446744073709551616", &output), 2) << output;
    EXPECT_EQ(run_command(verify + " --max-replays=0", &output), 2) << output;
    // A manifest field of the wrong type refuses the bundle: exit 2, a report,
    // and no abort.
    edit_manifest([](JsonValue& m) {
        m.as_object()["collector"].as_object()["collected_at"] = JsonValue{7};
    });
    EXPECT_EQ(run_command(verify + " --json", &output), 2) << output;
    EXPECT_NE(output.find("collector.collected_at"), std::string::npos) << output;
}

// The whole job's captured epochs with room to replay one of them. The one
// replayed once stood for all of them: both checks passed and the command
// exited 0. The epochs the bound left out are counted and named instead, and
// the result is partial.
TEST_F(IncidentReplayTest, ABoundBelowTheSelectionIsPartialAndCountsWhatItLeftOut) {
    const auto selected = bundle().captures.size();
    ASSERT_GE(selected, 3u);
    clink::sql::IncidentReplayOptions o;
    o.max_replays = 1;
    const auto checks = clink::sql::incident_replay_checks(bundle(), o);
    for (const char* id : {"replay.determinism", "replay.recorded_output"}) {
        const auto* c = find_check(checks, id);
        EXPECT_EQ(c->verdict, Verdict::Unavailable) << describe(c);
        EXPECT_EQ(count_of(c, "selected"), selected) << describe(c);
        EXPECT_EQ(count_of(c, "checked"), 1u) << describe(c);
        EXPECT_EQ(count_of(c, "omitted"), selected - 1) << describe(c);
        EXPECT_NE(c->reason.find("--max-replays=1"), std::string::npos) << describe(c);
        EXPECT_NE(c->next_action.find("at least " + std::to_string(selected)), std::string::npos)
            << describe(c);
        EXPECT_NE(describe(c).find("not checked, beyond --max-replays=1"), std::string::npos)
            << describe(c);
    }
    const auto report = work_ / "report.json";
    EXPECT_EQ(verify_cli("--max-replays=1", report), 3);
    const auto det = report_check(report, "replay.determinism");
    EXPECT_EQ(det.string_or("verdict", ""), "unavailable");
    EXPECT_EQ(det.at("counts").at("selected").as_int(), static_cast<std::int64_t>(selected));
    EXPECT_EQ(det.at("counts").at("checked").as_int(), 1);
    EXPECT_EQ(det.at("counts").at("omitted").as_int(), static_cast<std::int64_t>(selected - 1));
    EXPECT_EQ(report_check(report, "replay.recorded_output").string_or("verdict", ""),
              "unavailable");
    EXPECT_EQ(clink::config::parse(slurp_file(report)).string_or("result", ""), "partial");
}

// Production's output doctored where the bound does not reach: the bounded
// run cannot see it and must not pass, and the run that reaches it fails.
// What the window handed its consumer is the doctored record; only the
// window's comparison reads it, and the bound stops just short of the window.
TEST_F(IncidentReplayTest, ADivergenceBeyondTheBoundLeavesTheRunPartialAndTheWholeRunFindsIt) {
    const auto bound = position_of(op_of(work_, "tumbling_window_row"), 1);
    ASSERT_GE(bound, 1u) << "the premise: an operator replays before the window";
    doctor_the_windows_output();
    const auto selected = bundle().captures.size();
    clink::sql::IncidentReplayOptions o;
    o.max_replays = bound;
    auto checks = clink::sql::incident_replay_checks(bundle(), o);
    const auto* rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(rec->verdict, Verdict::Unavailable) << describe(rec);
    EXPECT_EQ(count_of(rec, "omitted"), selected - bound) << describe(rec);
    EXPECT_EQ(count_of(rec, "failed"), 0u) << describe(rec);
    o.max_replays = selected;
    checks = clink::sql::incident_replay_checks(bundle(), o);
    rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(rec->verdict, Verdict::Failed) << describe(rec);
    EXPECT_NE(describe(rec).find("999999"), std::string::npos) << describe(rec);

    const auto bounded = work_ / "bounded.json";
    const auto whole = work_ / "whole.json";
    EXPECT_EQ(verify_cli("--max-replays=" + std::to_string(bound), bounded), 3);
    EXPECT_EQ(report_check(bounded, "replay.recorded_output").string_or("verdict", ""),
              "unavailable");
    EXPECT_EQ(verify_cli("", whole), 1);
    EXPECT_EQ(report_check(whole, "replay.recorded_output").string_or("verdict", ""), "failed");
}

// One selection holding every outcome: epochs that pass, one whose capture
// was cut short (unavailable), the operator before the sink (unsupported for
// the comparison), and one that disagrees with production. The disagreement
// decides the comparison, and still does when a bound leaves an epoch out.
TEST_F(IncidentReplayTest, AFailureFailsTheSelectionWhateverElseIsUnavailableUnsupportedOrOmitted) {
    const auto window = add_second_epoch();
    const auto second = epoch_file(work_, window, 2);
    const auto bytes = slurp_file(second);
    spill_file(second, bytes.substr(0, bytes.size() * 2 / 3));
    relist(second, "capture_epoch");
    doctor_the_windows_output();
    auto checks = clink::sql::incident_replay_checks(bundle(), {});
    const auto* rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(rec->verdict, Verdict::Failed) << describe(rec);
    EXPECT_EQ(count_of(rec, "failed"), 1u) << describe(rec);
    EXPECT_EQ(count_of(rec, "unavailable"), 1u) << describe(rec);
    EXPECT_EQ(count_of(rec, "unsupported"), 1u) << describe(rec);
    EXPECT_GE(count_of(rec, "passed"), 1u) << describe(rec);
    EXPECT_NE(rec->reason.find("whatever the rest show"), std::string::npos) << describe(rec);
    // The replays agree with each other wherever they ran.
    const auto* det = find_check(checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unavailable) << describe(det);
    EXPECT_EQ(count_of(det, "failed"), 0u) << describe(det);

    // The window's first epoch is never last (its second follows it), so a
    // bound one short of the selection still reaches it.
    const auto selected = bundle().captures.size();
    clink::sql::IncidentReplayOptions o;
    o.max_replays = selected - 1;
    checks = clink::sql::incident_replay_checks(bundle(), o);
    rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(rec->verdict, Verdict::Failed) << describe(rec);
    EXPECT_EQ(count_of(rec, "omitted"), 1u) << describe(rec);

    const auto report = work_ / "report.json";
    EXPECT_EQ(verify_cli("--max-replays=" + std::to_string(selected - 1), report), 1);
    const auto json = report_check(report, "replay.recorded_output");
    EXPECT_EQ(json.string_or("verdict", ""), "failed");
    EXPECT_EQ(json.at("counts").at("failed").as_int(), 1);
    EXPECT_EQ(json.at("counts").at("omitted").as_int(), 1);
}

// The window operator renamed to one this build never registered, and the
// rest of the job replayable: an aggregate over the replayable epochs alone
// would pass.
TEST_F(IncidentReplayTest, SupportedAndUnsupportedEpochsTogetherAreNotAPass) {
    const auto window = op_of(work_, "tumbling_window_row");
    const auto spec_file = epoch_file(work_, window, 1).parent_path() / "op.json";
    auto spec = clink::config::parse(slurp_file(spec_file));
    spec.as_object()["op_type"] = JsonValue{"window_this_build_never_registered_row"};
    spill_file(spec_file, spec.serialize(0));
    relist(spec_file, "capture_spec");
    const auto checks = clink::sql::incident_replay_checks(bundle(), {});
    const auto* det = find_check(checks, "replay.determinism");
    EXPECT_EQ(det->verdict, Verdict::Unsupported) << describe(det);
    EXPECT_EQ(count_of(det, "unsupported"), 1u) << describe(det);
    EXPECT_EQ(count_of(det, "passed"), bundle().captures.size() - 1) << describe(det);
    // Nor does the comparison pass: the window it cannot replay is unsupported,
    // and, renamed, it no longer matches its graph operator, so the operator
    // before it has nothing to be compared with.
    const auto* rec = find_check(checks, "replay.recorded_output");
    EXPECT_EQ(rec->verdict, Verdict::Unavailable) << describe(rec);
    EXPECT_GE(count_of(rec, "unsupported"), 1u) << describe(rec);

    const auto report = work_ / "report.json";
    EXPECT_EQ(verify_cli("", report), 3);
    EXPECT_EQ(report_check(report, "replay.determinism").string_or("verdict", ""), "unsupported");
}

// The epoch the bounded whole-job run reached, selected on its own with the
// same bound: checked in full, it passes, report and exit code alike.
TEST_F(IncidentReplayTest, NarrowingTheSelectionToWhatIsCheckedInFullCanPass) {
    const auto window = op_of(work_, "tumbling_window_row");
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    o.epoch = 1;
    o.max_replays = 1;
    o.expected_path = (work_ / "expected.ndjson").string();
    {
        std::ofstream out(o.expected_path);
        for (const auto& row : expected_window_rows()) {
            out << row << "\n";
        }
    }
    const auto checks = clink::sql::incident_replay_checks(bundle(), o);
    for (const char* id :
         {"replay.determinism", "replay.recorded_output", "replay.expected_output"}) {
        const auto* c = find_check(checks, id);
        EXPECT_EQ(c->verdict, Verdict::Passed) << describe(c);
        EXPECT_EQ(count_of(c, "selected"), 1u) << describe(c);
        EXPECT_EQ(count_of(c, "omitted"), 0u) << describe(c);
    }
    const auto report = work_ / "report.json";
    EXPECT_EQ(verify_cli("--op=" + std::to_string(window) + " --epoch=1 --max-replays=1 " +
                             "--expected=" + o.expected_path,
                         report),
              0);
    EXPECT_EQ(clink::config::parse(slurp_file(report)).string_or("result", ""), "satisfied");
}

// An expectation over a selection the bound cut: a row the replay emitted
// that the expectation lacks fails at once; expected rows not emitted may
// belong to the epochs not replayed, so they leave it partial until the
// selection is checked in full, where they fail it.
TEST_F(IncidentReplayTest, AnExpectationOverABoundedSelectionCanFailButNotPass) {
    const auto window = add_second_epoch();
    clink::sql::IncidentReplayOptions o;
    o.op = window;
    o.max_replays = 1;
    o.expected_path = (work_ / "expected.ndjson").string();
    const auto expect = [&](const std::vector<std::string>& rows) {
        std::ofstream out(o.expected_path, std::ios::trunc);
        for (const auto& row : rows) {
            out << row << "\n";
        }
    };
    std::vector<Check> held;
    const auto exp_of = [&] {
        held = clink::sql::incident_replay_checks(bundle(), o);
        return find_check(held, "replay.expected_output");
    };
    auto rows = expected_window_rows();
    expect(rows);
    const auto* exp = exp_of();
    EXPECT_EQ(exp->verdict, Verdict::Unavailable) << describe(exp);
    EXPECT_EQ(count_of(exp, "selected"), 2u) << describe(exp);
    EXPECT_EQ(count_of(exp, "omitted"), 1u) << describe(exp);
    EXPECT_NE(exp->reason.find("--max-replays=1"), std::string::npos) << describe(exp);

    auto changed = rows;
    changed.front().replace(changed.front().find("\"total\":"), 8, "\"total\":1");
    expect(changed);
    exp = exp_of();
    EXPECT_EQ(exp->verdict, Verdict::Failed) << describe(exp);
    EXPECT_NE(describe(exp).find("only in the replay"), std::string::npos) << describe(exp);
    EXPECT_NE(exp->reason.find("whatever the epochs not compared would show"), std::string::npos)
        << describe(exp);

    auto more = rows;
    more.push_back(R"({"total":5,"usr":"nobody","window_end":1000,"window_start":0})");
    expect(more);
    exp = exp_of();
    EXPECT_EQ(exp->verdict, Verdict::Unavailable) << describe(exp);
    o.epoch = 1;
    exp = exp_of();
    EXPECT_EQ(exp->verdict, Verdict::Failed) << describe(exp);
    EXPECT_NE(describe(exp).find("only expected"), std::string::npos) << describe(exp);
}

}  // namespace
