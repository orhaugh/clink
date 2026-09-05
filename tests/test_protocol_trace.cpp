// The protocol trace (design record 012, increment 3): a real in-process
// cluster runs a checkpointed job through the recoverable two-phase sink with
// tracing on, and the trace it leaves must tell the protocol's story in the
// order the specification tells it - trigger, barrier, prepare, ack,
// completion, marker, broadcast, delivery, commit - with the ids and subtasks
// agreeing across events. The same trace is what `scripts/formal-check.sh
// --trace` model-checks against formal/ExactlyOnce.tla; run with
// CLINK_TRACE_VALIDATE=1 (and Java on PATH) to do that here too, and set
// CLINK_PROTOCOL_TRACE_OUT to keep the trace for the CI validator or as a
// fixture under formal/traces/.
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/protocol_trace.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/test/test_cluster.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

struct Ev {
    std::uint64_t seq{};
    std::uint64_t ts{};
    std::string event;
    clink::config::JsonValue raw;
    [[nodiscard]] std::int64_t num(const char* k) const { return raw.int_or(k, -1); }
    [[nodiscard]] std::string str(const char* k) const { return raw.string_or(k, ""); }
    [[nodiscard]] bool flag(const char* k) const { return raw.bool_or(k, false); }
};

std::vector<Ev> read_trace(const std::filesystem::path& dir) {
    std::vector<Ev> out;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() != ".ndjson") {
            continue;
        }
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) {
                continue;
            }
            Ev e;
            e.raw = clink::config::parse(line);
            e.seq = static_cast<std::uint64_t>(e.raw.int_or("seq", 0));
            e.ts = static_cast<std::uint64_t>(e.raw.int_or("ts", 0));
            e.event = e.raw.string_or("event", "");
            out.push_back(std::move(e));
        }
    }
    std::stable_sort(out.begin(), out.end(), [](const Ev& a, const Ev& b) {
        return a.ts != b.ts ? a.ts < b.ts : a.seq < b.seq;
    });
    return out;
}

// The position of the first event of `kind` for checkpoint `ckpt` (and
// subtask `sub` when given), or -1.
std::ptrdiff_t index_of(const std::vector<Ev>& ev,
                        const char* kind,
                        std::int64_t ckpt,
                        std::int64_t sub = -1) {
    for (std::size_t i = 0; i < ev.size(); ++i) {
        if (ev[i].event == kind && ev[i].num("ckpt") == ckpt &&
            (sub < 0 || ev[i].num("sub") == sub)) {
            return static_cast<std::ptrdiff_t>(i);
        }
    }
    return -1;
}

class ProtocolTraceTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        root_ = std::filesystem::temp_directory_path() /
                ("clink_ptrace_" + std::to_string(::getpid()) + "_" + info->name());
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_ / "trace");
        ::setenv("CLINK_PROTOCOL_TRACE_DIR", (root_ / "trace").c_str(), 1);
        protocol_trace::reset_for_tests();
        ASSERT_TRUE(protocol_trace::enabled());
    }
    void TearDown() override {
        // Keep the trace where the validator (CI) or a fixture refresh can find it.
        if (const char* out = std::getenv("CLINK_PROTOCOL_TRACE_OUT");
            out != nullptr && *out != '\0') {
            const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
            const auto dest = std::filesystem::path(out) /
                              (std::string(info->test_suite_name()) + "." + info->name());
            std::error_code ec;
            std::filesystem::remove_all(dest, ec);
            std::filesystem::create_directories(dest, ec);
            for (const auto& entry : std::filesystem::directory_iterator(root_ / "trace")) {
                std::filesystem::copy_file(entry.path(),
                                           dest / entry.path().filename(),
                                           std::filesystem::copy_options::overwrite_existing,
                                           ec);
            }
        }
        ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
        protocol_trace::reset_for_tests();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    // source -> int64_to_string -> file_2pc_sink_string, checkpointed every 40ms.
    JobGraphSpec job(std::int64_t count) const {
        JobGraphSpec g;
        OperatorSpec src;
        src.type = "int64_range_source";
        src.id = "src";
        src.parallelism = 1;
        src.out_channel = std::string{kChannelInt64};
        src.params = {{"count", std::to_string(count)}, {"delay_ms", "2"}};
        g.ops.push_back(src);
        OperatorSpec map;
        map.type = "int64_to_string";
        map.id = "fmt";
        map.inputs = {"src"};
        map.parallelism = 1;
        map.out_channel = std::string{kChannelString};
        g.ops.push_back(map);
        OperatorSpec snk;
        snk.type = "file_2pc_sink_string";
        snk.id = "snk";
        snk.inputs = {"fmt"};
        snk.parallelism = 1;
        snk.out_channel = std::string{kChannelString};
        snk.params = {{"dir", (root_ / "out").string()}};
        g.ops.push_back(snk);
        return g;
    }

    clink::test::TestCluster::Options options() const {
        clink::test::TestCluster::Options o;
        o.workers = 1;
        o.checkpoint.checkpoint_dir = (root_ / "ckpt").string();
        o.checkpoint.interval_ms = 40;
        return o;
    }

    std::filesystem::path root_;
};

TEST_F(ProtocolTraceTest, ACheckpointedRunTellsTheProtocolInTheSpecificationsOrder) {
    {
        clink::test::TestCluster cluster(options());
        cluster.execute(job(/*count=*/300), 60s);
    }
    const auto ev = read_trace(root_ / "trace");
    ASSERT_FALSE(ev.empty()) << "no trace was written to " << (root_ / "trace");

    // Every process's sequence is strictly monotonic (one process here).
    std::map<std::string, std::uint64_t> last_seq;
    for (const auto& e : ev) {
        const auto proc = e.str("proc");
        EXPECT_GT(e.seq, last_seq[proc]) << "seq went backwards in " << proc;
        last_seq[proc] = e.seq;
        EXPECT_FALSE(e.event.empty());
    }

    // Placement names the source and the sink's subtask.
    std::set<std::int64_t> sinks;
    bool source_placed = false;
    for (const auto& e : ev) {
        if (e.event == "SinkPrepare") {
            sinks.insert(e.num("sub"));
            EXPECT_EQ(e.str("family"), "recoverable");
        }
        if (e.event == "Placement" && e.flag("source")) {
            source_placed = true;
        }
    }
    ASSERT_EQ(sinks.size(), 1u) << "one two-phase sink subtask expected";
    EXPECT_TRUE(source_placed);
    const auto sink = *sinks.begin();

    // At least two checkpoints completed and committed during the run, and
    // for each the events come in the specification's order.
    std::vector<std::int64_t> committed;
    for (const auto& e : ev) {
        if (e.event == "SinkCommit" && e.num("sub") == sink) {
            committed.push_back(e.num("ckpt"));
        }
    }
    ASSERT_GE(committed.size(), 2u)
        << "expected at least two committed checkpoints, saw " << committed.size();
    for (const auto c : committed) {
        const auto trig = index_of(ev, "Trigger", c);
        const auto barrier = index_of(ev, "DeliverBarrier", c);
        const auto prepare = index_of(ev, "SinkPrepare", c, sink);
        const auto ack = index_of(ev, "SubtaskAck", c, sink);
        const auto complete = index_of(ev, "CoordComplete", c);
        const auto marker = index_of(ev, "WriteCompleted", c);
        const auto broadcast = index_of(ev, "Broadcast", c);
        const auto deliver = index_of(ev, "DeliverCommit", c, sink);
        const auto commit = index_of(ev, "SinkCommit", c, sink);
        ASSERT_GE(trig, 0) << "checkpoint " << c;
        ASSERT_GE(barrier, 0) << "checkpoint " << c;
        ASSERT_GE(prepare, 0) << "checkpoint " << c;
        ASSERT_GE(ack, 0) << "checkpoint " << c;
        ASSERT_GE(complete, 0) << "checkpoint " << c;
        ASSERT_GE(marker, 0) << "checkpoint " << c;
        ASSERT_GE(broadcast, 0) << "checkpoint " << c;
        ASSERT_GE(deliver, 0) << "checkpoint " << c;
        ASSERT_GE(commit, 0) << "checkpoint " << c;
        EXPECT_LT(trig, barrier) << "checkpoint " << c;
        EXPECT_LT(barrier, prepare) << "checkpoint " << c;
        EXPECT_LT(prepare, ack) << "checkpoint " << c;
        EXPECT_LT(ack, complete) << "checkpoint " << c;
        EXPECT_LT(complete, marker) << "checkpoint " << c;
        EXPECT_LT(marker, broadcast) << "checkpoint " << c;
        EXPECT_LT(broadcast, deliver) << "checkpoint " << c;
        EXPECT_LT(deliver, commit) << "checkpoint " << c;
        EXPECT_EQ(ev[static_cast<std::size_t>(complete)].str("outcome"), "completed");
        EXPECT_FALSE(ev[static_cast<std::size_t>(broadcast)].flag("withheld"));
        EXPECT_TRUE(ev[static_cast<std::size_t>(deliver)].flag("accepted"));
        EXPECT_TRUE(ev[static_cast<std::size_t>(ack)].flag("ok"));
    }

    // Optionally model-check the trace against the specification.
    if (const char* v = std::getenv("CLINK_TRACE_VALIDATE"); v != nullptr && *v == '1') {
        const auto script = std::filesystem::path(CLINK_SOURCE_DIR) / "scripts" / "formal-check.sh";
        const std::string cmd =
            "'" + script.string() + "' --trace '" + (root_ / "trace").string() + "'";
        const int rc = std::system(cmd.c_str());
        EXPECT_EQ(rc, 0) << "the recorded trace is not a behaviour the specification allows";
    }
}

TEST_F(ProtocolTraceTest, TheSwitchIsOffByDefaultAndCostsNothingWhenOff) {
    ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
    protocol_trace::reset_for_tests();
    EXPECT_FALSE(protocol_trace::enabled());
    EXPECT_TRUE(protocol_trace::directory().empty());
    // Emitting while off is a no-op, not a crash.
    protocol_trace::Event("Trigger").u("job", 1).u("ckpt", 1).emit();
    ::setenv("CLINK_PROTOCOL_TRACE_DIR", (root_ / "trace").c_str(), 1);
    protocol_trace::reset_for_tests();
    ASSERT_TRUE(protocol_trace::enabled());
    protocol_trace::Event("Trigger").u("job", 1).u("ckpt", 7).u("epoch", 1).emit();
    protocol_trace::Event("Placement")
        .u("job", 1)
        .u("sub", 0)
        .s("worker", "w\"1")
        .b("source", true)
        .emit();
    const auto ev = read_trace(root_ / "trace");
    ASSERT_EQ(ev.size(), 2u);
    EXPECT_EQ(ev[0].event, "Trigger");
    EXPECT_EQ(ev[0].num("ckpt"), 7);
    EXPECT_EQ(ev[0].seq, 1u);
    EXPECT_EQ(ev[1].seq, 2u);
    EXPECT_EQ(ev[1].str("worker"), "w\"1") << "strings are JSON-escaped";
    EXPECT_TRUE(ev[1].flag("source"));
}

}  // namespace
