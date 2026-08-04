// Two coordinators that both believe they are leader, at the same time.
//
// Follow-up item 3. Fencing is otherwise tested by killing a leader and
// checking that the successor's frames are accepted while the dead one's are
// refused - which is a test of the epoch comparison, not of the condition the
// epoch exists for. Nothing ran two LIVE leaders.
//
// How a genuine split brain is produced here, without any test-only hook in the
// product: leadership is an fcntl write lock on <ha_dir>/leader.lock, and a
// process holds that lock on the file's INODE, not on its name. Unlink the file
// and the holder keeps its lock while the name becomes free, so the next
// coordinator to poll creates a fresh inode, takes the lock on that, reads the
// previous epoch out of active-leader.json and announces itself one above it.
// Now there are two live coordinators, each holding a valid lock on a different
// inode, each believing it leads, with different epochs. That is the real
// failure - an HA directory on a filesystem where the lock does not hold, or an
// operator clearing what looks like a stale lock file.
//
// What this file does NOT cover, and why: one worker receiving frames from both
// coordinators. A worker holds a single control connection, so it hears from one
// coordinator at a time; the worker-side epoch check is exercised by the unit
// tests in test_coordinator_fencing.cpp. What two live leaders genuinely share is
// the HA DIRECTORY, and that is what is asserted here.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "tests/integration/await_port.hpp"
#include "tests/integration/cluster_harness.hpp"

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;

namespace {

std::filesystem::path node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

// active-leader.json is hand-written by HaCoordinator (JsonWriter output, no
// parser dependency), so it is read back the same way.
struct LeaderRecord {
    std::uint16_t port{};
    std::uint64_t epoch{};
    bool ok{false};
};

LeaderRecord read_active_leader(const std::filesystem::path& ha_dir) {
    LeaderRecord out;
    std::ifstream in(ha_dir / "active-leader.json");
    if (!in) {
        return out;
    }
    const std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    const auto field = [&body](const std::string& key) -> std::optional<std::uint64_t> {
        const auto k = "\"" + key + "\":";
        const auto pos = body.find(k);
        if (pos == std::string::npos) {
            return std::nullopt;
        }
        try {
            return std::stoull(body.substr(pos + k.size()));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    };
    const auto port = field("port");
    const auto epoch = field("epoch");
    if (!port.has_value() || !epoch.has_value()) {
        return out;
    }
    out.port = static_cast<std::uint16_t>(*port);
    out.epoch = *epoch;
    out.ok = true;
    return out;
}

class SplitBrainTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary())) {
            GTEST_SKIP() << "clink_node not built";
        }
        root_ = std::filesystem::temp_directory_path() /
                ("clink_sb_" + std::to_string(::getpid()) + "_" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(ha_dir());
        std::filesystem::create_directories(log_dir());
    }

    void TearDown() override {
        for (auto& p : procs_) {
            if (p) {
                p->kill_and_reap();
            }
        }
        if (!::testing::Test::HasFailure()) {
            std::error_code ec;
            std::filesystem::remove_all(root_, ec);
        } else {
            std::cerr << "\n[split-brain] artifacts kept at " << root_.string() << "\n";
        }
    }

    [[nodiscard]] std::filesystem::path ha_dir() const { return root_ / "ha"; }
    [[nodiscard]] std::filesystem::path log_dir() const { return root_ / "logs"; }

    // A coordinator on its OWN port, sharing the HA directory. The harness's
    // start_ha_coordinators gives every process the same port on purpose, so
    // that only the leader can bind; here both have to be able to listen at
    // once, which is the whole point.
    Process* start_coordinator(const std::string& name, std::uint16_t port) {
        std::vector<std::string> argv{node_binary().string(),
                                      "--role=coordinator",
                                      "--bind-host=127.0.0.1",
                                      "--advertise-host=127.0.0.1",
                                      "--port=" + std::to_string(port),
                                      "--ha-dir=" + ha_dir().string()};
        procs_.push_back(std::make_unique<Process>());
        if (!procs_.back()->spawn(name, node_binary(), std::move(argv), log_dir(), ProcOptions{})) {
            return nullptr;
        }
        return procs_.back().get();
    }

    std::filesystem::path root_;
    std::vector<std::unique_ptr<Process>> procs_;
};

}  // namespace

// The precondition: a second coordinator sharing the HA directory must NOT
// become leader while the first holds the lock. Asserted separately because
// every assertion in the split-brain test below is meaningless if leadership is
// simply handed out twice.
TEST_F(SplitBrainTest, ASecondCoordinatorDoesNotBecomeLeaderWhileTheLockIsHeld) {
    clink::itest::ReservedPort pa;
    clink::itest::ReservedPort pb;
    ASSERT_TRUE(pa.valid() && pb.valid());
    const auto port_a = pa.port();
    const auto port_b = pb.port();
    pa.release();
    pb.release();

    auto* a = start_coordinator("coordinator-a", port_a);
    ASSERT_NE(a, nullptr);
    ASSERT_TRUE(clink::itest::await_condition(
        [&] { return a->log_contains("coordinator became leader"); }, 15s))
        << "coordinator A never took leadership";

    auto* b = start_coordinator("coordinator-b", port_b);
    ASSERT_NE(b, nullptr);

    // Give B several poll intervals to try and fail.
    const bool b_became_leader = clink::itest::await_condition(
        [&] { return b->log_contains("coordinator became leader"); }, 3s);
    EXPECT_FALSE(b_became_leader)
        << "two coordinators sharing one HA directory both took leadership while the lock was "
           "held, so the lock is not doing anything";
    EXPECT_FALSE(clink::itest::await_port_accepting(port_b, 500ms))
        << "a coordinator that is not the leader bound its control port anyway";
}

// The split brain itself, and the assertion that matters: the SUPERSEDED
// coordinator must not be able to rewrite the record that tells workers where
// the leader is.
//
// Why that is the thing to assert. Both coordinators refresh
// active-leader.json on every poll of their leadership thread. A worker starting
// or reconnecting reads that file to find the coordinator to register with. If
// the superseded coordinator can overwrite it, a worker is handed the endpoint
// of a coordinator that has lost leadership, registers with it, and BINDS ITS
// EPOCH - after which the worker-side epoch check cannot help, because the
// worker's bound epoch is the stale one. Every subsequent frame from the
// superseded coordinator then looks authoritative.
TEST_F(SplitBrainTest, ASupersededCoordinatorCannotRewriteTheActiveLeaderRecord) {
    clink::itest::ReservedPort pa;
    clink::itest::ReservedPort pb;
    ASSERT_TRUE(pa.valid() && pb.valid());
    const auto port_a = pa.port();
    const auto port_b = pb.port();
    pa.release();
    pb.release();

    auto* a = start_coordinator("coordinator-a", port_a);
    ASSERT_NE(a, nullptr);
    ASSERT_TRUE(clink::itest::await_condition(
        [&] { return a->log_contains("coordinator became leader"); }, 15s))
        << "coordinator A never took leadership";
    ASSERT_TRUE(clink::itest::await_condition(
        [&] { return read_active_leader(ha_dir()).port == port_a; }, 10s))
        << "active-leader.json never named coordinator A";
    const auto epoch_a = read_active_leader(ha_dir()).epoch;
    ASSERT_GT(epoch_a, 0u) << "the leader recorded no epoch";

    auto* b = start_coordinator("coordinator-b", port_b);
    ASSERT_NE(b, nullptr);

    // Produce the split brain: unlink the lock file. A keeps its fcntl lock on
    // the now-unnamed inode and goes on believing it leads; B creates a new
    // inode, locks that, and takes leadership one epoch above what it reads.
    {
        std::error_code ec;
        ASSERT_TRUE(std::filesystem::remove(ha_dir() / "leader.lock", ec))
            << "no leader.lock to unlink: " << ec.message();
    }

    ASSERT_TRUE(clink::itest::await_condition(
        [&] { return b->log_contains("coordinator became leader"); }, 15s))
        << "coordinator B never took leadership after the lock was unlinked, so no split brain "
           "was created and this test proves nothing";

    // Both alive, and B's epoch strictly above A's.
    ASSERT_TRUE(a->running()) << "coordinator A died; this is a failover, not a split brain";
    ASSERT_TRUE(b->running());
    ASSERT_TRUE(clink::itest::await_condition(
        [&] { return read_active_leader(ha_dir()).epoch > epoch_a; }, 10s))
        << "B did not advance the epoch above A's " << epoch_a;
    const auto record_after_takeover = read_active_leader(ha_dir());
    ASSERT_TRUE(record_after_takeover.ok);
    EXPECT_EQ(record_after_takeover.port, port_b) << "active-leader.json did not flip to B";
    const auto epoch_b = record_after_takeover.epoch;

    // A is still a live leader by its own reckoning: it never logged a step-down
    // and its control port is still accepting. If either of those stops being
    // true, the product has grown a step-down path and this test is no longer
    // describing a split brain.
    EXPECT_TRUE(clink::itest::await_port_accepting(port_a, 2s))
        << "coordinator A stopped serving; it detected the loss of leadership, which would mean "
           "this is no longer a split brain";

    // THE ASSERTION. Sample across several poll intervals of BOTH coordinators.
    // A single read could catch the file in between two of A's writes.
    int samples_naming_a = 0;
    std::uint64_t lowest_epoch_seen = epoch_b;
    for (int i = 0; i < 40; ++i) {
        const auto rec = read_active_leader(ha_dir());
        if (rec.ok) {
            if (rec.port == port_a) {
                ++samples_naming_a;
            }
            lowest_epoch_seen = std::min(lowest_epoch_seen, rec.epoch);
        }
        std::this_thread::sleep_for(50ms);
    }

    EXPECT_EQ(samples_naming_a, 0)
        << "the superseded coordinator (epoch " << epoch_a
        << ") rewrote active-leader.json to point at itself, " << samples_naming_a
        << " of 40 samples. A worker reading it then registers with a coordinator that has lost "
           "leadership and binds its stale epoch, after which the worker-side epoch check cannot "
           "tell the difference.";
    EXPECT_EQ(lowest_epoch_seen, epoch_b)
        << "active-leader.json regressed to epoch " << lowest_epoch_seen << "; B holds epoch "
        << epoch_b << ". The record that directs workers must never move backwards.";
}
