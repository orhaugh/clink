// Checkpoint retention, judged from the DISK, end to end.
//
// The worker purges superseded snapshots when a checkpoint completes -
// but QUAL-09 put /qual/state on a bounded 4 GiB volume and the engine
// filled it in ~40 minutes: some subtask directories held EVERY snapshot
// since checkpoint 1 (81 of them against a configured retention of 3),
// and once the volume hit ENOSPC the job crashlooped through ~100
// rewind-restarts (followups item 77). The same asymmetry was visible in
// QUAL-08's local tree (dirs holding all 49 checkpoints beside dirs
// holding 1) and had been noted, unexplained, under item 74.
//
// This test states the retention contract the way the campaign measures
// it: after a real cluster job (real coordinator, real worker, real
// periodic checkpoints) has completed a number of checkpoints well above
// the retention depth, EVERY subtask directory on disk holds at most
// retained + a small in-flight slack. It is deliberately a disk walk,
// not a metric read: ckptsize.py sizes the newest snapshot per subtask,
// which is exactly how this class stayed invisible through five
// campaigns.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/worker.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

// Snapshot counts per subtask directory, recursively:
// <root>/**/checkpoint-<id>.snap grouped by parent directory.
std::map<std::string, int> snaps_per_dir(const std::filesystem::path& root) {
    std::map<std::string, int> out;
    static const std::regex snap_re(R"(checkpoint-\d+\.snap)");
    std::error_code ec;
    for (std::filesystem::recursive_directory_iterator it{root, ec}, end; !ec && it != end;
         it.increment(ec)) {
        if (!it->is_regular_file()) {
            continue;
        }
        const auto name = it->path().filename().string();
        if (std::regex_match(name, snap_re)) {
            out[it->path().parent_path().string()] += 1;
        }
    }
    return out;
}

}  // namespace

TEST(CheckpointRetentionE2E, EverySubtaskDirectoryOnDiskIsBoundedByTheRetentionDepth) {
    ensure_built_ins_registered();
    const auto root = std::filesystem::temp_directory_path() /
                      ("clink_retention_e2e_" + std::to_string(::getpid()));
    const auto ckpt_dir = root / "state";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(ckpt_dir);

    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"w-ret"});
    Worker::Config wcfg;
    wcfg.slot_count = 16;
    wcfg.checkpoint_num_retained = 2;
    Worker worker("w-ret", "127.0.0.1", wcfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    // A paced source so the job runs for the whole observation window and
    // every checkpoint has fresh state to snapshot - and an INTERIOR
    // operator, deliberately: the planner runs interior ops through the
    // chained-subtask path, whose RunnerContext is built separately from
    // the single-op path's, and that is exactly where retention
    // registration went missing (item 77a: the campaigns' never-purged
    // directories were all interior SQL operators; sources, bridges and
    // sinks purged). A fixture without an interior op passed against the
    // defect.
    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 2;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "100000"}, {"delay_ms", "2"}};
    g.ops.push_back(src);
    OperatorSpec mid;
    mid.type = "identity_int64";
    mid.id = "mid";
    mid.inputs = {"src"};
    mid.parallelism = 2;
    mid.out_channel = std::string{kChannelInt64};
    g.ops.push_back(mid);
    // TWO adjacent interior operators, so the planner's operator-chain
    // grouping emits a length-2 chain: chains of one op take the
    // single-op runner path, and only ops.size() >= 2 reaches the
    // DagBuilder chain path where the registration was missing.
    OperatorSpec mid2;
    mid2.type = "multiply_int64";
    mid2.id = "mid2";
    mid2.inputs = {"mid"};
    mid2.parallelism = 2;
    mid2.out_channel = std::string{kChannelInt64};
    mid2.params = {{"factor", "1"}};
    g.ops.push_back(mid2);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"mid2"};
    snk.parallelism = 2;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", (root / "out.txt").string()}};
    g.ops.push_back(snk);

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 150;
    const auto job_id = coordinator.submit_job(
        g, OperatorRegistry::default_instance(), std::vector<PluginBinary>{}, ckpt);
    ASSERT_GT(job_id, 0U);

    // Let a healthy multiple of the retention depth complete.
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (coordinator.latest_completed_checkpoint(job_id) < 12 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(100ms);
    }
    const auto completed = coordinator.latest_completed_checkpoint(job_id);
    ASSERT_GE(completed, 12U) << "the fixture never checkpointed enough to judge retention";

    // Judge the DISK while the job is still running (the campaign's view).
    // retained=2 plus slack for the checkpoint in flight and the one whose
    // commit broadcast is still travelling.
    const int bound = 2 + 2;
    const auto counts = snaps_per_dir(ckpt_dir);
    ASSERT_FALSE(counts.empty()) << "no snapshots on disk - the fixture proved nothing";
    for (const auto& [dir, n] : counts) {
        EXPECT_LE(n, bound) << dir << " holds " << n << " snapshots against a retention of 2 "
                            << "(completed=" << completed
                            << "): this directory is never purged, and on a bounded volume "
                               "that is a countdown to ENOSPC (item 77a)";
    }

    // --- the missed-broadcast orphan (QUAL-09, finding 77's residue) ----
    //
    // Purges ride the CommitCheckpoint broadcast, so a worker partitioned
    // at completion time (or restarted since) never hears about the ids
    // that completed while it was away - and a purge keyed on "ids I saw
    // complete" leaves those snapshots on disk FOREVER. Two 150-second
    // partitions at a 5-second checkpoint interval measured 30 orphans on
    // the campaign's bounded volume. The on-disk shape of a missed
    // broadcast is exactly an old-id snapshot the retention tracker does
    // not retain, so inject one into every subtask directory and require
    // the sweep that now rides every subsequent broadcast to remove them.
    const char* orphan = "checkpoint-1.snap";
    std::vector<std::filesystem::path> injected;
    for (const auto& [dir, n] : counts) {
        (void)n;
        const auto snap = std::filesystem::path(dir) / orphan;
        std::ofstream(snap) << "orphaned by a missed CommitCheckpoint";
        std::ofstream(snap.string() + ".meta") << "orphan sidecar";
        injected.push_back(snap);
    }
    const auto sweep_deadline = std::chrono::steady_clock::now() + 20s;
    auto orphans_left = [&] {
        int left = 0;
        for (const auto& snap : injected) {
            std::error_code ec;
            if (std::filesystem::exists(snap, ec) ||
                std::filesystem::exists(snap.string() + ".meta", ec)) {
                ++left;
            }
        }
        return left;
    };
    while (orphans_left() > 0 && std::chrono::steady_clock::now() < sweep_deadline) {
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_EQ(orphans_left(), 0)
        << "orphan snapshots from a missed completion broadcast were never swept: "
           "on a bounded volume every missed broadcast is a permanent leak (item 77)";

    (void)coordinator.cancel_job(job_id);
    (void)coordinator.await_job_completion(job_id, 10s);
    worker.stop();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}
