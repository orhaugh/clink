// Where a job's state lives and where a restore reads it
// (clink/cluster/state_location.hpp): the job segment a state base gains, the
// state= line a COMPLETED marker carries, and the resolution of a restore
// point from its marker - a job's marker directory, a checkpoint directory one
// job holds the checkpoint in, several jobs, and markers written before state
// was namespaced by job.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <set>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/cluster/state_location.hpp"

namespace {

namespace fs = std::filesystem;
using clink::cluster::completed_marker_body;
using clink::cluster::job_state_root;
using clink::cluster::parse_completed_marker;
using clink::cluster::recorded_state_location;
using clink::cluster::resolve_restore_source;

TEST(JobStateRoot, APathGainsTheJobSegment) {
    EXPECT_EQ(job_state_root("/ckpt", 7), "/ckpt/_state/7");
    EXPECT_EQ(job_state_root("/ckpt/", 7), "/ckpt/_state/7");
    EXPECT_EQ(job_state_root("relative/dir", 2), "relative/dir/_state/2");
    EXPECT_EQ(job_state_root("file:///ckpt", 7), "file:///ckpt/_state/7");
    EXPECT_EQ(job_state_root("rocksdb:///var/state", 3), "rocksdb:///var/state/_state/3");
    EXPECT_EQ(job_state_root("changelog+file:///s", 1), "changelog+file:///s/_state/1");
}

TEST(JobStateRoot, TheSegmentGoesBeforeTheQuery) {
    EXPECT_EQ(job_state_root("forst:///s?defer_reads=1", 4), "forst:///s/_state/4?defer_reads=1");
    // An endpoint URL inside the query is left alone: only the location and
    // the local working root are namespaced.
    EXPECT_EQ(job_state_root("remote-read://bucket/prefix?endpoint=http://h:9000&region=r", 5),
              "remote-read://bucket/prefix/_state/5?endpoint=http://h:9000&region=r");
    EXPECT_EQ(job_state_root("remote-read://bucket", 5), "remote-read://bucket/_state/5");
}

TEST(JobStateRoot, TheLocalWorkingRootIsNamespacedToo) {
    EXPECT_EQ(job_state_root("s3+rocksdb://b/p?local=/tmp/w&cas=1", 9),
              "s3+rocksdb://b/p/_state/9?local=/tmp/w/_state/9&cas=1");
}

TEST(JobStateRoot, ABaseWithNoLocationIsUnchanged) {
    EXPECT_EQ(job_state_root("", 1), "");
    EXPECT_EQ(job_state_root("memory://", 1), "memory://");
    EXPECT_EQ(job_state_root("memory+sharded://", 1), "memory+sharded://");
    EXPECT_EQ(job_state_root("changelog://", 1), "changelog://");
    EXPECT_EQ(job_state_root("disagg-local://?io_threads=4", 1), "disagg-local://?io_threads=4");
}

TEST(JobStateRoot, JobsSharingABaseNeverShareARoot) {
    EXPECT_NE(job_state_root("/ckpt", 1), job_state_root("/ckpt", 2));
    EXPECT_NE(job_state_root("/ckpt", 1), job_state_root("/ckpt", 10));
}

TEST(CompletedMarker, TheBodyRoundTrips) {
    const auto body = completed_marker_body(3, 12, 2, {0, 1, 4}, "_state/3");
    EXPECT_EQ(body, "job=3\ncheckpoint=12\ngeneration=2\nsubtasks=0,1,4\nstate=_state/3\n");
    const auto m = parse_completed_marker(body);
    EXPECT_EQ(m.job, 3U);
    EXPECT_EQ(m.checkpoint, 12U);
    EXPECT_EQ(m.generation, 2U);
    EXPECT_EQ(m.subtasks, (std::set<std::uint32_t>{0, 1, 4}));
    EXPECT_EQ(m.state, "_state/3");
}

TEST(CompletedMarker, AnOlderMarkerHasNoState) {
    const auto m = parse_completed_marker("job=3\ncheckpoint=12\ngeneration=1\nsubtasks=0,1\n");
    EXPECT_FALSE(m.state.has_value());
    EXPECT_EQ(m.subtasks, (std::set<std::uint32_t>{0, 1}));
}

TEST(CompletedMarker, AnUnparsableParticipantListIsNotHalfRead) {
    EXPECT_FALSE(parse_completed_marker("subtasks=0,x,2\n").subtasks.has_value());
    EXPECT_FALSE(parse_completed_marker("subtasks=\n").subtasks.has_value());
    EXPECT_FALSE(parse_completed_marker("job=1\nch").subtasks.has_value());
}

TEST(RecordedStateLocation, TheCheckpointDirectorysOwnRootIsRelative) {
    EXPECT_EQ(recorded_state_location("/ckpt", 7, "/ckpt/_state/7"), "_state/7");
    EXPECT_EQ(recorded_state_location("/ckpt/", 7, "/ckpt/_state/7"), "_state/7");
    EXPECT_EQ(recorded_state_location("/ckpt", 7, "file:///ckpt/_state/7"), "_state/7");
}

TEST(RecordedStateLocation, AnyOtherRootIsRecordedAsItIs) {
    EXPECT_EQ(recorded_state_location("/ckpt", 7, "rocksdb:///ckpt/_state/7"),
              "rocksdb:///ckpt/_state/7");
    EXPECT_EQ(recorded_state_location("/ckpt", 7, "/elsewhere/_state/7"), "/elsewhere/_state/7");
    EXPECT_EQ(recorded_state_location("", 7, "/x/_state/7"), "/x/_state/7");
}

// A scratch checkpoint directory, removed at the end of each test.
struct RestoreSourceFixture : ::testing::Test {
    fs::path dir;

    void SetUp() override {
        dir = fs::temp_directory_path() /
              ("clink_state_location_" + std::to_string(::getpid()) + "_" +
               ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fs::remove_all(dir);
        fs::create_directories(dir);
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    void marker(std::uint64_t job, std::uint64_t checkpoint, const std::string& body) const {
        const auto jobs = dir / "_jobs" / std::to_string(job);
        fs::create_directories(jobs);
        std::ofstream(jobs / ("COMPLETED-" + std::to_string(checkpoint))) << body;
    }
    void located_marker(std::uint64_t job,
                        std::uint64_t checkpoint,
                        std::uint32_t generation = 1,
                        const std::string& state = {}) const {
        marker(job,
               checkpoint,
               completed_marker_body(job,
                                     checkpoint,
                                     generation,
                                     {0, 1},
                                     state.empty() ? "_state/" + std::to_string(job) : state));
    }
    void older_marker(std::uint64_t job, std::uint64_t checkpoint) const {
        marker(job,
               checkpoint,
               "job=" + std::to_string(job) + "\ncheckpoint=" + std::to_string(checkpoint) +
                   "\ngeneration=1\nsubtasks=0,1\n");
    }
    std::string jobs_dir(std::uint64_t job) const {
        return (dir / "_jobs" / std::to_string(job)).string();
    }
};

TEST_F(RestoreSourceFixture, OneJobsCheckpointIsReadFromItsRoot) {
    located_marker(4, 9, 2);
    const auto source = resolve_restore_source(dir.string(), 9);
    EXPECT_FALSE(source.legacy);
    EXPECT_EQ(source.state_root, (dir / "_state" / "4").string());
    EXPECT_EQ(source.generation, 2U);
    EXPECT_EQ(source.job, 4U);
    EXPECT_EQ(source.participants, (std::set<std::uint32_t>{0, 1}));
}

TEST_F(RestoreSourceFixture, AJobsMarkerDirectoryNamesItsCheckpoint) {
    located_marker(4, 9);
    const auto source = resolve_restore_source(jobs_dir(4), 9);
    EXPECT_FALSE(source.legacy);
    EXPECT_EQ(source.state_root, (dir / "_state" / "4").string());
    EXPECT_EQ(source.job, 4U);
}

TEST_F(RestoreSourceFixture, SeveralJobsCompletingTheCheckpointMustBeToldApart) {
    located_marker(1, 5);
    located_marker(2, 5);
    try {
        (void)resolve_restore_source(dir.string(), 5);
        FAIL() << "a checkpoint two jobs completed was resolved to one of them";
    } catch (const std::runtime_error& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("jobs 1, 2"), std::string::npos) << what;
        EXPECT_NE(what.find(jobs_dir(1)), std::string::npos) << what;
    }
    EXPECT_EQ(resolve_restore_source(jobs_dir(1), 5).state_root, (dir / "_state" / "1").string());
    EXPECT_EQ(resolve_restore_source(jobs_dir(2), 5).state_root, (dir / "_state" / "2").string());
}

TEST_F(RestoreSourceFixture, AnOtherIdOfAnotherJobDoesNotMakeItAmbiguous) {
    located_marker(1, 5);
    located_marker(2, 6);
    EXPECT_EQ(resolve_restore_source(dir.string(), 5).job, 1U);
}

TEST_F(RestoreSourceFixture, AnOlderMarkersStateIsInTheCheckpointDirectory) {
    older_marker(3, 8);
    const auto source = resolve_restore_source(dir.string(), 8);
    EXPECT_TRUE(source.legacy);
    EXPECT_EQ(source.state_root, dir.string());
    EXPECT_EQ(source.participants, (std::set<std::uint32_t>{0, 1}));
    // Named by its marker directory, it is still the checkpoint directory.
    EXPECT_EQ(resolve_restore_source(jobs_dir(3), 8).state_root, dir.string());
    // And at an explicit backend's location when the caller says so.
    EXPECT_EQ(resolve_restore_source(jobs_dir(3), 8, "rocksdb:///data").state_root,
              "rocksdb:///data");
}

TEST_F(RestoreSourceFixture, SeveralOlderMarkersShareTheOldLocation) {
    older_marker(1, 5);
    older_marker(2, 5);
    const auto source = resolve_restore_source(dir.string(), 5);
    EXPECT_TRUE(source.legacy);
    EXPECT_EQ(source.state_root, dir.string());
    EXPECT_FALSE(source.participants.has_value());
}

TEST_F(RestoreSourceFixture, ADirectoryWithNoMarkerIsReadAsItIs) {
    fs::create_directories(dir / "v3" / "0");
    const auto source = resolve_restore_source(dir.string(), 5);
    EXPECT_TRUE(source.legacy);
    EXPECT_EQ(source.state_root, dir.string());
    EXPECT_EQ(source.generation, 3U);
    EXPECT_TRUE(source.marker.empty());
}

TEST_F(RestoreSourceFixture, ABackendUriIsReadAsItIs) {
    const auto source = resolve_restore_source("rocksdb:///data/state", 5);
    EXPECT_EQ(source.state_root, "rocksdb:///data/state");
    EXPECT_TRUE(source.legacy);
}

TEST_F(RestoreSourceFixture, AnAbsoluteRootIsReadWhereItIs) {
    located_marker(6, 2, 1, "rocksdb:///data/_state/6");
    const auto source = resolve_restore_source(dir.string(), 2);
    EXPECT_FALSE(source.legacy);
    EXPECT_EQ(source.state_root, "rocksdb:///data/_state/6");
}

TEST_F(RestoreSourceFixture, ACopiedCheckpointDirectoryRestoresFromTheCopy) {
    located_marker(4, 9);
    const auto copy = dir.string() + "_copy";
    fs::remove_all(copy);
    fs::copy(dir, copy, fs::copy_options::recursive);
    EXPECT_EQ(resolve_restore_source(copy, 9).state_root, copy + "/_state/4");
    fs::remove_all(copy);
}

TEST_F(RestoreSourceFixture, AMarkerDirectoryOutsideACheckpointDirectoryCannotPlaceItsState) {
    located_marker(4, 9);
    const auto loose = dir / "loose";
    fs::create_directories(loose);
    fs::copy_file(dir / "_jobs" / "4" / "COMPLETED-9", loose / "COMPLETED-9");
    EXPECT_THROW((void)resolve_restore_source(loose.string(), 9), std::runtime_error);
}

TEST_F(RestoreSourceFixture, ADamagedMarkerIsReadAtTheJobsOwnRoot) {
    // Every marker this build writes records its participants and its state,
    // so one with neither was cut short; the job's own root is where it was
    // written.
    marker(4, 9, "job=4\nch");
    fs::create_directories(dir / "_state" / "4" / "v1" / "0");
    const auto source = resolve_restore_source(dir.string(), 9);
    EXPECT_FALSE(source.legacy);
    EXPECT_EQ(source.state_root, (dir / "_state" / "4").string());
    EXPECT_EQ(source.generation, 1U);
}

TEST_F(RestoreSourceFixture, ADamagedMarkerWithNoRootOfItsOwnIsReadWhereStateUsedToBe) {
    marker(4, 9, "job=4\nch");
    const auto source = resolve_restore_source(dir.string(), 9);
    EXPECT_TRUE(source.legacy);
    EXPECT_EQ(source.state_root, dir.string());
}

}  // namespace
