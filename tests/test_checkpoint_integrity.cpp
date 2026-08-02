// Checkpoint integrity: partial writes, corruption, and fallback.
//
// Every scenario here is driven either by the fault-injection framework
// (so the failure lands at an exact point in the real write path, not in a
// test double) or by damaging the bytes on disk the way storage does.
// The property under test throughout is the same: a checkpoint that cannot
// be certified is never loaded, and its existence never strands a job that
// has an older checkpoint it CAN load.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/file_backed_state_backend.hpp"

namespace {

using clink::CheckpointId;
using clink::FileBackedStateBackend;
using clink::OperatorId;
using clink::Snapshot;
using clink::state::CheckpointStatus;

constexpr OperatorId kOp{1};

std::filesystem::path make_dir(const std::string& tag) {
    // Keyed on pid + test name: ctest -j runs each test as its own process
    // and a shared fixed path would collide across them.
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_integrity_" + std::to_string(::getpid()) + "_" + tag);
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

std::filesystem::path snap_path(const std::filesystem::path& dir, std::uint64_t id) {
    return dir / ("checkpoint-" + std::to_string(id) + ".snap");
}

void write_kv(FileBackedStateBackend& b, const std::string& k, const std::string& v) {
    b.put(kOp, k, v);
}

std::size_t count_keys(FileBackedStateBackend& b) {
    std::size_t n = 0;
    b.scan(kOp, [&](std::string_view, std::string_view) { ++n; });
    return n;
}

class CheckpointIntegrityTest : public ::testing::Test {
protected:
    void SetUp() override { clink::fault::Registry::instance().reset(); }
    void TearDown() override { clink::fault::Registry::instance().reset(); }
};

// --- CRC-32C ---------------------------------------------------------------

TEST_F(CheckpointIntegrityTest, Crc32cMatchesKnownVectors) {
    // Castagnoli reference vectors. Pinning these keeps a future
    // "optimisation" of the table from silently changing the checksum
    // written into every sidecar on disk.
    EXPECT_EQ(clink::state::crc32c(""), 0x00000000U);
    EXPECT_EQ(clink::state::crc32c("a"), 0xC1D04330U);
    EXPECT_EQ(clink::state::crc32c("123456789"), 0xE3069283U);
}

TEST_F(CheckpointIntegrityTest, Crc32cDetectsSingleBitFlip) {
    std::string a(4096, 'x');
    std::string b = a;
    b[2048] = static_cast<char>(static_cast<unsigned char>(b[2048]) ^ 0x01U);
    EXPECT_NE(clink::state::crc32c(a), clink::state::crc32c(b));
}

// --- sidecar round-trip ----------------------------------------------------

TEST_F(CheckpointIntegrityTest, MetaRoundTripsAndIgnoresUnknownKeys) {
    const clink::state::CheckpointMeta in{
        .version = 1, .checkpoint_id = 42, .payload_bytes = 1234, .payload_crc32c = 0xDEADBEEF};
    clink::state::CheckpointMeta out;
    ASSERT_TRUE(clink::state::CheckpointMeta::parse(in.serialise(), out));
    EXPECT_EQ(out.checkpoint_id, 42U);
    EXPECT_EQ(out.payload_bytes, 1234U);
    EXPECT_EQ(out.payload_crc32c, 0xDEADBEEFU);

    // Additive fields must not break a v1 reader.
    clink::state::CheckpointMeta fwd;
    ASSERT_TRUE(clink::state::CheckpointMeta::parse(in.serialise() + "future_key=whatever\n", fwd));
    EXPECT_EQ(fwd.checkpoint_id, 42U);
}

TEST_F(CheckpointIntegrityTest, MetaRejectsNonSidecarText) {
    clink::state::CheckpointMeta out;
    EXPECT_FALSE(clink::state::CheckpointMeta::parse("", out));
    EXPECT_FALSE(clink::state::CheckpointMeta::parse("hello world", out));
    EXPECT_FALSE(clink::state::CheckpointMeta::parse("checkpoint_id=1\n", out));
}

// --- status taxonomy -------------------------------------------------------

TEST_F(CheckpointIntegrityTest, HealthyCheckpointVerifies) {
    const auto dir = make_dir("healthy");
    FileBackedStateBackend b(dir);
    write_kv(b, "k1", "v1");
    b.snapshot(CheckpointId{1});
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{1}).status, CheckpointStatus::Valid);
}

TEST_F(CheckpointIntegrityTest, AbsentCheckpointIsMissingNotAnError) {
    const auto dir = make_dir("absent");
    FileBackedStateBackend b(dir);
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{9}).status, CheckpointStatus::Missing);
    // Missing is not an error: restore leaves the backend empty and the
    // caller decides whether a fresh start is acceptable.
    EXPECT_NO_THROW(b.restore(Snapshot{.checkpoint_id = CheckpointId{9}, .bytes = {}}));
}

TEST_F(CheckpointIntegrityTest, PayloadWithoutSidecarIsIncomplete) {
    const auto dir = make_dir("nosidecar");
    FileBackedStateBackend b(dir);
    write_kv(b, "k1", "v1");
    b.snapshot(CheckpointId{1});
    std::filesystem::remove(clink::state::meta_path_for(snap_path(dir, 1)));
    const auto verdict = b.verify_checkpoint(CheckpointId{1});
    EXPECT_EQ(verdict.status, CheckpointStatus::Incomplete);
    EXPECT_NE(verdict.detail.find("never published"), std::string::npos);
}

TEST_F(CheckpointIntegrityTest, TruncatedPayloadIsIncompleteNotCorrupt) {
    const auto dir = make_dir("truncated");
    FileBackedStateBackend b(dir);
    for (int i = 0; i < 50; ++i) {
        write_kv(b, "k" + std::to_string(i), "v" + std::to_string(i));
    }
    b.snapshot(CheckpointId{1});

    const auto path = snap_path(dir, 1);
    const auto full = std::filesystem::file_size(path);
    std::filesystem::resize_file(path, full / 2);

    const auto verdict = b.verify_checkpoint(CheckpointId{1});
    // Length disagreement means interrupted, not damaged. The taxonomy
    // matters: incomplete is expected after any crash, corrupt means the
    // storage layer lost bytes it had already acknowledged.
    EXPECT_EQ(verdict.status, CheckpointStatus::Incomplete);
}

TEST_F(CheckpointIntegrityTest, SameLengthDamageIsCorrupt) {
    const auto dir = make_dir("corrupt");
    FileBackedStateBackend b(dir);
    for (int i = 0; i < 50; ++i) {
        write_kv(b, "k" + std::to_string(i), "v" + std::to_string(i));
    }
    b.snapshot(CheckpointId{1});

    // Flip one byte in place - the file keeps its declared length, so only
    // the checksum can catch it.
    const auto path = snap_path(dir, 1);
    const auto size = std::filesystem::file_size(path);
    std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(f);
    f.seekg(static_cast<std::streamoff>(size / 2));
    char byte = 0;
    f.read(&byte, 1);
    byte = static_cast<char>(static_cast<unsigned char>(byte) ^ 0xFFU);
    f.seekp(static_cast<std::streamoff>(size / 2));
    f.write(&byte, 1);
    f.close();

    EXPECT_EQ(b.verify_checkpoint(CheckpointId{1}).status, CheckpointStatus::Corrupt);
}

TEST_F(CheckpointIntegrityTest, NewerSidecarVersionIsUnsupportedNotGuessed) {
    const auto dir = make_dir("future");
    FileBackedStateBackend b(dir);
    write_kv(b, "k1", "v1");
    b.snapshot(CheckpointId{1});

    const auto meta = clink::state::meta_path_for(snap_path(dir, 1));
    std::ofstream(meta, std::ios::trunc) << "clink_checkpoint_meta=999\ncheckpoint_id=1\n";

    const auto verdict = b.verify_checkpoint(CheckpointId{1});
    // Refusing beats guessing: a v999 sidecar may describe a payload layout
    // this binary would misread.
    EXPECT_EQ(verdict.status, CheckpointStatus::Unsupported);
    EXPECT_NE(verdict.detail.find("999"), std::string::npos);
}

TEST_F(CheckpointIntegrityTest, UnparseableSidecarIsCorrupt) {
    const auto dir = make_dir("badmeta");
    FileBackedStateBackend b(dir);
    write_kv(b, "k1", "v1");
    b.snapshot(CheckpointId{1});
    std::ofstream(clink::state::meta_path_for(snap_path(dir, 1)), std::ios::trunc) << "garbage";
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{1}).status, CheckpointStatus::Corrupt);
}

// --- restore refuses uncertified state -------------------------------------

TEST_F(CheckpointIntegrityTest, RestoreRefusesCorruptRatherThanLoadingPartialState) {
    const auto dir = make_dir("norestore");
    FileBackedStateBackend b(dir);
    for (int i = 0; i < 50; ++i) {
        write_kv(b, "k" + std::to_string(i), "v" + std::to_string(i));
    }
    b.snapshot(CheckpointId{1});
    std::filesystem::resize_file(snap_path(dir, 1),
                                 std::filesystem::file_size(snap_path(dir, 1)) / 2);

    FileBackedStateBackend fresh(dir);
    // Before integrity checking this call read the truncated stream and
    // left the backend holding whatever prefix happened to decode - a
    // silent partial restore. It now refuses.
    EXPECT_THROW(fresh.restore(Snapshot{.checkpoint_id = CheckpointId{1}, .bytes = {}}),
                 clink::state::CheckpointIntegrityError);
}

// --- fallback --------------------------------------------------------------

TEST_F(CheckpointIntegrityTest, LatestValidSkipsCorruptNewestAndFallsBack) {
    const auto dir = make_dir("fallback");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});
    write_kv(b, "b", "2");
    b.snapshot(CheckpointId{2});
    write_kv(b, "c", "3");
    b.snapshot(CheckpointId{3});

    // Damage the newest.
    std::filesystem::resize_file(snap_path(dir, 3), 3);

    std::vector<std::pair<CheckpointId, clink::state::VerifyResult>> rejected;
    const auto chosen = b.latest_valid_checkpoint(0, &rejected);
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->value(), 2U);
    // The skip is reported, never silent - rewinding a job by a checkpoint
    // without saying so is indistinguishable from losing data.
    ASSERT_EQ(rejected.size(), 1U);
    EXPECT_EQ(rejected[0].first.value(), 3U);
    EXPECT_EQ(rejected[0].second.status, CheckpointStatus::Incomplete);

    FileBackedStateBackend fresh(dir);
    fresh.restore(Snapshot{.checkpoint_id = *chosen, .bytes = {}});
    EXPECT_EQ(count_keys(fresh), 2U);
}

TEST_F(CheckpointIntegrityTest, LatestValidSkipsSeveralConsecutiveBadCheckpoints) {
    const auto dir = make_dir("multibad");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});
    for (std::uint64_t id = 2; id <= 4; ++id) {
        write_kv(b, "k" + std::to_string(id), "v");
        b.snapshot(CheckpointId{id});
        std::filesystem::remove(clink::state::meta_path_for(snap_path(dir, id)));
    }
    std::vector<std::pair<CheckpointId, clink::state::VerifyResult>> rejected;
    const auto chosen = b.latest_valid_checkpoint(0, &rejected);
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->value(), 1U);
    EXPECT_EQ(rejected.size(), 3U);
}

TEST_F(CheckpointIntegrityTest, LatestValidReturnsNulloptWhenNothingIsUsable) {
    const auto dir = make_dir("allbad");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});
    std::filesystem::remove(clink::state::meta_path_for(snap_path(dir, 1)));
    EXPECT_FALSE(b.latest_valid_checkpoint().has_value());
}

TEST_F(CheckpointIntegrityTest, LatestValidHonoursTheCeiling) {
    const auto dir = make_dir("ceiling");
    FileBackedStateBackend b(dir);
    for (std::uint64_t id = 1; id <= 3; ++id) {
        write_kv(b, "k" + std::to_string(id), "v");
        b.snapshot(CheckpointId{id});
    }
    const auto chosen = b.latest_valid_checkpoint(2);
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->value(), 2U);
}

TEST_F(CheckpointIntegrityTest, PurgeRemovesSidecarSoNoDanglingRecordRemains) {
    const auto dir = make_dir("purge");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});
    b.purge_checkpoint(CheckpointId{1});
    EXPECT_FALSE(std::filesystem::exists(snap_path(dir, 1)));
    EXPECT_FALSE(std::filesystem::exists(clink::state::meta_path_for(snap_path(dir, 1))));
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{1}).status, CheckpointStatus::Missing);
}

// --- fault-injected write failures -----------------------------------------

TEST_F(CheckpointIntegrityTest, ShortWriteInTheRealPathIsCaughtByVerification) {
    const auto dir = make_dir("faulttrunc");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});

    // Arm a short write at the exact place the durable writer emits bytes.
    // Ordinal 1 pins it to the PAYLOAD write: one snapshot drives the
    // durable writer twice (payload, then sidecar), and an unpinned rule
    // would truncate both - which is a different scenario, covered below.
    // The sidecar is computed from the FULL buffer the caller handed in, so
    // the mismatch is real: the file on disk is not what was promised.
    for (int i = 0; i < 40; ++i) {
        write_kv(b, "k" + std::to_string(i), "v");
    }
    {
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kCheckpointDuringWrite,
                               .ordinal = 1,
                               .action = clink::fault::Action::Truncate,
                               .arg = 16});
        b.snapshot(CheckpointId{2});
    }
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{2}).status, CheckpointStatus::Incomplete);

    // ... and the older good checkpoint is still what recovery picks.
    const auto chosen = b.latest_valid_checkpoint();
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->value(), 1U);
}

TEST_F(CheckpointIntegrityTest, ShortWriteOfTheSidecarItselfIsCaught) {
    const auto dir = make_dir("faulttruncmeta");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});
    for (int i = 0; i < 40; ++i) {
        write_kv(b, "k" + std::to_string(i), "v");
    }
    // Ordinal 2 = the sidecar write. A half-written certificate must not
    // certify anything; the payload beside it is fine but unverifiable.
    {
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kCheckpointDuringWrite,
                               .ordinal = 2,
                               .action = clink::fault::Action::Truncate,
                               .arg = 16});
        b.snapshot(CheckpointId{2});
    }
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{2}).status, CheckpointStatus::Corrupt);
    const auto chosen = b.latest_valid_checkpoint();
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->value(), 1U);
}

TEST_F(CheckpointIntegrityTest, DeathBeforePublishLeavesThePreviousCheckpointNewest) {
    const auto dir = make_dir("faultpublish");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});
    write_kv(b, "b", "2");

    // Throwing at the pre-rename point stands in for the process dying
    // there: the temp file exists, the final name does not.
    {
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kCheckpointBeforePublish,
                               .action = clink::fault::Action::Throw});
        EXPECT_THROW(b.snapshot(CheckpointId{2}), clink::fault::InjectedFault);
    }
    EXPECT_FALSE(std::filesystem::exists(snap_path(dir, 2)));
    const auto chosen = b.latest_valid_checkpoint();
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->value(), 1U);
}

TEST_F(CheckpointIntegrityTest, DeathBetweenPayloadAndSidecarReadsAsIncomplete) {
    const auto dir = make_dir("faultsidecar");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});
    write_kv(b, "b", "2");

    // The payload rename has happened; the sidecar has not. This is the
    // window the two-file design exists to make detectable.
    {
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kCheckpointAfterPublish,
                               .ordinal = 1,
                               .action = clink::fault::Action::Throw});
        EXPECT_THROW(b.snapshot(CheckpointId{2}), clink::fault::InjectedFault);
    }
    EXPECT_TRUE(std::filesystem::exists(snap_path(dir, 2)));
    EXPECT_FALSE(std::filesystem::exists(clink::state::meta_path_for(snap_path(dir, 2))));
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{2}).status, CheckpointStatus::Incomplete);

    const auto chosen = b.latest_valid_checkpoint();
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->value(), 1U);
}

TEST_F(CheckpointIntegrityTest, PermissionDeniedSurfacesAsAThrownWriteError) {
    const auto dir = make_dir("perm");
    FileBackedStateBackend b(dir);
    write_kv(b, "a", "1");
    b.snapshot(CheckpointId{1});

    std::filesystem::permissions(
        dir, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
    write_kv(b, "b", "2");
    const bool threw = [&] {
        try {
            b.snapshot(CheckpointId{2});
            return false;
        } catch (const std::exception&) {
            return true;
        }
    }();
    // Restore permissions before asserting so a failure still cleans up.
    std::filesystem::permissions(dir, std::filesystem::perms::owner_all);
    // Running as root defeats the mode bits entirely (CI containers do),
    // so only assert the outcome when the write really was denied.
    if (::geteuid() != 0) {
        EXPECT_TRUE(threw) << "a write into a read-only checkpoint dir must fail loudly";
    }
}

TEST_F(CheckpointIntegrityTest, EmptyStateSnapshotStillVerifies) {
    const auto dir = make_dir("empty");
    FileBackedStateBackend b(dir);
    b.snapshot(CheckpointId{1});
    EXPECT_EQ(b.verify_checkpoint(CheckpointId{1}).status, CheckpointStatus::Valid);
    FileBackedStateBackend fresh(dir);
    EXPECT_NO_THROW(fresh.restore(Snapshot{.checkpoint_id = CheckpointId{1}, .bytes = {}}));
    EXPECT_EQ(count_keys(fresh), 0U);
}

}  // namespace
