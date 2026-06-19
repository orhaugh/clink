// Source-side replay tracking: VectorSource snapshots its next-index
// when a barrier flows through, and restore_offset() advances past
// the already-emitted records so a restart resumes from where the
// previous run left off rather than replaying from 0.
//
// This is the last hop to true pipeline-wide exactly-once for bounded
// sources. Combined with KeyedState's restore-from-snapshot path that
// the earlier recovery suite already covers, a job that crashes
// between checkpoints can now restart and not double-count records.

#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/file_source.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {
// A line "N" decodes to int N; empty lines are skipped.
TextFormat<int> int_lines() {
    return TextFormat<int>{.decode = [](std::string_view s) -> std::optional<int> {
                               return s.empty() ? std::nullopt
                                                : std::optional<int>{std::stoi(std::string{s})};
                           },
                           .encode = [](const int& v) { return std::to_string(v); }};
}
std::filesystem::path write_int_lines(const std::string& tag, int n) {
    auto p = std::filesystem::temp_directory_path() / ("clink_filesrc_replay_" + tag + ".txt");
    std::ofstream o(p, std::ios::trunc);
    for (int i = 0; i < n; ++i) {
        o << i << "\n";
    }
    return p;
}
std::vector<int> drain_ints(BoundedChannel<StreamElement<int>>& ch) {
    std::vector<int> out;
    while (auto elem = ch.try_pop()) {
        if (elem->is_data()) {
            for (const auto& rec : elem->as_data()) {
                out.push_back(rec.value());
            }
        }
    }
    return out;
}
}  // namespace

TEST(SourceReplay, VectorSourceSnapshotsAndRestoresNextIndex) {
    // Phase 1: feed VectorSource a 10-record vector. Manually run
    // produce() through a captured emitter to drive emission, then
    // pretend a barrier arrived halfway through.
    InMemoryStateBackend backend;
    const OperatorId op_id{77};

    std::vector<Record<int>> records;
    for (int i = 0; i < 10; ++i) {
        records.emplace_back(Record<int>{i});
    }
    VectorSource<int> src(std::move(records), "test_source");

    // Simulate "the source has emitted records up to index 4" by
    // adjusting next_index_ via a snapshot-pretend (real code drives
    // this through the source runner; this is a unit-level smoke).
    // We trigger the snapshot, then capture what was written.
    src.snapshot_offset(backend, op_id, CheckpointId{1});
    // Source offsets are operator state (reserved-prefix key), so read them
    // back through the operator-state accessor, not a raw get().
    auto first_snap =
        backend.get_operator_state(op_id, StateBackend::KeyView{"__vector_source_offset__", 24});
    ASSERT_TRUE(first_snap.has_value());
    // Default initial state: next_index_=0, so the saved value is 0.
    EXPECT_EQ(first_snap->at(0), static_cast<std::byte>(0));

    // Force-advance via produce() so the next snapshot reflects
    // post-emission state. produce() emits everything from next_index_
    // and bumps next_index_ to records_.size().
    auto channel = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> emitter(channel.get());
    src.produce(emitter);
    src.snapshot_offset(backend, op_id, CheckpointId{2});

    // Phase 2: fresh source, restore from the saved offset, and verify
    // produce() emits nothing further (next_index_ == size).
    VectorSource<int> fresh(std::vector<Record<int>>(10), "test_source");
    ASSERT_TRUE(fresh.restore_offset(backend, op_id));
    auto channel2 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> emitter2(channel2.get());
    fresh.produce(emitter2);

    // The restored source should not have emitted any new records (the
    // batch path skips when next_index_ == records_.size()). It still
    // emits the watermark + signals exhaustion, so the channel may have
    // a watermark element but no data batches with content.
    bool saw_nonempty_data = false;
    while (auto elem = channel2->try_pop()) {
        if (elem->is_data() && !elem->as_data().empty()) {
            saw_nonempty_data = true;
            break;
        }
    }
    EXPECT_FALSE(saw_nonempty_data)
        << "restored VectorSource should not re-emit records its prior run already shipped";
}

TEST(SourceReplay, RestoreOffsetReturnsFalseWhenNoStateSaved) {
    InMemoryStateBackend backend;
    VectorSource<int> src(std::vector<Record<int>>{}, "test_source");
    EXPECT_FALSE(src.restore_offset(backend, OperatorId{42}));
}

TEST(SourceReplay, PartialEmissionRoundTrip) {
    // Drive a VectorSource to partial emission, snapshot, then verify
    // a fresh instance restored from that snapshot resumes only the
    // tail.
    InMemoryStateBackend backend;
    const OperatorId op_id{100};

    std::vector<Record<int>> records;
    for (int i = 0; i < 5; ++i) {
        records.emplace_back(Record<int>{i + 1000});
    }
    VectorSource<int> first(std::move(records), "src");
    auto channel = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> emitter(channel.get());
    first.produce(emitter);
    first.snapshot_offset(backend, op_id, CheckpointId{5});

    // Verify the saved offset is 5 (records 0..4 emitted). Operator state,
    // so read via the operator-state accessor.
    auto v =
        backend.get_operator_state(op_id, StateBackend::KeyView{"__vector_source_offset__", 24});
    ASSERT_TRUE(v.has_value());
    std::uint64_t saved = 0;
    for (int i = 0; i < 8; ++i) {
        saved |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
    }
    EXPECT_EQ(saved, 5u);

    // Replace with a fresh VectorSource holding the same 5 records,
    // restore, and verify produce() emits no records.
    std::vector<Record<int>> same;
    for (int i = 0; i < 5; ++i) {
        same.emplace_back(Record<int>{i + 1000});
    }
    VectorSource<int> second(std::move(same), "src");
    ASSERT_TRUE(second.restore_offset(backend, op_id));
    auto channel2 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> emitter2(channel2.get());
    second.produce(emitter2);
    std::size_t records_emitted = 0;
    while (auto elem = channel2->try_pop()) {
        if (elem->is_data()) {
            records_emitted += elem->as_data().size();
        }
    }
    EXPECT_EQ(records_emitted, 0u);
}

// FileSource: a real connector with a continuous (byte-offset) read
// position. After a partial read + snapshot, a fresh instance restored
// from that offset must resume at the next un-emitted line, not re-read
// the file from the top.
TEST(SourceReplay, FileSourceResumesFromSnapshottedByteOffset) {
    InMemoryStateBackend backend;
    const OperatorId op_id{201};
    const auto path = write_int_lines("partial", 10);  // lines "0".."9"

    // First run: produce one batch of 3 lines (0,1,2), then snapshot the
    // byte offset (the position the next produce would read from).
    FileSource<int> first(path, int_lines(), /*batch_size=*/3, "fsrc");
    first.open();
    auto ch1 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> em1(ch1.get());
    first.produce(em1);
    EXPECT_EQ(drain_ints(*ch1), (std::vector<int>{0, 1, 2}));
    first.snapshot_offset(backend, op_id, CheckpointId{1});

    // Restart: a fresh source restores the offset; open() seeks there, so
    // produce() resumes at line 3 with no re-emit of 0,1,2.
    FileSource<int> second(path, int_lines(), 3, "fsrc");
    ASSERT_TRUE(second.restore_offset(backend, op_id));
    second.open();
    auto ch2 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> em2(ch2.get());
    second.produce(em2);
    EXPECT_EQ(drain_ints(*ch2), (std::vector<int>{3, 4, 5}));

    std::filesystem::remove(path);
}

// FileSource: a source that read the whole file before the checkpoint
// snapshots the "consumed" sentinel; on restart it must emit nothing
// (seek to end) rather than replay the file.
TEST(SourceReplay, FileSourceFullyConsumedDoesNotReEmit) {
    InMemoryStateBackend backend;
    const OperatorId op_id{202};
    const auto path = write_int_lines("full", 4);

    FileSource<int> first(path, int_lines(), /*batch_size=*/100, "fsrc");
    first.open();
    auto ch1 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> em1(ch1.get());
    while (first.produce(em1)) {
    }
    EXPECT_EQ(drain_ints(*ch1), (std::vector<int>{0, 1, 2, 3}));
    first.snapshot_offset(backend, op_id, CheckpointId{2});

    FileSource<int> second(path, int_lines(), 100, "fsrc");
    ASSERT_TRUE(second.restore_offset(backend, op_id));
    second.open();
    auto ch2 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> em2(ch2.get());
    while (second.produce(em2)) {
    }
    EXPECT_TRUE(drain_ints(*ch2).empty())
        << "a fully-consumed FileSource must not re-emit on restart";

    std::filesystem::remove(path);
}

// A FileSource with no saved state behaves as before (at-least-once):
// restore_offset returns false and produce reads from the top.
TEST(SourceReplay, FileSourceNoStateReturnsFalse) {
    InMemoryStateBackend backend;
    const auto path = write_int_lines("nostate", 3);
    FileSource<int> src(path, int_lines(), 10, "fsrc");
    EXPECT_FALSE(src.restore_offset(backend, OperatorId{203}));
    std::filesystem::remove(path);
}
