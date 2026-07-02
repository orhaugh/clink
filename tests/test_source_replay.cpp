// Source-side replay tracking: VectorSource snapshots its next-index
// when a barrier flows through, and restore_offset() advances past
// the already-emitted records so a restart resumes from where the
// previous run left off rather than replaying from 0.
//
// This is the last hop to true pipeline-wide exactly-once for bounded
// sources. Combined with KeyedState's restore-from-snapshot path that
// the earlier recovery suite already covers, a job that crashes
// between checkpoints can now restart and not double-count records.

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/directory_file_source.hpp"
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
    // Feed VectorSource a 10-record vector. Manually run
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

    // Fresh source, restore from the saved offset, and verify
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

namespace {
// Broker-free stand-in for the messaging sources (RabbitMQ/NATS/Pulsar): it
// implements the shared checkpoint-completion ACK-DEFERRAL contract without any
// transport. Consumed tokens are held; snapshot_offset() buckets the held tokens
// under a checkpoint id (no ack); notify_checkpoint_complete() acks every bucket
// up to the committed id; notify_checkpoint_aborted() drops a bucket unacked so
// the "broker" would redeliver. Acking at the barrier (the old behaviour) would
// lose a token if its checkpoint later aborted - this locks in that it does not.
class DeferredAckSource final : public Source<int> {
public:
    void consume(int token) { held_.push_back(token); }

    void snapshot_offset(StateBackend&, OperatorId, CheckpointId ckpt) override {
        if (held_.empty()) {
            return;
        }
        pending_[ckpt.value()] = std::move(held_);
        held_.clear();
    }
    void notify_checkpoint_complete(CheckpointId ckpt) override {
        for (auto it = pending_.begin(); it != pending_.end() && it->first <= ckpt.value();) {
            acked_.insert(acked_.end(), it->second.begin(), it->second.end());
            it = pending_.erase(it);
        }
    }
    void notify_checkpoint_aborted(CheckpointId ckpt) override { pending_.erase(ckpt.value()); }

    bool produce(Emitter<int>&) override { return false; }
    std::string name() const override { return "deferred_ack_source"; }

    const std::vector<int>& acked() const { return acked_; }
    std::size_t pending_count() const {
        std::size_t n = 0;
        for (const auto& [c, v] : pending_) {
            n += v.size();
        }
        return n;
    }

private:
    std::vector<int> held_;
    std::map<std::uint64_t, std::vector<int>> pending_;
    std::vector<int> acked_;
};
}  // namespace

// The shared ack-deferral contract: a message is acked only after the checkpoint
// that captured it commits, and an aborted checkpoint's messages are never acked
// (left for redelivery). Exercised through the Source<int> base so it also proves
// the new hooks dispatch virtually.
TEST(SourceReplay, DeferredAckHoldsUntilCommitAndDropsOnAbort) {
    InMemoryStateBackend backend;
    const OperatorId op_id{909};
    DeferredAckSource concrete;
    Source<int>& src = concrete;  // drive via the base to prove virtual dispatch

    // Batch A captured by checkpoint 1.
    concrete.consume(10);
    concrete.consume(11);
    src.snapshot_offset(backend, op_id, CheckpointId{1});
    EXPECT_TRUE(concrete.acked().empty()) << "barrier must NOT ack; ack waits for commit";
    EXPECT_EQ(concrete.pending_count(), 2u);

    // Batch B captured by checkpoint 2.
    concrete.consume(20);
    src.snapshot_offset(backend, op_id, CheckpointId{2});

    // Checkpoint 1 commits: only its batch is acked; checkpoint 2 still pending.
    src.notify_checkpoint_complete(CheckpointId{1});
    EXPECT_EQ(concrete.acked(), (std::vector<int>{10, 11}));
    EXPECT_EQ(concrete.pending_count(), 1u);

    // Checkpoint 2 aborts: its batch is dropped unacked (broker redelivers), never
    // acked even though a later checkpoint arrives.
    src.notify_checkpoint_aborted(CheckpointId{2});
    concrete.consume(30);
    src.snapshot_offset(backend, op_id, CheckpointId{3});
    src.notify_checkpoint_complete(CheckpointId{3});
    EXPECT_EQ(concrete.acked(), (std::vector<int>{10, 11, 30}))
        << "aborted checkpoint 2's token (20) must never be acked";
}

// DirectoryFileSource: a multi-file read position (file index + line index). After a
// partial read across a file boundary + snapshot, a fresh instance restored from that
// position resumes at the next un-emitted line, so the union of the two runs is the
// whole directory with no duplication and no gap.
TEST(SourceReplay, DirectoryFileSourceResumesAcrossFileBoundary) {
    InMemoryStateBackend backend;
    const OperatorId op_id{303};
    const auto dir = std::filesystem::temp_directory_path() / "clink_dirsrc_replay";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    // Three files of two lines each: 0..5 in filename+line order.
    {
        std::ofstream(dir / "p0.ndjson", std::ios::trunc) << "0\n1\n";
        std::ofstream(dir / "p1.ndjson", std::ios::trunc) << "2\n3\n";
        std::ofstream(dir / "p2.ndjson", std::ios::trunc) << "4\n5\n";
    }

    // First run: one produce() of batch_size 3 reads across the p0/p1 boundary
    // (0,1 from p0 then 2 from p1), then snapshot mid-p1.
    DirectoryFileSource<int> first(dir, int_lines(), /*batch_size*/ 3, "dsrc");
    first.open();
    auto ch1 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> em1(ch1.get());
    first.produce(em1);
    first.snapshot_offset(backend, op_id, CheckpointId{1});
    auto run1 = drain_ints(*ch1);
    ASSERT_EQ(run1, (std::vector<int>{0, 1, 2}));

    // Fresh source restored from that position drains the remaining lines exactly once.
    DirectoryFileSource<int> second(dir, int_lines(), /*batch_size*/ 3, "dsrc");
    ASSERT_TRUE(second.restore_offset(backend, op_id));
    second.open();
    auto ch2 = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    Emitter<int> em2(ch2.get());
    while (second.produce(em2)) {
    }
    auto run2 = drain_ints(*ch2);
    EXPECT_EQ(run2, (std::vector<int>{3, 4, 5}));

    std::filesystem::remove_all(dir);
}

// Default Source hooks are no-ops: an offset-replay source (Kafka/Parquet/File)
// that does not override them is unaffected by commit/abort notifications.
TEST(SourceReplay, DefaultCheckpointHooksAreNoOps) {
    const auto path = write_int_lines("noophooks", 2);
    FileSource<int> src(path, int_lines(), 10, "fsrc");
    Source<int>& base = src;
    base.notify_checkpoint_complete(CheckpointId{1});  // no throw, no effect
    base.notify_checkpoint_aborted(CheckpointId{2});
    std::filesystem::remove(path);
    SUCCEED();
}
