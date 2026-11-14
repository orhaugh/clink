// Moving restored state from the operator ids an older build derived to the ids
// this build derives (StateBackend::remap_operator_ids), and the operator-id
// scheme and operator list every canonical snapshot records.

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/file_backed_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/restore_error.hpp"
#include "clink/state/sharded_in_memory_state_backend.hpp"
#include "clink/state/snapshot_arrow_writer.hpp"

using namespace clink;

namespace {

using KV = StateBackend::KeyView;
using VV = StateBackend::ValueView;

std::string text(const std::optional<StateBackend::Value>& v) {
    return v ? std::string(reinterpret_cast<const char*>(v->data()), v->size())
             : std::string{"<absent>"};
}

// A keyed key: key group 3, then the user key.
std::string keyed(const std::string& k) {
    return std::string(1, '\x03') + k;
}

std::shared_ptr<const arrow::KeyValueMetadata> metadata_of(const std::vector<std::byte>& bytes) {
    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(bytes.data()),
                                                  static_cast<int64_t>(bytes.size()));
    auto reader = *arrow::ipc::RecordBatchStreamReader::Open(
        std::make_shared<arrow::io::BufferReader>(buffer));
    return reader->schema()->metadata();
}

std::string meta(const std::vector<std::byte>& bytes, const std::string& key) {
    const auto m = metadata_of(bytes);
    const auto idx = m ? m->FindKey(key) : -1;
    return idx == -1 ? std::string{"<absent>"} : m->value(idx);
}

const OperatorId kOld{1001};
const OperatorId kNew{2002};

}  // namespace

// The in-memory backend moves an operator's keyed and operator-state rows,
// version stamps and fingerprints to the new id; a row already present under
// the new id wins; other operators are untouched; the scheme is recorded.
TEST(OperatorIdRemap, TheInMemoryBackendMovesRowsStampsAndFingerprints) {
    InMemoryStateBackend b;
    b.put(kOld, keyed("a"), VV{"A"});
    b.put(kOld, keyed("b"), VV{"OLD-B"});
    b.put_operator_state(kOld, KV{"pos"}, VV{"P"});
    b.put(kNew, keyed("b"), VV{"NEW-B"});
    b.put(OperatorId{7}, keyed("x"), VV{"X"});
    StateVersionMap versions;
    versions.set(kOld, "window", 3, "w");
    b.set_state_versions(versions);
    b.record_state_fingerprint(kOld, "w", 0xabcdef);

    const auto moved =
        b.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
    EXPECT_EQ(moved.at(kNew), 3U);
    EXPECT_EQ(text(b.get(kNew, keyed("a"))), "A");
    EXPECT_EQ(text(b.get(kNew, keyed("b"))), "NEW-B") << "the row under the new id wins";
    EXPECT_EQ(text(b.get_operator_state(kNew, KV{"pos"})), "P");
    EXPECT_EQ(text(b.get(kOld, keyed("a"))), "<absent>");
    EXPECT_EQ(text(b.get(OperatorId{7}, keyed("x"))), "X");
    EXPECT_EQ(b.restored_state_versions().get(kNew, "window"), 3U);
    EXPECT_EQ(b.restored_state_fingerprint(kNew, "w"), 0xabcdefU);
    EXPECT_EQ(b.operator_id_scheme(), OperatorIdScheme::Stable);
}

// A snapshot records its id scheme: legacy state as format version 1, as every
// earlier build wrote it; stable state as version 2 naming the scheme, which a
// build that reads only version 1 refuses rather than misattributing its rows.
// Its operators are listed with their identities and row counts.
TEST(OperatorIdRemap, ASnapshotRecordsItsSchemeAndItsOperators) {
    InMemoryStateBackend b;
    b.put(kOld, keyed("a"), VV{"A"});
    b.put_operator_state(kOld, KV{"pos"}, VV{"P"});
    b.set_operator_identities({{kOld, OperatorIdentity{"orders-src", "op_1"}},
                               {OperatorId{9}, OperatorIdentity{"", "op_2"}}});
    const auto legacy = b.snapshot(CheckpointId{1});
    EXPECT_EQ(meta(legacy.bytes, kSnapshotFormatVersionKey), "1");
    EXPECT_EQ(meta(legacy.bytes, kOperatorIdSchemeMetadataKey), "<absent>");
    const auto ops = meta(legacy.bytes, kOperatorsMetadataKey);
    EXPECT_NE(ops.find(R"({"op_id":"1001","uid":"orders-src","node":"op_1","keyed_rows":1,)"
                       R"("operator_rows":1})"),
              std::string::npos)
        << ops;
    EXPECT_NE(ops.find(R"({"op_id":"9","uid":"","node":"op_2","keyed_rows":0,"operator_rows":0})"),
              std::string::npos)
        << "an operator with no rows is listed too: " << ops;

    b.remap_operator_ids({}, OperatorIdScheme::Stable);
    const auto stable = b.snapshot(CheckpointId{2});
    EXPECT_EQ(meta(stable.bytes, kSnapshotFormatVersionKey), "2");
    EXPECT_EQ(meta(stable.bytes, kOperatorIdSchemeMetadataKey), "fnv1a-1");

    InMemoryStateBackend restored;
    restored.restore(legacy);
    EXPECT_EQ(restored.operator_id_scheme(), OperatorIdScheme::Legacy);
    restored.restore(stable);
    EXPECT_EQ(restored.operator_id_scheme(), OperatorIdScheme::Stable);
}

// Version 2 means stable ids; a version-2 stream that names another scheme, or
// none, is refused rather than read under a guess.
TEST(OperatorIdRemap, AVersionTwoStreamMustNameAKnownScheme) {
    auto m = std::make_shared<arrow::KeyValueMetadata>();
    m->Append(kSnapshotFormatVersionKey, "2");
    EXPECT_THROW((void)snapshot_operator_id_scheme(m, "test"), clink::state::RestoreRefused);
    m->Append(kOperatorIdSchemeMetadataKey, "murmur-9");
    EXPECT_THROW((void)snapshot_operator_id_scheme(m, "test"), clink::state::RestoreRefused);
    auto ok = std::make_shared<arrow::KeyValueMetadata>();
    ok->Append(kSnapshotFormatVersionKey, "2");
    ok->Append(kOperatorIdSchemeMetadataKey, "fnv1a-1");
    EXPECT_EQ(snapshot_operator_id_scheme(ok, "test"), OperatorIdScheme::Stable);
    EXPECT_EQ(snapshot_operator_id_scheme(nullptr, "test"), OperatorIdScheme::Legacy);
}

TEST(OperatorIdRemap, TheFileBackedAndShardedBackendsMoveEveryShare) {
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_remap_file_" + std::to_string(::getpid()));
    {
        FileBackedStateBackend file(dir);
        file.put(kOld, keyed("a"), VV{"A"});
        file.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
        EXPECT_EQ(text(file.get(kNew, keyed("a"))), "A");
        EXPECT_EQ(file.operator_id_scheme(), OperatorIdScheme::Stable);
    }
    std::filesystem::remove_all(dir);

    ShardedInMemoryStateBackend sharded;
    for (int i = 0; i < 64; ++i) {
        sharded.put(
            kOld, std::string(1, static_cast<char>(i % 100)) + "k" + std::to_string(i), VV{"v"});
    }
    const auto moved =
        sharded.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
    EXPECT_EQ(moved.at(kNew), 64U) << "every shard's share moves";
    std::size_t under_old = 0;
    sharded.scan(kOld, [&](KV, VV) { ++under_old; });
    EXPECT_EQ(under_old, 0U);
}

// The changelog backend logs the moves: a restore replays the log over a
// materialisation that may still hold the old id, and an erase of a moved key
// that was not logged against the old id would bring the old value back.
TEST(OperatorIdRemap, TheChangelogBackendLogsTheMoveSoAnEraseStaysErased) {
    ChangelogStateBackend b;
    b.put(kOld, keyed("a"), VV{"A"});
    b.put(kOld, keyed("b"), VV{"B"});
    const auto materialised = b.snapshot(CheckpointId{1});

    ChangelogStateBackend after;
    after.restore(materialised);
    after.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
    after.erase(kNew, keyed("b"));
    const auto next = after.snapshot(CheckpointId{2});

    ChangelogStateBackend again;
    again.restore(next);
    // The job derives the same alias at every deploy.
    again.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
    EXPECT_EQ(text(again.get(kNew, keyed("a"))), "A");
    EXPECT_EQ(text(again.get(kNew, keyed("b"))), "<absent>") << "the erased key came back";
    EXPECT_EQ(again.operator_id_scheme(), OperatorIdScheme::Stable);
}

// A backend that does not override the remap still has its state moved, through
// scan, get, put and erase, which every backend implements.
TEST(OperatorIdRemap, TheDefaultWalkMovesAnyBackendsState) {
    class MapBackend final : public StateBackend {
    public:
        void put(OperatorId op, KeyView key, ValueView value) override {
            rows_[{op.value(), std::string(key)}] =
                Value(reinterpret_cast<const std::byte*>(value.data()),
                      reinterpret_cast<const std::byte*>(value.data() + value.size()));
        }
        std::optional<Value> get(OperatorId op, KeyView key) const override {
            const auto it = rows_.find({op.value(), std::string(key)});
            return it == rows_.end() ? std::nullopt : std::optional<Value>{it->second};
        }
        void erase(OperatorId op, KeyView key) override {
            rows_.erase({op.value(), std::string(key)});
        }
        void scan(OperatorId op, const ScanVisitor& visit) const override {
            for (const auto& [k, v] : rows_) {
                if (k.first == op.value()) {
                    visit(k.second, ValueView{reinterpret_cast<const char*>(v.data()), v.size()});
                }
            }
        }
        Snapshot snapshot(CheckpointId id) override { return Snapshot{id, {}}; }
        void restore(const Snapshot&, const KeyGroupRange&) override {}
        std::string description() const override { return "map"; }

    private:
        std::map<std::pair<std::uint64_t, std::string>, Value> rows_;
    };
    MapBackend b;
    b.put(kOld, keyed("a"), VV{"A"});
    b.put(kOld, keyed("b"), VV{"OLD"});
    b.put(kNew, keyed("b"), VV{"NEW"});
    const auto moved =
        b.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
    EXPECT_EQ(moved.at(kNew), 2U);
    EXPECT_EQ(text(b.get(kNew, keyed("a"))), "A");
    EXPECT_EQ(text(b.get(kNew, keyed("b"))), "NEW");
    EXPECT_EQ(text(b.get(kOld, keyed("a"))), "<absent>");
}

// Remote-read state stays in the pool under the old id: reads fall back to it,
// scans and exports fold it in under the new id, and an erase deletes it under
// both, so a later restore does not bring it back.
TEST(OperatorIdRemap, RemoteReadReadsThePoolThroughTheAlias) {
    auto pool = std::make_shared<InMemoryRemotePool>();
    {
        RemoteReadBackend writer(pool);
        writer.put(kOld, keyed("a"), VV{"A"});
        writer.put(kOld, keyed("b"), VV{"B"});
        writer.put(kOld, keyed("c"), VV{"OLD-C"});
        writer.snapshot(CheckpointId{1});
    }
    {
        RemoteReadBackend b(pool);
        b.restore(Snapshot{CheckpointId{1}, {}});
        b.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
        EXPECT_EQ(text(b.get(kNew, keyed("a"))), "A") << "a cold read under the new id";
        b.put(kNew, keyed("c"), VV{"NEW-C"});
        b.erase(kNew, keyed("b"));
        std::map<std::string, std::string> seen;
        b.scan(kNew, [&](KV k, VV v) { seen[std::string(k)] = std::string(v); });
        EXPECT_EQ(seen.size(), 2U);
        EXPECT_EQ(seen[keyed("a")], "A");
        EXPECT_EQ(seen[keyed("c")], "NEW-C");
        b.snapshot(CheckpointId{2});
    }
    RemoteReadBackend again(pool);
    again.restore(Snapshot{CheckpointId{2}, {}});
    again.remap_operator_ids({OperatorIdAlias{kOld, kNew}}, OperatorIdScheme::Stable);
    EXPECT_EQ(text(again.get(kNew, keyed("a"))), "A");
    EXPECT_EQ(text(again.get(kNew, keyed("b"))), "<absent>") << "the erased key came back";
    EXPECT_EQ(text(again.get(kNew, keyed("c"))), "NEW-C");

    InMemoryStateBackend exported;
    exported.restore(Snapshot{CheckpointId{3}, again.export_arrow_snapshot()});
    EXPECT_EQ(text(exported.get(kNew, keyed("a"))), "A") << "exported under the new id";
    EXPECT_EQ(text(exported.get(kOld, keyed("a"))), "<absent>");
}

// LocalExecutor moves the restored state before any operator runs, so the
// operator finds its state under the id this build gives it.
TEST(OperatorIdRemap, LocalExecutorMovesRestoredStateBeforeTheJobRuns) {
    InMemoryStateBackend seed;
    seed.put(kOld, keyed("a"), VV{"A"});
    const auto snap = seed.snapshot(CheckpointId{1});

    Dag dag;
    auto h = dag.add_source<std::int64_t>(
        std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{}));
    dag.add_sink<std::int64_t>(h, std::make_shared<CollectingSink<std::int64_t>>());
    auto backend = std::make_shared<InMemoryStateBackend>();
    JobConfig cfg;
    cfg.state_backend = backend;
    cfg.restore_from = snap;
    cfg.operator_id_aliases = {OperatorIdAlias{kOld, kNew}};
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    EXPECT_EQ(text(backend->get(kNew, keyed("a"))), "A");
    EXPECT_EQ(text(backend->get(kOld, keyed("a"))), "<absent>");
}
