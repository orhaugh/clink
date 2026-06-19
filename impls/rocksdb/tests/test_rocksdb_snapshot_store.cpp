// DISAGG-1: the SnapshotStore seam on RocksDBStateBackend. A recording store
// (behaving like the local default but logging every call) proves that
// snapshot() publishes through write_checkpoint_dir, restore() pulls each
// source through fetch_checkpoint_dir (including the multi-path scale-down
// case), purge_checkpoint() routes through delete_checkpoint, and the handle
// round-trips through snap.bytes. The existing rocksdb tests, which construct
// the backend WITHOUT a store, prove the default LocalSnapshotStore preserves
// behaviour (zero test deltas).

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#if __has_include("clink/state/rocksdb_state_backend.hpp")
#include "clink/state/rocksdb_state_backend.hpp"
#include "clink/state/snapshot_store.hpp"

using namespace clink;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    if (!v.empty()) {
        std::memcpy(out.data(), v.data(), v.size());
    }
    return out;
}

std::string snap_bytes_str(const Snapshot& s) {
    std::string out(s.bytes.size(), '\0');
    if (!s.bytes.empty()) {
        std::memcpy(out.data(), s.bytes.data(), s.bytes.size());
    }
    return out;
}

std::filesystem::path fresh_dir(const std::string& tag) {
    static int n = 0;
    auto p =
        std::filesystem::temp_directory_path() / ("clink_disagg1_" + tag + std::to_string(n++));
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    return p;
}

// Behaves exactly like LocalSnapshotStore (handle == local path; fetch is the
// identity; delete is remove_all) but records every call so a test can assert
// the backend routes through the seam.
class RecordingSnapshotStore final : public SnapshotStore {
public:
    explicit RecordingSnapshotStore(bool defers = false) : defers_(defers) {}

    std::string write_checkpoint_dir(const std::string& local_cp_path, CheckpointId id) override {
        writes.push_back({local_cp_path, id.value()});
        return local_cp_path;  // local-equivalent: the dir IS the handle
    }
    std::string fetch_checkpoint_dir(const std::string& handle) override {
        fetches.push_back(handle);
        return handle;  // identity
    }
    void delete_checkpoint(const std::string& local_cp_path, CheckpointId id) override {
        deletes.push_back({local_cp_path, id.value()});
        std::error_code ec;
        std::filesystem::remove_all(local_cp_path, ec);
    }
    [[nodiscard]] bool defers_durable_write() const noexcept override { return defers_; }
    [[nodiscard]] std::string description() const override { return "recording"; }

    bool defers_;
    std::vector<std::pair<std::string, std::uint64_t>> writes;
    std::vector<std::string> fetches;
    std::vector<std::pair<std::string, std::uint64_t>> deletes;
};

}  // namespace

TEST(RocksDBSnapshotStore, SeamRoutesPublishFetchDelete) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "built without real RocksDB";
    }
    OperatorId op{7};
    auto store = std::make_shared<RecordingSnapshotStore>();

    const auto db_dir = fresh_dir("src");
    RocksDBStateBackend src(
        RocksDBStateBackend::Options{.path = db_dir.string(), .snapshot_store = store});
    src.put(op, sv(std::string{"a"}), sv(std::string{"1"}));
    src.put(op, sv(std::string{"b"}), sv(std::string{"2"}));

    auto snap = src.snapshot(CheckpointId{5});
    // Publish routed through the store exactly once, with the right id.
    ASSERT_EQ(store->writes.size(), 1u);
    EXPECT_EQ(store->writes[0].second, 5u);
    // The handle the store returned is what landed in the snapshot bytes.
    EXPECT_EQ(snap_bytes_str(snap), store->writes[0].first);

    // Restore into a fresh backend with its own recording store: the source
    // handle is pulled through fetch_checkpoint_dir, and the state recovers.
    auto store2 = std::make_shared<RecordingSnapshotStore>();
    const auto restore_dir = fresh_dir("dst");
    RocksDBStateBackend dst(
        RocksDBStateBackend::Options{.path = restore_dir.string(), .snapshot_store = store2});
    dst.restore(snap);
    ASSERT_EQ(store2->fetches.size(), 1u);
    EXPECT_EQ(store2->fetches[0], snap_bytes_str(snap));
    ASSERT_TRUE(dst.get(op, sv(std::string{"a"})).has_value());
    EXPECT_EQ(to_string(*dst.get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*dst.get(op, sv(std::string{"b"}))), "2");

    // Purge routed through delete_checkpoint with the right id.
    src.purge_checkpoint(CheckpointId{5});
    ASSERT_EQ(store->deletes.size(), 1u);
    EXPECT_EQ(store->deletes[0].second, 5u);
}

TEST(RocksDBSnapshotStore, MultiPathScaleDownFetchesEachSource) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "built without real RocksDB";
    }
    OperatorId op{7};

    // Two parent backends, each with a distinct key.
    auto store_a = std::make_shared<RecordingSnapshotStore>();
    RocksDBStateBackend a(
        RocksDBStateBackend::Options{.path = fresh_dir("a").string(), .snapshot_store = store_a});
    a.put(op, sv(std::string{"ka"}), sv(std::string{"va"}));
    auto snap_a = a.snapshot(CheckpointId{1});

    auto store_b = std::make_shared<RecordingSnapshotStore>();
    RocksDBStateBackend b(
        RocksDBStateBackend::Options{.path = fresh_dir("b").string(), .snapshot_store = store_b});
    b.put(op, sv(std::string{"kb"}), sv(std::string{"vb"}));
    auto snap_b = b.snapshot(CheckpointId{1});

    // Restore the combined (scale-down) snapshot into a fresh backend; both
    // parent handles must be pulled through fetch_checkpoint_dir, and both
    // keys must merge in.
    auto store_dst = std::make_shared<RecordingSnapshotStore>();
    RocksDBStateBackend dst(RocksDBStateBackend::Options{.path = fresh_dir("merge").string(),
                                                         .snapshot_store = store_dst});
    auto combined = dst.combine_snapshots({snap_a, snap_b});
    dst.restore(combined);

    EXPECT_EQ(store_dst->fetches.size(), 2u);  // one fetch per parent source
    ASSERT_TRUE(dst.get(op, sv(std::string{"ka"})).has_value());
    ASSERT_TRUE(dst.get(op, sv(std::string{"kb"})).has_value());
    EXPECT_EQ(to_string(*dst.get(op, sv(std::string{"ka"}))), "va");
    EXPECT_EQ(to_string(*dst.get(op, sv(std::string{"kb"}))), "vb");
}

// DISAGG-3: the async-persist split is enabled only when the store defers a
// durable write. capture() does the local checkpoint WITHOUT publishing;
// persist() publishes (the slow/remote write the SnapshotWorker runs
// off-thread). With a local store the backend stays synchronous.
TEST(RocksDBSnapshotStore, AsyncPersistSplitOnlyWhenStoreDefers) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "built without real RocksDB";
    }
    OperatorId op{7};

    // Default (local) store: synchronous, no async-persist split.
    {
        RocksDBStateBackend local(RocksDBStateBackend::Options{.path = fresh_dir("sync").string()});
        EXPECT_FALSE(local.supports_async_persist());
    }

    // A store that defers its durable write: the backend opts into the split.
    auto store = std::make_shared<RecordingSnapshotStore>(/*defers=*/true);
    RocksDBStateBackend be(
        RocksDBStateBackend::Options{.path = fresh_dir("async").string(), .snapshot_store = store});
    EXPECT_TRUE(be.supports_async_persist());
    be.put(op, sv(std::string{"k"}), sv(std::string{"v"}));

    // capture() runs the local checkpoint but must NOT publish.
    auto cap = be.capture(CheckpointId{9});
    EXPECT_TRUE(store->writes.empty()) << "capture() must not publish (no durable write)";
    EXPECT_EQ(cap.checkpoint_id.value(), 9u);
    const std::string cap_path(reinterpret_cast<const char*>(cap.bytes.data()), cap.bytes.size());
    EXPECT_TRUE(std::filesystem::exists(cap_path)) << "capture handle names a real local cp dir";

    // persist() publishes; only now does the store record the write.
    auto snap = be.persist(std::move(cap));
    ASSERT_EQ(store->writes.size(), 1u);
    EXPECT_EQ(store->writes[0].second, 9u);
    EXPECT_EQ(snap_bytes_str(snap), store->writes[0].first);

    // The captured-then-persisted snapshot restores like any other.
    auto store2 = std::make_shared<RecordingSnapshotStore>(/*defers=*/true);
    RocksDBStateBackend dst(RocksDBStateBackend::Options{.path = fresh_dir("arestore").string(),
                                                         .snapshot_store = store2});
    dst.restore(snap);
    ASSERT_TRUE(dst.get(op, sv(std::string{"k"})).has_value());
    EXPECT_EQ(to_string(*dst.get(op, sv(std::string{"k"}))), "v");
}

#endif  // __has_include
