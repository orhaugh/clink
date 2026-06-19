// Relocatable savepoints (FOUND-3). A RocksDB savepoint captured at one path
// must restore after the savepoint dir is MOVED to a new location, even though
// the snapshot bytes embed the capture-time absolute path. The restore base
// (Options::restore_base / set_restore_base / JobConfig::restore_base) makes the
// embedded directory non-load-bearing: only the cp-dir basename matters,
// resolved under the relocated base. Without a base, the embedded path is used
// verbatim (the historic same-location behaviour).

#include <cstring>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "clink/core/types.hpp"
#include "clink/state/rocksdb_state_backend.hpp"

using namespace clink;

namespace {

namespace fs = std::filesystem;

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

fs::path scratch(const std::string& tag) {
    static int n = 0;
    auto p = fs::temp_directory_path() / ("clink_sp_reloc_" + tag + std::to_string(n++));
    std::error_code ec;
    fs::remove_all(p, ec);
    fs::create_directories(p, ec);
    return p;
}

// Capture a one-checkpoint savepoint at <root>/db, return the produced cp-dir.
fs::path capture_savepoint(const fs::path& db_path) {
    RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = db_path.string()});
    backend.put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"1"}));
    backend.put(OperatorId{1}, sv(std::string{"b"}), sv(std::string{"2"}));
    (void)backend.snapshot(CheckpointId{1});
    return fs::path{db_path.string() + ".cp-1"};
}

Snapshot snapshot_for(const fs::path& cp_dir) {
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{1};
    const std::string s = cp_dir.string();
    snap.bytes.assign(reinterpret_cast<const std::byte*>(s.data()),
                      reinterpret_cast<const std::byte*>(s.data() + s.size()));
    return snap;
}

}  // namespace

TEST(SavepointRelocation, RestoresAfterMoveViaRestoreBase) {
    const auto src = scratch("src");
    const auto cp = capture_savepoint(src / "db");  // <src>/db.cp-1, snap.bytes embed this path
    const Snapshot snap = snapshot_for(cp);

    // Relocate the savepoint to a brand-new parent, then delete the original so
    // the embedded absolute path is genuinely stale.
    const auto dst = scratch("dst");
    std::error_code ec;
    fs::copy(cp, dst / "db.cp-1", fs::copy_options::recursive, ec);
    ASSERT_FALSE(ec) << "copy savepoint: " << ec.message();
    fs::remove_all(src, ec);
    ASSERT_FALSE(fs::exists(cp)) << "original savepoint path must be gone";

    // Fresh backend at an unrelated working dir, told where the savepoint now
    // lives. Restore rebases db.cp-1 under dst.
    const auto work = scratch("work");
    RocksDBStateBackend::Options opts;
    opts.path = (work / "db").string();
    opts.restore_base = dst.string();
    RocksDBStateBackend backend(std::move(opts));
    backend.restore(snap);

    ASSERT_TRUE(backend.get(OperatorId{1}, sv(std::string{"a"})).has_value());
    EXPECT_EQ(to_string(*backend.get(OperatorId{1}, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*backend.get(OperatorId{1}, sv(std::string{"b"}))), "2");
}

TEST(SavepointRelocation, SetRestoreBaseSetterAlsoRebases) {
    const auto src = scratch("src2");
    const auto cp = capture_savepoint(src / "db");
    const Snapshot snap = snapshot_for(cp);

    const auto dst = scratch("dst2");
    std::error_code ec;
    fs::copy(cp, dst / "db.cp-1", fs::copy_options::recursive, ec);
    fs::remove_all(src, ec);

    const auto work = scratch("work2");
    RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = (work / "db").string()});
    backend.set_restore_base(dst.string());  // setter instead of Options
    backend.restore(snap);
    EXPECT_EQ(to_string(*backend.get(OperatorId{1}, sv(std::string{"a"}))), "1");
}

TEST(SavepointRelocation, NoRestoreBaseUsesEmbeddedPathVerbatim) {
    // Back-compat: with no restore base and the original savepoint in place, the
    // embedded absolute path is used as-is (the historic same-location restart).
    const auto src = scratch("src3");
    const auto cp = capture_savepoint(src / "db");
    const Snapshot snap = snapshot_for(cp);  // points at the still-present cp dir

    const auto work = scratch("work3");
    RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = (work / "db").string()});
    backend.restore(snap);  // no restore_base
    EXPECT_EQ(to_string(*backend.get(OperatorId{1}, sv(std::string{"a"}))), "1");
}
