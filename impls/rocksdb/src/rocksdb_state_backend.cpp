#include "clink/state/rocksdb_state_backend.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/state/snapshot_arrow_writer.hpp"
#include "clink/state/snapshot_store.hpp"

#ifdef CLINK_HAS_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_level.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/checkpoint.h>
#endif

namespace clink {

#ifdef CLINK_HAS_ROCKSDB

namespace {

// Stable name for the per-OperatorId ColumnFamily. Hex-encoded so it's
// directory-safe and order-stable across restarts. The CF replaces the
// 8-byte op-id key prefix used by the previous single-CF layout - see
// Impl::cf_for() for the rationale.
[[nodiscard]] std::string cf_name_for(OperatorId op) {
    static constexpr std::array<char, 16> kHex{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    std::string name(3 + 16, '\0');
    name[0] = 'o';
    name[1] = 'p';
    name[2] = '_';
    const std::uint64_t v = op.value();
    for (int i = 0; i < 16; ++i) {
        const auto nibble = static_cast<std::uint8_t>((v >> ((15 - i) * 4)) & 0xF);
        name[3 + static_cast<std::size_t>(i)] = kHex[nibble];
    }
    return name;
}

[[nodiscard]] std::optional<OperatorId> op_from_cf_name(std::string_view cf_name) {
    if (cf_name.size() != 3 + 16 || cf_name.substr(0, 3) != "op_") {
        return std::nullopt;
    }
    std::uint64_t v = 0;
    for (int i = 0; i < 16; ++i) {
        const char c = cf_name[3 + static_cast<std::size_t>(i)];
        std::uint8_t nib = 0;
        if (c >= '0' && c <= '9') {
            nib = static_cast<std::uint8_t>(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            nib = static_cast<std::uint8_t>(10 + c - 'a');
        } else {
            return std::nullopt;
        }
        v = (v << 4) | nib;
    }
    return OperatorId{v};
}

// List SST basenames currently in a RocksDB instance. RocksDB names
// every SST file <N>.sst where N is a monotonically-increasing
// integer; the same file appears in every checkpoint dir for as long
// as it's referenced (immutable LSM segment). We use the basename set
// to compute the cross-checkpoint sharing ratio surfaced by
// last_snapshot_stats().
std::vector<std::string> list_sst_basenames(const std::filesystem::path& dir) {
    std::vector<std::string> out;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        const auto name = entry.path().filename().string();
        if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".sst") == 0) {
            out.push_back(name);
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Build ColumnFamilyOptions tuned for streaming keyed-state. Same
// MemTable sizing as the DB-level Options used to be, applied per CF
// so each operator's MemTable stays compact - small MemTable means
// shallower skip-lists and shorter BytewiseComparator paths on Put,
// which was the dominant hot frame in the strict-A:A profile.
[[nodiscard]] rocksdb::ColumnFamilyOptions make_cf_options() {
    rocksdb::ColumnFamilyOptions cfo;
    cfo.write_buffer_size = 64ull * 1024 * 1024;
    cfo.max_write_buffer_number = 4;
    return cfo;
}

// Build the rocksdb::WriteOptions used for every Put/Delete on the
// keyed-state path. The key choices:
//
//   * disableWAL = true: durability is provided by the engine's own
//     checkpoint mechanism (the snapshot taken at every barrier), not
//     RocksDB's WAL.
//   * sync = false: even if a WAL is present (for unkeyed/other state
//     slots not yet wired) we don't fsync per write. Snapshots fsync
//     explicitly via the Checkpoint object.
inline const rocksdb::WriteOptions& keyed_state_write_options() {
    static const rocksdb::WriteOptions opts = [] {
        rocksdb::WriteOptions o;
        o.disableWAL = true;
        o.sync = false;
        return o;
    }();
    return opts;
}

}  // namespace

// ---------------------------------------------------------------------------
// Real implementation
// ---------------------------------------------------------------------------
struct RocksDBStateBackend::Impl {
    std::unique_ptr<rocksdb::DB> db;
    std::string path;
    // DISAGG-1: where checkpoint dirs are published/fetched. Defaults to
    // LocalSnapshotStore (set in the ctor), so snapshot/restore/purge route
    // through the seam with byte-for-byte the historic local behaviour.
    std::shared_ptr<SnapshotStore> store;
    // FOUND-3: relocated-savepoint rebase anchor (empty = use embedded paths).
    std::string restore_base;

    // CF-per-OperatorId. Each operator's keyed state lives in its
    // own RocksDB ColumnFamily (one CF per state slot). This matters
    // for performance in two ways:
    //
    //   1. Keys drop the 8-byte op-id prefix the previous single-CF
    //      layout needed - shorter keys mean fewer bytes per
    //      BytewiseComparator call during skip-list insertion, which
    //      was the #1 hot frame in profiling (>15% of self time
    //      across multiple call paths).
    //   2. Each CF's MemTable holds only one operator's data, so the
    //      skip-list stays shallower under steady-state load.
    //
    // CF handles are owned by RocksDB once registered with the DB.
    // Impl destructor calls DestroyColumnFamilyHandle on each before
    // releasing db.
    //
    // cf_mu_ protects the map. Reads (common path) take a shared
    // lock; CF creation (first put/get/erase/scan for a new
    // operator) upgrades to exclusive. The double-checked pattern
    // inside cf_for() keeps the hot path lock-free for already-known
    // operators.
    mutable std::shared_mutex cf_mu_;
    std::unordered_map<std::uint64_t, rocksdb::ColumnFamilyHandle*> cfs_;
    rocksdb::ColumnFamilyHandle* default_cf_{nullptr};

    // Buffered-write path: every Put appends to write_batch_ instead
    // of calling db->Put directly. Flushed when:
    //   * the batch's encoded size exceeds kWriteBatchFlushBytes
    //   * a Get/erase/scan/snapshot needs an up-to-date view of state
    //
    // The WriteBatch supports per-CF entries via Put(cf, key, value);
    // mixing CFs in a single batch is fine.
    mutable std::mutex buffer_mu_;
    rocksdb::WriteBatch write_batch_;
    // Default 1 MiB flush threshold; overridable via CLINK_RDB_WB_BYTES
    // for tuning. The flush amortises WriteThread + memtable arena
    // cost over many puts, so smaller batches are pure overhead, but
    // very large batches block the bench thread for too long during
    // flush. 1 MiB landed best on the 10M-record strict-A:A profile.
    static std::size_t wb_flush_bytes() {
        static const std::size_t v = [] {
            if (const char* e = std::getenv("CLINK_RDB_WB_BYTES")) {
                try {
                    return static_cast<std::size_t>(std::stoull(e));
                } catch (...) {
                    // fall through to default
                }
            }
            return static_cast<std::size_t>(1ull * 1024 * 1024);
        }();
        return v;
    }

    // Most recent snapshot's introspection, served by
    // last_snapshot_stats().
    std::mutex snap_mu;
    std::optional<SnapshotStats> last_stats;

    [[nodiscard]] rocksdb::ColumnFamilyHandle* cf_for(OperatorId op) {
        const auto key = op.value();
        {
            std::shared_lock lk(cf_mu_);
            if (auto it = cfs_.find(key); it != cfs_.end()) {
                return it->second;
            }
        }
        std::unique_lock lk(cf_mu_);
        if (auto it = cfs_.find(key); it != cfs_.end()) {
            return it->second;
        }
        rocksdb::ColumnFamilyHandle* handle = nullptr;
        const auto name = cf_name_for(op);
        auto st = db->CreateColumnFamily(make_cf_options(), name, &handle);
        if (!st.ok()) {
            throw std::runtime_error("RocksDBStateBackend::cf_for create failed: " + st.ToString());
        }
        cfs_.emplace(key, handle);
        return handle;
    }

    // Look up a CF by op-id without creating one. Read paths
    // (get/scan) use this so a query against an unknown operator
    // returns empty instead of materialising an empty CF.
    [[nodiscard]] rocksdb::ColumnFamilyHandle* cf_lookup(OperatorId op) const {
        std::shared_lock lk(cf_mu_);
        auto it = cfs_.find(op.value());
        return it == cfs_.end() ? nullptr : it->second;
    }

    void flush_write_batch_locked() {
        if (write_batch_.Count() == 0) {
            return;
        }
        auto st = db->Write(keyed_state_write_options(), &write_batch_);
        if (!st.ok()) {
            throw std::runtime_error("RocksDBStateBackend::flush_write_batch failed: " +
                                     st.ToString());
        }
        write_batch_.Clear();
    }
};

bool RocksDBStateBackend::is_real_implementation() {
    return true;
}

namespace {

// Open the DB at `path` with every existing ColumnFamily reattached.
// RocksDB requires every CF that exists on disk to be enumerated in
// the OpenColumnFamilies call - missing one is a hard error. So we
// list the CFs first, build a descriptor list, then open. The
// returned handles in `out_handles` line up positionally with the
// descriptors.
void open_db_with_all_cfs(const std::string& path,
                          bool create_if_missing,
                          std::unique_ptr<rocksdb::DB>& out_db,
                          std::vector<rocksdb::ColumnFamilyHandle*>& out_handles,
                          std::vector<std::string>& out_cf_names) {
    rocksdb::DBOptions db_opts;
    db_opts.create_if_missing = create_if_missing;
    db_opts.create_missing_column_families = true;
    db_opts.IncreaseParallelism(
        static_cast<int>(std::max<unsigned>(2u, std::thread::hardware_concurrency() / 2)));

    // Discover existing CFs. ListColumnFamilies fails on a fresh
    // path (no MANIFEST yet); treat that as "default only".
    std::vector<std::string> cf_names;
    {
        rocksdb::Options probe_opts;
        probe_opts.create_if_missing = create_if_missing;
        auto st = rocksdb::DB::ListColumnFamilies(probe_opts, path, &cf_names);
        if (!st.ok()) {
            cf_names = {rocksdb::kDefaultColumnFamilyName};
        }
    }
    if (cf_names.empty()) {
        cf_names = {rocksdb::kDefaultColumnFamilyName};
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size());
    for (const auto& name : cf_names) {
        descriptors.emplace_back(name, make_cf_options());
    }

    std::unique_ptr<rocksdb::DB> owned;
    auto st = rocksdb::DB::Open(db_opts, path, descriptors, &out_handles, &owned);
    if (!st.ok()) {
        throw std::runtime_error("RocksDBStateBackend::open failed: " + st.ToString());
    }
    out_db = std::move(owned);
    out_cf_names = std::move(cf_names);
}

// Open `path` READ-ONLY with every existing CF reattached. Used by the
// scale-down restore merge: each additional parent checkpoint is opened
// read-only and its rows copied into the live DB. Read-only avoids
// mutating or locking the parent checkpoint dir (it may be shared).
void open_readonly_with_all_cfs(const std::string& path,
                                std::unique_ptr<rocksdb::DB>& out_db,
                                std::vector<rocksdb::ColumnFamilyHandle*>& out_handles,
                                std::vector<std::string>& out_cf_names) {
    std::vector<std::string> cf_names;
    {
        rocksdb::Options probe_opts;
        auto st = rocksdb::DB::ListColumnFamilies(probe_opts, path, &cf_names);
        if (!st.ok()) {
            cf_names = {rocksdb::kDefaultColumnFamilyName};
        }
    }
    if (cf_names.empty()) {
        cf_names = {rocksdb::kDefaultColumnFamilyName};
    }
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size());
    for (const auto& name : cf_names) {
        descriptors.emplace_back(name, make_cf_options());
    }
    rocksdb::DBOptions db_opts;
    auto st = rocksdb::DB::OpenForReadOnly(db_opts, path, descriptors, &out_handles, &out_db);
    if (!st.ok()) {
        throw std::runtime_error("RocksDBStateBackend::open(read-only) failed: " + st.ToString());
    }
    out_cf_names = std::move(cf_names);
}

}  // namespace

RocksDBStateBackend::RocksDBStateBackend(Options opts) : impl_(std::make_unique<Impl>()) {
    // RocksDB updates perf_context stats on every op behind a
    // thread-local check; the wrapper showed up at ~5% of self time
    // in profiles. Disable for production-style state-backend usage.
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    std::vector<std::string> names;
    open_db_with_all_cfs(opts.path, opts.create_if_missing, impl_->db, handles, names);

    // Wire up the CF map. The default CF is kept around as
    // default_cf_ (we never write to it on the keyed path, but
    // RocksDB requires it stay open for the DB to be usable).
    for (std::size_t i = 0; i < handles.size(); ++i) {
        if (names[i] == rocksdb::kDefaultColumnFamilyName) {
            impl_->default_cf_ = handles[i];
            continue;
        }
        if (auto op = op_from_cf_name(names[i])) {
            impl_->cfs_.emplace(op->value(), handles[i]);
        } else {
            // Unrecognised CF name - leak the handle into the
            // default-CF bucket so the DB stays well-formed (RocksDB
            // owns handles; we DestroyColumnFamilyHandle in the
            // destructor). We don't expect this branch in normal
            // operation; it would only fire if the DB was written
            // by something other than this backend.
            (void)handles[i];
        }
    }
    impl_->path = std::move(opts.path);
    impl_->store = opts.snapshot_store ? std::move(opts.snapshot_store)
                                       : std::make_shared<LocalSnapshotStore>();
    impl_->restore_base = std::move(opts.restore_base);
}

void RocksDBStateBackend::set_restore_base(const std::string& dir) {
    impl_->restore_base = dir;
}

RocksDBStateBackend::~RocksDBStateBackend() {
    // Drain the WriteBatch before tearing down the DB.
    try {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    } catch (...) {
        // swallow - destructor must not throw
    }
    // Release CF handles before db. DestroyColumnFamilyHandle is the
    // RocksDB-blessed way to drop a handle; the underlying CF stays
    // in the DB (no actual data loss). Default CF handle must be
    // destroyed too.
    if (impl_->db) {
        for (auto& [_, handle] : impl_->cfs_) {
            (void)impl_->db->DestroyColumnFamilyHandle(handle);
        }
        impl_->cfs_.clear();
        if (impl_->default_cf_ != nullptr) {
            (void)impl_->db->DestroyColumnFamilyHandle(impl_->default_cf_);
            impl_->default_cf_ = nullptr;
        }
    }
}

void RocksDBStateBackend::put(OperatorId op, KeyView key, ValueView value) {
    auto* cf = impl_->cf_for(op);
    const rocksdb::Slice k_slice(key.data(), key.size());
    const rocksdb::Slice v_slice(value.data(), value.size());
    if (const char* env = std::getenv("CLINK_RDB_DIRECT_PUT"); env != nullptr && env[0] == '1') {
        auto st = impl_->db->Put(keyed_state_write_options(), cf, k_slice, v_slice);
        if (!st.ok()) {
            throw std::runtime_error("RocksDBStateBackend::put failed: " + st.ToString());
        }
        return;
    }
    std::lock_guard lock(impl_->buffer_mu_);
    auto st = impl_->write_batch_.Put(cf, k_slice, v_slice);
    if (!st.ok()) {
        throw std::runtime_error("RocksDBStateBackend::put failed: " + st.ToString());
    }
    if (impl_->write_batch_.GetDataSize() >= Impl::wb_flush_bytes()) {
        impl_->flush_write_batch_locked();
    }
}

std::optional<RocksDBStateBackend::Value> RocksDBStateBackend::get(OperatorId op,
                                                                   KeyView key) const {
    auto* cf = impl_->cf_lookup(op);
    if (cf == nullptr) {
        return std::nullopt;
    }
    {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    }
    const rocksdb::Slice k_slice(key.data(), key.size());
    std::string out;
    auto st = impl_->db->Get(rocksdb::ReadOptions{}, cf, k_slice, &out);
    if (st.IsNotFound()) {
        return std::nullopt;
    }
    if (!st.ok()) {
        throw std::runtime_error("RocksDBStateBackend::get failed: " + st.ToString());
    }
    Value bytes(out.size());
    if (!out.empty()) {
        std::memcpy(bytes.data(), out.data(), out.size());
    }
    return bytes;
}

void RocksDBStateBackend::erase(OperatorId op, KeyView key) {
    auto* cf = impl_->cf_for(op);
    const rocksdb::Slice k_slice(key.data(), key.size());
    std::lock_guard lock(impl_->buffer_mu_);
    auto st = impl_->write_batch_.Delete(cf, k_slice);
    if (!st.ok()) {
        throw std::runtime_error("RocksDBStateBackend::erase failed: " + st.ToString());
    }
    if (impl_->write_batch_.GetDataSize() >= Impl::wb_flush_bytes()) {
        impl_->flush_write_batch_locked();
    }
}

void RocksDBStateBackend::scan(OperatorId op, const ScanVisitor& visit) const {
    auto* cf = impl_->cf_lookup(op);
    if (cf == nullptr) {
        return;  // unknown operator => empty range
    }
    // Drain any buffered writes so the iterator sees the post-write
    // view of state.
    {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    }
    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(ro, cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        auto key_slice = it->key();
        const std::string_view user_key(key_slice.data(), key_slice.size());
        const auto value_slice = it->value();
        const std::string_view value(value_slice.data(), value_slice.size());
        visit(user_key, value);
    }
}

bool RocksDBStateBackend::supports_async_persist() const noexcept {
    // Split snapshot into capture (local, op-thread) + persist (publish,
    // off-thread) only when the store's publish is a slow durable write.
    return impl_->store && impl_->store->defers_durable_write();
}

CaptureHandle RocksDBStateBackend::capture(CheckpointId id) {
    // Operator-thread phase: the cheap local RocksDB checkpoint. Flush
    // buffered writes so the checkpoint captures them, then CreateCheckpoint
    // hard-links the immutable SSTs into a DETACHED dir - ongoing put/get on
    // the live DB do not change it, so persist() can publish it off-thread.
    {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    }
    rocksdb::Checkpoint* cp_raw = nullptr;
    auto st = rocksdb::Checkpoint::Create(impl_->db.get(), &cp_raw);
    std::unique_ptr<rocksdb::Checkpoint> cp(cp_raw);
    if (!st.ok()) {
        throw std::runtime_error("Checkpoint::Create failed: " + st.ToString());
    }

    const std::string snap_path = impl_->path + ".cp-" + std::to_string(id.value());
    std::error_code ec;
    std::filesystem::remove_all(snap_path, ec);
    st = cp->CreateCheckpoint(snap_path);
    if (!st.ok()) {
        throw std::runtime_error("CreateCheckpoint failed: " + st.ToString());
    }

    SnapshotStats stats;
    stats.checkpoint_id = id.value();
    stats.sst_files = list_sst_basenames(snap_path);
    stats.total_sst_count = stats.sst_files.size();
    {
        std::lock_guard lock(impl_->snap_mu);
        if (impl_->last_stats.has_value()) {
            const auto& prev = impl_->last_stats->sst_files;
            std::unordered_set<std::string> prev_set(prev.begin(), prev.end());
            for (const auto& name : stats.sst_files) {
                if (prev_set.count(name) != 0) {
                    ++stats.shared_sst_count;
                }
            }
        }
        impl_->last_stats = stats;
    }

    // The capture handle carries the LOCAL cp-dir path; persist() publishes it.
    CaptureHandle h;
    h.checkpoint_id = id;
    h.bytes.resize(snap_path.size());
    std::memcpy(h.bytes.data(), snap_path.data(), snap_path.size());
    return h;
}

Snapshot RocksDBStateBackend::persist(CaptureHandle handle) {
    // Worker-thread phase: publish the captured local cp-dir through the store
    // (a remote upload for an S3 store) and embed the returned handle.
    // LocalSnapshotStore returns the path unchanged, so the bytes match the
    // pre-seam behaviour. Touches only the captured dir + the store, never the
    // live DB, so it is safe off the operator thread.
    std::string local_path(handle.bytes.size(), '\0');
    if (!handle.bytes.empty()) {
        std::memcpy(local_path.data(), handle.bytes.data(), handle.bytes.size());
    }
    const std::string published =
        impl_->store->write_checkpoint_dir(local_path, handle.checkpoint_id);
    Snapshot snap;
    snap.checkpoint_id = handle.checkpoint_id;
    snap.bytes.resize(published.size());
    std::memcpy(snap.bytes.data(), published.data(), published.size());
    return snap;
}

Snapshot RocksDBStateBackend::snapshot(CheckpointId id) {
    // Synchronous path = capture (local) then persist (publish), inline. The
    // runner uses capture()/persist() separately (off-thread persist) when
    // supports_async_persist() is true; the result is identical either way.
    return persist(capture(id));
}

std::optional<RocksDBStateBackend::SnapshotStats> RocksDBStateBackend::last_snapshot_stats() const {
    std::lock_guard lock(impl_->snap_mu);
    return impl_->last_stats;
}

void RocksDBStateBackend::purge_checkpoint(CheckpointId id) {
    const auto snap_path = impl_->path + ".cp-" + std::to_string(id.value());
    // Drop the published checkpoint through the store. LocalSnapshotStore
    // remove_all's the dir (identical to before); a remote store deletes its
    // objects for this id.
    impl_->store->delete_checkpoint(snap_path, id);
}

void RocksDBStateBackend::restore(const Snapshot& snap, const KeyGroupRange& kg_filter) {
    // snap.bytes carries one or more newline-separated checkpoint dir paths.
    // The first is re-homed onto (fast hard-link); any additional ones
    // (scale-down: several parent subtasks merged into this one) are
    // iterate-merged below. A single path is the same-parallelism / scale-up
    // case and behaves exactly as before.
    std::string raw(snap.bytes.size(), '\0');
    if (!snap.bytes.empty()) {
        std::memcpy(raw.data(), snap.bytes.data(), snap.bytes.size());
    }
    std::vector<std::string> source_paths;
    for (std::size_t start = 0; start <= raw.size();) {
        const auto nl = raw.find('\n', start);
        if (nl == std::string::npos) {
            if (start < raw.size()) {
                source_paths.push_back(raw.substr(start));
            }
            break;
        }
        source_paths.push_back(raw.substr(start, nl - start));
        start = nl + 1;
    }
    if (source_paths.empty()) {
        source_paths.push_back(raw);  // empty bytes: preserve the old (failing) behaviour
    }
    // FOUND-3 (relocatable savepoints): when a restore base is configured, a
    // cp-dir reference's embedded directory is non-load-bearing - only its
    // basename matters, resolved under the (possibly relocated) base. This lets
    // a savepoint moved to a new path/machine restore even though snap.bytes
    // embed the capture-time absolute path. Empty base = use the path verbatim
    // (same-location restart / object-store handles, the historic behaviour).
    if (!impl_->restore_base.empty()) {
        for (auto& p : source_paths) {
            p = (std::filesystem::path(impl_->restore_base) / std::filesystem::path(p).filename())
                    .string();
        }
    }
    // Fetch the first handle to a local path the DB can open (identity for a
    // local store; a download for a remote one), then re-home onto it.
    const std::string source_path = impl_->store->fetch_checkpoint_dir(source_paths.front());

    // Tear down the live DB before re-opening on the restored dir.
    // Drop CF handles first - they're owned by the DB and become
    // invalid the moment db goes away.
    if (impl_->db) {
        for (auto& [_, handle] : impl_->cfs_) {
            (void)impl_->db->DestroyColumnFamilyHandle(handle);
        }
        impl_->cfs_.clear();
        if (impl_->default_cf_ != nullptr) {
            (void)impl_->db->DestroyColumnFamilyHandle(impl_->default_cf_);
            impl_->default_cf_ = nullptr;
        }
    }
    impl_->db.reset();

    const std::string working_path = source_path + ".restored-" +
                                     std::to_string(snap.checkpoint_id.value()) + "-" +
                                     std::to_string(reinterpret_cast<std::uintptr_t>(this));
    std::error_code ec;
    std::filesystem::remove_all(working_path, ec);
    std::filesystem::create_directories(working_path, ec);

    for (const auto& entry : std::filesystem::directory_iterator(source_path, ec)) {
        const auto target = std::filesystem::path{working_path} / entry.path().filename();
        std::error_code link_ec;
        std::filesystem::create_hard_link(entry.path(), target, link_ec);
        if (link_ec) {
            std::filesystem::copy_file(
                entry.path(), target, std::filesystem::copy_options::overwrite_existing, link_ec);
        }
    }

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    std::vector<std::string> names;
    open_db_with_all_cfs(working_path, /*create_if_missing=*/false, impl_->db, handles, names);
    for (std::size_t i = 0; i < handles.size(); ++i) {
        if (names[i] == rocksdb::kDefaultColumnFamilyName) {
            impl_->default_cf_ = handles[i];
            continue;
        }
        if (auto op = op_from_cf_name(names[i])) {
            impl_->cfs_.emplace(op->value(), handles[i]);
        }
    }
    impl_->path = working_path;

    // Scale-down: merge any additional parent checkpoints into the live DB.
    // Each is opened read-only and its rows copied in; the kg-filter below
    // narrows the merged keyed rows to this subtask's range (the union of the
    // parents' ranges) and keeps operator-state rows. All subtasks of a job
    // share the same operators, so cf_for() finds or creates the matching CF.
    for (std::size_t p = 1; p < source_paths.size(); ++p) {
        std::unique_ptr<rocksdb::DB> parent_db;
        std::vector<rocksdb::ColumnFamilyHandle*> parent_handles;
        std::vector<std::string> parent_names;
        const std::string parent_path = impl_->store->fetch_checkpoint_dir(source_paths[p]);
        open_readonly_with_all_cfs(parent_path, parent_db, parent_handles, parent_names);
        for (std::size_t i = 0; i < parent_handles.size(); ++i) {
            if (parent_names[i] == rocksdb::kDefaultColumnFamilyName) {
                continue;
            }
            const auto op = op_from_cf_name(parent_names[i]);
            if (!op) {
                continue;
            }
            auto* live_cf = impl_->cf_for(*op);
            std::unique_ptr<rocksdb::Iterator> it(
                parent_db->NewIterator(rocksdb::ReadOptions{}, parent_handles[i]));
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                auto st =
                    impl_->db->Put(keyed_state_write_options(), live_cf, it->key(), it->value());
                if (!st.ok()) {
                    throw std::runtime_error("RocksDBStateBackend::restore merge put failed: " +
                                             st.ToString());
                }
            }
        }
        for (auto* h : parent_handles) {
            (void)parent_db->DestroyColumnFamilyHandle(h);
        }
        // parent_db destructor closes the read-only handle.
    }

    // Apply the key-group filter by deleting entries outside the
    // covered range. With CF-per-OperatorId there's no op-id prefix
    // any more: the kg byte sits at offset 0 of the raw user key
    // (see KeyedState::encode_key - kg | slot | '|' | user_key).
    // Walk each CF independently.
    if (!kg_filter.covers_all()) {
        std::vector<std::pair<rocksdb::ColumnFamilyHandle*, std::string>> to_delete;
        std::shared_lock lk(impl_->cf_mu_);
        for (const auto& [_, cf] : impl_->cfs_) {
            rocksdb::ReadOptions ro;
            std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(ro, cf));
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                auto key_slice = it->key();
                if (key_slice.size() == 0) {
                    continue;
                }
                const auto first = static_cast<std::uint8_t>(key_slice.data()[0]);
                // Operator-state rows carry a reserved prefix >= kNumKeyGroups
                // (kOperatorStateKeyPrefix); they have no key group and must be
                // kept on every subtask (broadcast/union semantics, e.g. source
                // offsets), so they are exempt from key-group narrowing on a
                // rescale. Mirrors ChangelogStateBackend::key_in_range_ and the
                // in-memory restore. Without this, a narrowed (rescale) restore
                // would silently drop offsets and break exactly-once.
                if (first >= kNumKeyGroups) {
                    continue;
                }
                const auto kg = static_cast<KeyGroup>(first);
                if (!kg_filter.contains(kg)) {
                    to_delete.emplace_back(cf, std::string(key_slice.data(), key_slice.size()));
                }
            }
        }
        for (auto& [cf, k] : to_delete) {
            (void)impl_->db->Delete(rocksdb::WriteOptions{}, cf, k);
        }
    }
}

Snapshot RocksDBStateBackend::combine_snapshots(std::vector<Snapshot> parts) const {
    if (parts.empty()) {
        return Snapshot{};
    }
    if (parts.size() == 1) {
        return std::move(parts.front());
    }
    // Each part.bytes is a checkpoint-dir path (see snapshot()); join them
    // newline-separated so restore() re-homes onto the first and
    // iterate-merges the rest.
    std::string joined;
    for (const auto& p : parts) {
        if (!joined.empty()) {
            joined.push_back('\n');
        }
        joined.append(reinterpret_cast<const char*>(p.bytes.data()), p.bytes.size());
    }
    Snapshot out;
    out.checkpoint_id = parts.front().checkpoint_id;
    out.bytes.assign(reinterpret_cast<const std::byte*>(joined.data()),
                     reinterpret_cast<const std::byte*>(joined.data() + joined.size()));
    return out;
}

std::string RocksDBStateBackend::description() const {
    return "rocksdb state backend at " + impl_->path;
}

namespace {

// Shared iterate-and-encode core for the live and checkpoint-dir Arrow
// exports. Walks the op_* column families in ascending op-id order
// (deterministic output) and appends every row to the canonical writer.
// `read_opts` lets the live path pin a RocksDB snapshot for the walk.
std::vector<std::byte> export_cfs_to_arrow(
    rocksdb::DB& db,
    const std::vector<std::pair<std::uint64_t, rocksdb::ColumnFamilyHandle*>>& cfs,
    const rocksdb::ReadOptions& read_opts) {
    auto sorted = cfs;
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    SnapshotArrowWriter writer;
    for (const auto& [op_id, cf] : sorted) {
        std::unique_ptr<rocksdb::Iterator> it(db.NewIterator(read_opts, cf));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const auto k = it->key();
            const auto v = it->value();
            writer.append(
                op_id, std::string_view(k.data(), k.size()), std::string_view(v.data(), v.size()));
        }
    }
    // RocksDB checkpoints carry no StateVersionMap: emit the bare schema.
    return writer.finish();
}

}  // namespace

std::vector<std::byte> RocksDBStateBackend::export_arrow_snapshot() const {
    // Drain buffered writes so the export sees the post-write view, then
    // pin a RocksDB snapshot so the walk is a consistent point in time
    // even while the operator keeps writing.
    {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    }
    const rocksdb::Snapshot* snap = impl_->db->GetSnapshot();
    rocksdb::ReadOptions ro;
    ro.snapshot = snap;
    std::vector<std::pair<std::uint64_t, rocksdb::ColumnFamilyHandle*>> cfs;
    {
        std::shared_lock lk(impl_->cf_mu_);
        cfs.reserve(impl_->cfs_.size());
        for (const auto& [op_id, cf] : impl_->cfs_) {
            cfs.emplace_back(op_id, cf);
        }
    }
    std::vector<std::byte> bytes;
    try {
        bytes = export_cfs_to_arrow(*impl_->db, cfs, ro);
    } catch (...) {
        impl_->db->ReleaseSnapshot(snap);
        throw;
    }
    impl_->db->ReleaseSnapshot(snap);
    return bytes;
}

std::vector<std::byte> rocksdb_checkpoint_to_arrow(const std::string& checkpoint_dir) {
    std::unique_ptr<rocksdb::DB> db;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    std::vector<std::string> names;
    open_readonly_with_all_cfs(checkpoint_dir, db, handles, names);

    std::vector<std::pair<std::uint64_t, rocksdb::ColumnFamilyHandle*>> cfs;
    cfs.reserve(handles.size());
    for (std::size_t i = 0; i < handles.size(); ++i) {
        if (auto op = op_from_cf_name(names[i])) {
            cfs.emplace_back(op->value(), handles[i]);
        }
    }
    std::vector<std::byte> bytes;
    try {
        bytes = export_cfs_to_arrow(*db, cfs, rocksdb::ReadOptions{});
    } catch (...) {
        for (auto* h : handles) {
            (void)db->DestroyColumnFamilyHandle(h);
        }
        throw;
    }
    for (auto* h : handles) {
        (void)db->DestroyColumnFamilyHandle(h);
    }
    return bytes;
}

#else  // !CLINK_HAS_ROCKSDB

// ---------------------------------------------------------------------------
// Stub implementation: builds without the dependency, throws on construction.
// ---------------------------------------------------------------------------
struct RocksDBStateBackend::Impl {};

bool RocksDBStateBackend::is_real_implementation() {
    return false;
}

RocksDBStateBackend::RocksDBStateBackend(Options /*opts*/) {
    throw std::runtime_error(
        "RocksDBStateBackend: built without RocksDB support. Install rocksdb "
        "(e.g. `brew install rocksdb`) and reconfigure cmake with "
        "CLINK_WITH_ROCKSDB=ON, or use InMemoryStateBackend instead.");
}

RocksDBStateBackend::~RocksDBStateBackend() = default;

void RocksDBStateBackend::put(OperatorId, KeyView, ValueView) {}
std::optional<RocksDBStateBackend::Value> RocksDBStateBackend::get(OperatorId, KeyView) const {
    return std::nullopt;
}
void RocksDBStateBackend::erase(OperatorId, KeyView) {}
void RocksDBStateBackend::scan(OperatorId, const ScanVisitor&) const {}
Snapshot RocksDBStateBackend::snapshot(CheckpointId) {
    return {};
}
bool RocksDBStateBackend::supports_async_persist() const noexcept {
    return false;
}
CaptureHandle RocksDBStateBackend::capture(CheckpointId) {
    throw std::runtime_error("RocksDBStateBackend: unavailable in this build");
}
Snapshot RocksDBStateBackend::persist(CaptureHandle) {
    throw std::runtime_error("RocksDBStateBackend: unavailable in this build");
}
void RocksDBStateBackend::restore(const Snapshot&, const KeyGroupRange&) {}
std::optional<RocksDBStateBackend::SnapshotStats> RocksDBStateBackend::last_snapshot_stats() const {
    return std::nullopt;
}
void RocksDBStateBackend::purge_checkpoint(CheckpointId) {}
Snapshot RocksDBStateBackend::combine_snapshots(std::vector<Snapshot>) const {
    return Snapshot{};
}
std::string RocksDBStateBackend::description() const {
    return "rocksdb state backend (stub)";
}
std::vector<std::byte> RocksDBStateBackend::export_arrow_snapshot() const {
    throw std::runtime_error("RocksDBStateBackend: unavailable in this build");
}
std::vector<std::byte> rocksdb_checkpoint_to_arrow(const std::string& /*checkpoint_dir*/) {
    throw std::runtime_error(
        "rocksdb_checkpoint_to_arrow: built without RocksDB support. Install rocksdb and "
        "reconfigure cmake with CLINK_WITH_ROCKSDB=ON.");
}

#endif

}  // namespace clink
