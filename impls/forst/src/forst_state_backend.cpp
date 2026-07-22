#include "clink/state/forst_state_backend.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <coroutine>
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

#include "clink/async/completion_executor.hpp"
#include "clink/state/snapshot_arrow_writer.hpp"
#include "clink/state/snapshot_store.hpp"

// These resolve against ForSt's header tree (the imported engine target's
// include dir), NOT the bundled RocksDB's - ForSt keeps the `rocksdb/`
// include layout but compiles everything under the `forstdb` namespace,
// which is what keeps the two engines link-compatible in one binary.
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_level.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/checkpoint.h>

namespace clink {

namespace {

// Stable name for the per-OperatorId ColumnFamily. Hex-encoded so it's
// directory-safe and order-stable across restarts. Same layout as the
// bundled RocksDB backend (the two backends' on-disk formats are
// structurally parallel, though instances are never shared).
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

// Reserved key in the DEFAULT column family carrying the packed
// StateVersionMap. The keyed path never writes the default CF, so the
// key cannot collide with state. Written through set_state_versions, it
// rides every checkpoint (capture flushes memtables), so restore and the
// offline Arrow export recover the stamps from any checkpoint dir.
constexpr const char* kStateVersionsKey = "__clink.state_versions";

// List SST basenames currently in a ForSt instance. The engine names
// every SST file <N>.sst where N is a monotonically-increasing integer;
// the same file appears in every checkpoint dir for as long as it's
// referenced (immutable LSM segment). We use the basename set to compute
// the cross-checkpoint sharing ratio surfaced by last_snapshot_stats().
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
// MemTable sizing rationale as the bundled RocksDB backend: a compact
// per-operator MemTable keeps skip-lists shallow and comparator paths
// short on the Put hot path.
[[nodiscard]] forstdb::ColumnFamilyOptions make_cf_options() {
    forstdb::ColumnFamilyOptions cfo;
    cfo.write_buffer_size = 64ull * 1024 * 1024;
    cfo.max_write_buffer_number = 4;
    return cfo;
}

// Build the forstdb::WriteOptions used for every Put/Delete on the
// keyed-state path. The key choices:
//
//   * disableWAL = true: durability is provided by the engine's own
//     checkpoint mechanism (the snapshot taken at every barrier), not
//     ForSt's WAL.
//   * sync = false: even if a WAL is present we don't fsync per write.
//     Snapshots fsync explicitly via the Checkpoint object.
inline const forstdb::WriteOptions& keyed_state_write_options() {
    static const forstdb::WriteOptions opts = [] {
        forstdb::WriteOptions o;
        o.disableWAL = true;
        o.sync = false;
        return o;
    }();
    return opts;
}

}  // namespace

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------
struct ForStStateBackend::Impl {
    // Optional caller-supplied engine Env (live remote data files). The
    // holder owns it; env is the typed view. Declared BEFORE db so member
    // destruction (reverse order) tears the DB down while the Env is
    // still alive - the engine calls into its Env during close.
    std::shared_ptr<void> env_holder;
    forstdb::Env* env{nullptr};
    // Remote half of checkpoint-dir handling when data files live
    // remotely (see DataFileMirror in the header). Null = all-local.
    std::shared_ptr<DataFileMirror> mirror;

    // Deferred-read machinery (Options::defer_reads). The executor runs
    // the (potentially remote-blocking) engine reads off the runner
    // thread; per-THREAD resume targets route each completion back to
    // the runner that issued the read (a JobConfig-shared backend serves
    // several operator threads). Mirrors RemoteReadBackend's contract.
    // Declared before db: the executor is explicitly reset first in the
    // destructor, but keep the member order safe regardless.
    bool defer_reads{false};
    std::shared_ptr<async::CompletionExecutor> io;
    struct ResumeTarget {
        StateBackend::AsyncResumeScheduler resume;
        StateBackend::DeadlineResumeScheduler deadline;
    };
    mutable std::mutex sched_mu;
    std::unordered_map<std::thread::id, ResumeTarget> schedulers;
    mutable std::atomic<std::uint64_t> deferred_reads{0};

    [[nodiscard]] ResumeTarget sched_for_thread() const {
        std::lock_guard lk(sched_mu);
        auto it = schedulers.find(std::this_thread::get_id());
        return it == schedulers.end() ? ResumeTarget{} : it->second;
    }

    std::unique_ptr<forstdb::DB> db;
    std::string path;
    // Where checkpoint dirs are published/fetched. Defaults to
    // LocalSnapshotStore (set in the ctor), so snapshot/restore/purge route
    // through the seam with plain local-filesystem behaviour.
    std::shared_ptr<SnapshotStore> store;
    // Relocated-savepoint rebase anchor (empty = use embedded paths).
    std::string restore_base;

    // CF-per-OperatorId. Each operator's keyed state lives in its own
    // ColumnFamily (one CF per state slot): keys drop any op-id prefix
    // (shorter comparator paths on the Put hot path) and each CF's
    // MemTable holds only one operator's data.
    //
    // CF handles are owned by the engine once registered with the DB.
    // Impl destructor calls DestroyColumnFamilyHandle on each before
    // releasing db.
    //
    // cf_mu_ protects the map. Reads (common path) take a shared lock;
    // CF creation (first put/get/erase/scan for a new operator) upgrades
    // to exclusive. The double-checked pattern inside cf_for() keeps the
    // hot path lock-free for already-known operators.
    mutable std::shared_mutex cf_mu_;
    std::unordered_map<std::uint64_t, forstdb::ColumnFamilyHandle*> cfs_;
    forstdb::ColumnFamilyHandle* default_cf_{nullptr};

    // Buffered-write path: every Put appends to write_batch_ instead of
    // calling db->Put directly. Flushed when:
    //   * the batch's encoded size exceeds the flush threshold
    //   * a Get/erase/scan/snapshot needs an up-to-date view of state
    //
    // The WriteBatch supports per-CF entries via Put(cf, key, value);
    // mixing CFs in a single batch is fine.
    mutable std::mutex buffer_mu_;
    forstdb::WriteBatch write_batch_;
    // Default 1 MiB flush threshold; overridable via CLINK_FORST_WB_BYTES
    // for tuning. The flush amortises WriteThread + memtable arena cost
    // over many puts; very large batches block the operator thread for
    // too long during flush.
    static std::size_t wb_flush_bytes() {
        static const std::size_t v = [] {
            if (const char* e = std::getenv("CLINK_FORST_WB_BYTES")) {
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

    // The state-version stamps (schema evolution). Authoritative copy in
    // memory; persisted under kStateVersionsKey in the default CF so
    // every checkpoint carries it. Guarded by versions_mu (set by the
    // control plane, read by snapshot/export/restore paths).
    mutable std::mutex versions_mu;
    StateVersionMap versions;

    // Read the persisted stamps from the default CF (empty when absent).
    [[nodiscard]] StateVersionMap read_persisted_versions() const {
        if (default_cf_ == nullptr) {
            return {};
        }
        std::string out;
        auto st = db->Get(forstdb::ReadOptions{}, default_cf_, kStateVersionsKey, &out);
        if (!st.ok()) {
            return {};
        }
        return StateVersionMap::unpack(out);
    }

    [[nodiscard]] forstdb::ColumnFamilyHandle* cf_for(OperatorId op) {
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
        forstdb::ColumnFamilyHandle* handle = nullptr;
        const auto name = cf_name_for(op);
        auto st = db->CreateColumnFamily(make_cf_options(), name, &handle);
        if (!st.ok()) {
            throw std::runtime_error("ForStStateBackend::cf_for create failed: " + st.ToString());
        }
        cfs_.emplace(key, handle);
        return handle;
    }

    // Look up a CF by op-id without creating one. Read paths (get/scan)
    // use this so a query against an unknown operator returns empty
    // instead of materialising an empty CF.
    [[nodiscard]] forstdb::ColumnFamilyHandle* cf_lookup(OperatorId op) const {
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
            throw std::runtime_error("ForStStateBackend::flush_write_batch failed: " +
                                     st.ToString());
        }
        write_batch_.Clear();
    }

    // The one blocking read (the exact get() semantics: drain buffered
    // writes, read through the engine). Callable from the runner (sync
    // path, inline fallback) or an IO executor thread (deferred path):
    // the buffer flush is mutex-guarded and the engine's Get is safe
    // against concurrent runner-thread writes.
    [[nodiscard]] std::optional<StateBackend::Value> read_one(OperatorId op, std::string_view key) {
        auto* cf = cf_lookup(op);
        if (cf == nullptr) {
            return std::nullopt;
        }
        {
            std::lock_guard lock(buffer_mu_);
            flush_write_batch_locked();
        }
        const forstdb::Slice k_slice(key.data(), key.size());
        std::string out;
        auto st = db->Get(forstdb::ReadOptions{}, cf, k_slice, &out);
        if (st.IsNotFound()) {
            return std::nullopt;
        }
        if (!st.ok()) {
            throw std::runtime_error("ForStStateBackend::get failed: " + st.ToString());
        }
        StateBackend::Value bytes(out.size());
        if (!out.empty()) {
            std::memcpy(bytes.data(), out.data(), out.size());
        }
        return bytes;
    }

    [[nodiscard]] std::vector<std::optional<StateBackend::Value>> read_many(
        OperatorId op, const std::vector<std::string>& keys) {
        std::vector<std::optional<StateBackend::Value>> out;
        out.reserve(keys.size());
        for (const auto& k : keys) {
            out.push_back(read_one(op, k));
        }
        return out;
    }

    // Route a completed deferred read back to the issuing runner (called
    // on the IO thread). Prefers the deadline-aware scheduler when wired
    // so an order_key participates in priority resume.
    static void post_resume(const ResumeTarget& t,
                            std::coroutine_handle<> h,
                            std::uint64_t order_key) {
        if (t.deadline) {
            t.deadline(h, order_key);
        } else if (t.resume) {
            t.resume(h);
        }
    }

    // Awaiter for one deferred read: the IO thread runs the engine read
    // and posts the handle back to the issuing runner; await_resume (on
    // the runner) surfaces the value, rethrowing any engine error there
    // rather than letting it escape an executor worker.
    struct Load {
        Impl* self;
        OperatorId op;
        std::string key;
        std::uint64_t order_key{0};
        ResumeTarget target;
        std::optional<StateBackend::Value> value{};
        std::exception_ptr error{};

        [[nodiscard]] bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            self->io->submit_blocking([this, h]() {
                try {
                    value = self->read_one(op, key);
                } catch (...) {
                    error = std::current_exception();
                }
                post_resume(target, h, order_key);
            });
        }
        std::optional<StateBackend::Value> await_resume() {
            if (error) {
                std::rethrow_exception(error);
            }
            return std::move(value);
        }
    };

    // Batched twin: the whole batch runs as ONE executor job - a single
    // suspension for N keys. Keys are owned by the awaiter across the
    // suspension.
    struct LoadMany {
        Impl* self;
        OperatorId op;
        std::vector<std::string> keys;
        ResumeTarget target;
        std::vector<std::optional<StateBackend::Value>> values{};
        std::exception_ptr error{};

        [[nodiscard]] bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            self->io->submit_blocking([this, h]() {
                try {
                    values = self->read_many(op, keys);
                } catch (...) {
                    error = std::current_exception();
                }
                post_resume(target, h, 0);
            });
        }
        std::vector<std::optional<StateBackend::Value>> await_resume() {
            if (error) {
                std::rethrow_exception(error);
            }
            return std::move(values);
        }
    };
};

namespace {

// Open the DB at `path` with every existing ColumnFamily reattached.
// The engine requires every CF that exists on disk to be enumerated in
// the OpenColumnFamilies call - missing one is a hard error. So we list
// the CFs first, build a descriptor list, then open. The returned
// handles in `out_handles` line up positionally with the descriptors.
void open_db_with_all_cfs(const std::string& path,
                          bool create_if_missing,
                          forstdb::Env* env,
                          std::unique_ptr<forstdb::DB>& out_db,
                          std::vector<forstdb::ColumnFamilyHandle*>& out_handles,
                          std::vector<std::string>& out_cf_names) {
    forstdb::DBOptions db_opts;
    db_opts.create_if_missing = create_if_missing;
    db_opts.create_missing_column_families = true;
    if (env != nullptr) {
        db_opts.env = env;
    }
    db_opts.IncreaseParallelism(
        static_cast<int>(std::max<unsigned>(2u, std::thread::hardware_concurrency() / 2)));

    // Discover existing CFs. ListColumnFamilies fails on a fresh path
    // (no MANIFEST yet); treat that as "default only". The CF list lives
    // in the (local) MANIFEST, but the probe still carries the custom
    // env so a remote-data-file backend never consults the wrong FS.
    std::vector<std::string> cf_names;
    {
        forstdb::Options probe_opts;
        probe_opts.create_if_missing = create_if_missing;
        if (env != nullptr) {
            probe_opts.env = env;
        }
        auto st = forstdb::DB::ListColumnFamilies(probe_opts, path, &cf_names);
        if (!st.ok()) {
            cf_names = {forstdb::kDefaultColumnFamilyName};
        }
    }
    if (cf_names.empty()) {
        cf_names = {forstdb::kDefaultColumnFamilyName};
    }

    std::vector<forstdb::ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size());
    for (const auto& name : cf_names) {
        descriptors.emplace_back(name, make_cf_options());
    }

    std::unique_ptr<forstdb::DB> owned;
    forstdb::DB* raw = nullptr;
    auto st = forstdb::DB::Open(db_opts, path, descriptors, &out_handles, &raw);
    if (!st.ok()) {
        throw std::runtime_error("ForStStateBackend::open failed: " + st.ToString());
    }
    owned.reset(raw);
    out_db = std::move(owned);
    out_cf_names = std::move(cf_names);
}

// Open `path` READ-ONLY with every existing CF reattached. Used by the
// scale-down restore merge: each additional parent checkpoint is opened
// read-only and its rows copied into the live DB. Read-only avoids
// mutating or locking the parent checkpoint dir (it may be shared).
void open_readonly_with_all_cfs(const std::string& path,
                                forstdb::Env* env,
                                std::unique_ptr<forstdb::DB>& out_db,
                                std::vector<forstdb::ColumnFamilyHandle*>& out_handles,
                                std::vector<std::string>& out_cf_names) {
    std::vector<std::string> cf_names;
    {
        forstdb::Options probe_opts;
        if (env != nullptr) {
            probe_opts.env = env;
        }
        auto st = forstdb::DB::ListColumnFamilies(probe_opts, path, &cf_names);
        if (!st.ok()) {
            cf_names = {forstdb::kDefaultColumnFamilyName};
        }
    }
    if (cf_names.empty()) {
        cf_names = {forstdb::kDefaultColumnFamilyName};
    }
    std::vector<forstdb::ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size());
    for (const auto& name : cf_names) {
        descriptors.emplace_back(name, make_cf_options());
    }
    forstdb::DBOptions db_opts;
    if (env != nullptr) {
        db_opts.env = env;
    }
    forstdb::DB* raw = nullptr;
    auto st = forstdb::DB::OpenForReadOnly(db_opts, path, descriptors, &out_handles, &raw);
    if (!st.ok()) {
        throw std::runtime_error("ForStStateBackend::open(read-only) failed: " + st.ToString());
    }
    out_db.reset(raw);
    out_cf_names = std::move(cf_names);
}

}  // namespace

ForStStateBackend::ForStStateBackend(Options opts) : impl_(std::make_unique<Impl>()) {
    // The engine updates perf_context stats on every op behind a
    // thread-local check; disable for production-style state-backend
    // usage (same rationale as the bundled RocksDB backend).
    forstdb::SetPerfLevel(forstdb::PerfLevel::kDisable);

    // Live-remote-data-files wiring (see Options): the holder keeps the
    // caller's Env alive for the backend's whole life; every open below
    // routes through it.
    impl_->env_holder = std::move(opts.engine_env_holder);
    impl_->env = static_cast<forstdb::Env*>(opts.engine_env);
    impl_->mirror = std::move(opts.data_mirror);
    // Deferred reads (see Options): a sized IO pool for the engine reads.
    impl_->defer_reads = opts.defer_reads;
    if (impl_->defer_reads) {
        constexpr std::size_t kDefaultIoThreads = 8;
        impl_->io = std::make_shared<async::ThreadPoolCompletionExecutor>(
            opts.io_threads == 0 ? kDefaultIoThreads : opts.io_threads);
    }

    std::vector<forstdb::ColumnFamilyHandle*> handles;
    std::vector<std::string> names;
    open_db_with_all_cfs(opts.path, opts.create_if_missing, impl_->env, impl_->db, handles, names);

    // Wire up the CF map. The default CF is kept around as default_cf_
    // (we never write keyed state to it, but the engine requires it stay
    // open for the DB to be usable).
    for (std::size_t i = 0; i < handles.size(); ++i) {
        if (names[i] == forstdb::kDefaultColumnFamilyName) {
            impl_->default_cf_ = handles[i];
            continue;
        }
        if (auto op = op_from_cf_name(names[i])) {
            impl_->cfs_.emplace(op->value(), handles[i]);
        } else {
            // Unrecognised CF name - would only fire if the DB was
            // written by something other than this backend. The handle
            // is still destroyed with the others in the destructor path
            // via the engine's ownership.
            (void)handles[i];
        }
    }
    impl_->path = std::move(opts.path);
    impl_->store = opts.snapshot_store ? std::move(opts.snapshot_store)
                                       : std::make_shared<LocalSnapshotStore>();
    impl_->restore_base = std::move(opts.restore_base);
    // A restart over an existing working dir recovers the persisted stamps.
    impl_->versions = impl_->read_persisted_versions();
}

void ForStStateBackend::set_restore_base(const std::string& dir) {
    impl_->restore_base = dir;
}

ForStStateBackend::~ForStStateBackend() {
    // Join the IO workers FIRST: any in-flight deferred read references
    // the DB (the runner's drain has already quiesced the async paths;
    // this is the belt-and-braces ordering).
    impl_->io.reset();
    // Drain the WriteBatch before tearing down the DB.
    try {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    } catch (...) {
        // swallow - destructor must not throw
    }
    // Release CF handles before db. DestroyColumnFamilyHandle is the
    // engine-blessed way to drop a handle; the underlying CF stays in
    // the DB (no data loss). Default CF handle must be destroyed too.
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

void ForStStateBackend::put(OperatorId op, KeyView key, ValueView value) {
    auto* cf = impl_->cf_for(op);
    const forstdb::Slice k_slice(key.data(), key.size());
    const forstdb::Slice v_slice(value.data(), value.size());
    if (const char* env = std::getenv("CLINK_FORST_DIRECT_PUT"); env != nullptr && env[0] == '1') {
        auto st = impl_->db->Put(keyed_state_write_options(), cf, k_slice, v_slice);
        if (!st.ok()) {
            throw std::runtime_error("ForStStateBackend::put failed: " + st.ToString());
        }
        return;
    }
    std::lock_guard lock(impl_->buffer_mu_);
    auto st = impl_->write_batch_.Put(cf, k_slice, v_slice);
    if (!st.ok()) {
        throw std::runtime_error("ForStStateBackend::put failed: " + st.ToString());
    }
    if (impl_->write_batch_.GetDataSize() >= Impl::wb_flush_bytes()) {
        impl_->flush_write_batch_locked();
    }
}

std::optional<ForStStateBackend::Value> ForStStateBackend::get(OperatorId op, KeyView key) const {
    return impl_->read_one(op, key);
}

bool ForStStateBackend::supports_async_get() const noexcept {
    return impl_->defer_reads;
}

async::Task<std::optional<ForStStateBackend::Value>> ForStStateBackend::get_async(
    OperatorId op, KeyView key) const {
    return get_async(op, key, 0);
}

async::Task<std::optional<ForStStateBackend::Value>> ForStStateBackend::get_async(
    OperatorId op, KeyView key, std::uint64_t order_key) const {
    std::string owned(key);  // own the bytes across any suspension
    if (!impl_->defer_reads) {
        co_return impl_->read_one(op, owned);
    }
    const auto rt = impl_->sched_for_thread();
    if (!rt.resume && !rt.deadline) {
        // No runner to marshal resumes to: safe inline blocking read.
        co_return impl_->read_one(op, owned);
    }
    impl_->deferred_reads.fetch_add(1, std::memory_order_relaxed);
    co_return co_await Impl::Load{impl_.get(), op, std::move(owned), order_key, rt};
}

async::Task<std::vector<std::optional<ForStStateBackend::Value>>> ForStStateBackend::get_many_async(
    OperatorId op, const std::vector<std::string>& keys) const {
    if (!impl_->defer_reads) {
        co_return impl_->read_many(op, keys);
    }
    const auto rt = impl_->sched_for_thread();
    if (!rt.resume && !rt.deadline) {
        co_return impl_->read_many(op, keys);
    }
    impl_->deferred_reads.fetch_add(1, std::memory_order_relaxed);  // one batch, one deferral
    co_return co_await Impl::LoadMany{impl_.get(), op, keys, rt};
}

void ForStStateBackend::set_async_resume_scheduler(AsyncResumeScheduler schedule) {
    std::lock_guard lk(impl_->sched_mu);
    const auto tid = std::this_thread::get_id();
    if (!schedule) {
        auto it = impl_->schedulers.find(tid);
        if (it != impl_->schedulers.end()) {
            it->second.resume = nullptr;
            if (!it->second.deadline) {
                impl_->schedulers.erase(it);
            }
        }
        return;
    }
    impl_->schedulers[tid].resume = std::move(schedule);
}

void ForStStateBackend::set_deadline_resume_scheduler(DeadlineResumeScheduler schedule) {
    std::lock_guard lk(impl_->sched_mu);
    const auto tid = std::this_thread::get_id();
    if (!schedule) {
        auto it = impl_->schedulers.find(tid);
        if (it != impl_->schedulers.end()) {
            it->second.deadline = nullptr;
            if (!it->second.resume) {
                impl_->schedulers.erase(it);
            }
        }
        return;
    }
    impl_->schedulers[tid].deadline = std::move(schedule);
}

std::uint64_t ForStStateBackend::deferred_reads() const noexcept {
    return impl_->deferred_reads.load(std::memory_order_relaxed);
}

void ForStStateBackend::erase(OperatorId op, KeyView key) {
    auto* cf = impl_->cf_for(op);
    const forstdb::Slice k_slice(key.data(), key.size());
    std::lock_guard lock(impl_->buffer_mu_);
    auto st = impl_->write_batch_.Delete(cf, k_slice);
    if (!st.ok()) {
        throw std::runtime_error("ForStStateBackend::erase failed: " + st.ToString());
    }
    if (impl_->write_batch_.GetDataSize() >= Impl::wb_flush_bytes()) {
        impl_->flush_write_batch_locked();
    }
}

void ForStStateBackend::scan(OperatorId op, const ScanVisitor& visit) const {
    auto* cf = impl_->cf_lookup(op);
    if (cf == nullptr) {
        return;  // unknown operator => empty range
    }
    // Drain any buffered writes so the iterator sees the post-write view
    // of state.
    {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    }
    forstdb::ReadOptions ro;
    std::unique_ptr<forstdb::Iterator> it(impl_->db->NewIterator(ro, cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        auto key_slice = it->key();
        const std::string_view user_key(key_slice.data(), key_slice.size());
        const auto value_slice = it->value();
        const std::string_view value(value_slice.data(), value_slice.size());
        visit(user_key, value);
    }
}

bool ForStStateBackend::supports_async_persist() const noexcept {
    // Split snapshot into capture (local, op-thread) + persist (publish,
    // off-thread) only when the store's publish is a slow durable write.
    return impl_->store && impl_->store->defers_durable_write();
}

void ForStStateBackend::set_state_versions(StateVersionMap versions) {
    std::lock_guard lock(impl_->versions_mu);
    impl_->versions = std::move(versions);
    if (impl_->default_cf_ == nullptr) {
        return;
    }
    // Write through immediately so the NEXT checkpoint carries the stamps
    // (capture flushes memtables, so the key lands in the checkpoint's
    // SSTs). An empty map still writes (its packed form), so clearing
    // stamps propagates too.
    auto st = impl_->db->Put(
        keyed_state_write_options(), impl_->default_cf_, kStateVersionsKey, impl_->versions.pack());
    if (!st.ok()) {
        throw std::runtime_error("ForStStateBackend::set_state_versions failed: " + st.ToString());
    }
}

StateVersionMap ForStStateBackend::restored_state_versions() const {
    std::lock_guard lock(impl_->versions_mu);
    return impl_->versions;
}

CaptureHandle ForStStateBackend::capture(CheckpointId id) {
    // Operator-thread phase: the cheap local checkpoint. Flush buffered
    // writes so the checkpoint captures them, then CreateCheckpoint
    // hard-links the immutable SSTs into a DETACHED dir - ongoing put/get
    // on the live DB do not change it, so persist() can publish it
    // off-thread.
    {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    }
    forstdb::Checkpoint* cp_raw = nullptr;
    auto st = forstdb::Checkpoint::Create(impl_->db.get(), &cp_raw);
    std::unique_ptr<forstdb::Checkpoint> cp(cp_raw);
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
    if (impl_->mirror) {
        // Remote data files: the local cp dir holds only metadata, so the
        // SST list for stats comes from the mirror's side of the mapping.
        auto remote = impl_->mirror->list_dir(snap_path);
        stats.sst_files.insert(stats.sst_files.end(), remote.begin(), remote.end());
        std::sort(stats.sst_files.begin(), stats.sst_files.end());
    }
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

Snapshot ForStStateBackend::persist(CaptureHandle handle) {
    // Worker-thread phase: publish the captured local cp-dir through the
    // store (a remote upload for an S3 store) and embed the returned
    // handle. LocalSnapshotStore returns the path unchanged. Touches only
    // the captured dir + the store, never the live DB, so it is safe off
    // the operator thread.
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

Snapshot ForStStateBackend::snapshot(CheckpointId id) {
    // Synchronous path = capture (local) then persist (publish), inline.
    // The runner uses capture()/persist() separately (off-thread persist)
    // when supports_async_persist() is true; the result is identical.
    return persist(capture(id));
}

std::optional<ForStStateBackend::SnapshotStats> ForStStateBackend::last_snapshot_stats() const {
    std::lock_guard lock(impl_->snap_mu);
    return impl_->last_stats;
}

void ForStStateBackend::purge_checkpoint(CheckpointId id) {
    const auto snap_path = impl_->path + ".cp-" + std::to_string(id.value());
    // Drop the published checkpoint through the store. LocalSnapshotStore
    // remove_all's the dir; a remote store deletes its objects for this id.
    impl_->store->delete_checkpoint(snap_path, id);
    if (impl_->mirror) {
        // Remote data files: the store only removed the local metadata
        // dir; drop the checkpoint's remote data files too.
        impl_->mirror->delete_dir(snap_path);
    }
}

void ForStStateBackend::restore(const Snapshot& snap, const KeyGroupRange& kg_filter) {
    // snap.bytes carries one or more newline-separated checkpoint dir
    // paths. The first is re-homed onto (fast hard-link); any additional
    // ones (scale-down: several parent subtasks merged into this one) are
    // iterate-merged below. A single path is the same-parallelism /
    // scale-up case.
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
    // Relocatable savepoints: when a restore base is configured, a cp-dir
    // reference's embedded directory is non-load-bearing - only its
    // basename matters, resolved under the (possibly relocated) base.
    // Empty base = use the path verbatim (same-location restart /
    // object-store handles).
    if (!impl_->restore_base.empty()) {
        for (auto& p : source_paths) {
            p = (std::filesystem::path(impl_->restore_base) / std::filesystem::path(p).filename())
                    .string();
        }
    }
    // Fetch the first handle to a local path the DB can open (identity for
    // a local store; a download for a remote one), then re-home onto it.
    const std::string source_path = impl_->store->fetch_checkpoint_dir(source_paths.front());

    // Tear down the live DB before re-opening on the restored dir. Drop
    // CF handles first - they're owned by the DB and become invalid the
    // moment db goes away.
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
    if (impl_->mirror) {
        // Remote data files: the local loop above re-homed only the
        // metadata files. Replicate the checkpoint's remote data files to
        // the working dir's side of the mapping BEFORE opening - the
        // MANIFEST references them by name and the engine resolves them
        // through the filesystem the moment the DB opens.
        impl_->mirror->copy_dir(source_path, working_path);
    }

    std::vector<forstdb::ColumnFamilyHandle*> handles;
    std::vector<std::string> names;
    open_db_with_all_cfs(
        working_path, /*create_if_missing=*/false, impl_->env, impl_->db, handles, names);
    for (std::size_t i = 0; i < handles.size(); ++i) {
        if (names[i] == forstdb::kDefaultColumnFamilyName) {
            impl_->default_cf_ = handles[i];
            continue;
        }
        if (auto op = op_from_cf_name(names[i])) {
            impl_->cfs_.emplace(op->value(), handles[i]);
        }
    }
    impl_->path = working_path;
    {
        // Recover the version stamps the restored checkpoint carries (the
        // first/primary parent; scale-down parents share one job-level map).
        std::lock_guard vlock(impl_->versions_mu);
        impl_->versions = impl_->read_persisted_versions();
    }

    // Scale-down: merge any additional parent checkpoints into the live
    // DB. Each is opened read-only and its rows copied in; the kg-filter
    // below narrows the merged keyed rows to this subtask's range (the
    // union of the parents' ranges) and keeps operator-state rows. All
    // subtasks of a job share the same operators, so cf_for() finds or
    // creates the matching CF.
    for (std::size_t p = 1; p < source_paths.size(); ++p) {
        std::unique_ptr<forstdb::DB> parent_db;
        std::vector<forstdb::ColumnFamilyHandle*> parent_handles;
        std::vector<std::string> parent_names;
        const std::string parent_path = impl_->store->fetch_checkpoint_dir(source_paths[p]);
        // Read-only: the parent's remote data files are read in place
        // through the env's mapping (no copy needed for a merge source).
        open_readonly_with_all_cfs(
            parent_path, impl_->env, parent_db, parent_handles, parent_names);
        for (std::size_t i = 0; i < parent_handles.size(); ++i) {
            if (parent_names[i] == forstdb::kDefaultColumnFamilyName) {
                continue;
            }
            const auto op = op_from_cf_name(parent_names[i]);
            if (!op) {
                continue;
            }
            auto* live_cf = impl_->cf_for(*op);
            std::unique_ptr<forstdb::Iterator> it(
                parent_db->NewIterator(forstdb::ReadOptions{}, parent_handles[i]));
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                auto st =
                    impl_->db->Put(keyed_state_write_options(), live_cf, it->key(), it->value());
                if (!st.ok()) {
                    throw std::runtime_error("ForStStateBackend::restore merge put failed: " +
                                             st.ToString());
                }
            }
        }
        for (auto* h : parent_handles) {
            (void)parent_db->DestroyColumnFamilyHandle(h);
        }
        // parent_db destructor closes the read-only handle.
    }

    // Apply the key-group filter by deleting entries outside the covered
    // range. With CF-per-OperatorId there's no op-id prefix: the kg byte
    // sits at offset 0 of the raw user key (see KeyedState::encode_key -
    // kg | slot | '|' | user_key). Walk each CF independently.
    if (!kg_filter.covers_all()) {
        std::vector<std::pair<forstdb::ColumnFamilyHandle*, std::string>> to_delete;
        std::shared_lock lk(impl_->cf_mu_);
        for (const auto& [_, cf] : impl_->cfs_) {
            forstdb::ReadOptions ro;
            std::unique_ptr<forstdb::Iterator> it(impl_->db->NewIterator(ro, cf));
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
                // rescale. Without this, a narrowed (rescale) restore would
                // silently drop offsets and break exactly-once.
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
            (void)impl_->db->Delete(forstdb::WriteOptions{}, cf, k);
        }
    }
}

Snapshot ForStStateBackend::combine_snapshots(std::vector<Snapshot> parts) const {
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

std::string ForStStateBackend::description() const {
    return "forst state backend at " + impl_->path;
}

namespace {

// Shared iterate-and-encode core for the live and checkpoint-dir Arrow
// exports. Walks the op_* column families in ascending op-id order
// (deterministic output) and appends every row to the canonical writer.
// `read_opts` lets the live path pin an engine snapshot for the walk.
std::vector<std::byte> export_cfs_to_arrow(
    forstdb::DB& db,
    const std::vector<std::pair<std::uint64_t, forstdb::ColumnFamilyHandle*>>& cfs,
    const forstdb::ReadOptions& read_opts,
    const StateVersionMap& versions) {
    auto sorted = cfs;
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    SnapshotArrowWriter writer;
    for (const auto& [op_id, cf] : sorted) {
        std::unique_ptr<forstdb::Iterator> it(db.NewIterator(read_opts, cf));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const auto k = it->key();
            const auto v = it->value();
            writer.append(
                op_id, std::string_view(k.data(), k.size()), std::string_view(v.data(), v.size()));
        }
    }
    return writer.finish(versions);
}

}  // namespace

std::vector<std::byte> ForStStateBackend::export_arrow_snapshot() const {
    // Drain buffered writes so the export sees the post-write view, then
    // pin an engine snapshot so the walk is a consistent point in time
    // even while the operator keeps writing.
    {
        std::lock_guard lock(impl_->buffer_mu_);
        impl_->flush_write_batch_locked();
    }
    const forstdb::Snapshot* snap = impl_->db->GetSnapshot();
    forstdb::ReadOptions ro;
    ro.snapshot = snap;
    std::vector<std::pair<std::uint64_t, forstdb::ColumnFamilyHandle*>> cfs;
    {
        std::shared_lock lk(impl_->cf_mu_);
        cfs.reserve(impl_->cfs_.size());
        for (const auto& [op_id, cf] : impl_->cfs_) {
            cfs.emplace_back(op_id, cf);
        }
    }
    StateVersionMap versions;
    {
        std::lock_guard lock(impl_->versions_mu);
        versions = impl_->versions;
    }
    std::vector<std::byte> bytes;
    try {
        bytes = export_cfs_to_arrow(*impl_->db, cfs, ro, versions);
    } catch (...) {
        impl_->db->ReleaseSnapshot(snap);
        throw;
    }
    impl_->db->ReleaseSnapshot(snap);
    return bytes;
}

std::vector<std::byte> forst_checkpoint_to_arrow(const std::string& checkpoint_dir) {
    std::unique_ptr<forstdb::DB> db;
    std::vector<forstdb::ColumnFamilyHandle*> handles;
    std::vector<std::string> names;
    // Default filesystem: this offline helper reads all-local checkpoint
    // dirs. A checkpoint whose data files live remotely (a DataFileMirror
    // backend) is out of its reach - export that state through the live
    // backend's export_arrow_snapshot() instead.
    open_readonly_with_all_cfs(checkpoint_dir, /*env=*/nullptr, db, handles, names);

    std::vector<std::pair<std::uint64_t, forstdb::ColumnFamilyHandle*>> cfs;
    cfs.reserve(handles.size());
    StateVersionMap versions;
    for (std::size_t i = 0; i < handles.size(); ++i) {
        if (auto op = op_from_cf_name(names[i])) {
            cfs.emplace_back(op->value(), handles[i]);
        } else if (names[i] == forstdb::kDefaultColumnFamilyName) {
            // The checkpoint's persisted version stamps ride the default CF.
            std::string out;
            if (db->Get(forstdb::ReadOptions{}, handles[i], kStateVersionsKey, &out).ok()) {
                versions = StateVersionMap::unpack(out);
            }
        }
    }
    std::vector<std::byte> bytes;
    try {
        bytes = export_cfs_to_arrow(*db, cfs, forstdb::ReadOptions{}, versions);
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

}  // namespace clink
