// S3RemotePool + RemoteReadBackend end-to-end against a live S3
// (MinIO / LocalStack), gated on CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET.
// Proves the production disaggregation binding: state committed incrementally
// to S3 by one backend instance is lazily restored by a FRESH instance (a
// simulated restart) - cold reads fetch per key from the pool, unchanged keys
// are inherited across checkpoints, and deletes propagate.

#include <chrono>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/filesystem/s3fs.h>
#include <gtest/gtest.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // ensure_arrow_s3_initialised
#include "clink/core/types.hpp"
#include "clink/s3/install.hpp"
#include "clink/s3/s3_remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

using clink::CheckpointId;
using clink::OperatorId;
using clink::RemoteReadBackend;
using clink::Snapshot;
using clink::StateBackend;
using clink::s3::S3RemotePool;

namespace {

bool s3_available() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

S3RemotePool::Options pool_opts(const std::string& prefix) {
    S3RemotePool::Options o;
    o.bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    o.prefix = prefix;
    o.endpoint_override = std::string{std::getenv("CLINK_S3_TEST_ENDPOINT")};
    o.region = "us-east-1";
    return o;
}

StateBackend::ValueView vv(const char* s) {
    return StateBackend::ValueView{s};
}

std::string str(const std::optional<StateBackend::Value>& v) {
    return v ? std::string(reinterpret_cast<const char*>(v->data()), v->size()) : std::string{};
}

// Unique per-process prefix so reruns / parallel runs never collide.
std::string unique_prefix() {
    return "clink-remote-pool-it/" + std::to_string(::getpid());
}

}  // namespace

TEST(S3RemotePool, RemoteReadBackendIncrementalCommitAndLazyRestoreOverS3) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string prefix = unique_prefix();
    const OperatorId op{7};

    // Writer instance: two incremental checkpoints to S3, then gone.
    {
        auto pool = std::make_shared<S3RemotePool>(pool_opts(prefix));
        RemoteReadBackend b(pool);
        b.put(op, vv("a"), vv("v1"));
        b.put(op, vv("b"), vv("v2"));
        b.put(op, vv("d"), vv("v4"));
        b.snapshot(CheckpointId{1});
        // cp2 delta: update a, delete b, add c, leave d untouched (inherited).
        b.put(op, vv("a"), vv("v1b"));
        b.erase(op, vv("b"));
        b.put(op, vv("c"), vv("v3"));
        b.snapshot(CheckpointId{2});
    }

    // Fresh pool + backend (simulated restart): nothing local, all in S3.
    auto pool2 = std::make_shared<S3RemotePool>(pool_opts(prefix));
    RemoteReadBackend b2(pool2);
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{2};
    b2.restore(snap);
    EXPECT_EQ(b2.remote_loads(), 0u);  // lazy: restore fetched nothing

    EXPECT_EQ(str(b2.get(op, vv("a"))), "v1b");     // updated in cp2
    EXPECT_EQ(str(b2.get(op, vv("c"))), "v3");      // added in cp2
    EXPECT_EQ(str(b2.get(op, vv("d"))), "v4");      // inherited from cp1 across checkpoints
    EXPECT_FALSE(b2.get(op, vv("b")).has_value());  // deleted in cp2
    EXPECT_GT(b2.remote_loads(), 0u);               // cold reads were served from S3

    // Best-effort cleanup of this run's checkpoints.
    pool2->purge(CheckpointId{1});
    pool2->purge(CheckpointId{2});
}

// ASYNC-10: read_many over a content-addressed pool coalesces keys that share a
// content hash into ONE object GET. k1 and k2 hold identical bytes (same hash),
// k3 differs, absent is missing -> 3 present keys, but only 2 object GETs.
TEST(S3RemotePool, ReadManyCoalescesSameContentHashIntoOneGet) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string prefix = unique_prefix() + "/coalesce";
    const OperatorId op{9};
    auto pool = std::make_shared<S3RemotePool>(pool_opts(prefix));

    auto val = [](const char* s) {
        return StateBackend::Value{
            reinterpret_cast<const std::byte*>(s),
            reinterpret_cast<const std::byte*>(s + std::char_traits<char>::length(s))};
    };
    std::vector<clink::RemotePoolEntry> changed{
        {op, "k1", val("same")}, {op, "k2", val("same")}, {op, "k3", val("other")}};
    pool->commit(CheckpointId{1}, CheckpointId{0}, changed, {});

    const std::uint64_t before = pool->object_gets();
    auto out = pool->read_many(CheckpointId{1}, op, {"k1", "k2", "k3", "absent"});
    const std::uint64_t gets = pool->object_gets() - before;

    ASSERT_EQ(out.size(), 4u);
    EXPECT_EQ(str(out[0]), "same");
    EXPECT_EQ(str(out[1]), "same");
    EXPECT_EQ(str(out[2]), "other");
    EXPECT_FALSE(out[3].has_value());
    // k1+k2 share a hash -> one GET; k3 -> one GET; absent -> none. 2, not 3.
    EXPECT_EQ(gets, 2u);

    pool->purge(CheckpointId{1});
}

// 2c-1: bounded hot tier over real S3. A small byte budget forces eviction of
// clean (committed) keys, and a later read transparently re-fetches them from
// S3 - working state genuinely exceeds the hot budget yet every key is correct.
TEST(S3RemotePool, BoundedHotTierEvictsAndRefetchesFromS3) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string prefix = unique_prefix() + "-evict";
    const OperatorId op{3};
    auto pool = std::make_shared<S3RemotePool>(pool_opts(prefix));
    // "k0".."k9"/"v0".."v9" are 4 bytes each; a 12-byte budget holds ~3.
    RemoteReadBackend b(pool, /*io_threads=*/1, /*hot_max_bytes=*/12);
    for (int i = 0; i < 10; ++i) {
        const std::string k = "k" + std::to_string(i);
        const std::string v = "v" + std::to_string(i);
        b.put(op, StateBackend::ValueView{k}, StateBackend::ValueView{v});
    }
    b.snapshot(CheckpointId{1});  // commit to S3 -> clean -> evict to budget
    EXPECT_GT(b.hot_evictions(), 0u);
    EXPECT_LE(b.hot_resident_bytes(), 12u);
    EXPECT_LT(b.hot_resident_keys(), 10u);

    const auto loads_before = b.remote_loads();
    for (int i = 0; i < 10; ++i) {
        const std::string k = "k" + std::to_string(i);
        EXPECT_EQ(str(b.get(op, StateBackend::ValueView{k})), "v" + std::to_string(i));
    }
    EXPECT_GT(b.remote_loads(), loads_before);  // evicted keys re-fetched from S3
    EXPECT_LE(b.hot_resident_bytes(), 12u);

    pool->purge(CheckpointId{1});
}

// 2c-2: object-GC sweep reclaims value objects orphaned by a purged checkpoint
// while objects still referenced by a live manifest survive.
TEST(S3RemotePool, SweepReclaimsOrphanedObjectsKeepsLiveOnes) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string prefix = unique_prefix() + "-sweep";
    const OperatorId op{5};
    auto pool = std::make_shared<S3RemotePool>(pool_opts(prefix));
    {
        RemoteReadBackend b(pool);
        b.put(op, vv("a"), vv("v1"));  // object hash(v1)
        b.put(op, vv("b"), vv("v2"));  // object hash(v2)
        b.snapshot(CheckpointId{1});
        b.put(op, vv("a"), vv("v1b"));  // object hash(v1b); v1 now only in cp1
        b.snapshot(CheckpointId{2});    // cp2 references v1b + v2 (inherited)
    }
    // Drop cp1: hash(v1) is now referenced by no live manifest (orphan).
    pool->purge(CheckpointId{1});
    const std::uint64_t reclaimed = pool->sweep(std::chrono::seconds{0});
    EXPECT_EQ(reclaimed, 1u);  // exactly the orphaned v1 object
    EXPECT_EQ(pool->objects_reclaimed(), 1u);

    // cp2 is intact: its referenced objects (v1b, v2) survived the sweep.
    RemoteReadBackend b2(pool);
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{2};
    b2.restore(snap);
    EXPECT_EQ(str(b2.get(op, vv("a"))), "v1b");
    EXPECT_EQ(str(b2.get(op, vv("b"))), "v2");

    // A second sweep with cp2 still live reclaims nothing more.
    EXPECT_EQ(pool->sweep(std::chrono::seconds{0}), 0u);

    pool->purge(CheckpointId{2});
}

namespace {
// A key-group-prefixed state key: the leading byte IS the key group (mirrors
// KeyedState::encode_key, which a backend reads at key[0] to filter on rescale).
std::string kkey(int kg, const std::string& suffix) {
    std::string k;
    k.push_back(static_cast<char>(kg));
    k += suffix;
    return k;
}
S3RemotePool::Options sub_opts(const std::string& job, std::uint32_t subtask) {
    return pool_opts(job + "/" + std::to_string(subtask));
}
}  // namespace

// 2d rescale SCALE-UP (1 -> 2): each new subtask inherits the single parent's
// checkpoint narrowed to its assigned key-group range, and - the core trap -
// the inherited state SURVIVES the new subtask's first incremental checkpoint.
TEST(S3RemotePool, RescaleScaleUpKeyGroupFiltersAndSurvivesCheckpoint) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string job = unique_prefix() + "-up";
    const OperatorId op{8};
    const std::string kLo = kkey(5, "a");     // kg 5  -> new subtask 0 [0,64)
    const std::string kZero = kkey(0, "d");   // kg 0  -> new subtask 0
    const std::string kHi = kkey(66, "b");    // kg 66 -> new subtask 1 [64,128)
    const std::string kTop = kkey(127, "c");  // kg 127-> new subtask 1
    const std::string kNew = kkey(100, "e");  // kg 100-> new subtask 1 (added post-rescale)

    // Old parallelism 1: subtask 0 holds the whole key space, checkpoint 100.
    {
        auto p0 = std::make_shared<S3RemotePool>(sub_opts(job, 0));
        RemoteReadBackend b(p0);
        b.put(op, std::string_view{kLo}, vv("va"));
        b.put(op, std::string_view{kZero}, vv("vd"));
        b.put(op, std::string_view{kHi}, vv("vb"));
        b.put(op, std::string_view{kTop}, vv("vc"));
        b.snapshot(CheckpointId{100});
    }

    // New subtask 1 owns key-groups [64,128): inherits parent 0 filtered.
    auto p1 = std::make_shared<S3RemotePool>(sub_opts(job, 1));
    p1->set_restore_sources({job + "/0"});
    RemoteReadBackend b1(p1);
    Snapshot s;
    s.checkpoint_id = CheckpointId{100};
    b1.restore(s, clink::KeyGroupRange{64, 128});
    EXPECT_EQ(str(b1.get(op, std::string_view{kHi})), "vb");      // kg 66 in range
    EXPECT_EQ(str(b1.get(op, std::string_view{kTop})), "vc");     // kg 127 in range
    EXPECT_FALSE(b1.get(op, std::string_view{kLo}).has_value());  // kg 5 filtered out
    EXPECT_FALSE(b1.get(op, std::string_view{kZero}).has_value());

    // Failover BEFORE the first post-rescale checkpoint: a plain self-restore
    // (no rescale sources) of cp-100 must recover the filtered state from the
    // durable sidecar - not lose it (the cache is gone on a fresh process).
    {
        auto p1f = std::make_shared<S3RemotePool>(sub_opts(job, 1));
        RemoteReadBackend b1f(p1f);
        Snapshot sf;
        sf.checkpoint_id = CheckpointId{100};
        b1f.restore(sf);  // no sources -> reads job/1's rescaled-cp-100 sidecar
        EXPECT_EQ(str(b1f.get(op, std::string_view{kHi})), "vb");
        EXPECT_EQ(str(b1f.get(op, std::string_view{kTop})), "vc");
        EXPECT_FALSE(b1f.get(op, std::string_view{kLo}).has_value());
    }

    // The parent's full cp-100 was NOT clobbered by the rescale (sidecar only),
    // so a sibling reading parent 0 still sees the whole key space.
    {
        auto p0parent = std::make_shared<S3RemotePool>(sub_opts(job, 0));
        RemoteReadBackend bp(p0parent);
        Snapshot sp;
        sp.checkpoint_id = CheckpointId{100};
        bp.restore(sp);                                           // plain read of parent 0's cp-100
        EXPECT_EQ(str(bp.get(op, std::string_view{kLo})), "va");  // kg 5 still there
        EXPECT_EQ(str(bp.get(op, std::string_view{kHi})), "vb");  // kg 66 still there
    }

    // Incremental-base survival: add a new key, checkpoint, then a FRESH backend
    // restores the new checkpoint and must still see the inherited keys (the
    // merged base was carried into cp-101, not dropped).
    b1.put(op, std::string_view{kNew}, vv("vnew"));
    b1.snapshot(CheckpointId{101});
    auto p1b = std::make_shared<S3RemotePool>(sub_opts(job, 1));
    RemoteReadBackend b1b(p1b);
    Snapshot s101;
    s101.checkpoint_id = CheckpointId{101};
    b1b.restore(s101);
    EXPECT_EQ(str(b1b.get(op, std::string_view{kHi})), "vb");      // inherited, survived
    EXPECT_EQ(str(b1b.get(op, std::string_view{kTop})), "vc");     // inherited, survived
    EXPECT_EQ(str(b1b.get(op, std::string_view{kNew})), "vnew");   // added post-rescale
    EXPECT_FALSE(b1b.get(op, std::string_view{kLo}).has_value());  // never owned

    // New subtask 0 owns [0,64): self-merge of parent 0 (its own prefix),
    // filtered. The parent's full cp-100 is never overwritten by the filter.
    auto p0n = std::make_shared<S3RemotePool>(sub_opts(job, 0));
    p0n->set_restore_sources({job + "/0"});
    RemoteReadBackend b0n(p0n);
    Snapshot s100;
    s100.checkpoint_id = CheckpointId{100};
    b0n.restore(s100, clink::KeyGroupRange{0, 64});
    EXPECT_EQ(str(b0n.get(op, std::string_view{kLo})), "va");      // kg 5 in [0,64)
    EXPECT_EQ(str(b0n.get(op, std::string_view{kZero})), "vd");    // kg 0 in [0,64)
    EXPECT_FALSE(b0n.get(op, std::string_view{kHi}).has_value());  // kg 66 filtered out

    p1->purge(CheckpointId{101});
    p1->purge(CheckpointId{100});
}

// 2d rescale SCALE-DOWN (2 -> 1): the new subtask MERGES two parents' state and
// the merge survives its first incremental checkpoint.
TEST(S3RemotePool, RescaleScaleDownMergesParentsAndSurvivesCheckpoint) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string job = unique_prefix() + "-down";
    const OperatorId op{9};
    const std::string k10 = kkey(10, "a"), k20 = kkey(20, "d");  // parent 0
    const std::string k70 = kkey(70, "b"), k90 = kkey(90, "c");  // parent 1
    const std::string kNew = kkey(50, "e");

    {
        auto p0 = std::make_shared<S3RemotePool>(sub_opts(job, 0));
        RemoteReadBackend b(p0);
        b.put(op, std::string_view{k10}, vv("v10"));
        b.put(op, std::string_view{k20}, vv("v20"));
        b.snapshot(CheckpointId{200});
    }
    {
        auto p1 = std::make_shared<S3RemotePool>(sub_opts(job, 1));
        RemoteReadBackend b(p1);
        b.put(op, std::string_view{k70}, vv("v70"));
        b.put(op, std::string_view{k90}, vv("v90"));
        b.snapshot(CheckpointId{200});
    }

    // New subtask 0 owns everything, inheriting parents 0 and 1.
    auto p = std::make_shared<S3RemotePool>(sub_opts(job, 0));
    p->set_restore_sources({job + "/0", job + "/1"});
    RemoteReadBackend b(p);
    Snapshot s;
    s.checkpoint_id = CheckpointId{200};
    b.restore(s, clink::KeyGroupRange{0, 128});
    EXPECT_EQ(str(b.get(op, std::string_view{k10})), "v10");
    EXPECT_EQ(str(b.get(op, std::string_view{k20})), "v20");
    EXPECT_EQ(str(b.get(op, std::string_view{k70})), "v70");  // merged from parent 1
    EXPECT_EQ(str(b.get(op, std::string_view{k90})), "v90");

    b.put(op, std::string_view{kNew}, vv("vnew"));
    b.snapshot(CheckpointId{201});
    auto pb = std::make_shared<S3RemotePool>(sub_opts(job, 0));
    RemoteReadBackend b2(pb);
    Snapshot s201;
    s201.checkpoint_id = CheckpointId{201};
    b2.restore(s201);
    EXPECT_EQ(str(b2.get(op, std::string_view{k10})), "v10");  // parent 0, survived
    EXPECT_EQ(str(b2.get(op, std::string_view{k90})), "v90");  // parent 1, survived
    EXPECT_EQ(str(b2.get(op, std::string_view{kNew})), "vnew");

    p->purge(CheckpointId{201});
    p->purge(CheckpointId{200});
}

// 2d cross-location relocate: export a checkpoint to a different base, then a
// fresh pool pointed there restores it as an ordinary same-location restore.
TEST(S3RemotePool, ExportCheckpointRelocatesToNewBase) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string base_a = unique_prefix() + "-loc-a";
    const std::string base_b = unique_prefix() + "-loc-b";
    const OperatorId op{11};
    const std::string ka = kkey(3, "a"), kb = kkey(40, "b");

    auto pa = std::make_shared<S3RemotePool>(sub_opts(base_a, 0));
    {
        RemoteReadBackend b(pa);
        b.put(op, std::string_view{ka}, vv("va"));
        b.put(op, std::string_view{kb}, vv("vb"));
        b.snapshot(CheckpointId{300});
    }
    // Relocate cp-300 (this subtask's manifest + objects) to base B.
    pa->export_checkpoint(CheckpointId{300}, base_b + "/0");

    // A fresh pool at base B restores it with no rescale plan (same-location).
    auto pb = std::make_shared<S3RemotePool>(sub_opts(base_b, 0));
    RemoteReadBackend b2(pb);
    Snapshot s;
    s.checkpoint_id = CheckpointId{300};
    b2.restore(s);
    EXPECT_EQ(str(b2.get(op, std::string_view{ka})), "va");
    EXPECT_EQ(str(b2.get(op, std::string_view{kb})), "vb");

    pa->purge(CheckpointId{300});
    pb->purge(CheckpointId{300});
}

// 2d cross-location of a RESCALED subtask: export must carry the subtask's
// EFFECTIVE slice (the rescale sidecar - it has no plain cp-<id>), so a job at
// the new base restores that slice. Closes the rescale->export->restore gap.
TEST(S3RemotePool, ExportOfRescaledSubtaskCarriesItsEffectiveSlice) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    const std::string job = unique_prefix() + "-rxp";
    const std::string dst = unique_prefix() + "-rxp-dst";
    const OperatorId op{12};
    const std::string kLo = kkey(5, "a"), kHi = kkey(66, "b");

    // Parent 0 holds the full key space at cp-400.
    {
        auto p0 = std::make_shared<S3RemotePool>(sub_opts(job, 0));
        RemoteReadBackend b(p0);
        b.put(op, std::string_view{kLo}, vv("va"));
        b.put(op, std::string_view{kHi}, vv("vb"));
        b.snapshot(CheckpointId{400});
    }
    // Rescale new subtask 1 onto [64,128): writes only the sidecar (no cp-400).
    auto p1 = std::make_shared<S3RemotePool>(sub_opts(job, 1));
    p1->set_restore_sources({job + "/0"});
    RemoteReadBackend b1(p1);
    Snapshot s;
    s.checkpoint_id = CheckpointId{400};
    b1.restore(s, clink::KeyGroupRange{64, 128});

    // Relocate subtask 1's effective slice to a new base.
    p1->export_checkpoint(CheckpointId{400}, dst + "/1");

    // A job at the new base restores it (plain same-location restore) and sees
    // subtask 1's slice - kg 66 present, kg 5 (subtask 0's) absent.
    auto pd = std::make_shared<S3RemotePool>(sub_opts(dst, 1));
    RemoteReadBackend bd(pd);
    Snapshot sd;
    sd.checkpoint_id = CheckpointId{400};
    bd.restore(sd);
    EXPECT_EQ(str(bd.get(op, std::string_view{kHi})), "vb");
    EXPECT_FALSE(bd.get(op, std::string_view{kLo}).has_value());

    p1->purge(CheckpointId{400});
    pd->purge(CheckpointId{400});
}

// Scheme resolution + backend build (no S3 I/O, so not gated): remote-read://
// registers and builds a RemoteReadBackend; a restore spec yields the marker
// Snapshot carrying the checkpoint id.
TEST(S3RemotePoolScheme, RegistersAndBuildsBackendFromUri) {
    clink::s3::install_state_backend();  // idempotent
    auto& f = clink::StateBackendFactory::default_instance();
    ASSERT_TRUE(f.has_scheme("remote-read"));

    clink::StateBackendSpec spec;
    spec.uri = "remote-read://my-bucket/job/p?endpoint=http://localhost:4566&region=us-east-1";
    spec.subtask_idx = 2;
    auto built = f.build(spec);
    ASSERT_NE(built.backend, nullptr);
    EXPECT_EQ(built.backend->description(), "remote-read");
    EXPECT_TRUE(built.backend->supports_async_get());
    EXPECT_FALSE(built.restore_from.has_value());  // no restore requested

    clink::StateBackendSpec rspec = spec;
    rspec.restore_uri = spec.uri;
    rspec.restore_checkpoint_id = 7;
    auto rbuilt = f.build(rspec);
    ASSERT_TRUE(rbuilt.restore_from.has_value());
    EXPECT_EQ(rbuilt.restore_from->checkpoint_id.value(), 7u);
}

// Construction-path symmetry: the hot_max_bytes URI query must reach the
// backend ctor, else it silently falls back to an unbounded hot tier (no
// eviction). Build via the factory from a URI carrying a tiny budget and prove
// eviction actually fires - a broken parse would leave hot_evictions()==0.
TEST(S3RemotePoolScheme, HotMaxBytesUriProducesEvictingBackend) {
    if (!s3_available()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack)";
    }
    clink::s3::install_state_backend();
    const std::string bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    const std::string endpoint = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const std::string prefix = unique_prefix() + "-uri";

    clink::StateBackendSpec spec;
    spec.uri = "remote-read://" + bucket + "/" + prefix + "?endpoint=" + endpoint +
               "&region=us-east-1&hot_max_bytes=12";
    spec.subtask_idx = 0;
    auto built = clink::StateBackendFactory::default_instance().build(spec);
    auto* rrb = dynamic_cast<RemoteReadBackend*>(built.backend.get());
    ASSERT_NE(rrb, nullptr);

    const OperatorId op{1};
    for (int i = 0; i < 10; ++i) {
        const std::string k = "k" + std::to_string(i);
        const std::string v = "v" + std::to_string(i);
        rrb->put(op, StateBackend::ValueView{k}, StateBackend::ValueView{v});
    }
    rrb->snapshot(CheckpointId{1});
    EXPECT_GT(rrb->hot_evictions(), 0u);  // the URI budget took effect
    EXPECT_LE(rrb->hot_resident_bytes(), 12u);

    rrb->purge_checkpoint(CheckpointId{1});
}
