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
