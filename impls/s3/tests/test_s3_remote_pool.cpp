// S3RemotePool + RemoteReadBackend end-to-end against a live S3
// (MinIO / LocalStack), gated on CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET.
// Proves the production disaggregation binding: state committed incrementally
// to S3 by one backend instance is lazily restored by a FRESH instance (a
// simulated restart) - cold reads fetch per key from the pool, unchanged keys
// are inherited across checkpoints, and deletes propagate.

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
