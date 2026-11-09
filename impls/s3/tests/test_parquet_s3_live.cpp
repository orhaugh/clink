// The S3 Parquet source and exactly-once sink against a live store, through
// their registered factories, as a job builds them. SKIPPED unless
// CLINK_S3_TEST_ENDPOINT and CLINK_S3_TEST_BUCKET name one (credentials through
// the AWS environment chain); scripts/s3-live.sh runs it in its parquet phase,
// against SILO, and fails when any case is skipped.
//
// The objects the source reads are parts the rolling sink wrote, one per
// barrier, so each is one batch. A restore case reads part of them with one
// source and checkpoints it, changes the objects or not, then restores a second
// source and reads to the end: a value read twice is allowed, one never read is
// not.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arrow/filesystem/s3fs.h>
#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/connectors/parquet_s3_sink.hpp"  // ensure_arrow_s3_initialised
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::RuntimeContext;
using clink::Sink;
using clink::Source;
using clink::StreamElement;

namespace {

bool configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

std::string endpoint() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT");
}

std::string bucket() {
    return std::getenv("CLINK_S3_TEST_BUCKET");
}

std::string prefix(const std::string& tag) {
    return "parquet-live-" + std::to_string(static_cast<long>(::getpid())) + "/" + tag;
}

// The test's own view of the store, to count, read and remove objects.
std::shared_ptr<arrow::fs::FileSystem> store() {
    clink::detail::ensure_arrow_s3_initialised();
    auto o = arrow::fs::S3Options::Defaults();
    o.endpoint_override = endpoint();
    o.scheme = "http";
    o.region = "us-east-1";
    return arrow::fs::S3FileSystem::Make(o).ValueOrDie();
}

// The .parquet objects under bucket/dir, sorted.
std::vector<std::string> objects(const std::string& dir) {
    arrow::fs::FileSelector sel;
    sel.base_dir = bucket() + "/" + dir;
    sel.recursive = true;
    sel.allow_not_found = true;
    std::vector<std::string> out;
    for (const auto& fi : store()->GetFileInfo(sel).ValueOrDie()) {
        if (fi.type() == arrow::fs::FileType::File && fi.path().ends_with(".parquet")) {
            out.push_back(fi.path());
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::map<std::string, std::string> params(const std::string& dir) {
    return {{"bucket", bucket()},
            {"prefix", dir},
            {"region", "us-east-1"},
            {"endpoint_override", endpoint()}};
}

std::shared_ptr<Sink<std::int64_t>> sink(const std::string& type,
                                         const std::string& dir,
                                         RuntimeContext& rctx) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(type, "int64");
    if (f == nullptr) {
        throw std::logic_error(type + " is not registered");
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = params(dir);
    ctx.parallelism = 1;
    auto s = std::static_pointer_cast<Sink<std::int64_t>>(f->build(ctx));
    s->set_id(rctx.operator_id());
    s->attach_runtime(&rctx);
    return s;
}

std::shared_ptr<Source<std::int64_t>> source(const std::string& dir) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "s3_parquet_int64_source", "int64");
    if (f == nullptr) {
        throw std::logic_error("s3_parquet_int64_source is not registered");
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = params(dir);
    ctx.parallelism = 1;
    return std::static_pointer_cast<Source<std::int64_t>>(f->build(ctx));
}

Batch<std::int64_t> values(std::initializer_list<std::int64_t> vs) {
    Batch<std::int64_t> b;
    for (const auto v : vs) {
        b.emplace(v);
    }
    return b;
}

// Three parts under dir, one per barrier of the rolling sink: {1, 2}, {3, 4}
// and {5, 6}.
void write_three_parts(const std::string& dir) {
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{21}, "rolling", &state, nullptr);
    auto s = sink("s3_parquet_int64_sink", dir, rctx);
    s->open();
    std::int64_t next = 1;
    for (std::uint64_t ckpt = 1; ckpt <= 3; ++ckpt) {
        s->on_data(values({next, next + 1}));
        next += 2;
        s->on_barrier(CheckpointBarrier{CheckpointId{ckpt}});
    }
    s->close();
}

// The values `calls` produce() calls emit, or with calls < 0 every value to
// the end; sorted.
std::vector<std::int64_t> read_values(Source<std::int64_t>& src, int calls) {
    std::vector<std::int64_t> out;
    Emitter<std::int64_t> em([&out](StreamElement<std::int64_t> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.push_back(r.value());
            }
        }
        return true;
    });
    for (int i = 0; calls < 0 || i < calls; ++i) {
        if (!src.produce(em)) {
            break;
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

#define REQUIRE_STORE()                                                                       \
    do {                                                                                      \
        if (!configured()) {                                                                  \
            GTEST_SKIP()                                                                      \
                << "set CLINK_S3_TEST_ENDPOINT and CLINK_S3_TEST_BUCKET (scripts/s3-live.sh " \
                   "does)";                                                                   \
        }                                                                                     \
    } while (0)

const OperatorId kSourceOp{31};

}  // namespace

// Over the same objects the restored source resumes after the batch its
// checkpoint covered.
TEST(ParquetS3Live, TheSourceResumesAfterItsCheckpointOverTheSameObjects) {
    REQUIRE_STORE();
    const auto dir = prefix("same");
    write_three_parts(dir);
    ASSERT_EQ(objects(dir).size(), 3U);
    InMemoryStateBackend backend;
    std::vector<std::int64_t> first;
    {
        auto src = source(dir);
        src->open();
        first = read_values(*src, 1);
        src->snapshot_offset(backend, kSourceOp, CheckpointId{1});
        src->close();
    }
    ASSERT_EQ(first.size(), 2U);
    auto src = source(dir);
    ASSERT_TRUE(src->restore_offset(backend, kSourceOp));
    src->open();
    auto rest = read_values(*src, -1);
    src->close();
    EXPECT_EQ(rest.size(), 4U);
    rest.insert(rest.end(), first.begin(), first.end());
    std::sort(rest.begin(), rest.end());
    EXPECT_EQ(rest, (std::vector<std::int64_t>{1, 2, 3, 4, 5, 6}));
}

// An object removed after the checkpoint moved the count: restored by the
// count alone, the source skipped one of the objects that remained. It now
// reads every remaining object again.
TEST(ParquetS3Live, ARestoreAfterAnObjectWasRemovedReadsEveryRemainingObject) {
    REQUIRE_STORE();
    const auto dir = prefix("removed");
    write_three_parts(dir);
    const auto before = objects(dir);
    ASSERT_EQ(before.size(), 3U);
    InMemoryStateBackend backend;
    std::vector<std::int64_t> first;
    {
        auto src = source(dir);
        src->open();
        first = read_values(*src, 1);
        src->snapshot_offset(backend, kSourceOp, CheckpointId{1});
        src->close();
    }
    ASSERT_TRUE(store()->DeleteFile(before.front()).ok());
    auto src = source(dir);
    ASSERT_TRUE(src->restore_offset(backend, kSourceOp));
    src->open();
    const auto rest = read_values(*src, -1);
    src->close();
    // The two objects that remain, whole.
    EXPECT_EQ(rest.size(), 4U);
    auto all = rest;
    all.insert(all.end(), first.begin(), first.end());
    std::sort(all.begin(), all.end());
    all.erase(std::unique(all.begin(), all.end()), all.end());
    EXPECT_EQ(all, (std::vector<std::int64_t>{1, 2, 3, 4, 5, 6}));
}

// The exactly-once sink stages each interval and publishes it at commit; a
// handle a crash left prepared is committed by the next open.
TEST(ParquetS3Live, TheExactlyOnceSinkPublishesAtCommitAndRecoversAPreparedHandle) {
    REQUIRE_STORE();
    const auto dir = prefix("two-phase");
    InMemoryStateBackend state;
    {
        RuntimeContext rctx(OperatorId{41}, "s3pq2pc", &state, nullptr);
        auto s = sink("s3_parquet_2pc_int64_sink", dir, rctx);
        s->open();
        s->on_data(values({7, 8}));
        s->on_barrier(CheckpointBarrier{CheckpointId{1}});
        EXPECT_EQ(objects(dir + "/staging").size(), 1U);
        EXPECT_EQ(objects(dir + "/committed").size(), 0U);
        s->on_commit(1);
        EXPECT_EQ(objects(dir + "/staging").size(), 0U);
        EXPECT_EQ(objects(dir + "/committed").size(), 1U);
        s->on_data(values({9}));
        s->on_barrier(CheckpointBarrier{CheckpointId{2}});
        // A crash: no commit, no close.
    }
    {
        RuntimeContext rctx(OperatorId{41}, "s3pq2pc", &state, nullptr);
        auto s = sink("s3_parquet_2pc_int64_sink", dir, rctx);
        s->open();
        s->close();
    }
    EXPECT_EQ(objects(dir + "/staging").size(), 0U);
    EXPECT_EQ(objects(dir + "/committed").size(), 2U);
    auto src = source(dir + "/committed");
    src->open();
    EXPECT_EQ(read_values(*src, -1), (std::vector<std::int64_t>{7, 8, 9}));
    src->close();
}

// A recovery that finds neither the staged nor the committed object fails and
// keeps the handle, rather than report the interval published.
TEST(ParquetS3Live, ARecoveryThatFindsNeitherObjectFailsAndKeepsTheHandle) {
    REQUIRE_STORE();
    const auto dir = prefix("gone");
    InMemoryStateBackend state;
    {
        RuntimeContext rctx(OperatorId{42}, "s3pq2pc", &state, nullptr);
        auto s = sink("s3_parquet_2pc_int64_sink", dir, rctx);
        s->open();
        s->on_data(values({11}));
        s->on_barrier(CheckpointBarrier{CheckpointId{1}});
    }
    const auto staged = objects(dir + "/staging");
    ASSERT_EQ(staged.size(), 1U);
    ASSERT_TRUE(store()->DeleteFile(staged.front()).ok());
    RuntimeContext rctx(OperatorId{42}, "s3pq2pc", &state, nullptr);
    auto s = sink("s3_parquet_2pc_int64_sink", dir, rctx);
    EXPECT_THROW(s->open(), std::runtime_error);
    EXPECT_EQ(objects(dir + "/committed").size(), 0U);
    EXPECT_TRUE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_1").has_value());
}
