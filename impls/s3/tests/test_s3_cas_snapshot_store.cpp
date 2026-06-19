// S3CasSnapshotStore (DISAGG-6): content-addressed manifest checkpoint store.
// Construction validation (no network), then opt-in MinIO/localstack round-trips
// (gated on CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET) proving: a checkpoint
// write+fetch reconstructs byte-identically; consecutive checkpoints sharing
// objects upload only the changed ones (dedup); a cached second fetch hits local
// disk with zero downloads; a re-write of the same id is idempotent; and an
// older manifest still restores (time-travel).

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <unistd.h>

#include <arrow/filesystem/s3fs.h>
#include <gtest/gtest.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/core/types.hpp"
#include "clink/s3/s3_cas_snapshot_store.hpp"

using clink::CheckpointId;
using clink::s3::S3CasSnapshotStore;

namespace {

std::filesystem::path make_local_dir(const std::string& tag,
                                     const std::map<std::string, std::string>& files) {
    static int n = 0;
    auto dir =
        std::filesystem::temp_directory_path() / ("clink_disagg6_" + tag + std::to_string(n++));
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);
    for (const auto& [name, content] : files) {
        std::ofstream out(dir / name, std::ios::binary | std::ios::trunc);
        out.write(content.data(), static_cast<std::streamsize>(content.size()));
    }
    return dir;
}

std::string read_file(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

int count_regular_files(const std::filesystem::path& dir) {
    int n = 0;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(dir, ec)) {
        if (e.is_regular_file()) {
            ++n;
        }
    }
    return n;
}

// Assert the staging dir reconstructs `files` exactly.
void expect_dir_matches(const std::string& staging,
                        const std::map<std::string, std::string>& files) {
    ASSERT_EQ(count_regular_files(staging), static_cast<int>(files.size()));
    for (const auto& [name, content] : files) {
        const auto p = std::filesystem::path{staging} / name;
        ASSERT_TRUE(std::filesystem::exists(p)) << "missing " << name;
        EXPECT_EQ(read_file(p), content) << "content mismatch for " << name;
    }
}

bool minio_configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

// A unique prefix so the object pool starts COLD each run - the upload-count
// assertions assume a fresh pool, and content-addressed objects persist in a
// re-used test bucket. pid makes it unique across process runs; the static
// counter makes it unique across repeats within one process (--gtest_repeat).
std::string unique_prefix(const std::string& tag) {
    static int n = 0;
    return "clink-test/" + tag + "-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(n++);
}

// Raw delete of one remote object - used to simulate the crash-after-manifest-
// delete state (manifest gone, its objects orphaned) that the sweep reclaims.
void delete_remote_object(const std::string& key) {
    clink::detail::ensure_arrow_s3_initialised();
    auto o = arrow::fs::S3Options::Defaults();
    o.endpoint_override = std::string{std::getenv("CLINK_S3_TEST_ENDPOINT")};
    o.scheme = "http";
    o.region = "us-east-1";
    auto fs = arrow::fs::S3FileSystem::Make(o).ValueOrDie();
    (void)fs->DeleteFile(key);
}

std::shared_ptr<arrow::fs::S3FileSystem> minio_fs() {
    clink::detail::ensure_arrow_s3_initialised();
    auto o = arrow::fs::S3Options::Defaults();
    o.endpoint_override = std::string{std::getenv("CLINK_S3_TEST_ENDPOINT")};
    o.scheme = "http";
    o.region = "us-east-1";
    return arrow::fs::S3FileSystem::Make(o).ValueOrDie();
}

// Physically relocate every object under <bucket>/<from> to <bucket>/<to>
// (server-side copy), simulating moving a savepoint bundle to a new location.
void relocate_s3_prefix(const std::string& bucket, const std::string& from, const std::string& to) {
    auto fs = minio_fs();
    const std::string from_base = bucket + "/" + from;
    const std::string to_base = bucket + "/" + to;
    arrow::fs::FileSelector sel;
    sel.base_dir = from_base;
    sel.recursive = true;
    auto infos = fs->GetFileInfo(sel).ValueOrDie();
    for (const auto& info : infos) {
        if (info.type() != arrow::fs::FileType::File) {
            continue;
        }
        const std::string& src = info.path();
        const std::string dst = to_base + src.substr(from_base.size());
        (void)fs->CopyFile(src, dst);
    }
}

S3CasSnapshotStore::Options minio_opts(const std::string& prefix, std::uint64_t cache_bytes = 0) {
    S3CasSnapshotStore::Options o;
    o.bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    o.prefix = prefix;
    o.endpoint_override = std::string{std::getenv("CLINK_S3_TEST_ENDPOINT")};
    o.region = "us-east-1";
    o.cache_bytes = cache_bytes;
    if (cache_bytes > 0) {
        o.cache_dir = make_local_dir("cas-cache", {});
    }
    return o;
}

}  // namespace

TEST(S3CasSnapshotStore, RejectsEmptyBucket) {
    S3CasSnapshotStore::Options opts;
    opts.bucket = "";
    EXPECT_THROW(S3CasSnapshotStore{std::move(opts)}, std::invalid_argument);
}

TEST(S3CasSnapshotStore, WriteFetchRoundTrip) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> files{
        {"CURRENT", "MANIFEST-000005\n"},
        {"MANIFEST-000005", std::string(1024, 'm')},
        {"000007.sst", std::string(4096, 'S')},
        {"000009.blob", std::string(2048, 'B')},
        {"OPTIONS-000008", "opts"},
    };
    const auto local = make_local_dir("rt-src", files);
    const std::string prefix = unique_prefix("cas-rt");

    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string handle = store.write_checkpoint_dir(local.string(), CheckpointId{77});
    EXPECT_NE(handle.find("cp-77.manifest"), std::string::npos);
    EXPECT_EQ(store.objects_uploaded(), 5u);  // cold pool: all five objects new

    // A fresh store instance (no shared state) fetches byte-identically.
    S3CasSnapshotStore reader(minio_opts(prefix));
    const std::string fetched = reader.fetch_checkpoint_dir(handle);
    expect_dir_matches(fetched, files);
}

TEST(S3CasSnapshotStore, DedupSkipsSharedObjects) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    // cp-1 and cp-2 share 000001.sst + 000002.sst (identical bytes => identical
    // hash); cp-2 adds 000003.sst and a changed CURRENT.
    const std::string a(4096, 'A'), b(4096, 'B'), c(4096, 'C');
    const std::map<std::string, std::string> cp1{
        {"000001.sst", a}, {"000002.sst", b}, {"CURRENT", "MANIFEST-1\n"}};
    const std::map<std::string, std::string> cp2{
        {"000001.sst", a}, {"000002.sst", b}, {"000003.sst", c}, {"CURRENT", "MANIFEST-2\n"}};
    const std::string prefix = unique_prefix("cas-dedup");

    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string h1 =
        store.write_checkpoint_dir(make_local_dir("d1", cp1).string(), CheckpointId{1});
    EXPECT_EQ(store.objects_uploaded(), 3u);  // a, b, CURRENT-1
    const std::string h2 =
        store.write_checkpoint_dir(make_local_dir("d2", cp2).string(), CheckpointId{2});
    // Only the 2 NEW objects (000003.sst + CURRENT-2) upload; a + b are skipped
    // via HEAD - dedup proven (O(changed objects)).
    EXPECT_EQ(store.objects_uploaded(), 5u);

    // Both checkpoints still reconstruct byte-identically.
    S3CasSnapshotStore reader(minio_opts(prefix));
    expect_dir_matches(reader.fetch_checkpoint_dir(h1), cp1);
    expect_dir_matches(reader.fetch_checkpoint_dir(h2), cp2);
}

TEST(S3CasSnapshotStore, IdempotentRewrite) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> files{{"000007.sst", std::string(4096, 'S')},
                                                   {"CURRENT", "x"}};
    const auto local = make_local_dir("idem", files);
    S3CasSnapshotStore store(minio_opts(unique_prefix("cas-idem")));
    const std::string h1 = store.write_checkpoint_dir(local.string(), CheckpointId{9});
    EXPECT_EQ(store.objects_uploaded(), 2u);
    // Re-writing the same deterministic dir uploads nothing new (objects exist)
    // and returns the same handle.
    const std::string h2 = store.write_checkpoint_dir(local.string(), CheckpointId{9});
    EXPECT_EQ(store.objects_uploaded(), 2u);
    EXPECT_EQ(h1, h2);
}

TEST(S3CasSnapshotStore, CachedSecondFetchHitsLocalDisk) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> files{
        {"000007.sst", std::string(4096, 'S')},
        {"000009.blob", std::string(2048, 'B')},
        {"CURRENT", "x"},
    };
    const auto local = make_local_dir("cache-src", files);
    const std::string prefix = unique_prefix("cas-cache");
    // Writer (no cache) publishes; reader has a cache.
    S3CasSnapshotStore writer(minio_opts(prefix));
    const std::string handle = writer.write_checkpoint_dir(local.string(), CheckpointId{42});

    S3CasSnapshotStore reader(minio_opts(prefix, /*cache_bytes=*/1 << 20));
    const std::string first = reader.fetch_checkpoint_dir(handle);
    EXPECT_EQ(reader.objects_downloaded(), 3u);  // cold cache: all three
    expect_dir_matches(first, files);
    const std::string second = reader.fetch_checkpoint_dir(handle);
    EXPECT_EQ(reader.objects_downloaded(), 3u);  // all served from cache: no new GETs
    expect_dir_matches(second, files);
}

TEST(S3CasSnapshotStore, TimeTravelOlderManifest) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> cp1{{"000001.sst", std::string(1024, 'A')},
                                                 {"CURRENT", "v1"}};
    const std::map<std::string, std::string> cp2{{"000001.sst", std::string(1024, 'A')},
                                                 {"000002.sst", std::string(1024, 'B')},
                                                 {"CURRENT", "v2"}};
    const std::string prefix = unique_prefix("cas-tt");
    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string h1 =
        store.write_checkpoint_dir(make_local_dir("t1", cp1).string(), CheckpointId{1});
    const std::string h2 =
        store.write_checkpoint_dir(make_local_dir("t2", cp2).string(), CheckpointId{2});
    // The older manifest still resolves to the older state byte-identically.
    S3CasSnapshotStore reader(minio_opts(prefix));
    expect_dir_matches(reader.fetch_checkpoint_dir(h1), cp1);
    expect_dir_matches(reader.fetch_checkpoint_dir(h2), cp2);
}

// GC: purging cp-1 reclaims only the objects no surviving manifest references;
// the object shared with cp-2 survives and cp-2 still restores byte-identically.
TEST(S3CasSnapshotStore, PurgeReclaimsOnlyUnsharedObjects) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::string a(4096, 'A'), b(4096, 'B'), c(4096, 'C');
    const std::map<std::string, std::string> cp1{
        {"000001.sst", a}, {"000002.sst", b}, {"CURRENT", "M1"}};  // A shared, B + M1 unique
    const std::map<std::string, std::string> cp2{
        {"000001.sst", a}, {"000003.sst", c}, {"CURRENT", "M2"}};  // shares A
    const std::string prefix = unique_prefix("cas-gc-shared");
    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string h1 =
        store.write_checkpoint_dir(make_local_dir("g1", cp1).string(), CheckpointId{1});
    const std::string h2 =
        store.write_checkpoint_dir(make_local_dir("g2", cp2).string(), CheckpointId{2});

    S3CasSnapshotStore gc(minio_opts(prefix));
    gc.delete_checkpoint("", CheckpointId{1});
    EXPECT_EQ(gc.objects_deleted(), 2u);  // B + CURRENT-M1; A is shared, kept

    S3CasSnapshotStore reader(minio_opts(prefix));
    expect_dir_matches(reader.fetch_checkpoint_dir(h2), cp2);  // cp-2 intact (A survived)
    EXPECT_THROW((void)reader.fetch_checkpoint_dir(h1), std::runtime_error);  // cp-1 gone
}

// GC race guard: an object shared with a NEWER checkpoint (the in-flight /
// just-committed one) must NOT be deleted when purging the older one.
TEST(S3CasSnapshotStore, PurgeKeepsObjectsSharedWithNewerCheckpoint) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::string a(4096, 'A'), e(4096, 'E');
    const std::map<std::string, std::string> cp1{{"000001.sst", a}, {"CURRENT", "M1"}};
    const std::map<std::string, std::string> cp3{
        {"000001.sst", a}, {"000005.sst", e}, {"CURRENT", "M3"}};  // newer, still references A
    const std::string prefix = unique_prefix("cas-gc-newer");
    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string h1 =
        store.write_checkpoint_dir(make_local_dir("n1", cp1).string(), CheckpointId{1});
    const std::string h3 =
        store.write_checkpoint_dir(make_local_dir("n3", cp3).string(), CheckpointId{3});

    S3CasSnapshotStore gc(minio_opts(prefix));
    gc.delete_checkpoint("", CheckpointId{1});
    EXPECT_EQ(gc.objects_deleted(), 1u);  // only CURRENT-M1; A kept (referenced by cp-3)

    S3CasSnapshotStore reader(minio_opts(prefix));
    expect_dir_matches(reader.fetch_checkpoint_dir(h3), cp3);  // cp-3 intact (shared A survived)
    EXPECT_THROW((void)reader.fetch_checkpoint_dir(h1), std::runtime_error);
}

// Re-running a purge is a safe no-op (M's manifest already gone): no further
// deletes, and a live checkpoint is never corrupted.
TEST(S3CasSnapshotStore, PurgeIsIdempotent) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::string a(2048, 'A'), b(2048, 'B');
    const std::map<std::string, std::string> cp1{{"000001.sst", a}, {"000002.sst", b}};
    const std::map<std::string, std::string> cp2{{"000001.sst", a}, {"CURRENT", "M2"}};
    const std::string prefix = unique_prefix("cas-gc-idem");
    S3CasSnapshotStore store(minio_opts(prefix));
    store.write_checkpoint_dir(make_local_dir("i1", cp1).string(), CheckpointId{1});
    const std::string h2 =
        store.write_checkpoint_dir(make_local_dir("i2", cp2).string(), CheckpointId{2});

    S3CasSnapshotStore gc(minio_opts(prefix));
    gc.delete_checkpoint("", CheckpointId{1});
    const std::uint64_t after_first = gc.objects_deleted();  // B reclaimed (A shared)
    EXPECT_EQ(after_first, 1u);
    gc.delete_checkpoint("", CheckpointId{1});  // re-run: manifest already gone
    EXPECT_EQ(gc.objects_deleted(), after_first);

    S3CasSnapshotStore reader(minio_opts(prefix));
    expect_dir_matches(reader.fetch_checkpoint_dir(h2), cp2);  // cp-2 never corrupted
}

// Purging the only checkpoint reclaims all of its objects (nothing references
// them).
TEST(S3CasSnapshotStore, PurgeOfOnlyCheckpointReclaimsAll) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> cp1{{"000001.sst", std::string(2048, 'A')},
                                                 {"000002.sst", std::string(2048, 'B')}};
    const std::string prefix = unique_prefix("cas-gc-only");
    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string h1 =
        store.write_checkpoint_dir(make_local_dir("o1", cp1).string(), CheckpointId{1});

    S3CasSnapshotStore gc(minio_opts(prefix));
    gc.delete_checkpoint("", CheckpointId{1});
    EXPECT_EQ(gc.objects_deleted(), 2u);  // both objects unreferenced

    S3CasSnapshotStore reader(minio_opts(prefix));
    EXPECT_THROW((void)reader.fetch_checkpoint_dir(h1), std::runtime_error);
}

// Sweep backstop: orphans (objects referenced by no live manifest, e.g. left by
// a crash mid-purge) are reclaimed; objects of a live checkpoint are kept.
TEST(S3CasSnapshotStore, SweepReclaimsOrphansKeepsLive) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> cp1{{"000001.sst", std::string(2048, 'A')},
                                                 {"000002.sst", std::string(2048, 'B')}};
    const std::map<std::string, std::string> cp2{{"000003.sst", std::string(2048, 'C')},
                                                 {"CURRENT", "M2"}};
    const std::string prefix = unique_prefix("cas-sweep");
    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string h1 =
        store.write_checkpoint_dir(make_local_dir("s1", cp1).string(), CheckpointId{1});
    const std::string h2 =
        store.write_checkpoint_dir(make_local_dir("s2", cp2).string(), CheckpointId{2});

    // Crash-after-manifest-delete: drop cp-1's manifest, orphaning A + B.
    delete_remote_object(h1);

    // Quiescent sweep (grace 0) reclaims exactly the 2 orphans; cp-2's C + M2 are
    // still referenced and survive.
    S3CasSnapshotStore sweeper(minio_opts(prefix));
    EXPECT_EQ(sweeper.sweep(std::chrono::seconds{0}), 2u);

    S3CasSnapshotStore reader(minio_opts(prefix));
    expect_dir_matches(reader.fetch_checkpoint_dir(h2), cp2);
    // A second sweep finds nothing more.
    EXPECT_EQ(sweeper.sweep(std::chrono::seconds{0}), 0u);
}

// Sweep age guard: a large min_age protects freshly-written objects, so an
// in-flight checkpoint's objects are never swept before its manifest lands.
TEST(S3CasSnapshotStore, SweepAgeGuardProtectsYoungObjects) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> cp1{{"000001.sst", std::string(2048, 'A')},
                                                 {"000002.sst", std::string(2048, 'B')}};
    const std::string prefix = unique_prefix("cas-sweep-age");
    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string h1 =
        store.write_checkpoint_dir(make_local_dir("a1", cp1).string(), CheckpointId{1});
    delete_remote_object(h1);  // orphan the just-written A + B

    // With a 1h grace the orphans are too young to reclaim (they could belong to
    // an in-flight checkpoint whose manifest has not landed yet).
    S3CasSnapshotStore sweeper(minio_opts(prefix));
    EXPECT_EQ(sweeper.sweep(std::chrono::seconds{3600}), 0u);
    // Grace 0 then reclaims them.
    EXPECT_EQ(sweeper.sweep(std::chrono::seconds{0}), 2u);
}

// FOUND-4: an exported savepoint is self-contained - it survives a refcount-GC
// of the source checkpoint, because it holds its own copy of the objects.
TEST(S3CasSnapshotStore, SavepointSurvivesSourceCheckpointGc) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> files{
        {"000007.sst", std::string(4096, 'S')},
        {"000009.blob", std::string(2048, 'B')},
        {"CURRENT", "MANIFEST-1\n"},
    };
    const auto local = make_local_dir("sp-src", files);
    const std::string prefix = unique_prefix("cas-sp");
    const std::string bucket = std::getenv("CLINK_S3_TEST_BUCKET");

    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string ckpt = store.write_checkpoint_dir(local.string(), CheckpointId{1});

    const std::string sp_prefix = unique_prefix("cas-sp-export");
    const std::string sp_handle = store.export_savepoint(ckpt, bucket, sp_prefix);
    EXPECT_EQ(store.savepoint_objects(), 3u);  // a self-contained copy of every object

    // GC the source checkpoint (the only one -> all its pool objects deleted).
    store.delete_checkpoint("", CheckpointId{1});
    EXPECT_THROW((void)store.fetch_checkpoint_dir(ckpt), std::runtime_error);  // source gone

    // The savepoint still restores byte-identically from its own objects.
    S3CasSnapshotStore reader(minio_opts(sp_prefix));
    expect_dir_matches(reader.fetch_checkpoint_dir(sp_handle), files);
}

// FOUND-4: the savepoint bundle is relocatable - moved to a new prefix, it
// restores byte-identically (the manifest references objects by hash, never by
// location).
TEST(S3CasSnapshotStore, SavepointIsRelocatable) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> files{
        {"000007.sst", std::string(4096, 'S')},
        {"CURRENT", "MANIFEST-1\n"},
    };
    const auto local = make_local_dir("sp-reloc-src", files);
    const std::string prefix = unique_prefix("cas-spr");
    const std::string bucket = std::getenv("CLINK_S3_TEST_BUCKET");

    S3CasSnapshotStore store(minio_opts(prefix));
    const std::string ckpt = store.write_checkpoint_dir(local.string(), CheckpointId{1});
    const std::string sp1 = unique_prefix("cas-spr-sp1");
    (void)store.export_savepoint(ckpt, bucket, sp1);

    // Physically move the savepoint bundle to a new prefix.
    const std::string sp2 = unique_prefix("cas-spr-sp2");
    relocate_s3_prefix(bucket, sp1, sp2);

    // Restore from the relocated copy by pointing a store at the new prefix.
    S3CasSnapshotStore reader(minio_opts(sp2));
    const std::string sp2_handle = bucket + "/" + sp2 + "/manifests/cp-1.manifest";
    expect_dir_matches(reader.fetch_checkpoint_dir(sp2_handle), files);
}
