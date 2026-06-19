// LocalObjectCache (DISAGG-5): a bounded local-disk cache of large immutable
// checkpoint objects. Pure local-disk logic, so it is fully testable without
// any S3 backend: hit/miss, the immutable-name filter, namespace isolation,
// byte-bounded + frequency-aware eviction, the lossless on-disk naming that
// keeps distinct keys from colliding, durable reload across instances (the
// cross-restart reuse the feature exists for), the byte-budget reclaim of
// orphaned files on reload, and the size guard against stale cross-lineage hits.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "clink/s3/local_object_cache.hpp"

using clink::s3::LocalObjectCache;

namespace {

namespace fs = std::filesystem;

fs::path scratch(const std::string& tag) {
    static int n = 0;
    auto p = fs::temp_directory_path() / ("clink_objcache_" + tag + std::to_string(n++));
    std::error_code ec;
    fs::remove_all(p, ec);
    fs::create_directories(p, ec);
    return p;
}

fs::path write_file(const fs::path& dir, const std::string& name, const std::string& content) {
    const auto p = dir / name;
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
    return p;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

int count_files(const fs::path& dir) {
    int n = 0;
    std::error_code ec;
    for (const auto& e : fs::directory_iterator(dir, ec)) {
        if (e.is_regular_file()) {
            ++n;
        }
    }
    return n;
}

}  // namespace

TEST(LocalObjectCache, HitMissAndImmutableFilter) {
    const auto src = scratch("src");
    LocalObjectCache cache(scratch("cache"), /*max_bytes=*/1 << 20);
    const auto sst = write_file(src, "000007.sst", "sst-bytes");

    // Immutable file: miss, then put, then hit (materialized into dest).
    EXPECT_FALSE(cache.get("ns", "000007.sst", src / "out1"));
    cache.put("ns", "000007.sst", sst);
    ASSERT_TRUE(cache.get("ns", "000007.sst", src / "out1"));
    EXPECT_EQ(read_file(src / "out1"), "sst-bytes");
    EXPECT_EQ(cache.entry_count(), 1u);

    // .blob files (integrated BlobDB) are also immutable-by-number and cache.
    cache.put("ns", "000009.blob", write_file(src, "000009.blob", "blob-bytes"));
    ASSERT_TRUE(cache.get("ns", "000009.blob", src / "outb"));
    EXPECT_EQ(read_file(src / "outb"), "blob-bytes");
    EXPECT_EQ(cache.entry_count(), 2u);

    // Mutable / appended-to / tiny names are NOT cached: put is a no-op, get
    // always misses. CURRENT is mutable; MANIFEST-/OPTIONS- are appended-to
    // within a lineage (same number, growing bytes) so are deliberately excluded.
    for (const char* name : {"CURRENT", "MANIFEST-000006", "OPTIONS-000008", "LOG", ".sst"}) {
        cache.put("ns", name, write_file(src, "f", "x"));
        EXPECT_FALSE(cache.get("ns", name, src / "no")) << name << " must not cache";
    }
    EXPECT_EQ(cache.entry_count(), 2u);  // unchanged: only the sst + blob
}

TEST(LocalObjectCache, NamespaceIsolation) {
    const auto src = scratch("src");
    LocalObjectCache cache(scratch("cache"), 1 << 20);
    // Same basename, different namespaces (different subtask DB lineages),
    // different content - must not collide.
    cache.put("sub0", "000123.sst", write_file(src, "a", "from-subtask-0"));
    cache.put("sub1", "000123.sst", write_file(src, "b", "from-subtask-1"));

    ASSERT_TRUE(cache.get("sub0", "000123.sst", src / "g0"));
    ASSERT_TRUE(cache.get("sub1", "000123.sst", src / "g1"));
    EXPECT_EQ(read_file(src / "g0"), "from-subtask-0");
    EXPECT_EQ(read_file(src / "g1"), "from-subtask-1");
}

// Regression: a naive '/'->'_' folding made "a/b_c" and "a/b/c" share one
// on-disk file and serve each other's bytes (silent state corruption). The
// lossless encoding keeps them distinct.
TEST(LocalObjectCache, DistinctKeysNeverShareAnOnDiskFile) {
    const auto src = scratch("src");
    const auto cdir = scratch("cache");
    LocalObjectCache cache(cdir, 1 << 20);
    cache.put("a/b_c", "000007.sst", write_file(src, "x", "X-bytes"));
    cache.put("a/b/c", "000007.sst", write_file(src, "y", "Y-bytes"));

    EXPECT_EQ(cache.entry_count(), 2u);
    EXPECT_EQ(count_files(cdir), 2);  // two genuinely distinct files on disk
    ASSERT_TRUE(cache.get("a/b_c", "000007.sst", src / "gx"));
    ASSERT_TRUE(cache.get("a/b/c", "000007.sst", src / "gy"));
    EXPECT_EQ(read_file(src / "gx"), "X-bytes");
    EXPECT_EQ(read_file(src / "gy"), "Y-bytes");
}

TEST(LocalObjectCache, ByteBoundedEviction) {
    const auto src = scratch("src");
    // Budget holds ~2 of the 100-byte objects.
    LocalObjectCache cache(scratch("cache"), /*max_bytes=*/250);
    const std::string blob(100, 'x');
    for (int i = 0; i < 5; ++i) {
        cache.put("ns", std::to_string(i) + ".sst", write_file(src, "f" + std::to_string(i), blob));
    }
    EXPECT_LE(cache.bytes(), 250u);
    EXPECT_LE(cache.entry_count(), 2u);
}

// Frequency-aware eviction: a hot object (accessed repeatedly) survives a scan
// of cold objects that would evict it under pure LRU.
TEST(LocalObjectCache, FrequencyAwareEvictionKeepsHotObject) {
    const auto src = scratch("src");
    LocalObjectCache cache(scratch("cache"), /*max_bytes=*/250);  // ~2 x 100B
    const std::string blob(100, 'h');
    const auto hot = write_file(src, "hot", blob);

    cache.put("ns", "hot.sst", hot);
    // Make it hot: many accesses raise its frequency.
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(cache.get("ns", "hot.sst", src / "tmp"));
    }
    // Now scan a stream of cold objects (each touched once).
    for (int i = 0; i < 6; ++i) {
        cache.put("ns",
                  "cold" + std::to_string(i) + ".sst",
                  write_file(src, "c" + std::to_string(i), blob));
    }
    // The hot object is still resident; pure LRU would have evicted it.
    EXPECT_TRUE(cache.contains("ns", "hot.sst"));
}

// Regression: the index must be rebuilt from the on-disk files, so a fresh
// instance over the same dir (a process restart) re-uses what a prior run left
// behind. Without reload the headline cross-restart reuse is a no-op.
TEST(LocalObjectCache, DurableReloadAcrossInstances) {
    const auto src = scratch("src");
    const auto cdir = scratch("cache");
    {
        LocalObjectCache cache(cdir, 1 << 20);
        cache.put("ns", "000007.sst", write_file(src, "s", "persist-me"));
        ASSERT_EQ(cache.entry_count(), 1u);
    }
    // A new instance over the same dir rebuilds the index from disk: a hit
    // without any put() in this instance.
    LocalObjectCache reopened(cdir, 1 << 20);
    EXPECT_EQ(reopened.entry_count(), 1u);
    ASSERT_TRUE(reopened.contains("ns", "000007.sst"));
    ASSERT_TRUE(reopened.get("ns", "000007.sst", src / "out"));
    EXPECT_EQ(read_file(src / "out"), "persist-me");
}

// Regression: across restarts the byte budget must hold. A prior run can leave
// more files than the new budget allows; reload reclaims the excess instead of
// leaking it on disk.
TEST(LocalObjectCache, ReloadEnforcesBudgetAndReclaimsOrphans) {
    const auto src = scratch("src");
    const auto cdir = scratch("cache");
    const std::string blob(100, 'x');
    {
        LocalObjectCache generous(cdir, 1 << 20);  // keep all five
        for (int i = 0; i < 5; ++i) {
            generous.put(
                "ns", std::to_string(i) + ".sst", write_file(src, "f" + std::to_string(i), blob));
        }
        ASSERT_EQ(generous.entry_count(), 5u);
        ASSERT_EQ(count_files(cdir), 5);
    }
    // Reopen with a tight budget: reload evicts down to budget AND deletes the
    // reclaimed files (no unbounded on-disk growth across restarts).
    LocalObjectCache tight(cdir, 250);
    EXPECT_LE(tight.entry_count(), 2u);
    EXPECT_LE(tight.bytes(), 250u);
    EXPECT_LE(count_files(cdir), 2);
}

// Size guard: a same-named object whose size differs from what the caller
// expects (a different lineage that recycled the SST number) is treated as a
// miss and the stale entry dropped, so the re-fetch repopulates correct bytes.
TEST(LocalObjectCache, SizeMismatchTreatedAsMiss) {
    const auto src = scratch("src");
    LocalObjectCache cache(scratch("cache"), 1 << 20);
    cache.put("ns", "000007.sst", write_file(src, "s", "four"));  // 4 bytes

    EXPECT_FALSE(cache.get("ns", "000007.sst", src / "o", /*expected_size=*/5));
    EXPECT_FALSE(cache.contains("ns", "000007.sst"));  // stale entry dropped

    cache.put("ns", "000008.sst", write_file(src, "t", "12345"));  // 5 bytes
    ASSERT_TRUE(cache.get("ns", "000008.sst", src / "o2", /*expected_size=*/5));
    EXPECT_EQ(read_file(src / "o2"), "12345");
}

// An object bigger than the whole budget is never cached (it would evict
// everything else just to hold itself).
TEST(LocalObjectCache, OversizedObjectNotCached) {
    const auto src = scratch("src");
    LocalObjectCache cache(scratch("cache"), /*max_bytes=*/10);
    cache.put("ns", "big.sst", write_file(src, "b", std::string(100, 'x')));
    EXPECT_EQ(cache.entry_count(), 0u);
    EXPECT_FALSE(cache.contains("ns", "big.sst"));
}
