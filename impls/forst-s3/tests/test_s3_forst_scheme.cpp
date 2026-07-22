// The ForSt remote-state factory schemes (s3+forst://,
// changelog+s3+forst://). Non-gated tests prove the schemes are
// registered and construct a working backend without touching the
// network (the local DB opens; the S3 store is lazy). The MinIO-gated
// tests drive a full snapshot -> object storage -> restore through the
// factory, plus the async-persist split the S3 store enables (set
// CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET).

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <gtest/gtest.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // ensure_arrow_s3_initialised
#include "clink/state/state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

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

std::string tmp_local(const std::string& tag) {
    static int n = 0;
    auto p =
        std::filesystem::temp_directory_path() / ("clink_forst_s3_" + tag + std::to_string(n++));
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    return p.string();
}

// True iff any *.sst exists anywhere under the local dir - the
// distinguishing negative assert for the live-remote-data-file mode.
bool local_tree_has_sst(const std::string& root) {
    std::error_code ec;
    for (auto it = std::filesystem::recursive_directory_iterator(root, ec);
         !ec && it != std::filesystem::recursive_directory_iterator();
         ++it) {
        const auto name = it->path().filename().string();
        if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".sst") == 0) {
            return true;
        }
    }
    return false;
}

// Recursive object listing under <bucket>/<prefix> against the gated
// test endpoint; returns full paths ("bucket/key").
std::vector<std::string> list_objects(const char* ep, const std::string& bucket_and_prefix) {
    clink::detail::ensure_arrow_s3_initialised();
    auto opts = arrow::fs::S3Options::Defaults();
    opts.endpoint_override = ep;
    opts.scheme = "http";
    opts.region = "us-east-1";
    auto fs = arrow::fs::S3FileSystem::Make(opts);
    if (!fs.ok()) {
        throw std::runtime_error(fs.status().ToString());
    }
    arrow::fs::FileSelector sel;
    sel.base_dir = bucket_and_prefix;
    sel.allow_not_found = true;
    sel.recursive = true;
    auto infos = (*fs)->GetFileInfo(sel);
    if (!infos.ok()) {
        throw std::runtime_error(infos.status().ToString());
    }
    std::vector<std::string> out;
    for (const auto& info : *infos) {
        if (info.type() == arrow::fs::FileType::File) {
            out.push_back(info.path());
        }
    }
    return out;
}

}  // namespace

// The checkpoint-publication schemes are registered and build a working
// backend without network (a dead endpoint is fine: the local DB opens
// and the S3 store is lazy). s3sst+forst is deliberately NOT here: its
// engine filesystem lists the remote prefix at DB open (the merged
// GetChildren view), so construction is endpoint-dependent by design -
// the gated tests cover it.
TEST(S3ForstSchemes, BothRegisteredAndConstruct) {
    auto& f = StateBackendFactory::default_instance();
    const std::string dead = "&endpoint=http://127.0.0.1:1&region=us-east-1";
    for (const std::string scheme : {"s3+forst", "changelog+s3+forst"}) {
        StateBackendSpec spec;
        spec.uri = scheme + "://bucket/job?local=" + tmp_local(scheme) + dead;
        spec.subtask_idx = 0;
        auto built = f.build(spec);
        ASSERT_TRUE(built.backend != nullptr) << "scheme not registered/constructed: " << scheme;
        // A keyed put/get works (the live state is local; S3 is only for
        // checkpoints / materialization payloads).
        built.backend->put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v"}));
        EXPECT_EQ(to_string(*built.backend->get(OperatorId{1}, sv(std::string{"k"}))), "v");
    }
}

// The point of the S3 composition for the async substrate: the remote
// store defers the durable write, so the backend reports
// supports_async_persist() and the runner may split capture (operator
// thread) from persist (snapshot worker). Constructible without network -
// the flag is a property of the store, not a round-trip.
TEST(S3ForstSchemes, S3StoreEnablesAsyncPersistSplit) {
    auto& f = StateBackendFactory::default_instance();
    StateBackendSpec spec;
    spec.uri = "s3+forst://bucket/job?local=" + tmp_local("apflag") +
               "&endpoint=http://127.0.0.1:1&region=us-east-1";
    spec.subtask_idx = 0;
    auto built = f.build(spec);
    ASSERT_TRUE(built.backend != nullptr);
    EXPECT_TRUE(built.backend->supports_async_persist())
        << "an S3-published forst checkpoint must ride the capture/persist split";
}

// Live: s3+forst checkpoint uploads to object storage and restores into a
// fresh backend (fresh local dir) that pulls the checkpoint back from S3.
// Driven through capture()/persist() explicitly - the exact split the
// snapshot worker runs - rather than the synchronous snapshot() wrapper.
TEST(S3ForstSchemes, S3ForstCapturePersistRestoreAgainstLiveEndpoint) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    // Per-process-unique prefix so reruns start clean.
    const std::string prefix =
        std::string{"forst-s3-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string write_uri = std::string{"s3+forst://"} + bk + "/" + prefix +
                                  "?local=" + tmp_local("w") + "&endpoint=" + ep +
                                  "&region=us-east-1";

    StateBackendSpec spec1;
    spec1.uri = write_uri;
    spec1.subtask_idx = 0;
    auto built1 = f.build(spec1);
    ASSERT_TRUE(built1.backend != nullptr);
    ASSERT_TRUE(built1.backend->supports_async_persist());
    built1.backend->put(op, sv(std::string{"a"}), sv(std::string{"1"}));
    built1.backend->put(op, sv(std::string{"b"}), sv(std::string{"2"}));
    // capture on the "operator thread", persist as the worker would.
    auto handle = built1.backend->capture(CheckpointId{1});
    auto snap = built1.backend->persist(std::move(handle));  // uploads cp dir to S3
    EXPECT_EQ(snap.checkpoint_id.value(), 1u);

    // Fresh backend, fresh local dir, restoring from the same bucket/prefix.
    StateBackendSpec spec2;
    spec2.uri = std::string{"s3+forst://"} + bk + "/" + prefix + "?local=" + tmp_local("r") +
                "&endpoint=" + ep + "&region=us-east-1";
    spec2.subtask_idx = 0;
    spec2.restore_uri = write_uri;
    spec2.restore_checkpoint_id = 1;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.backend != nullptr);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    ASSERT_TRUE(built2.backend->get(op, sv(std::string{"a"})).has_value());
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"b"}))), "2");
}

// Live: s3+forst with ?cas=1 (content-addressed manifest store) drives the
// backend through capture/persist/restore end-to-end, and an OLDER
// checkpoint restores the older state (time-travel through the factory).
TEST(S3ForstSchemes, S3ForstCasRoundTripAndTimeTravel) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    const std::string prefix =
        std::string{"forst-s3-cas-"} + std::to_string(static_cast<long>(::getpid()));
    auto uri = [&](const std::string& local_tag) {
        return std::string{"s3+forst://"} + bk + "/" + prefix + "?local=" + tmp_local(local_tag) +
               "&endpoint=" + ep + "&region=us-east-1&cas=1";
    };
    const std::string write_uri = uri("cas-w");

    // cp-1 captures {a,b}; then add c and capture cp-2 {a,b,c}. Consecutive
    // checkpoints share most files - the CAS store dedups them.
    StateBackendSpec spec1;
    spec1.uri = write_uri;
    spec1.subtask_idx = 0;
    auto built1 = f.build(spec1);
    ASSERT_TRUE(built1.backend != nullptr);
    built1.backend->put(op, sv(std::string{"a"}), sv(std::string{"1"}));
    built1.backend->put(op, sv(std::string{"b"}), sv(std::string{"2"}));
    (void)built1.backend->snapshot(CheckpointId{1});
    built1.backend->put(op, sv(std::string{"c"}), sv(std::string{"3"}));
    (void)built1.backend->snapshot(CheckpointId{2});

    // Restore cp-2 into a fresh backend (fresh local dir): all three keys.
    StateBackendSpec spec2;
    spec2.uri = uri("cas-r2");
    spec2.subtask_idx = 0;
    spec2.restore_uri = write_uri;
    spec2.restore_checkpoint_id = 2;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"b"}))), "2");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"c"}))), "3");

    // Time-travel: restore the OLDER cp-1 -> only {a,b}, c was not yet present.
    StateBackendSpec spec3;
    spec3.uri = uri("cas-r1");
    spec3.subtask_idx = 0;
    spec3.restore_uri = write_uri;
    spec3.restore_checkpoint_id = 1;
    auto built3 = f.build(spec3);
    ASSERT_TRUE(built3.restore_from.has_value());
    built3.backend->restore(*built3.restore_from);
    EXPECT_EQ(to_string(*built3.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built3.backend->get(op, sv(std::string{"b"}))), "2");
    EXPECT_FALSE(built3.backend->get(op, sv(std::string{"c"})).has_value());
}

// Live remote data files: the working DB's SSTs live on object storage,
// not local disk. Writes flush to S3 (multipart), the checkpoint's
// "hard links" become server-side copies under the cp prefix, reads are
// served from S3 through the engine, and a fresh build restores by
// replicating the cp's remote data files to its working prefix.
TEST(S3ForstSchemes, S3SstDataFilesLiveRemotelyAndRoundTrip) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    const std::string prefix =
        std::string{"forst-s3sst-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string local = tmp_local("sst");
    const std::string uri = std::string{"s3sst+forst://"} + bk + "/" + prefix + "?local=" + local +
                            "&endpoint=" + ep + "&region=us-east-1";

    {
        StateBackendSpec spec1;
        spec1.uri = uri;
        spec1.subtask_idx = 0;
        auto built1 = f.build(spec1);
        ASSERT_TRUE(built1.backend != nullptr);
        built1.backend->put(op, sv(std::string{"a"}), sv(std::string{"1"}));
        built1.backend->put(op, sv(std::string{"b"}), sv(std::string{"2"}));
        // Bulk so the flush produces a real SST.
        for (int i = 0; i < 4096; ++i) {
            built1.backend->put(op, sv("warm_" + std::to_string(i)), sv(std::string(64, 'x')));
        }
        // The checkpoint flushes the memtable (SSTs are born HERE, on S3)
        // and link-copies them under the cp prefix.
        (void)built1.backend->snapshot(CheckpointId{1});

        // The distinguishing asserts: data files on S3, none on local disk.
        EXPECT_FALSE(local_tree_has_sst(local))
            << "an SST landed on local disk - the remote routing is not engaged";
        int working_ssts = 0;
        int cp_ssts = 0;
        for (const auto& obj : list_objects(ep, std::string{bk} + "/" + prefix)) {
            if (obj.size() < 4 || obj.compare(obj.size() - 4, 4, ".sst") != 0) {
                continue;
            }
            if (obj.find("/0.cp-1/") != std::string::npos) {
                ++cp_ssts;
            } else if (obj.find("/0/") != std::string::npos) {
                ++working_ssts;
            }
        }
        EXPECT_GE(working_ssts, 1) << "flush must have written the SST to the working prefix";
        EXPECT_GE(cp_ssts, 1) << "checkpoint must have copied the SST under the cp prefix";

        // Live remote read: the flushed rows now come back through the
        // engine's table reader over S3.
        ASSERT_TRUE(built1.backend->get(op, sv(std::string{"a"})).has_value());
        EXPECT_EQ(to_string(*built1.backend->get(op, sv(std::string{"a"}))), "1");
    }  // writer torn down (exclusive local LOCK) before the restoring build

    StateBackendSpec spec2;
    spec2.uri = uri;
    spec2.subtask_idx = 0;
    spec2.restore_uri = uri;
    spec2.restore_checkpoint_id = 1;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.backend != nullptr);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    ASSERT_TRUE(built2.backend->get(op, sv(std::string{"a"})).has_value());
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"b"}))), "2");
    EXPECT_FALSE(local_tree_has_sst(local)) << "restore must not materialise SSTs locally";
}

// Live remote data files: purging a checkpoint deletes its remote data
// files (the local metadata dir handling alone would leak the objects),
// and the working DB keeps serving.
TEST(S3ForstSchemes, S3SstPurgeCleansRemoteCheckpointObjects) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    const std::string prefix =
        std::string{"forst-s3sst-purge-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string uri = std::string{"s3sst+forst://"} + bk + "/" + prefix +
                            "?local=" + tmp_local("sstp") + "&endpoint=" + ep + "&region=us-east-1";

    StateBackendSpec spec;
    spec.uri = uri;
    spec.subtask_idx = 0;
    auto built = f.build(spec);
    ASSERT_TRUE(built.backend != nullptr);
    for (int i = 0; i < 4096; ++i) {
        built.backend->put(op, sv("warm_" + std::to_string(i)), sv(std::string(64, 'x')));
    }
    (void)built.backend->snapshot(CheckpointId{7});

    auto cp_objects = [&] {
        int n = 0;
        for (const auto& obj : list_objects(ep, std::string{bk} + "/" + prefix)) {
            if (obj.find("/0.cp-7/") != std::string::npos) {
                ++n;
            }
        }
        return n;
    };
    ASSERT_GE(cp_objects(), 1);

    built.backend->purge_checkpoint(CheckpointId{7});
    EXPECT_EQ(cp_objects(), 0) << "purge must delete the checkpoint's remote data files";

    // Working DB unaffected: its own prefix still serves reads.
    built.backend->put(op, sv(std::string{"k"}), sv(std::string{"v"}));
    EXPECT_EQ(to_string(*built.backend->get(op, sv(std::string{"k"}))), "v");
}

// Cross-machine restore: the metadata store uploads the checkpoint's
// metadata files at persist, so a machine with an EMPTY local disk (same
// URI) restores from the bucket alone - fetch re-materialises the
// metadata dir, the mirror replicates the data files, the engine reads
// them from S3. Also pins that the metadata store flips the backend to
// the capture/persist split.
TEST(S3ForstSchemes, S3SstCrossMachineRestoreFromBucketAlone) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    const std::string prefix =
        std::string{"forst-s3sst-xm-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string local = tmp_local("xm");
    const std::string uri = std::string{"s3sst+forst://"} + bk + "/" + prefix + "?local=" + local +
                            "&endpoint=" + ep + "&region=us-east-1";

    {
        StateBackendSpec spec1;
        spec1.uri = uri;
        spec1.subtask_idx = 0;
        auto built1 = f.build(spec1);
        ASSERT_TRUE(built1.backend != nullptr);
        EXPECT_TRUE(built1.backend->supports_async_persist())
            << "the metadata store's upload must ride the capture/persist split";
        built1.backend->put(op, sv(std::string{"a"}), sv(std::string{"1"}));
        built1.backend->put(op, sv(std::string{"b"}), sv(std::string{"2"}));
        for (int i = 0; i < 4096; ++i) {
            built1.backend->put(op, sv("warm_" + std::to_string(i)), sv(std::string(64, 'x')));
        }
        (void)built1.backend->snapshot(CheckpointId{1});  // metadata uploads at persist
    }

    // "New machine": the same URI over an empty disk.
    std::filesystem::remove_all(local);
    ASSERT_FALSE(std::filesystem::exists(local));

    StateBackendSpec spec2;
    spec2.uri = uri;
    spec2.subtask_idx = 0;
    spec2.restore_uri = uri;
    spec2.restore_checkpoint_id = 1;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.backend != nullptr);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    ASSERT_TRUE(built2.backend->get(op, sv(std::string{"a"})).has_value());
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"b"}))), "2");
    // The metadata dir was re-materialised locally; the data files were not.
    EXPECT_TRUE(std::filesystem::exists(local + "/0.cp-1"))
        << "fetch must re-materialise the checkpoint metadata dir";
    EXPECT_FALSE(local_tree_has_sst(local));
}

// Local SST read cache: the write-tee lands each uploaded SST in the
// cache dir (so flush/compaction outputs read locally), and the LRU byte
// budget bounds the cache's size.
TEST(S3ForstSchemes, S3SstLocalCacheTeesAndRespectsBudget) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};

    auto cache_tree_bytes = [](const std::string& dir) {
        std::uint64_t total = 0;
        std::error_code ec;
        for (auto it = std::filesystem::recursive_directory_iterator(dir, ec);
             !ec && it != std::filesystem::recursive_directory_iterator();
             ++it) {
            if (it->is_regular_file()) {
                total += it->file_size(ec);
            }
        }
        return total;
    };

    // Generous budget: the flushed SST must be teed into the cache.
    {
        const std::string prefix =
            std::string{"forst-s3sst-cache-"} + std::to_string(static_cast<long>(::getpid()));
        const std::string cache_dir = tmp_local("cachedir");
        const std::string uri = std::string{"s3sst+forst://"} + bk + "/" + prefix +
                                "?local=" + tmp_local("cache") + "&endpoint=" + ep +
                                "&region=us-east-1&sst_cache=" + cache_dir +
                                "&sst_cache_bytes=104857600";
        StateBackendSpec spec;
        spec.uri = uri;
        spec.subtask_idx = 0;
        auto built = f.build(spec);
        for (int i = 0; i < 4096; ++i) {
            built.backend->put(op, sv("warm_" + std::to_string(i)), sv(std::string(64, 'x')));
        }
        (void)built.backend->snapshot(CheckpointId{1});  // flush -> SST -> tee
        bool cached_sst = false;
        std::error_code ec;
        for (auto it = std::filesystem::recursive_directory_iterator(cache_dir, ec);
             !ec && it != std::filesystem::recursive_directory_iterator();
             ++it) {
            const auto name = it->path().filename().string();
            if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".sst") == 0) {
                cached_sst = true;
                break;
            }
        }
        EXPECT_TRUE(cached_sst) << "the uploaded SST must be teed into the local cache";
        // Reads (served from the cache on hit) stay correct.
        EXPECT_EQ(to_string(*built.backend->get(op, sv(std::string{"warm_1"}))),
                  std::string(64, 'x'));
    }

    // Tiny budget: whatever the tee inserts is evicted back down to the
    // budget immediately - the cache never outgrows it.
    {
        const std::string prefix =
            std::string{"forst-s3sst-evict-"} + std::to_string(static_cast<long>(::getpid()));
        const std::string cache_dir = tmp_local("evictdir");
        const std::string uri = std::string{"s3sst+forst://"} + bk + "/" + prefix +
                                "?local=" + tmp_local("evict") + "&endpoint=" + ep +
                                "&region=us-east-1&sst_cache=" + cache_dir +
                                "&sst_cache_bytes=1024";
        StateBackendSpec spec;
        spec.uri = uri;
        spec.subtask_idx = 0;
        auto built = f.build(spec);
        for (int i = 0; i < 4096; ++i) {
            built.backend->put(op, sv("warm_" + std::to_string(i)), sv(std::string(64, 'x')));
        }
        (void)built.backend->snapshot(CheckpointId{1});
        EXPECT_LE(cache_tree_bytes(cache_dir), 1024u)
            << "the cache must evict down to its byte budget";
        // A cache miss reads S3 - values stay correct either way.
        EXPECT_EQ(to_string(*built.backend->get(op, sv(std::string{"warm_2"}))),
                  std::string(64, 'x'));
    }
}

// Stale-staging sweep: a checkpoint that crashed mid-stage leaves its
// objects at "<subtask>.cp-<id>.tmp/..." (the engine's local staging-dir
// cleanup never reaches the remote side). A fresh backend construction
// sweeps them; real checkpoints are untouched.
TEST(S3ForstSchemes, S3SstConstructionSweepsStaleStaging) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    const std::string prefix =
        std::string{"forst-s3sst-sweep-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string local = tmp_local("sweep");
    const std::string uri = std::string{"s3sst+forst://"} + bk + "/" + prefix + "?local=" + local +
                            "&endpoint=" + ep + "&region=us-east-1";

    // Real checkpoint first.
    {
        StateBackendSpec spec;
        spec.uri = uri;
        spec.subtask_idx = 0;
        auto built = f.build(spec);
        for (int i = 0; i < 4096; ++i) {
            built.backend->put(op, sv("warm_" + std::to_string(i)), sv(std::string(64, 'x')));
        }
        (void)built.backend->snapshot(CheckpointId{1});
    }

    // Simulate a crashed staging: plant an object at the .tmp prefix.
    {
        clink::detail::ensure_arrow_s3_initialised();
        auto opts = arrow::fs::S3Options::Defaults();
        opts.endpoint_override = ep;
        opts.scheme = "http";
        opts.region = "us-east-1";
        auto s3 = arrow::fs::S3FileSystem::Make(opts);
        ASSERT_TRUE(s3.ok());
        auto out =
            (*s3)->OpenOutputStream(std::string{bk} + "/" + prefix + "/0.cp-9.tmp/000099.sst");
        ASSERT_TRUE(out.ok());
        ASSERT_TRUE((*out)->Write("stranded", 8).ok());
        ASSERT_TRUE((*out)->Close().ok());
    }
    auto count = [&](const char* needle) {
        int n = 0;
        for (const auto& obj : list_objects(ep, std::string{bk} + "/" + prefix)) {
            if (obj.find(needle) != std::string::npos) {
                ++n;
            }
        }
        return n;
    };
    ASSERT_EQ(count("/0.cp-9.tmp/"), 1);
    ASSERT_GE(count("/0.cp-1/"), 1);

    // A fresh construction (restart) sweeps the stale staging and leaves
    // the real checkpoint alone.
    {
        StateBackendSpec spec;
        spec.uri = uri;
        spec.subtask_idx = 0;
        auto built = f.build(spec);
        ASSERT_TRUE(built.backend != nullptr);
    }
    EXPECT_EQ(count("/0.cp-9.tmp/"), 0) << "stale staging must be swept at construction";
    EXPECT_GE(count("/0.cp-1/"), 1) << "real checkpoints must survive the sweep";
}

// Live: changelog+s3+forst materialization payload goes to object storage
// (framing blob stays local); restore replays it back into a fresh ForSt
// inner.
TEST(S3ForstSchemes, ChangelogS3ForstRoundTripAgainstLiveEndpoint) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    // Same local dir for write + restore: the changelog framing blob is local
    // (the disaggregated part is the materialization payload, which is in S3).
    const std::string local = tmp_local("cl");
    const std::string prefix =
        std::string{"forst-s3-cl-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string uri = std::string{"changelog+s3+forst://"} + bk + "/" + prefix +
                            "?local=" + local + "&endpoint=" + ep + "&region=us-east-1";

    // Scoped: the ForSt inner holds an exclusive LOCK on its working dir
    // while open, so the writer must be torn down (as a process restart
    // would) before the restoring build re-opens the same local dir. The
    // framing blob and the inner's checkpoint dirs persist on disk.
    {
        StateBackendSpec spec1;
        spec1.uri = uri;
        spec1.subtask_idx = 0;
        auto built1 = f.build(spec1);
        ASSERT_TRUE(built1.backend != nullptr);
        // Enough writes to push the changelog over its materialization threshold.
        for (int i = 0; i < 2000; ++i) {
            built1.backend->put(
                op, sv("key-" + std::to_string(i % 50)), sv(std::string("v") + std::to_string(i)));
        }
        (void)built1.backend->snapshot(CheckpointId{1});
    }

    StateBackendSpec spec2;
    spec2.uri = uri;
    spec2.subtask_idx = 0;
    spec2.restore_uri = uri;
    spec2.restore_checkpoint_id = 1;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.backend != nullptr);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    // The last write per key wins (i runs 0..1999 over 50 keys).
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"key-0"}))), "v1950");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"key-49"}))), "v1999");
}
