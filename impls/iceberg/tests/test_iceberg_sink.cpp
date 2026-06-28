// Apache Iceberg sink: a local filesystem round-trip through make_iceberg_row_sink -
// write a real Iceberg table (SQLite catalog + typed Parquet data files + snapshots)
// to a temp warehouse, then verify the on-disk table structure (catalog DB, table
// metadata, snapshot manifests, data files). PyIceberg/Spark read-back is the external
// validation (set CLINK_ICEBERG_KEEP_DIR to keep the warehouse for it).

#ifndef CLINK_HAS_ICEBERG
#error "test_iceberg_sink requires iceberg-cpp"
#endif

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/iceberg/iceberg_row_sink.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::RuntimeContext;
using clink::iceberg::IcebergRowSinkOptions;
using clink::iceberg::make_iceberg_row_sink;
using clink::sql::make_row_columnar_arrow_batcher;
using clink::sql::Row;
using clink::sql::RowColumn;
namespace cfg = clink::config;
namespace fs = std::filesystem;

namespace {

std::vector<RowColumn> schema() {
    return {{"id", arrow::int64()}, {"name", arrow::utf8()}};
}

Row make_row(std::int64_t id, const std::string& name) {
    Row r;
    r.values["id"] = cfg::JsonValue{id};
    r.values["name"] = cfg::JsonValue{name};
    return r;
}

// Partitioned-table schema {id:int64, region:string} + a matching row helper.
std::vector<RowColumn> part_schema() {
    return {{"id", arrow::int64()}, {"region", arrow::utf8()}};
}

Row make_region_row(std::int64_t id, const std::string& region) {
    Row r;
    r.values["id"] = cfg::JsonValue{id};
    r.values["region"] = cfg::JsonValue{region};
    return r;
}

// A changelog row {id:int64, name:string} carrying a __row_kind (insert/delete/update_after).
Row make_cdc_row(std::int64_t id, const std::string& name, const std::string& kind) {
    Row r;
    r.values["id"] = cfg::JsonValue{id};
    r.values["name"] = cfg::JsonValue{name};
    r.values["__row_kind"] = cfg::JsonValue{kind};
    return r;
}

// Recursively count files under `root` whose path contains `needle`.
int count_paths_containing(const fs::path& root, const std::string& needle) {
    int n = 0;
    if (!fs::exists(root)) {
        return 0;
    }
    for (const auto& e : fs::recursive_directory_iterator(root)) {
        if (e.is_regular_file() && e.path().string().find(needle) != std::string::npos) {
            ++n;
        }
    }
    return n;
}

}  // namespace

TEST(IcebergSink, WritesRealIcebergTableWithTwoSnapshots) {
    const char* keep = std::getenv("CLINK_ICEBERG_KEEP_DIR");
    auto wh = keep != nullptr
                  ? fs::path(keep)
                  : fs::temp_directory_path() /
                        ("clink_iceberg_" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(wh);
    fs::create_directories(wh);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.namespace_levels = {"default"};
    o.table = "events";
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->open();

    // Snapshot 0 (two rows), barrier; snapshot 1 (one row), close.
    Batch<Row> b0;
    b0.emplace(make_row(1, "a"));
    b0.emplace(make_row(2, "b"));
    sink->on_data(b0);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    Batch<Row> b1;
    b1.emplace(make_row(3, "c"));
    sink->on_data(b1);
    sink->close();  // commits the tail interval

    // The SQLite catalog DB was created.
    EXPECT_TRUE(fs::exists(wh / "catalog.db"));
    // Two data files (one per snapshot interval).
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 2);
    // Iceberg table metadata + manifests were written.
    EXPECT_GE(count_paths_containing(wh, ".metadata.json"), 1);
    EXPECT_GE(count_paths_containing(wh, ".avro"), 1) << "snapshot manifests";

    if (keep == nullptr) {
        fs::remove_all(wh);
    }
}

// Subtask != 0 is dormant (single-writer table): no warehouse files are produced.
TEST(IcebergSink, NonZeroSubtaskIsDormant) {
    auto wh = fs::temp_directory_path() /
              ("clink_iceberg_dormant_" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(wh);
    fs::create_directories(wh);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.subtask_idx = 1;  // not the writer
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->open();
    Batch<Row> b;
    b.emplace(make_row(1, "a"));
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->close();

    EXPECT_FALSE(fs::exists(wh / "catalog.db"));
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 0);

    fs::remove_all(wh);
}

// ---- Exactly-once 2PC (with a state backend) ----
// A snapshot manifest list is "snap-<id>-...avro"; one per committed snapshot, so counting
// "snap-" paths counts committed snapshots.
namespace {
fs::path make_2pc_wh(const std::string& tag) {
    auto wh = fs::temp_directory_path() /
              ("clink_iceberg_2pc_" + tag + "_" + std::to_string(static_cast<long>(::getpid())));
    fs::remove_all(wh);
    fs::create_directories(wh);
    return wh;
}
}  // namespace

// With a state backend, on_barrier STAGES (writes the data file, no snapshot); the snapshot
// appears only on on_commit. A redelivered commit is idempotent.
TEST(IcebergSink2PC, StagesOnBarrierCommitsOnCommit) {
    auto wh = make_2pc_wh("commit");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "iceberg_sink", &state, /*metrics=*/nullptr);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);
    sink->open();

    Batch<Row> b0;
    b0.emplace(make_row(1, "a"));
    b0.emplace(make_row(2, "b"));
    sink->on_data(b0);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    // Staged, NOT committed: data file on disk, but NO snapshot yet.
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 1);
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 0)
        << "a barrier alone must not create a snapshot (exactly-once)";

    sink->on_commit(1);
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 1) << "on_commit creates the snapshot";

    sink->on_commit(1);  // redelivered / recovery replay
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 1) << "commit is idempotent";

    sink->close();
    fs::remove_all(wh);
}

// on_abort drops the staged (unreferenced) data file and creates no snapshot.
TEST(IcebergSink2PC, AbortDropsStagedDataNoSnapshot) {
    auto wh = make_2pc_wh("abort");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "iceberg_sink", &state, /*metrics=*/nullptr);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);
    sink->open();

    Batch<Row> b;
    b.emplace(make_row(1, "a"));
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{7}});
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 1);

    sink->on_abort(7);
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 0) << "aborted staged data file deleted";
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 0);

    sink->close();
    fs::remove_all(wh);
}

// Recovery: a sink that staged a checkpoint then crashed (no on_commit) is replaced by a new
// sink sharing the same state backend + id; open() commits the pending staged data.
TEST(IcebergSink2PC, RecoveryCommitsPendingStagedData) {
    auto wh = make_2pc_wh("recover");
    InMemoryStateBackend state;  // shared across the two sink "lives"

    auto opts = [&]() {
        IcebergRowSinkOptions o;
        o.warehouse = wh.string();
        o.table = "events";
        o.batcher = make_row_columnar_arrow_batcher(schema());
        return o;
    };

    // Life 1: stage checkpoint 5, then "crash" (drop the sink without on_commit/close).
    {
        RuntimeContext rctx(OperatorId{42}, "iceberg_sink", &state, /*metrics=*/nullptr);
        auto sink = make_iceberg_row_sink(opts());
        sink->set_id(OperatorId{42});
        sink->attach_runtime(&rctx);
        sink->open();
        Batch<Row> b;
        b.emplace(make_row(1, "a"));
        b.emplace(make_row(2, "b"));
        sink->on_data(b);
        sink->on_barrier(CheckpointBarrier{CheckpointId{5}});
        EXPECT_EQ(count_paths_containing(wh, "snap-"), 0) << "staged, not committed";
    }

    // Life 2: same state backend + id; open() recovery commits checkpoint 5.
    {
        RuntimeContext rctx(OperatorId{42}, "iceberg_sink", &state, /*metrics=*/nullptr);
        auto sink = make_iceberg_row_sink(opts());
        sink->set_id(OperatorId{42});
        sink->attach_runtime(&rctx);
        sink->open();  // recover_pending_ commits the staged checkpoint
        EXPECT_EQ(count_paths_containing(wh, "snap-"), 1)
            << "recovery committed the pending staged checkpoint";
        sink->close();
    }

    fs::remove_all(wh);
}

// A corrupt staged-commit blob surfaces as a clear error during recovery (not a raw
// std::stoll exception escaping the recovery scan).
TEST(IcebergSink2PC, CorruptStagedStateFailsLoudlyOnRecovery) {
    auto wh = make_2pc_wh("corrupt");
    InMemoryStateBackend state;
    state.put(OperatorId{42}, "_2pc_pending_sub0_99", "not-a-valid-blob");
    RuntimeContext rctx(OperatorId{42}, "iceberg_sink", &state, /*metrics=*/nullptr);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);
    EXPECT_THROW(sink->open(), std::runtime_error);

    fs::remove_all(wh);
}

// ---- Identity partitioning ----
// A batch spanning N partition values fans out to N data files in ONE snapshot.
TEST(IcebergSinkPartitioned, OneDataFilePerPartitionValue) {
    const char* keep = std::getenv("CLINK_ICEBERG_KEEP_DIR");
    auto wh = keep != nullptr ? fs::path(keep) : make_2pc_wh("partition");
    if (keep != nullptr) {
        fs::remove_all(wh);
        fs::create_directories(wh);
    }
    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.partition_by = {"region"};
    o.batcher = make_row_columnar_arrow_batcher(part_schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->open();  // standalone -> immediate commit on barrier

    Batch<Row> b;
    b.emplace(make_region_row(1, "us"));
    b.emplace(make_region_row(2, "eu"));
    b.emplace(make_region_row(3, "us"));
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // one snapshot, two data files
    sink->close();

    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 2) << "one data file per (us, eu)";
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 1) << "a single snapshot referencing both";

    if (keep == nullptr) {
        fs::remove_all(wh);
    }
}

// Partitioning composes with 2PC: a partitioned interval stages multiple files, all
// committed atomically in one snapshot on on_commit.
TEST(IcebergSinkPartitioned, MultiFileStageThenCommit) {
    auto wh = make_2pc_wh("partition2pc");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "iceberg_sink", &state, /*metrics=*/nullptr);

    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.partition_by = {"region"};
    o.batcher = make_row_columnar_arrow_batcher(part_schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);
    sink->open();

    Batch<Row> b;
    b.emplace(make_region_row(1, "us"));
    b.emplace(make_region_row(2, "eu"));
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    // Staged: two data files, but NO snapshot yet.
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 2);
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 0);

    sink->on_commit(1);
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 1) << "both staged files in one snapshot";
    sink->close();

    fs::remove_all(wh);
}

// ---- Upsert (changelog -> v2 equality deletes) ----
// An interval writes ONE data file (the surviving inserts) + ONE equality-delete file (every
// touched key), all in one snapshot. CLINK_ICEBERG_KEEP_DIR keeps the table so an external
// iceberg-cpp reader can verify merge-on-read (PyIceberg cannot read equality deletes).
TEST(IcebergUpsert, WritesDataPlusEqualityDeletePerInterval) {
    const char* keep = std::getenv("CLINK_ICEBERG_KEEP_DIR");
    auto wh = keep != nullptr ? fs::path(keep) : make_2pc_wh("upsert");
    if (keep != nullptr) {
        fs::remove_all(wh);
        fs::create_directories(wh);
    }
    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.equality_key = {"id"};
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->open();  // standalone -> immediate commit per barrier

    // Interval 1: insert id=1,2,3.
    Batch<Row> b0;
    b0.emplace(make_cdc_row(1, "a", "insert"));
    b0.emplace(make_cdc_row(2, "b", "insert"));
    b0.emplace(make_cdc_row(3, "c", "insert"));
    sink->on_data(b0);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_EQ(count_paths_containing(wh, ".parquet"), 2) << "1 data file + 1 equality-delete file";
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 1);

    // Interval 2: update id=1 -> a2, delete id=2 (id=3 untouched).
    Batch<Row> b1;
    b1.emplace(make_cdc_row(1, "a2", "update_after"));
    b1.emplace(make_cdc_row(2, "b", "delete"));
    sink->on_data(b1);
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 2);
    sink->close();

    // Net table (verified by iceberg-cpp merge-on-read, external): {1:a2, 3:c}; id=2 deleted.
    if (keep == nullptr) {
        fs::remove_all(wh);
    }
}

// Within one interval, the last op per key wins: insert -> delete -> insert (same key) nets to
// a single insert, so the data file has exactly one row for that key.
TEST(IcebergUpsert, NetsLastOpPerKeyWithinInterval) {
    auto wh = make_2pc_wh("upsert_net");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "iceberg_sink", &state, /*metrics=*/nullptr);
    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.equality_key = {"id"};
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);
    sink->open();

    Batch<Row> b;
    b.emplace(make_cdc_row(5, "x", "insert"));
    b.emplace(make_cdc_row(5, "x", "delete"));
    b.emplace(make_cdc_row(5, "z", "insert"));  // net: insert id=5 = "z"
    sink->on_data(b);
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // staged (2PC), not committed
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 0);
    sink->on_commit(1);
    EXPECT_EQ(count_paths_containing(wh, "snap-"), 1) << "one snapshot (data + delete) on commit";
    sink->close();
    fs::remove_all(wh);
}

// upsert + partition_by is rejected in v1.
TEST(IcebergUpsert, PartitionedUpsertRejected) {
    auto wh = make_2pc_wh("upsert_part");
    IcebergRowSinkOptions o;
    o.warehouse = wh.string();
    o.table = "events";
    o.equality_key = {"id"};
    o.partition_by = {"name"};
    o.batcher = make_row_columnar_arrow_batcher(schema());
    auto sink = make_iceberg_row_sink(std::move(o));
    EXPECT_THROW(sink->open(), std::runtime_error);
    fs::remove_all(wh);
}

// Live REST catalog round-trip: write an Iceberg table whose catalog ops go through a REST
// server (Polaris/Nessie/iceberg-rest-fixture) and whose data lives on the S3 warehouse the
// catalog manages, then re-open it (LoadTable via REST). Gated on CLINK_ICEBERG_REST_URI
// (+ CLINK_ICEBERG_REST_WAREHOUSE + the S3 env). PyIceberg-over-REST is the external oracle.
TEST(IcebergRestLive, WritesViaRestCatalog) {
    const char* rest = std::getenv("CLINK_ICEBERG_REST_URI");
    if (rest == nullptr) {
        GTEST_SKIP() << "set CLINK_ICEBERG_REST_URI (+ CLINK_ICEBERG_REST_WAREHOUSE, S3 env)";
    }
    const char* wh = std::getenv("CLINK_ICEBERG_REST_WAREHOUSE");
    const char* s3ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* ak = std::getenv("AWS_ACCESS_KEY_ID");
    const char* sk = std::getenv("AWS_SECRET_ACCESS_KEY");
    const char* tok = std::getenv("CLINK_ICEBERG_REST_TOKEN");

    const std::string table = "rest_events_" + std::to_string(static_cast<long>(::getpid()));
    auto make_opts = [&]() {
        IcebergRowSinkOptions o;
        o.catalog_uri = rest;
        if (wh != nullptr)
            o.warehouse = wh;
        o.namespace_levels = {"default"};
        o.table = table;
        if (tok != nullptr)
            o.rest_auth_token = tok;
        if (s3ep != nullptr)
            o.file_io_props["s3.endpoint"] = s3ep;
        o.file_io_props["s3.path-style-access"] = "true";
        o.file_io_props["s3.region"] = "us-east-1";
        if (ak != nullptr)
            o.file_io_props["s3.access-key-id"] = ak;
        if (sk != nullptr)
            o.file_io_props["s3.secret-access-key"] = sk;
        o.batcher = make_row_columnar_arrow_batcher(schema());
        return o;
    };

    {
        auto sink = make_iceberg_row_sink(make_opts());
        ASSERT_NO_THROW(sink->open());
        Batch<Row> b0;
        b0.emplace(make_row(1, "a"));
        b0.emplace(make_row(2, "b"));
        sink->on_data(b0);
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
        Batch<Row> b1;
        b1.emplace(make_row(3, "c"));
        sink->on_data(b1);
        ASSERT_NO_THROW(sink->close());
    }
    {
        auto sink = make_iceberg_row_sink(make_opts());
        EXPECT_NO_THROW(sink->open());  // LoadTable via REST proves the catalog round-trips
        EXPECT_NO_THROW(sink->close());
    }
}

// An s3:// warehouse needs an explicit local catalog_uri (SQLite cannot live on S3).
// Offline: the precondition throws in open() before any S3 call.
TEST(IcebergSink, S3WarehouseRequiresLocalCatalogUri) {
    IcebergRowSinkOptions o;
    o.warehouse = "s3://bucket/warehouse";
    o.table = "events";
    o.batcher = make_row_columnar_arrow_batcher(schema());  // catalog_uri left empty
    auto sink = make_iceberg_row_sink(std::move(o));
    EXPECT_THROW(sink->open(), std::runtime_error);
}

// Live S3 round-trip via MinIO/LocalStack: write a real Iceberg table whose data + table
// metadata live on S3 (SQLite catalog stays local), then re-open it to prove the metadata
// reads back from S3. Gated on CLINK_S3_TEST_ENDPOINT (+ CLINK_S3_TEST_BUCKET); skips in
// CI. PyIceberg-over-S3 is the external data oracle (set CLINK_ICEBERG_KEEP_DIR for the
// local catalog.db so an external reader can load the table).
TEST(IcebergS3Live, WritesToS3AndReopens) {
    const char* endpoint = std::getenv("CLINK_S3_TEST_ENDPOINT");
    if (endpoint == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT (+ CLINK_S3_TEST_BUCKET) to run";
    }
    const char* bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    const std::string bkt = bucket != nullptr ? bucket : "clink-iceberg";
    const char* ak = std::getenv("AWS_ACCESS_KEY_ID");
    const char* sk = std::getenv("AWS_SECRET_ACCESS_KEY");

    const std::string warehouse =
        "s3://" + bkt + "/wh_" + std::to_string(static_cast<long>(::getpid()));
    const char* keep = std::getenv("CLINK_ICEBERG_KEEP_DIR");
    const auto cat =
        keep != nullptr
            ? fs::path(keep) / "catalog.db"
            : fs::temp_directory_path() /
                  ("clink_ice_s3_" + std::to_string(static_cast<long>(::getpid())) + ".db");
    fs::remove(cat);

    auto make_opts = [&]() {
        IcebergRowSinkOptions o;
        o.warehouse = warehouse;
        o.namespace_levels = {"default"};
        o.table = "events";
        o.catalog_uri = cat.string();  // SQLite catalog stays local
        o.file_io_props["s3.endpoint"] = endpoint;
        o.file_io_props["s3.path-style-access"] = "true";  // required for MinIO
        o.file_io_props["s3.region"] = "us-east-1";
        if (ak != nullptr)
            o.file_io_props["s3.access-key-id"] = ak;
        if (sk != nullptr)
            o.file_io_props["s3.secret-access-key"] = sk;
        o.batcher = make_row_columnar_arrow_batcher(schema());
        return o;
    };

    // Write two snapshots to S3.
    {
        auto sink = make_iceberg_row_sink(make_opts());
        ASSERT_NO_THROW(sink->open());
        Batch<Row> b0;
        b0.emplace(make_row(1, "a"));
        b0.emplace(make_row(2, "b"));
        sink->on_data(b0);
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
        Batch<Row> b1;
        b1.emplace(make_row(3, "c"));
        sink->on_data(b1);
        ASSERT_NO_THROW(sink->close());
    }
    // Re-open the now-existing table: open() does LoadTable, which reads the table metadata
    // back from S3 (proves the S3 write + commit round-trips).
    {
        auto sink = make_iceberg_row_sink(make_opts());
        EXPECT_NO_THROW(sink->open());
        EXPECT_NO_THROW(sink->close());
    }

    if (keep == nullptr) {
        fs::remove(cat);
    }
}
