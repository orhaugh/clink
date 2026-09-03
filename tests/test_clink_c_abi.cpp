// libclink C ABI end to end. This suite deliberately links ONLY the shared
// library (plus Arrow C++ on the consumer side to import the C stream, and
// gtest) - the engine, its registries and every connector live inside
// libclink, mirroring a real embedding. Everything crosses the boundary
// through the pure-C surface in clink/embed/clink.h.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>

#include "clink/embed/clink.h"

namespace {

namespace fs = std::filesystem;

void write_orders(const fs::path& path) {
    std::ofstream out(path, std::ios::trunc);
    out << R"({"user_id":1,"amount":10})" << "\n"
        << R"({"user_id":2,"amount":20})" << "\n"
        << R"({"user_id":1,"amount":30})" << "\n"
        << R"({"user_id":2,"amount":5})" << "\n"
        << R"({"user_id":1,"amount":7})" << "\n";
}

std::string pipeline_sql(const fs::path& in_path) {
    return "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
           "WITH (connector='file', format='json', path='" +
           in_path.string() +
           "');"
           "CREATE TABLE results (user_id BIGINT, amount BIGINT) "
           "WITH (connector='collect');"
           "INSERT INTO results SELECT user_id, amount FROM orders";
}

TEST(ClinkCAbi, AbiVersionMatchesHeader) {
    EXPECT_EQ(clink_abi_version(), CLINK_EMBED_ABI_VERSION);
}

// --- the v2 growth contract (design record 011) -----------------------------

TEST(ClinkCAbi, AbiVersionTwoIsTheStructSizeContract) {
    // v2 introduced clink_engine_options.struct_size so the struct can grow
    // without another bump; within 1.x this number never moves again.
    EXPECT_EQ(clink_abi_version(), 2);
}

TEST(ClinkCAbi, LibraryVersionIsAReleaseString) {
    const char* v = clink_version();
    ASSERT_NE(v, nullptr);
    EXPECT_GT(std::strlen(v), 0u);
    EXPECT_NE(std::string{v}, "unknown");
    // Static storage: the same pointer every call, for the life of the process.
    EXPECT_EQ(v, clink_version());
}

TEST(ClinkCAbi, InitialisedOptionsOpen) {
    clink_engine_options opts = CLINK_ENGINE_OPTIONS_INIT;
    EXPECT_EQ(opts.struct_size, sizeof(clink_engine_options));
    EXPECT_EQ(opts.parallelism, 0u);
    EXPECT_EQ(opts.catalog_dir, nullptr);
    opts.parallelism = 2;
    clink_engine* e = clink_engine_open(&opts);
    ASSERT_NE(e, nullptr) << clink_open_error();
    clink_engine_close(e);
}

TEST(ClinkCAbi, ZeroStructSizeIsRefusedByName) {
    clink_engine_options opts{};  // zero-initialised WITHOUT the INIT macro
    ASSERT_EQ(opts.struct_size, 0u);
    EXPECT_EQ(clink_engine_open(&opts), nullptr);
    EXPECT_NE(std::string{clink_open_error()}.find("struct_size"), std::string::npos);
}

TEST(ClinkCAbi, AShorterStructIsReadOnlyAsFarAsItDeclares) {
    // A caller compiled against an earlier v2 header whose struct ended after
    // `parallelism`. Emulated with a buffer that declares that size and holds
    // poison where the later pointer fields would be: the library must not
    // read them, or it would dereference 0xFF.. as a C string.
    alignas(clink_engine_options) unsigned char buf[sizeof(clink_engine_options)];
    std::memset(buf, 0xFF, sizeof(buf));
    const std::size_t declared =
        offsetof(clink_engine_options, parallelism) + sizeof(std::uint32_t);
    const std::uint32_t parallelism = 1;
    std::memcpy(buf + offsetof(clink_engine_options, struct_size), &declared, sizeof(declared));
    std::memcpy(
        buf + offsetof(clink_engine_options, parallelism), &parallelism, sizeof(parallelism));
    const auto* opts = reinterpret_cast<const clink_engine_options*>(buf);
    clink_engine* e = clink_engine_open(opts);
    ASSERT_NE(e, nullptr) << clink_open_error();
    clink_engine_close(e);
}

TEST(ClinkCAbi, ALargerStructIsReadOnlyAsFarAsTheLibraryKnows) {
    // A caller compiled against a FUTURE header with an appended field: it
    // declares a larger size, and this library reads only what it knows.
    struct Future {
        clink_engine_options base;
        std::uint64_t appended_later;
    };
    Future f{};
    f.base = CLINK_ENGINE_OPTIONS_INIT;
    f.base.struct_size = sizeof(Future);
    f.base.parallelism = 1;
    f.appended_later = 7;
    clink_engine* e = clink_engine_open(&f.base);
    ASSERT_NE(e, nullptr) << clink_open_error();
    clink_engine_close(e);
}

TEST(ClinkCAbi, ExecErrorSetsLastError) {
    clink_engine* e = clink_engine_open(nullptr);
    ASSERT_NE(e, nullptr) << clink_open_error();
    EXPECT_NE(clink_exec(e, "THIS IS NOT SQL"), 0);
    EXPECT_NE(std::string{clink_last_error(e)}.size(), 0u);
    // A subsequent good call clears the error.
    EXPECT_EQ(clink_exec(e,
                         "CREATE TABLE t (a BIGINT) "
                         "WITH (connector='file', format='json', path='/tmp/x')"),
              0)
        << clink_last_error(e);
    EXPECT_EQ(std::string{clink_last_error(e)}, "");
    clink_engine_close(e);
}

TEST(ClinkCAbi, CollectEndToEndThroughArrowCStream) {
    const auto in_path = fs::temp_directory_path() / "clink_cabi_orders.ndjson";
    fs::remove(in_path);
    write_orders(in_path);

    clink_engine* e = clink_engine_open(nullptr);
    ASSERT_NE(e, nullptr) << clink_open_error();
    ASSERT_EQ(clink_exec(e, pipeline_sql(in_path).c_str()), 0) << clink_last_error(e);
    ASSERT_EQ(clink_job_count(e), 1u);
    EXPECT_NE(clink_job_id_at(e, 0), 0u);

    ArrowArrayStream stream;
    ASSERT_EQ(clink_collect_stream(e, "results", &stream), 0) << clink_last_error(e);

    // Consumer side: import through Arrow C++ (zero-copy across the C ABI).
    auto reader_r = arrow::ImportRecordBatchReader(&stream);
    ASSERT_TRUE(reader_r.ok()) << reader_r.status().ToString();
    auto reader = *reader_r;
    ASSERT_EQ(reader->schema()->num_fields(), 2);
    EXPECT_TRUE(reader->schema()->field(1)->type()->Equals(arrow::int64()));

    std::int64_t rows = 0;
    std::int64_t amount_sum = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto st = reader->ReadNext(&batch);
        ASSERT_TRUE(st.ok()) << st.ToString();
        if (!batch) {
            break;
        }
        rows += batch->num_rows();
        const auto& amounts = static_cast<const arrow::Int64Array&>(*batch->column(1));
        for (std::int64_t i = 0; i < amounts.length(); ++i) {
            amount_sum += amounts.Value(i);
        }
    }
    EXPECT_EQ(rows, 5);
    EXPECT_EQ(amount_sum, 72);

    EXPECT_EQ(clink_await_all(e, 30'000), 0) << clink_last_error(e);
    // Second consumer on the same table is refused.
    ArrowArrayStream second;
    EXPECT_NE(clink_collect_stream(e, "results", &second), 0);
    clink_engine_close(e);
    fs::remove(in_path);
}

TEST(ClinkCAbi, EngineCloseAbortsALiveStream) {
    clink_engine* e = clink_engine_open(nullptr);
    ASSERT_NE(e, nullptr) << clink_open_error();
    // A collect table that never gets a producing job: a blocked reader
    // must be woken with an error by engine close, not hang or crash.
    ASSERT_EQ(clink_exec(e, "CREATE TABLE never (a BIGINT) WITH (connector='collect')"), 0)
        << clink_last_error(e);
    ArrowArrayStream stream;
    ASSERT_EQ(clink_collect_stream(e, "never", &stream), 0) << clink_last_error(e);

    ArrowSchema schema;
    ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
    schema.release(&schema);

    clink_engine_close(e);

    ArrowArray array;
    const int rc = stream.get_next(&stream, &array);
    EXPECT_NE(rc, 0);  // aborted, not end-of-stream
    const char* err = stream.get_last_error(&stream);
    EXPECT_NE(err, nullptr);
    stream.release(&stream);
}

TEST(ClinkCAbi, CollectStreamOnUnknownTableFails) {
    clink_engine* e = clink_engine_open(nullptr);
    ASSERT_NE(e, nullptr) << clink_open_error();
    ArrowArrayStream stream;
    EXPECT_NE(clink_collect_stream(e, "missing", &stream), 0);
    EXPECT_NE(std::string{clink_last_error(e)}.find("missing"), std::string::npos);
    clink_engine_close(e);
}

}  // namespace
