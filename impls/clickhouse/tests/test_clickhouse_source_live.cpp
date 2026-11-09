// The ClickHouse SELECT source against a live server. SKIPPED unless
// CLINK_CLICKHOUSE_TEST_HOST names one; scripts/clickhouse-live.sh runs it on
// each line's plain server and fails when any case is skipped.
//
// Each case drives the source as a job does at the operator level: one source
// reads part of the result and is checkpointed into a state backend, the table
// changes or not, and a second source restores from that checkpoint, runs the
// query again and reads to the end. Rows repeated are allowed (at least once);
// a row skipped is not.

#include <cstdint>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <clickhouse/client.h>
#include <gtest/gtest.h>

#include "clink/connectors/clickhouse_source.hpp"
#include "clink/core/record.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::CheckpointId;
using clink::ClickHouseRow;
using clink::ClickHouseSource;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StreamElement;

namespace {

bool server_configured() {
    const char* host = std::getenv("CLINK_CLICKHOUSE_TEST_HOST");
    return host != nullptr && *host != '\0';
}

#define REQUIRE_SERVER()                                                                        \
    do {                                                                                        \
        if (!server_configured()) {                                                             \
            GTEST_SKIP() << "set CLINK_CLICKHOUSE_TEST_HOST (scripts/clickhouse-live.sh does)"; \
        }                                                                                       \
    } while (0)

std::uint16_t port() {
    const char* p = std::getenv("CLINK_CLICKHOUSE_TEST_PORT");
    return p != nullptr && *p != '\0' ? static_cast<std::uint16_t>(std::stoi(p))
                                      : std::uint16_t{9000};
}

// The test's own connection, for the table the source reads.
clickhouse::Client client() {
    clickhouse::ClientOptions o;
    o.SetHost(std::getenv("CLINK_CLICKHOUSE_TEST_HOST"));
    o.SetPort(port());
    if (const char* user = std::getenv("CLINK_CLICKHOUSE_TEST_USER"); user != nullptr) {
        o.SetUser(user);
    }
    if (const char* pw = std::getenv("CLINK_CLICKHOUSE_TEST_PASSWORD"); pw != nullptr) {
        o.SetPassword(pw);
    }
    o.SetRethrowException(true);
    return clickhouse::Client(o);
}

std::string table_name() {
    static int n = 0;
    return "clink_select_src_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(++n);
}

// A table of the given ids, each with a value naming it, replacing any
// earlier contents.
void fill(clickhouse::Client& c, const std::string& t, const std::vector<int>& ids) {
    c.Execute("CREATE TABLE IF NOT EXISTS " + t +
              " (id UInt64, v String) ENGINE = MergeTree ORDER BY id");
    c.Execute("TRUNCATE TABLE " + t);
    std::string values;
    for (const int id : ids) {
        values += (values.empty() ? "" : ",") + std::string{"("} + std::to_string(id) + ", 'v" +
                  std::to_string(id) + "')";
    }
    if (!values.empty()) {
        c.Execute("INSERT INTO " + t + " VALUES " + values);
    }
}

ClickHouseSource::Options options(const std::string& query, std::size_t batch) {
    ClickHouseSource::Options o;
    o.host = std::getenv("CLINK_CLICKHOUSE_TEST_HOST");
    o.port = port();
    if (const char* user = std::getenv("CLINK_CLICKHOUSE_TEST_USER"); user != nullptr) {
        o.user = user;
    }
    if (const char* pw = std::getenv("CLINK_CLICKHOUSE_TEST_PASSWORD"); pw != nullptr) {
        o.password = pw;
    }
    o.query = query;
    o.batch_size = batch;
    return o;
}

// The first column of what one produce() call, or every call, emits.
std::vector<std::string> read(ClickHouseSource& src, bool to_end) {
    std::vector<std::string> ids;
    Emitter<ClickHouseRow> em(
        Emitter<ClickHouseRow>::Forward([&ids](StreamElement<ClickHouseRow> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    ids.push_back(r.value().values()[0]);
                }
            }
            return true;
        }));
    while (src.produce(em) && to_end) {
    }
    return ids;
}

std::vector<std::string> ids(const std::vector<int>& v) {
    std::vector<std::string> out;
    for (const int i : v) {
        out.push_back(std::to_string(i));
    }
    return out;
}

const OperatorId kOp{4343};

}  // namespace

// Over the same rows in the same order the restored source resumes after
// those the checkpoint covered.
TEST(ClickHouseSourceLive, ARestoreOverTheSameRowsResumesAfterThem) {
    REQUIRE_SERVER();
    auto c = client();
    const auto t = table_name();
    fill(c, t, {1, 2, 3, 4, 5, 6});
    const auto q = "SELECT id, v FROM " + t + " ORDER BY id";
    InMemoryStateBackend backend;
    ClickHouseSource first(options(q, 3));
    first.open();
    EXPECT_EQ(read(first, false), ids({1, 2, 3}));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();
    ClickHouseSource second(options(q, 3));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read(second, true), ids({4, 5, 6}));
    second.close();
    c.Execute("DROP TABLE " + t);
}

// A row removed among those the checkpoint covered moves the rest up one
// place: resumed by position, the source skipped the row that moved into the
// last covered place (id 4). It now sees the rows differ and emits every row
// again.
TEST(ClickHouseSourceLive, ARestoreAfterAnEarlierRowWasRemovedEmitsEveryRowAgain) {
    REQUIRE_SERVER();
    auto c = client();
    const auto t = table_name();
    fill(c, t, {1, 2, 3, 4, 5, 6});
    const auto q = "SELECT id, v FROM " + t + " ORDER BY id";
    InMemoryStateBackend backend;
    ClickHouseSource first(options(q, 3));
    first.open();
    EXPECT_EQ(read(first, false), ids({1, 2, 3}));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();
    fill(c, t, {1, 3, 4, 5, 6});
    ClickHouseSource second(options(q, 3));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read(second, true), ids({1, 3, 4, 5, 6}));
    second.close();
    c.Execute("DROP TABLE " + t);
}

// Rows added after those the checkpoint covered leave them in place.
TEST(ClickHouseSourceLive, ARestoreAfterRowsWereAddedAtTheEndResumes) {
    REQUIRE_SERVER();
    auto c = client();
    const auto t = table_name();
    fill(c, t, {1, 2, 3, 4});
    const auto q = "SELECT id, v FROM " + t + " ORDER BY id";
    InMemoryStateBackend backend;
    ClickHouseSource first(options(q, 2));
    first.open();
    EXPECT_EQ(read(first, false), ids({1, 2}));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();
    fill(c, t, {1, 2, 3, 4, 5});
    ClickHouseSource second(options(q, 2));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read(second, true), ids({3, 4, 5}));
    second.close();
    c.Execute("DROP TABLE " + t);
}

// A position from an earlier release holds no hash: every row is emitted
// again rather than resumed on trust.
TEST(ClickHouseSourceLive, AnEarlierReleasesPositionEmitsEveryRowAgain) {
    REQUIRE_SERVER();
    auto c = client();
    const auto t = table_name();
    fill(c, t, {1, 2, 3});
    InMemoryStateBackend backend;
    const std::string key = "__clickhouse_source_row__";
    std::string legacy(8, '\0');
    legacy[0] = 2;
    backend.put_operator_state(kOp,
                               clink::StateBackend::KeyView{key.data(), key.size()},
                               clink::StateBackend::ValueView{legacy.data(), legacy.size()});
    ClickHouseSource src(options("SELECT id, v FROM " + t + " ORDER BY id", 256));
    ASSERT_TRUE(src.restore_offset(backend, kOp));
    src.open();
    EXPECT_EQ(read(src, true), ids({1, 2, 3}));
    src.close();
    c.Execute("DROP TABLE " + t);
}
