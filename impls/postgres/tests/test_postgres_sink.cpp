// Unit-test surface for PostgresSink that does not require a live
// Postgres. The real INSERT path is exercised in
// tests/integration/test_postgres_sink_integration.cpp under Docker.

#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_sink.hpp"

namespace {

TEST(PostgresSinkUnit, IsRealImplementationWhenBuiltWithLibpq) {
    EXPECT_TRUE(clink::PostgresSink::is_real_implementation());
}

TEST(PostgresSinkUnit, EmptySqlThrowsAtConstruction) {
    clink::PostgresSink::Options opts;
    opts.conninfo = "host=127.0.0.1 dbname=nope user=nope";
    // sql intentionally empty.
    EXPECT_THROW({ clink::PostgresSink sink(opts); }, std::invalid_argument);
}

TEST(PostgresSinkUnit, OpenWithUnreachableConninfoThrows) {
    // 127.0.0.1:1 is a reserved low port; libpq fails fast on connect.
    clink::PostgresSink::Options opts;
    opts.conninfo = "host=127.0.0.1 port=1 dbname=does_not_exist user=nope connect_timeout=1";
    opts.sql = "INSERT INTO sink_unit_t VALUES ($1)";
    clink::PostgresSink sink(opts);
    EXPECT_THROW(sink.open(), std::runtime_error);
}

}  // namespace
