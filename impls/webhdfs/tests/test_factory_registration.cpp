// Verifies clink::webhdfs::install() registers the Parquet sink + source factories on both
// channels.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(WebHdfsFactoryRegistration, ParquetSinkAndSourceAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("webhdfs_parquet_int64_sink", "int64"), nullptr);
    EXPECT_NE(rr.find_sink("webhdfs_parquet_string_sink", "string"), nullptr);
    EXPECT_NE(rr.find_source("webhdfs_parquet_int64_source", "int64"), nullptr);
    EXPECT_NE(rr.find_source("webhdfs_parquet_string_source", "string"), nullptr);
}

// The SQL planner emits <connector>_2pc_string_sink for delivery_guarantee='exactly_once'
// (src/sql/physical_plan.cpp); a name it emits that nothing registers fails only at deploy.
TEST(WebHdfsFactoryRegistration, ExactlyOnceParquetSinksThePlannerNamesAreRegistered) {
    const auto& rr = clink::cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("webhdfs_parquet_2pc_int64_sink", "int64"), nullptr);
    EXPECT_NE(rr.find_sink("webhdfs_parquet_2pc_string_sink", "string"), nullptr);
}

}  // namespace
