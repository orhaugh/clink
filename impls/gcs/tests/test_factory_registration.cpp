// Verifies clink::gcs::install() registers the Parquet sink + source factories on both channels.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(GcsFactoryRegistration, ParquetSinkAndSourceAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("gcs_parquet_int64_sink", "int64"), nullptr);
    EXPECT_NE(rr.find_sink("gcs_parquet_string_sink", "string"), nullptr);
    EXPECT_NE(rr.find_source("gcs_parquet_int64_source", "int64"), nullptr);
    EXPECT_NE(rr.find_source("gcs_parquet_string_source", "string"), nullptr);
}

}  // namespace
