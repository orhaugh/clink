// Verifies clink::s3::install() makes the s3_text_sink factory
// reachable through the RunnerRegistry.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(S3FactoryRegistration, S3TextSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("s3_text_sink", "string"), nullptr);
}

TEST(S3FactoryRegistration, S3ExactlyOnceSinkIsRegistered) {
    // The multipart-complete-on-commit 2PC sink selected by exactly_once.
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("s3_2pc_string_sink", "string"), nullptr);
}

}  // namespace
