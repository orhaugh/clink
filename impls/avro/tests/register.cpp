// Test fixture init for the Avro tests. The Avro impl ships only
// Codec<T> templates so there's nothing to install at the registry
// level - this file exists so test compilation has a TU that links
// `clink::avro::install` (and so the build asserts the symbol exists).

#include <gtest/gtest.h>

#include "clink/avro/install.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

class AvroTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        clink::plugin::PluginRegistry reg;
        clink::avro::install(reg);
    }
};

::testing::Environment* const kAvroEnv =
    ::testing::AddGlobalTestEnvironment(new AvroTestEnvironment);

}  // namespace
