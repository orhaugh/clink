// The schema registry library installs nothing at the registry level; this
// TU exists so the test binary links clink::schema_registry::install and the
// build asserts the symbol exists, as the other impl test suites do.
#include <gtest/gtest.h>

#include "clink/plugin/plugin.hpp"
#include "clink/schema_registry/install.hpp"

namespace {
class SchemaRegistryTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        clink::plugin::PluginRegistry reg;
        clink::schema_registry::install(reg);
    }
};
::testing::Environment* const kEnv =
    ::testing::AddGlobalTestEnvironment(new SchemaRegistryTestEnvironment);
}  // namespace
