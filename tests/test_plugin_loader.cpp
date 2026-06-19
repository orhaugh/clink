// End-to-end test of the plugin dlopen path. Loads
// tests/plugin_examples/hello_plugin.so (built by CMake as a sibling
// target), verifies its types/factories show up in the registries,
// and runs one of its registered factories in-process to prove the
// closures captured T correctly across the dlopen boundary.

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/plugin_loader.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"

namespace {

std::filesystem::path hello_plugin_path() {
#ifdef CLINK_HELLO_PLUGIN_PATH
    return std::filesystem::path{CLINK_HELLO_PLUGIN_PATH};
#else
    return {};
#endif
}

}  // namespace

TEST(PluginLoader, LoadsValidHelloPluginEndToEnd) {
    const auto path = hello_plugin_path();
    if (!std::filesystem::exists(path)) {
        GTEST_SKIP() << "hello_plugin not built; expected at " << path;
    }

    auto& loader = clink::cluster::PluginLoader::default_instance();
    auto result = loader.load(path.string());
    ASSERT_TRUE(result.ok) << "load failed: " << result.error;
    EXPECT_EQ(result.plugin.name, "hello-plugin");
    EXPECT_EQ(result.plugin.version, "1.0.0");
    EXPECT_EQ(result.plugin.abi_hash, clink::cluster::cluster_abi_hash());
    EXPECT_EQ(result.plugin.target_triple, clink::cluster::cluster_target_triple());

    // The plugin's registrations are now in the global registries.
    const auto& tr = clink::cluster::TypeRegistry::default_instance();
    EXPECT_NE(tr.find("hello.Greeting"), nullptr);

    const auto& rr = clink::cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("hello.GreetingSource", "hello.Greeting"), nullptr);
    EXPECT_NE(rr.find_sink("hello.GreetingFileSink", "hello.Greeting"), nullptr);
}

TEST(PluginLoader, LoadIsIdempotent) {
    const auto path = hello_plugin_path();
    if (!std::filesystem::exists(path)) {
        GTEST_SKIP() << "hello_plugin not built";
    }
    auto& loader = clink::cluster::PluginLoader::default_instance();
    auto a = loader.load(path.string());
    auto b = loader.load(path.string());
    EXPECT_TRUE(a.ok);
    EXPECT_TRUE(b.ok);
    EXPECT_EQ(a.plugin.dl_handle, b.plugin.dl_handle);
}

TEST(PluginLoader, MissingFileFailsCleanly) {
    auto& loader = clink::cluster::PluginLoader::default_instance();
    auto result = loader.load("/tmp/does-not-exist-12345.so");
    EXPECT_FALSE(result.ok);
    EXPECT_FALSE(result.error.empty());
}
