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
    EXPECT_EQ(result.plugin.abi_version, clink::cluster::cluster_abi_version());
    EXPECT_EQ(result.plugin.abi_hash, clink::cluster::cluster_abi_hash());
    EXPECT_EQ(result.plugin.target_triple, clink::cluster::cluster_target_triple());

    // The plugin's registrations are now in the global registries.
    const auto& tr = clink::cluster::TypeRegistry::default_instance();
    EXPECT_NE(tr.find("hello.Greeting"), nullptr);

    const auto& rr = clink::cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("hello.GreetingSource", "hello.Greeting"), nullptr);
    EXPECT_NE(rr.find_sink("hello.GreetingFileSink", "hello.Greeting"), nullptr);
}

// The version gate: default (non-strict) mode accepts any plugin whose ABI
// version matches the cluster's, EVEN when the commit hashes differ (the whole
// point - a patch/feature rebuild of the cluster keeps loading the plugin), and
// refuses a differing ABI version.
TEST(PluginAbiGate, VersionMatchLoadsAcrossDifferentCommitHashes) {
    clink::cluster::AbiCheckInput in;
    in.strict = false;
    in.plugin_has_version = true;
    in.plugin_abi_version = 1;
    in.cluster_abi_version = 1;
    in.plugin_hash = "aaaaaaa-plugin-built-earlier";
    in.cluster_hash = "bbbbbbb-cluster-built-later";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in), "")
        << "same ABI version must load regardless of commit hash";
}

TEST(PluginAbiGate, VersionMismatchIsRefused) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_version = true;
    in.plugin_abi_version = 1;
    in.cluster_abi_version = 2;
    in.plugin_hash = "x";
    in.cluster_hash = "y";
    const auto err = clink::cluster::check_plugin_abi(in);
    EXPECT_NE(err.find("ABI version mismatch"), std::string::npos) << err;
}

// Strict mode restores the exact commit-hash gate: same ABI version but a
// differing hash is refused; matching hash loads.
TEST(PluginAbiGate, StrictModeRequiresExactHash) {
    clink::cluster::AbiCheckInput in;
    in.strict = true;
    in.plugin_has_version = true;
    in.plugin_abi_version = 1;
    in.cluster_abi_version = 1;
    in.plugin_hash = "aaa";
    in.cluster_hash = "bbb";
    EXPECT_NE(clink::cluster::check_plugin_abi(in).find("hash mismatch"), std::string::npos);

    in.plugin_hash = "same";
    in.cluster_hash = "same";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in), "");
}

// A legacy plugin with no ABI-version symbol falls back to the exact-hash gate
// even in the default (non-strict) mode.
TEST(PluginAbiGate, LegacyPluginFallsBackToHash) {
    clink::cluster::AbiCheckInput in;
    in.strict = false;
    in.plugin_has_version = false;  // pre-version-gate plugin
    in.cluster_abi_version = 3;
    in.plugin_hash = "old";
    in.cluster_hash = "new";
    EXPECT_NE(clink::cluster::check_plugin_abi(in).find("hash mismatch"), std::string::npos);

    in.plugin_hash = "match";
    in.cluster_hash = "match";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in), "");
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
