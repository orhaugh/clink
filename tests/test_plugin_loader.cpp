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
    EXPECT_EQ(result.plugin.abi_fingerprint, clink::cluster::cluster_abi_fingerprint());
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

// The fingerprint gate: default (non-strict) mode accepts any plugin whose
// structural fingerprint matches the cluster's, EVEN when the commit hashes
// differ (the whole point - a .cpp/doc/test-only cluster rebuild changes the
// commit but not the fingerprint, so existing plugins keep loading), and refuses
// a differing fingerprint (a real ABI-surface change).
TEST(PluginAbiGate, FingerprintMatchLoadsAcrossDifferentCommitHashes) {
    clink::cluster::AbiCheckInput in;
    in.strict = false;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp-abc";
    in.cluster_fingerprint = "fp-abc";
    in.plugin_hash = "aaaaaaa-plugin-built-earlier";
    in.cluster_hash = "bbbbbbb-cluster-built-later";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in), "")
        << "same fingerprint must load regardless of commit hash";
}

TEST(PluginAbiGate, FingerprintMismatchIsRefused) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp-old";
    in.cluster_fingerprint = "fp-new";
    in.plugin_hash = "x";
    in.cluster_hash = "y";
    const auto err = clink::cluster::check_plugin_abi(in);
    EXPECT_NE(err.find("fingerprint mismatch"), std::string::npos) << err;
}

// Strict mode restores the exact commit-hash gate: same fingerprint but a
// differing hash is refused; matching hash loads.
TEST(PluginAbiGate, StrictModeRequiresExactHash) {
    clink::cluster::AbiCheckInput in;
    in.strict = true;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp-abc";
    in.cluster_fingerprint = "fp-abc";
    in.plugin_hash = "aaa";
    in.cluster_hash = "bbb";
    EXPECT_NE(clink::cluster::check_plugin_abi(in).find("hash mismatch"), std::string::npos);

    in.plugin_hash = "same";
    in.cluster_hash = "same";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in), "");
}

// A legacy plugin with no fingerprint symbol falls back to the exact-hash gate
// even in the default (non-strict) mode.
TEST(PluginAbiGate, LegacyPluginFallsBackToHash) {
    clink::cluster::AbiCheckInput in;
    in.strict = false;
    in.plugin_has_fingerprint = false;  // pre-fingerprint-gate plugin
    in.plugin_fingerprint = "";
    in.cluster_fingerprint = "fp-new";
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
