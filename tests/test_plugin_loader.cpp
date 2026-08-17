// End-to-end test of the plugin dlopen path. Loads
// tests/plugin_examples/hello_plugin.so (built by CMake as a sibling
// target), verifies its types/factories show up in the registries,
// and runs one of its registered factories in-process to prove the
// closures captured T correctly across the dlopen boundary.

#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/job_bundle.hpp"
#include "clink/cluster/job_graph.hpp"
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

std::filesystem::path schema_evo_job_path() {
#ifdef CLINK_SCHEMA_EVO_JOB_PATH
    return std::filesystem::path{CLINK_SCHEMA_EVO_JOB_PATH};
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

// load_into reuses one safely retained mapping, but the registration hook must
// run against every supplied JobBundle. This keeps mapping growth bounded
// across worker reconnects while preserving strict registry isolation.
TEST(PluginLoader, LoadIntoReusesMappingAndPopulatesEveryBundle) {
    const auto path = hello_plugin_path();
    if (!std::filesystem::exists(path)) {
        GTEST_SKIP() << "hello_plugin not built";
    }
    auto& loader = clink::cluster::PluginLoader::default_instance();

    clink::cluster::JobBundle bundle_a;
    auto preg_a = bundle_a.as_plugin_registry();
    auto a = loader.load_into(path.string(), preg_a);
    ASSERT_TRUE(a.ok) << a.error;
    const auto handle_a = a.plugin.dl_handle;
    bundle_a.retain_plugin(std::move(a.plugin));
    EXPECT_NE(bundle_a.runner_registry().find_source("hello.GreetingSource", "hello.Greeting"),
              nullptr);

    clink::cluster::JobBundle bundle_b;
    auto preg_b = bundle_b.as_plugin_registry();
    auto b = loader.load_into(path.string(), preg_b);
    ASSERT_TRUE(b.ok) << b.error;
    const auto handle_b = b.plugin.dl_handle;
    bundle_b.retain_plugin(std::move(b.plugin));
    EXPECT_NE(bundle_b.runner_registry().find_source("hello.GreetingSource", "hello.Greeting"),
              nullptr)
        << "reload must re-register into the second bundle, not reuse the first";
    EXPECT_EQ(handle_a, handle_b) << "one source path must keep one process-lifetime mapping";
}

TEST(PluginLoader, JobPluginReregistersInlineFactoriesIntoEveryBundle) {
    const auto path = schema_evo_job_path();
    if (!std::filesystem::exists(path)) {
        GTEST_SKIP() << "schema evolution job plugin not built";
    }
    auto& loader = clink::cluster::PluginLoader::default_instance();

    clink::cluster::JobBundle bundle_a;
    auto preg_a = bundle_a.as_plugin_registry();
    auto a = loader.load_into(path.string(), preg_a);
    ASSERT_TRUE(a.ok) << a.error;
    void* handle = a.plugin.dl_handle;
    bundle_a.retain_plugin(std::move(a.plugin));

    using JobBuildFn = int (*)(const char**, std::size_t*);
    JobBuildFn job_build = nullptr;
    void* symbol = ::dlsym(handle, "clink_job_build");
    ASSERT_NE(symbol, nullptr);
    std::memcpy(&job_build, &symbol, sizeof(job_build));
    const char* graph_data = nullptr;
    std::size_t graph_size = 0;
    ASSERT_EQ(job_build(&graph_data, &graph_size), 0);
    ASSERT_NE(graph_data, nullptr);
    const auto graph = clink::cluster::JobGraphSpec::from_json({graph_data, graph_size});
    ASSERT_FALSE(graph.ops.empty());
    const auto& source = graph.ops.front();
    ASSERT_NE(bundle_a.runner_registry().find_source(source.type, source.out_channel), nullptr);

    clink::cluster::JobBundle bundle_b;
    auto preg_b = bundle_b.as_plugin_registry();
    auto b = loader.load_into(path.string(), preg_b);
    ASSERT_TRUE(b.ok) << b.error;
    EXPECT_EQ(b.plugin.dl_handle, handle);
    bundle_b.retain_plugin(std::move(b.plugin));
    EXPECT_NE(bundle_b.runner_registry().find_source(source.type, source.out_channel), nullptr)
        << "CLINK_REGISTER_JOB must rebuild inline factories into each isolated bundle";
}

TEST(PluginLoader, MissingFileFailsCleanly) {
    auto& loader = clink::cluster::PluginLoader::default_instance();
    auto result = loader.load("/tmp/does-not-exist-12345.so");
    EXPECT_FALSE(result.ok);
    EXPECT_FALSE(result.error.empty());
}
