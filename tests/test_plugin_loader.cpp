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

std::filesystem::path mismatched_fingerprint_plugin_path() {
#ifdef CLINK_MISMATCHED_FINGERPRINT_PLUGIN_PATH
    return std::filesystem::path{CLINK_MISMATCHED_FINGERPRINT_PLUGIN_PATH};
#else
    return {};
#endif
}

std::filesystem::path mismatched_triple_plugin_path() {
#ifdef CLINK_MISMATCHED_TRIPLE_PLUGIN_PATH
    return std::filesystem::path{CLINK_MISMATCHED_TRIPLE_PLUGIN_PATH};
#else
    return {};
#endif
}

std::filesystem::path mismatched_toolchain_plugin_path() {
#ifdef CLINK_MISMATCHED_TOOLCHAIN_PLUGIN_PATH
    return std::filesystem::path{CLINK_MISMATCHED_TOOLCHAIN_PLUGIN_PATH};
#else
    return {};
#endif
}

// Loads a deliberately incompatible fixture .so through a private loader and
// asserts the refusal: !ok, the expected classification, the expected message
// content, and - via the fixture's poisoned register hook (rc=99) - that
// registration never ran.
void expect_so_refused(const std::filesystem::path& path,
                       clink::cluster::PluginLoadFailure want,
                       const std::string& want_in_error) {
    if (!std::filesystem::exists(path)) {
        GTEST_SKIP() << "mismatched-abi fixture not built; expected at " << path;
    }
    clink::cluster::PluginLoader loader;
    clink::cluster::JobBundle bundle;
    auto preg = bundle.as_plugin_registry();
    auto result = loader.load_into(path.string(), preg);
    ASSERT_FALSE(result.ok) << "an incompatible plugin was admitted";
    EXPECT_EQ(result.failure, want) << result.error;
    EXPECT_NE(result.error.find(want_in_error), std::string::npos) << result.error;
    EXPECT_EQ(result.error.find("register hook"), std::string::npos)
        << "the register hook ran on a plugin the gate should have refused: " << result.error;
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
    EXPECT_EQ(result.plugin.toolchain, clink::cluster::cluster_toolchain());

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
    const auto check = clink::cluster::check_plugin_abi(in);
    EXPECT_EQ(check.error, "") << "same fingerprint must load regardless of commit hash";
    EXPECT_EQ(check.failure, clink::cluster::PluginLoadFailure::None);
}

TEST(PluginAbiGate, FingerprintMismatchIsRefused) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp-old";
    in.cluster_fingerprint = "fp-new";
    in.plugin_hash = "x";
    in.cluster_hash = "y";
    const auto check = clink::cluster::check_plugin_abi(in);
    EXPECT_NE(check.error.find("fingerprint mismatch"), std::string::npos) << check.error;
    EXPECT_EQ(check.failure, clink::cluster::PluginLoadFailure::AbiMismatch);
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
    EXPECT_NE(clink::cluster::check_plugin_abi(in).error.find("hash mismatch"), std::string::npos);

    in.plugin_hash = "same";
    in.cluster_hash = "same";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in).error, "");
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
    EXPECT_NE(clink::cluster::check_plugin_abi(in).error.find("hash mismatch"), std::string::npos);

    in.plugin_hash = "match";
    in.cluster_hash = "match";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in).error, "");
}

// The target-triple gate, previously applied by the loader outside the pure
// function and therefore untestable without a real .so.
TEST(PluginAbiGate, TripleMismatchIsRefused) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp";
    in.cluster_fingerprint = "fp";
    in.plugin_triple = "linux-x86_64";
    in.cluster_triple = "darwin-arm64";
    const auto check = clink::cluster::check_plugin_abi(in);
    EXPECT_NE(check.error.find("target-triple mismatch"), std::string::npos) << check.error;
    EXPECT_EQ(check.failure, clink::cluster::PluginLoadFailure::TripleMismatch);
}

// The toolchain gate refuses a plugin whose stdlib / sanitizer identity
// differs from the cluster's - the same headers hash to the same fingerprint
// under ASan and without it, so the fingerprint alone cannot catch this.
TEST(PluginAbiGate, ToolchainMismatchIsRefused) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp";
    in.cluster_fingerprint = "fp";
    in.plugin_has_toolchain = true;
    in.plugin_toolchain = "libstdc++-cxx11abi1";
    in.cluster_toolchain = "libstdc++-cxx11abi1;tsan";
    const auto check = clink::cluster::check_plugin_abi(in);
    EXPECT_NE(check.error.find("toolchain mismatch"), std::string::npos) << check.error;
    EXPECT_EQ(check.failure, clink::cluster::PluginLoadFailure::ToolchainMismatch);
}

// A plugin without the toolchain symbol predates fingerprint v2 (which
// introduced it), so the fingerprint gate already refuses it; when the
// fingerprints DO match, the absent symbol must not refuse a compatible
// plugin.
TEST(PluginAbiGate, MissingToolchainSymbolSkipsTheToolchainGate) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp";
    in.cluster_fingerprint = "fp";
    in.plugin_has_toolchain = false;
    in.cluster_toolchain = "libc++";
    EXPECT_EQ(clink::cluster::check_plugin_abi(in).error, "");
}

// A fingerprint refusal with both manifests available names the headers that
// differ instead of only reporting two opaque hashes.
TEST(PluginAbiGate, FingerprintMismatchNamesTheDifferingHeaders) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp-old";
    in.cluster_fingerprint = "fp-new";
    in.plugin_manifest = "include/clink/runtime/dag.hpp=aaa\ninclude/clink/core/codec.hpp=x\n";
    in.cluster_manifest = "include/clink/runtime/dag.hpp=bbb\ninclude/clink/core/codec.hpp=x\n";
    const auto check = clink::cluster::check_plugin_abi(in);
    EXPECT_NE(check.error.find("include/clink/runtime/dag.hpp (changed)"), std::string::npos)
        << check.error;
    EXPECT_EQ(check.error.find("codec.hpp"), std::string::npos)
        << "an unchanged header must not be named: " << check.error;
}

// Identical manifests with differing fingerprints mean the difference is in
// the non-header material (build options, Arrow pin, ABI version) - say so
// rather than presenting an empty diff.
TEST(PluginAbiGate, FingerprintMismatchWithIdenticalManifestsBlamesTheOptions) {
    clink::cluster::AbiCheckInput in;
    in.plugin_has_fingerprint = true;
    in.plugin_fingerprint = "fp-asan-build";
    in.cluster_fingerprint = "fp-plain-build";
    in.plugin_manifest = "include/clink/runtime/dag.hpp=aaa\n";
    in.cluster_manifest = "include/clink/runtime/dag.hpp=aaa\n";
    const auto check = clink::cluster::check_plugin_abi(in);
    EXPECT_NE(check.error.find("build options"), std::string::npos) << check.error;
}

TEST(ManifestDiff, ChangedAndOneSidedEntriesAreNamed) {
    const std::string plugin =
        "a.hpp=1\n"
        "b.hpp=2\n"
        "c.hpp=3\n";
    const std::string cluster =
        "a.hpp=1\n"
        "b.hpp=CHANGED\n"
        "d.hpp=4\n";
    const auto diff = clink::cluster::summarise_manifest_diff(plugin, cluster, 10);
    EXPECT_NE(diff.find("b.hpp (changed)"), std::string::npos) << diff;
    EXPECT_NE(diff.find("c.hpp (only in plugin)"), std::string::npos) << diff;
    EXPECT_NE(diff.find("d.hpp (only in cluster)"), std::string::npos) << diff;
    EXPECT_EQ(diff.find("a.hpp"), std::string::npos) << diff;
}

TEST(ManifestDiff, TruncatesAtMaxNames) {
    std::string plugin;
    std::string cluster;
    for (int i = 0; i < 8; ++i) {
        plugin += "h" + std::to_string(i) + ".hpp=old\n";
        cluster += "h" + std::to_string(i) + ".hpp=new\n";
    }
    const auto diff = clink::cluster::summarise_manifest_diff(plugin, cluster, 3);
    EXPECT_NE(diff.find("and 5 more"), std::string::npos) << diff;
}

TEST(ManifestDiff, EqualOrEmptyManifestsProduceNothing) {
    EXPECT_EQ(clink::cluster::summarise_manifest_diff("a.hpp=1\n", "a.hpp=1\n", 5), "");
    EXPECT_EQ(clink::cluster::summarise_manifest_diff("", "a.hpp=1\n", 5), "");
    EXPECT_EQ(clink::cluster::summarise_manifest_diff("a.hpp=1\n", "", 5), "");
}

// The deploy-time fatal contract: exactly the three deterministic gate
// refusals fail the job with the cause; everything else (environmental or
// job-logic failures) keeps ordinary retry semantics.
TEST(PluginAbiGate, OnlyDeterministicGateRefusalsAreFatalForDeploys) {
    using F = clink::cluster::PluginLoadFailure;
    EXPECT_TRUE(clink::cluster::is_plugin_gate_refusal(F::AbiMismatch));
    EXPECT_TRUE(clink::cluster::is_plugin_gate_refusal(F::TripleMismatch));
    EXPECT_TRUE(clink::cluster::is_plugin_gate_refusal(F::ToolchainMismatch));
    EXPECT_FALSE(clink::cluster::is_plugin_gate_refusal(F::None));
    EXPECT_FALSE(clink::cluster::is_plugin_gate_refusal(F::Dlopen));
    EXPECT_FALSE(clink::cluster::is_plugin_gate_refusal(F::MissingSymbol));
    EXPECT_FALSE(clink::cluster::is_plugin_gate_refusal(F::NullMetadata));
    EXPECT_FALSE(clink::cluster::is_plugin_gate_refusal(F::RegisterFailed));
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
    EXPECT_EQ(result.failure, clink::cluster::PluginLoadFailure::Dlopen);
}

// The refusal paths against REAL dlopen'd modules (fixtures built from
// plugin_examples/mismatched_abi_plugin.cpp, one doctored identity value
// each). The pure-function PluginAbiGate tests cannot prove the loader reads
// the symbols, classifies the failure, or leaves the register hook unrun.

TEST(MismatchedPluginSo, BogusFingerprintIsRefusedWithTheNamedDiff) {
    // The fixture's manifest differs from the cluster's by one appended
    // header, so the refusal must name it.
    expect_so_refused(mismatched_fingerprint_plugin_path(),
                      clink::cluster::PluginLoadFailure::AbiMismatch,
                      "include/clink/imaginary/added_by_fixture.hpp (only in plugin)");
}

TEST(MismatchedPluginSo, BogusTripleIsRefused) {
    expect_so_refused(mismatched_triple_plugin_path(),
                      clink::cluster::PluginLoadFailure::TripleMismatch,
                      "vax-780");
}

TEST(MismatchedPluginSo, BogusToolchainIsRefused) {
    expect_so_refused(mismatched_toolchain_plugin_path(),
                      clink::cluster::PluginLoadFailure::ToolchainMismatch,
                      "fortran-iv");
}
