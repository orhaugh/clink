// clink plugin contract.
//
// A clink plugin is a shared library compiled against these headers.
// The library defines its own record types, sources, sinks, operators,
// and selectors, and registers them by string name. A cluster client
// uploads the .so as part of SubmitJob; the JM distributes it to TMs;
// TMs dlopen and call into it.
//
// ABI compatibility:
//   * The plugin and the cluster MUST be built from the same clink
//     commit. The build system embeds git rev-parse HEAD into both;
//     the loader compares byte-for-byte. Mismatches are rejected.
//   * STL types (std::string, std::function, std::shared_ptr, codecs)
//     ARE used across the plugin/cluster boundary. The commit-hash
//     gate guarantees both sides use the same standard-library ABI,
//     same compiler, and same template instantiations.
//
// Crash safety:
//   * Plugin loaded with RTLD_LOCAL so its symbols don't pollute the
//     global namespace.
//   * A crash in plugin code terminates the TM process; JM watchdog
//     restarts per the job's max_restarts policy. v1 contract is
//     "trust your own plugins"; sandboxing is on the v2 roadmap.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>

#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/operator_registry.hpp"  // SelectorRegistry, KeyExtractorRegistry
#include "clink/cluster/runner_helpers.hpp"     // SideOutputAttacherRegistry
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/async_co_process_function.hpp"  // AsyncKeyedCoProcessFunction
#include "clink/operators/operator_base.hpp"
#include "clink/operators/process_function.hpp"  // KeyedProcessFunction, KeyedCoProcessFunction
#include "clink/plugin/abi_version.hpp"

namespace clink::plugin {

// Plugin metadata advertised via the C-ABI handshake. Strings must
// outlive any call to load the plugin; CLINK_DECLARE_PLUGIN
// emits a static instance of this from string literals.
struct PluginMetadata {
    const char* name;
    const char* version;
    const char* description;
    const char* author;
};

// What user-supplied factories receive when the runtime instantiates
// a source/operator/sink/selector. The plugin reads its op-spec
// params from `params`; subtask_idx and parallelism let
// parallelism-aware factories partition their work.
struct BuildContext {
    std::map<std::string, std::string> params;
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};

    std::string param_or(const std::string& key, std::string fallback = {}) const {
        auto it = params.find(key);
        return it == params.end() ? std::move(fallback) : resolve_secret(it->second);
    }

    // A connector option may reference a secret indirectly as `env://VAR`
    // instead of embedding it, so a job spec / catalog stores a reference rather
    // than a plaintext password or key. Resolved at build (deploy) time from the
    // environment; an unset variable yields empty (a clear failure, never a
    // leak). A literal value not prefixed `env://` is returned verbatim.
    static std::string resolve_secret(const std::string& value) {
        constexpr std::string_view kEnvPrefix = "env://";
        if (value.rfind(kEnvPrefix, 0) != 0) {
            return value;
        }
        const std::string var = value.substr(kEnvPrefix.size());
        const char* resolved = var.empty() ? nullptr : std::getenv(var.c_str());
        return resolved != nullptr ? std::string(resolved) : std::string{};
    }
    std::int64_t param_int64_or(const std::string& key, std::int64_t fallback = 0) const {
        auto it = params.find(key);
        if (it == params.end()) {
            return fallback;
        }
        try {
            return std::stoll(it->second);
        } catch (...) {
            return fallback;
        }
    }
};

// PluginRegistry is the registration sink the user populates from
// their register function. v2 design: a concrete class that holds
// references to the cluster's internal registries. Default
// construction wires up the process-wide singletons; a test or
// future job-scoped registry can supply different ones.
//
// Why concrete (not abstract): the templated methods need to know T
// to build typed bridge constructors and runner closures, and
// virtual templates don't exist in C++. The commit-hash ABI gate
// makes "STL types across the boundary" safe.
class PluginRegistry {
public:
    PluginRegistry()
        : type_registry_(clink::cluster::TypeRegistry::default_instance()),
          runner_registry_(clink::cluster::RunnerRegistry::default_instance()),
          selector_registry_(clink::cluster::SelectorRegistry::default_instance()),
          key_extractor_registry_(clink::cluster::KeyExtractorRegistry::default_instance()),
          side_output_attacher_registry_(
              clink::cluster::SideOutputAttacherRegistry::default_instance()),
          operator_registry_(clink::cluster::OperatorRegistry::default_instance()),
          dag_builder_registry_(clink::cluster::DagBuilderRegistry::default_instance()) {}

    // 3-arg legacy constructor: routes everything else to the default
    // singletons.
    PluginRegistry(clink::cluster::TypeRegistry& tr,
                   clink::cluster::RunnerRegistry& rr,
                   clink::cluster::SelectorRegistry& sr)
        : type_registry_(tr),
          runner_registry_(rr),
          selector_registry_(sr),
          key_extractor_registry_(clink::cluster::KeyExtractorRegistry::default_instance()),
          side_output_attacher_registry_(
              clink::cluster::SideOutputAttacherRegistry::default_instance()),
          operator_registry_(clink::cluster::OperatorRegistry::default_instance()),
          dag_builder_registry_(clink::cluster::DagBuilderRegistry::default_instance()) {}

    // 5-arg legacy constructor.
    PluginRegistry(clink::cluster::TypeRegistry& tr,
                   clink::cluster::RunnerRegistry& rr,
                   clink::cluster::SelectorRegistry& sr,
                   clink::cluster::KeyExtractorRegistry& ker,
                   clink::cluster::SideOutputAttacherRegistry& soar)
        : type_registry_(tr),
          runner_registry_(rr),
          selector_registry_(sr),
          key_extractor_registry_(ker),
          side_output_attacher_registry_(soar),
          operator_registry_(clink::cluster::OperatorRegistry::default_instance()),
          dag_builder_registry_(clink::cluster::DagBuilderRegistry::default_instance()) {}

    // 6-arg legacy constructor.
    PluginRegistry(clink::cluster::TypeRegistry& tr,
                   clink::cluster::RunnerRegistry& rr,
                   clink::cluster::SelectorRegistry& sr,
                   clink::cluster::KeyExtractorRegistry& ker,
                   clink::cluster::SideOutputAttacherRegistry& soar,
                   clink::cluster::OperatorRegistry& orr)
        : type_registry_(tr),
          runner_registry_(rr),
          selector_registry_(sr),
          key_extractor_registry_(ker),
          side_output_attacher_registry_(soar),
          operator_registry_(orr),
          dag_builder_registry_(clink::cluster::DagBuilderRegistry::default_instance()) {}

    // 7-arg constructor: per-job bundle form. The DagBuilderRegistry
    // gets the inline-lambda DagBuilders so the chain dispatcher
    // (task_manager.cpp) can find them via the per-job bundle. Without
    // it, the dispatch falls through to the TM process's DagBuilderRegistry
    // singleton which (due to RTLD_LOCAL) doesn't see the .so's writes.
    PluginRegistry(clink::cluster::TypeRegistry& tr,
                   clink::cluster::RunnerRegistry& rr,
                   clink::cluster::SelectorRegistry& sr,
                   clink::cluster::KeyExtractorRegistry& ker,
                   clink::cluster::SideOutputAttacherRegistry& soar,
                   clink::cluster::OperatorRegistry& orr,
                   clink::cluster::DagBuilderRegistry& dbr)
        : type_registry_(tr),
          runner_registry_(rr),
          selector_registry_(sr),
          key_extractor_registry_(ker),
          side_output_attacher_registry_(soar),
          operator_registry_(orr),
          dag_builder_registry_(dbr) {}

    // Register a channel type. Stamps typed bridge builders that capture
    // T and codec into TypeRegistry; later register_source/operator/sink
    // calls referencing T resolve to the channel name set here. Also
    // installs a typed side-output attacher so an operator emitting
    // side records of type T can be wired across the cluster wire.
    //
    // The codec-only overload uses a binary-fallback ArrowBatcher<T>
    // (wraps the codec in a single value_bytes:binary column on the
    // wire). The 3-argument overload accepts a specialised ArrowBatcher
    // for the columnar fast-path - use it for primitive types and
    // user schemas that map cleanly to Arrow.
    template <typename T>
    void register_type(const std::string& name, Codec<T> codec);

    template <typename T>
    void register_type(const std::string& name, Codec<T> codec, ArrowBatcher<T> batcher);

    // Look up the Codec<T> the user registered for T. Used by fluent
    // API methods that need to wire the codec into operators with a
    // persistent state path (e.g. the tumbling aggregate operator's
    // RocksDB-backed constructor). Returns nullopt if T wasn't
    // registered. The result is a shared_ptr<const Codec<T>> so the
    // codec is captured cheaply into closures.
    template <typename T>
    std::shared_ptr<const Codec<T>> codec_for() const;

    // Register a source. T must have been registered via register_type<T>
    // first - the registry uses typeid(T) to resolve T to its channel
    // name, which keys the runner.
    template <typename T>
    void register_source(const std::string& op_type,
                         std::function<std::shared_ptr<Source<T>>(const BuildContext&)> factory);

    // Register a mid-chain operator. Both In and Out must be registered
    // via register_type<...> first.
    template <typename In, typename Out>
    void register_operator(
        const std::string& op_type,
        std::function<std::shared_ptr<Operator<In, Out>>(const BuildContext&)> factory);

    // Register a sink. T must be registered first.
    template <typename T>
    void register_sink(const std::string& op_type,
                       std::function<std::shared_ptr<Sink<T>>(const BuildContext&)> factory);

    // Register a two-input co-operator (the
    // CoProcessFunction). In1, In2, Out must each be registered via
    // register_type<...> first. The user's CoOperator subclass owns its
    // own state via runtime()->keyed_state<>(); the two process_element
    // methods share that state.
    template <typename In1, typename In2, typename Out>
    void register_co_operator(
        const std::string& op_type,
        std::function<std::shared_ptr<CoOperator<In1, In2, Out>>(const BuildContext&)> factory);

    // Register a split-routing selector function for typed channel T.
    template <typename T>
    void register_selector(const std::string& name, std::function<int(const T&)> fn);

    // Register a key extractor for typed channel T. Used by
    // RoutingMode::Hash to hash-partition records across the downstream
    // op's subtasks. T must have been registered via register_type<T>
    // first; the channel name is looked up from TypeRegistry. Same
    // extractor name across multiple T's is fine - a keyed CoOperator
    // typically has one extractor per upstream type, both under the
    // same name (matches keyBy convention).
    template <typename T>
    void register_key_extractor(const std::string& name, std::function<std::int64_t(const T&)> fn);

    // Sugar around `register_operator<In, Out>` for a `KeyedProcessFunction
    // <K, In, Out>`: wraps the user's factory in a closure that builds a
    // `KeyedProcessFunctionAdapter<K, In, Out>` with the supplied per-record
    // key extractor (and optional timer-key decoder). Eliminates ~10 lines
    // of boilerplate per consumer site. K, In, Out must all be registered
    // as channel types first.
    //
    // The `key_fn` returns the K the user's `KeyedProcessFunction::
    // current_key()` should see during dispatch. It is independent of the
    // int64 partition hash registered via `register_key_extractor<In>` -
    // the two extractors can produce different K shapes if the user
    // chooses (e.g. partition by `hash(string_key)` for routing, hand the
    // raw `string_key` to `current_key()` for state slot naming).
    template <typename K, typename In, typename Out>
    void register_keyed_operator(
        const std::string& op_type,
        std::function<std::shared_ptr<KeyedProcessFunction<K, In, Out>>(const BuildContext&)>
            fn_factory,
        std::function<K(const In&)> key_fn,
        std::function<K(const std::string&)> timer_key_fn = nullptr);

    // Same as `register_keyed_operator` but for a two-input
    // `KeyedCoProcessFunction<K, In1, In2, Out>`. Each side supplies its
    // own per-record key extractor; the adapter swaps `current_key()` to
    // the right typed K before invoking `process_element1` /
    // `process_element2`.
    template <typename K, typename In1, typename In2, typename Out>
    void register_keyed_co_operator(
        const std::string& op_type,
        std::function<std::shared_ptr<KeyedCoProcessFunction<K, In1, In2, Out>>(
            const BuildContext&)> fn_factory,
        std::function<K(const In1&)> key1,
        std::function<K(const In2&)> key2,
        std::function<K(const std::string&)> timer_key_fn = nullptr);

    // Async-state two-input co-operator: an AsyncKeyedCoProcessFunction whose
    // process_element{1,2} co_await keyed-state reads under the per-key gate.
    // The adapter owns the key Codec<K> (the gate + timer key encoding); each
    // side supplies its own per-record extractor.
    template <typename K, typename In1, typename In2, typename Out>
    void register_async_keyed_co_operator(
        const std::string& op_type,
        std::function<std::shared_ptr<AsyncKeyedCoProcessFunction<K, In1, In2, Out>>(
            const BuildContext&)> fn_factory,
        std::function<K(const In1&)> key1,
        std::function<K(const In2&)> key2,
        Codec<K> key_codec);

    // (`install_defaults` lives in clink/plugin/install_defaults.hpp as
    // a free function - keeps this header free of every impl include.)

    // Accessors for code that needs to capture a reference to one of
    // the underlying registries (e.g. inline-lambda fluent methods that
    // build closures looking up key extractors at operator-construct
    // time). Capturing via the held reference avoids the dlopen
    // RTLD_LOCAL trap where `Registry::default_instance()` inside a .so
    // resolves to the .so's private static, not the host's.
    clink::cluster::TypeRegistry& type_registry() noexcept { return type_registry_; }
    clink::cluster::RunnerRegistry& runner_registry() noexcept { return runner_registry_; }
    clink::cluster::SelectorRegistry& selector_registry() noexcept { return selector_registry_; }
    clink::cluster::KeyExtractorRegistry& key_extractor_registry() noexcept {
        return key_extractor_registry_;
    }
    clink::cluster::SideOutputAttacherRegistry& side_output_attacher_registry() noexcept {
        return side_output_attacher_registry_;
    }
    clink::cluster::OperatorRegistry& operator_registry() noexcept { return operator_registry_; }
    clink::cluster::DagBuilderRegistry& dag_builder_registry() noexcept {
        return dag_builder_registry_;
    }

private:
    clink::cluster::TypeRegistry& type_registry_;
    clink::cluster::RunnerRegistry& runner_registry_;
    clink::cluster::SelectorRegistry& selector_registry_;
    clink::cluster::KeyExtractorRegistry& key_extractor_registry_;
    clink::cluster::SideOutputAttacherRegistry& side_output_attacher_registry_;
    clink::cluster::OperatorRegistry& operator_registry_;
    clink::cluster::DagBuilderRegistry& dag_builder_registry_;

    // Keep the user-supplied codecs accessible by typeid name. The
    // closures inside TypeRegistry already capture them, but for
    // operators that need the codec to construct a persistent state
    // store (e.g. TumblingWindowOperator with codec ctor) it's
    // cleaner to keep a typed lookup table here than to plumb
    // codec-extraction through every TypeOps closure. Single-
    // threaded access (define_job populates, fluent API reads
    // before submit) - no mutex needed.
    std::unordered_map<std::string, std::shared_ptr<const void>> codecs_;
};

// User function signature for plugin registration. Plugins define
// one such function and pass it to CLINK_REGISTER_PLUGIN().
using RegisterFn = void (*)(PluginRegistry&);

}  // namespace clink::plugin

// ============================================================================
// Inline template implementations
// ============================================================================
//
// These templates instantiate in the plugin's compilation unit (or the
// cluster's, when the same code path is exercised in-process). The
// instantiations reference symbols from the clink shared library
// (TypeRegistry::register_typed, RunnerRegistry::register_source, etc.)
// which resolve at link or dlopen time.

#include "clink/plugin/plugin_impl.hpp"
