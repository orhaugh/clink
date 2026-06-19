// Job-as-plugin contract.
//
// A *job* is a shared library a user compiles from a single .cpp that
// uses clink's fluent StreamExecutionEnvironment API to describe a
// pipeline. The library is shipped from the submitter to the cluster
// alongside the JobGraphSpec it produces: when a TaskManager dlopens
// it, the same inline-lambda registrations that ran in the submitter
// fire in the TM process, so operator types like _inline_map_<n> are
// resolvable on both sides.
//
// Contract - a job .so MUST export:
//
//   1. clink_plugin_abi_hash()       -- inherited from CLINK_DECLARE_PLUGIN
//   2. clink_plugin_target_triple()  -- ditto
//   3. clink_plugin_metadata()       -- ditto
//   4. clink_plugin_register(...)    -- runs the user's build_fn under
//                                         std::call_once; registers all
//                                         inline ops as a side effect.
//                                         Returns 0 on success, non-zero
//                                         with err_buf filled on failure.
//   5. clink_job_build(out_json, out_size)
//                                      -- after calling clink_plugin_register
//                                         (or as part of one shared call_once),
//                                         exposes the JobGraphSpec JSON
//                                         the build_fn produced. The pointer
//                                         is valid for the lifetime of the
//                                         loaded .so.
//   6. clink_job_check_restore_compatibility(stored_packed, out_packed, out_size)
//                                      -- state schema-evolution pre-deploy
//                                         check (D). Given a savepoint's packed
//                                         StateVersionMap, returns the packed
//                                         list of (op, state_type) the job
//                                         CANNOT migrate to its expected
//                                         versions. Empty output = compatible.
//                                         Runs .so-SIDE on purpose: the
//                                         StateMigrationRegistry is .so-local
//                                         (clink_core is statically linked, so
//                                         the host's global() is a different,
//                                         empty instance), so only the .so can
//                                         see the migrations build_fn
//                                         registered. The host (CLI / JM) just
//                                         dlopens, calls, and reports.
//
// Use CLINK_REGISTER_JOB(name, version, description, build_fn) at file
// scope to emit all six symbols. You may NOT also use
// CLINK_DECLARE_PLUGIN / CLINK_REGISTER_PLUGIN in the same .so -
// pick one authoring style per shared library.
//
// Lifecycle:
//   * Submitter process: dlopen(.so) -> clink_plugin_register fires
//     build_fn once (call_once gate); clink_job_build returns the
//     captured JSON; submitter uploads .so + JSON to the JM.
//   * TM process:        dlopen(.so) -> clink_plugin_register fires
//     build_fn once (in this process; call_once is per-process); the
//     side-effect registrations populate THIS process's
//     RunnerRegistry. TM ignores clink_job_build.
//
// Determinism caveat: build_fn must register operators in a stable
// order across processes. The inline-op counter
// (StreamExecutionEnvironment::mint_inline_op_type) is per-env, so two
// jobs in the same process get independent _inline_<kind>_0, _<kind>_1,
// ... sequences. Cross-process matching still relies on registration
// ORDER being deterministic in build_fn.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <exception>
#include <mutex>
#include <string>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/state_migration_on_restore.hpp"

namespace clink::job {

// Per-.so state for the call_once-gated build. Each translation unit
// that uses CLINK_REGISTER_JOB gets exactly one instance via the
// macro's anonymous-namespace local-function-static.
struct JobBuilderState {
    std::once_flag flag;
    std::string graph_json;
    std::string build_error;  // populated if build_fn threw
    // Host-supplied PluginRegistry passed to clink_plugin_register.
    // Captured before call_once fires so the wrapped build_fn can route
    // its inline-lambda registrations into the host's singletons rather
    // than the .so's private copies (clink_core is statically linked
    // into both, so the singletons are *distinct* across the dlopen
    // boundary). nullptr is valid: the submit-tool path passes nullptr
    // because it only needs the captured graph JSON, not runtime
    // registrations.
    ::clink::plugin::PluginRegistry* host_registry{nullptr};
    // Holds the most recent packed compatibility result so the C-ABI
    // export can hand back a stable pointer. Recomputed (overwritten) on
    // every clink_job_check_restore_compatibility call, so the returned
    // pointer is valid only until the next call - single-shot per load,
    // which is all the CLI / JM gate need.
    std::string check_result;
};

// Implementation detail: runs build_fn under call_once, captures the
// graph JSON or the error message. Called by both the plugin-register
// entry point and clink_job_build so they share the gate.
template <typename BuildFn>
inline void ensure_built(JobBuilderState& state, BuildFn&& build_fn) {
    std::call_once(state.flag, [&state, build_fn = std::forward<BuildFn>(build_fn)]() mutable {
        try {
            auto env = state.host_registry != nullptr
                           ? ::clink::api::StreamExecutionEnvironment::create_with_registry(
                                 state.host_registry)
                           : ::clink::api::StreamExecutionEnvironment::create();
            build_fn(env);
            state.graph_json = env.graph().to_json();
        } catch (const std::exception& e) {
            state.build_error = std::string{"build_fn threw: "} + e.what();
        } catch (...) {
            state.build_error = "build_fn threw an unknown exception";
        }
    });
}

}  // namespace clink::job

// CLINK_REGISTER_JOB(name, version, description, build_fn)
//
// Emit the full job-as-plugin C-ABI surface. `build_fn` is the user's
// pipeline-building function with signature
//
//     void(clink::api::StreamExecutionEnvironment&)
//
// Example:
//
//     void define_job(clink::api::StreamExecutionEnvironment& env) {
//         env.from_elements<int64_t>({1, 2, 3, 4, 5})
//            .map<int64_t>([](int64_t v) { return v * 2; })
//            .sink(FileInt64Sink::builder().path("/tmp/out").build());
//     }
//     CLINK_REGISTER_JOB("my-job", "1.0", "demo", define_job);
//
#define CLINK_REGISTER_JOB(job_name, job_version, job_description, build_fn)                     \
    CLINK_DECLARE_PLUGIN(job_name, job_version, job_description);                                \
    namespace {                                                                                  \
    inline ::clink::job::JobBuilderState& clink_job_state_() {                                   \
        static ::clink::job::JobBuilderState state;                                              \
        return state;                                                                            \
    }                                                                                            \
    }                                                                                            \
    extern "C" int clink_plugin_register(                                                        \
        void* registry_ptr, char* err_buf, ::std::size_t err_buf_size) {                         \
        auto& s = clink_job_state_();                                                            \
        /* Stash the host registry BEFORE call_once fires; ensure_built()  */                    \
        /* reads it from state to wire the env at construction time. Safe  */                    \
        /* to assign on every call: call_once only invokes build_fn once   */                    \
        /* per process, and the first non-null registry wins (subsequent   */                    \
        /* loads from the same process re-dlopen the same .so and pass the */                    \
        /* same singletons). */                                                                  \
        if (registry_ptr != nullptr) {                                                           \
            s.host_registry = static_cast<::clink::plugin::PluginRegistry*>(registry_ptr);       \
        }                                                                                        \
        ::clink::job::ensure_built(s, (build_fn));                                               \
        if (!s.build_error.empty()) {                                                            \
            if (err_buf != nullptr && err_buf_size > 0) {                                        \
                const auto n = ::std::min(s.build_error.size(), err_buf_size - 1);               \
                ::std::memcpy(err_buf, s.build_error.data(), n);                                 \
                err_buf[n] = '\0';                                                               \
            }                                                                                    \
            return 1;                                                                            \
        }                                                                                        \
        return 0;                                                                                \
    }                                                                                            \
    extern "C" int clink_job_build(const char** out_json, ::std::size_t* out_size) {             \
        auto& s = clink_job_state_();                                                            \
        ::clink::job::ensure_built(s, (build_fn));                                               \
        if (!s.build_error.empty()) {                                                            \
            return 1;                                                                            \
        }                                                                                        \
        if (out_json != nullptr) {                                                               \
            *out_json = s.graph_json.data();                                                     \
        }                                                                                        \
        if (out_size != nullptr) {                                                               \
            *out_size = s.graph_json.size();                                                     \
        }                                                                                        \
        return 0;                                                                                \
    }                                                                                            \
    extern "C" int clink_job_check_restore_compatibility(                                        \
        const char* stored_packed, const char** out_packed, ::std::size_t* out_size) {           \
        auto& s = clink_job_state_();                                                            \
        /* Fire build_fn so the env's expect_state_version calls land in    */                   \
        /* graph_json AND the migrations build_fn registered populate THIS  */                   \
        /* .so's StateMigrationRegistry::global(). */                                            \
        ::clink::job::ensure_built(s, (build_fn));                                               \
        if (!s.build_error.empty()) {                                                            \
            return 1;                                                                            \
        }                                                                                        \
        try {                                                                                    \
            const auto expected =                                                                \
                ::clink::cluster::JobGraphSpec::from_json(s.graph_json).expected_state_versions; \
            const auto stored =                                                                  \
                ::clink::StateVersionMap::unpack(stored_packed != nullptr ? stored_packed : ""); \
            const auto incompat = ::clink::check_restore_compatibility(                          \
                stored, expected, ::clink::StateMigrationRegistry::global());                    \
            s.check_result = ::clink::pack_incompatibilities(incompat);                          \
        } catch (...) {                                                                          \
            /* Malformed stored map or graph JSON: report as a hard error   */                   \
            /* (exit code distinct from build failure) rather than silently */                   \
            /* claiming compatible. */                                                           \
            return 2;                                                                            \
        }                                                                                        \
        if (out_packed != nullptr) {                                                             \
            *out_packed = s.check_result.data();                                                 \
        }                                                                                        \
        if (out_size != nullptr) {                                                               \
            *out_size = s.check_result.size();                                                   \
        }                                                                                        \
        return 0;                                                                                \
    }                                                                                            \
    static_assert(::std::is_invocable_r_v<void,                                                  \
                                          decltype(build_fn)&,                                   \
                                          ::clink::api::StreamExecutionEnvironment&>,            \
                  "CLINK_REGISTER_JOB build_fn must have signature "                             \
                  "void(clink::api::StreamExecutionEnvironment&)")
