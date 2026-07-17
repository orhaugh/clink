#pragma once

namespace clink::cluster {

class RunnerRegistry;

// Registers all of clink's built-in channel types, sources,
// operators, sinks, and selectors with the process-wide
// TypeRegistry / RunnerRegistry / SelectorRegistry via a
// PluginRegistry. Idempotent (std::call_once-gated) so it's safe to
// call from multiple entry points.
//
// Built-ins go through the same registration path as user plugins to
// keep one code path for dispatch. Callers should invoke this from
// any entry point that needs the built-ins resolvable - the worker's
// generic role, the planner's validation step, integration tests.
void ensure_built_ins_registered();

// Registers built-in join SubtaskRunners (currently just the
// int64_int64_match_join used by tests and the gateway parity
// path) into `rr`. Called by ensure_built_ins_registered. Exposed
// separately so per-job RunnerRegistries can also include the
// built-in joins via their parent fallthrough or via a direct call.
void register_builtin_joins(RunnerRegistry& rr);

}  // namespace clink::cluster
