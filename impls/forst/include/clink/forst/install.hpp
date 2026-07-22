// clink::forst::install - register the "forst" and "changelog+forst"
// schemes with clink_core's StateBackendFactory so a job's state_backend
// URI of the form "forst:///path/to/dir" resolves to a ForSt-backed
// keyed-state store at runtime. Optional query params:
// ?defer_reads=1 reports supports_async_get() and runs reads on an IO
// executor (operators then ride their async KeyedState paths, so
// per-record hot state lives in the engine); &io_threads=<n> sizes the
// pool.
//
// Callers (clink_node, test entry points) invoke this once at startup.
// Idempotent - repeated calls overwrite the prior builder.

#pragma once

namespace clink::forst {

void install();

}  // namespace clink::forst
