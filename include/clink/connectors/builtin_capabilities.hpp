#pragma once

// Capability declarations for the connectors compiled into clink_core.
// Called from ensure_built_ins_registered() so the manifest is populated
// by the same act that makes the factories usable - a connector cannot be
// registered-but-undeclared, which is the state that let free-text
// delivery claims drift from what the code does.

namespace clink::connectors {

void declare_builtin_capabilities();

}  // namespace clink::connectors
