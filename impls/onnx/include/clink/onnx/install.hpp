// clink::onnx::install - register the ONNX Runtime model-inference provider
// (provider='onnx') with the process-wide ModelProviderRegistry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::onnx {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::onnx
