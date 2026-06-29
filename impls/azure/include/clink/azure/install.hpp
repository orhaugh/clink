#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::azure {

// Register the Azure Blob Parquet source + sink factories
// (azure_parquet_{int64,string}_{sink,source}).
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::azure
