#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::gcs {

// Register the GCS Parquet source + sink factories (gcs_parquet_{int64,string}_{sink,source}).
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::gcs
