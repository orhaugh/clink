#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::webhdfs {

// Register the WebHDFS Parquet source + sink factories
// (webhdfs_parquet_{int64,string}_{sink,source}).
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::webhdfs
