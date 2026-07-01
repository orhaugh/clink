// clink::vector_search::install - register the vector_search_row operator with a
// PluginRegistry. Must be called AFTER the SQL Row channel type is registered
// (clink::sql::install), since register_operator<Row, Row> requires the Row channel.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::vector_search {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::vector_search
