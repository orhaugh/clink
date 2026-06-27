#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::mysql {

// Register the MySQL connector factories (mysql_source, mysql_sink) into the
// given registry. Called by clink_node at startup when CLINK_LINKED_MYSQL.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::mysql
