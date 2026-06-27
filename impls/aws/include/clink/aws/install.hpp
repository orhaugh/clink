// clink::aws::install - register the AWS-family connector factories (Kinesis,
// Firehose, DynamoDB) with a PluginRegistry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::aws {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::aws
