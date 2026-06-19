// Static initializer that wires clink_core's registries and then
// installs clink::kafka before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/kafka/install.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::kafka::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
