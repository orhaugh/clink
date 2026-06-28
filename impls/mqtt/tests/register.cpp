// Static initializer: wire clink_core's registries and install clink::mqtt
// before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/mqtt/install.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::mqtt::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
