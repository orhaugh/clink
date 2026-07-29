// Static initializer: wire clink_core's registries and install
// clink::websocket before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/websocket/install.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::websocket::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
