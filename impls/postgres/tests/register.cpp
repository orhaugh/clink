// Static initializer that wires clink_core's registries and then
// installs clink::postgres before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/postgres/install.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::postgres::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
