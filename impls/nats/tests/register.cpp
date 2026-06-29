// Static initializer that wires clink_core's registries and installs clink::nats
// before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/nats/install.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::nats::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
