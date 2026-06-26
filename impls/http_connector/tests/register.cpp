// Static initializer: wire clink_core's registries and install
// clink::http_connector before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/http_connector/install.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::http_connector::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
