// Static initializer: wire clink_core's registries and install clink::mysql
// before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/mysql/install.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::mysql::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
