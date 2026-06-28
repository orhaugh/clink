// Static initializer: wire clink_core's registries and install clink::iceberg
// before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/iceberg/install.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::iceberg::install(reg);
    }
};

const Installer kInstaller{};

}  // namespace
