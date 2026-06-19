// Static initializer that ensures clink_core's built-ins are
// registered before any gtest TEST() body runs. clink::tls provides
// no factories, so there's nothing impl-specific to install - this is
// here for parity with the other impls' test setups.

#include "clink/cluster/built_in_factories.hpp"

namespace {

struct Installer {
    Installer() { clink::cluster::ensure_built_ins_registered(); }
};

const Installer kInstaller{};

}  // namespace
