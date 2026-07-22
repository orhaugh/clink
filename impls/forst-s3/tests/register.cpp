// Static initializer that wires clink_core's registries and installs
// the clink::forst_s3 "s3+forst" / "changelog+s3+forst" schemes into the
// StateBackendFactory before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/forst_s3/install.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::forst_s3::install();
    }
};

const Installer kInstaller{};

}  // namespace
