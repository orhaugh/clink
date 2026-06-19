// Static initializer that wires clink_core's registries and
// installs the clink::rocksdb "rocksdb" scheme into the
// StateBackendFactory before any gtest TEST() body runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/rocksdb/install.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::rocksdb::install();
    }
};

const Installer kInstaller{};

}  // namespace
