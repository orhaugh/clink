// Static initializer that wires clink_core's registries and installs the
// rocksdb_s3 remote-state schemes (s3+rocksdb, changelog+s3+rocksdb,
// changelog+s3) into the StateBackendFactory before any gtest TEST() runs.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/rocksdb_s3/install.hpp"

namespace {

struct Installer {
    Installer() {
        clink::cluster::ensure_built_ins_registered();
        clink::rocksdb_s3::install();
    }
};

const Installer kInstaller{};

}  // namespace
