// Test-only static initializer that wires every linked clink::<x>
// impl module into the process-wide PluginRegistry / StateBackendFactory
// before gtest's TEST() bodies run.
//
// Without this, the old `ensure_built_ins_registered()` only covers
// core file/range/int64 factories - kafka_text_source, postgres_*,
// clickhouse_sink, etc. would be unreachable from tests, and the
// "rocksdb" state-backend scheme wouldn't resolve.
//
// CLINK_LINKED_<X> compile defs are set by tests/CMakeLists.txt
// when the corresponding clink::<x> target exists at configure time.

#include "clink/cluster/built_in_factories.hpp"
#include "clink/plugin/plugin.hpp"

#ifdef CLINK_LINKED_KAFKA
#include "clink/kafka/install.hpp"
#endif
#ifdef CLINK_LINKED_POSTGRES
#include "clink/postgres/install.hpp"
#endif
#ifdef CLINK_LINKED_CLICKHOUSE
#include "clink/clickhouse/install.hpp"
#endif
#ifdef CLINK_LINKED_S3
#include "clink/s3/install.hpp"
#endif
#ifdef CLINK_LINKED_ROCKSDB
#include "clink/rocksdb/install.hpp"
#endif

namespace {

struct LinkedImplsInstaller {
    LinkedImplsInstaller() {
        // Core registrations first; idempotent (std::call_once).
        clink::cluster::ensure_built_ins_registered();

        // PluginRegistry's default constructor binds to the process-wide
        // singletons that ensure_built_ins_registered() just populated.
        clink::plugin::PluginRegistry reg;

#ifdef CLINK_LINKED_KAFKA
        clink::kafka::install(reg);
#endif
#ifdef CLINK_LINKED_POSTGRES
        clink::postgres::install(reg);
#endif
#ifdef CLINK_LINKED_CLICKHOUSE
        clink::clickhouse::install(reg);
#endif
#ifdef CLINK_LINKED_S3
        clink::s3::install(reg);
#endif
#ifdef CLINK_LINKED_ROCKSDB
        clink::rocksdb::install();
#endif
    }
};

const LinkedImplsInstaller _installer{};

}  // namespace
