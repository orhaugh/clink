// install_defaults.hpp - one-call helper that installs every clink-shipped
// piece a user is likely to need:
//
//   * Built-in channel types + their bridges (int64, string, ...)
//   * Each impl's `install()` hook available at link time
//
// Eliminates the "T not registered (call register_type<T> first)" footgun
// where users forget to call `ensure_built_ins_registered()` plus every
// `clink::<impl>::install(reg)` for every impl they link.
//
// Build-time detection: each impl's `install()` is invoked only when its
// `CLINK_HAS_<NAME>` define is set at compile time. The impl static libs
// set their respective define on their target's PUBLIC interface, so
// consumers that `target_link_libraries(... clink::<impl>)` automatically
// pick up the right defines.
//
// Idempotent: calling twice is safe - the underlying registries
// canonicalize on (channel, op_type) keys and last-write-wins on a
// duplicate.

#pragma once

#include "clink/cluster/built_in_factories.hpp"
#include "clink/plugin/plugin.hpp"

#ifdef CLINK_HAS_KAFKA
#include "clink/kafka/install.hpp"
#endif

#ifdef CLINK_HAS_POSTGRES
#include "clink/postgres/install.hpp"
#endif

#ifdef CLINK_HAS_CLICKHOUSE
#include "clink/clickhouse/install.hpp"
#endif

#ifdef CLINK_HAS_S3
#include "clink/s3/install.hpp"
#endif

#ifdef CLINK_HAS_ROCKSDB
#include "clink/rocksdb/install.hpp"
#endif

#ifdef CLINK_HAS_TLS
#include "clink/tls/install.hpp"
#endif

namespace clink::plugin {

inline void install_defaults(PluginRegistry& reg) {
    clink::cluster::ensure_built_ins_registered();
#ifdef CLINK_HAS_KAFKA
    clink::kafka::install(reg);
#endif
#ifdef CLINK_HAS_POSTGRES
    clink::postgres::install(reg);
#endif
#ifdef CLINK_HAS_CLICKHOUSE
    clink::clickhouse::install(reg);
#endif
#ifdef CLINK_HAS_S3
    clink::s3::install(reg);
#endif
#ifdef CLINK_HAS_ROCKSDB
    clink::rocksdb::install(reg);
#endif
#ifdef CLINK_HAS_TLS
    clink::tls::install(reg);
#endif
    // Suppress unused-parameter warning when no impls are linked.
    (void)reg;
}

}  // namespace clink::plugin
