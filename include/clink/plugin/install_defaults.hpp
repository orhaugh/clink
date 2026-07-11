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
// pick up the right defines. NOTE this is evaluated in the INCLUDING
// translation unit: call install_defaults from a TU whose target links the
// impls (e.g. one linking clink::clink), not from a core-only library.
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
#ifdef CLINK_HAS_AVRO
#include "clink/avro/install.hpp"
#endif
#ifdef CLINK_HAS_AWS
#include "clink/aws/install.hpp"
#endif
#ifdef CLINK_HAS_HTTP_CONNECTOR
#include "clink/http_connector/install.hpp"
#endif
#ifdef CLINK_HAS_REDIS
#include "clink/redis/install.hpp"
#endif
#ifdef CLINK_HAS_MYSQL
#include "clink/mysql/install.hpp"
#endif
#ifdef CLINK_HAS_MQTT
#include "clink/mqtt/install.hpp"
#endif
#ifdef CLINK_HAS_MONGODB
#include "clink/mongodb/install.hpp"
#endif
#ifdef CLINK_HAS_ICEBERG
#include "clink/iceberg/install.hpp"
#endif
#ifdef CLINK_HAS_RABBITMQ
#include "clink/rabbitmq/install.hpp"
#endif
#ifdef CLINK_HAS_NATS
#include "clink/nats/install.hpp"
#endif
#ifdef CLINK_HAS_PULSAR
#include "clink/pulsar/install.hpp"
#endif
#ifdef CLINK_HAS_CASSANDRA
#include "clink/cassandra/install.hpp"
#endif
#ifdef CLINK_HAS_WEBHDFS
#include "clink/webhdfs/install.hpp"
#endif
#ifdef CLINK_HAS_GCS
#include "clink/gcs/install.hpp"
#endif
#ifdef CLINK_HAS_AZURE
#include "clink/azure/install.hpp"
#endif
#ifdef CLINK_HAS_ROCKSDB_S3
#include "clink/rocksdb_s3/install.hpp"
#endif
#ifdef CLINK_HAS_VECTOR_SEARCH
#include "clink/vector_search/install.hpp"
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
    clink::rocksdb::install();
#endif
#ifdef CLINK_HAS_AVRO
    clink::avro::install(reg);
#endif
#ifdef CLINK_HAS_AWS
    clink::aws::install(reg);
#endif
#ifdef CLINK_HAS_HTTP_CONNECTOR
    clink::http_connector::install(reg);
#endif
#ifdef CLINK_HAS_REDIS
    clink::redis::install(reg);
#endif
#ifdef CLINK_HAS_MYSQL
    clink::mysql::install(reg);
#endif
#ifdef CLINK_HAS_MQTT
    clink::mqtt::install(reg);
#endif
#ifdef CLINK_HAS_MONGODB
    clink::mongodb::install(reg);
#endif
#ifdef CLINK_HAS_ICEBERG
    clink::iceberg::install(reg);
#endif
#ifdef CLINK_HAS_RABBITMQ
    clink::rabbitmq::install(reg);
#endif
#ifdef CLINK_HAS_NATS
    clink::nats::install(reg);
#endif
#ifdef CLINK_HAS_PULSAR
    clink::pulsar::install(reg);
#endif
#ifdef CLINK_HAS_CASSANDRA
    clink::cassandra::install(reg);
#endif
#ifdef CLINK_HAS_WEBHDFS
    clink::webhdfs::install(reg);
#endif
#ifdef CLINK_HAS_GCS
    clink::gcs::install(reg);
#endif
#ifdef CLINK_HAS_AZURE
    clink::azure::install(reg);
#endif
#ifdef CLINK_HAS_ROCKSDB_S3
    clink::rocksdb_s3::install();
#endif
#ifdef CLINK_HAS_VECTOR_SEARCH
    clink::vector_search::install(reg);
#endif
    // Suppress unused-parameter warning when no impls are linked.
    (void)reg;
}

}  // namespace clink::plugin
