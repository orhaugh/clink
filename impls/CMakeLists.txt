# impls/ - out-of-core impl modules.
#
# Each subdirectory is a separate static library that depends on
# clink::core. The module is built only when its underlying
# dependency is found via find_package (with the same AUTO/ON/OFF
# tri-state knobs that gated the legacy single-library build); when the
# dep is absent, the subdirectory returns early without defining a
# target. Call sites should therefore guard on `if(TARGET clink::kafka)`
# rather than on CLINK_HAS_KAFKA.
#
# The aggregator target clink::impls_all is an INTERFACE library that
# transitively links every impl that was built. It exists as a
# compatibility shim so legacy tests/examples that used to link
# clink::clink (one target, everything) can keep working during the
# migration. New code should link the explicit per-impl targets it
# actually needs.
#
# THIS DIRECTORY IS A DRAFT. None of the source files referenced by the
# child CMakeLists exist yet - they live under src/connectors/ and
# src/state/. The migration steps that need to happen before the parent
# CMakeLists.txt calls add_subdirectory(impls) are listed in the audit
# response, repeated here for completeness:
#
#   1. Move connector .cpp files from src/connectors/<x>_*.cpp into
#      impls/<x>/src/. Adapter helpers currently in
#      src/cluster/built_in_factories.cpp (StringKafkaSource etc.) move
#      to the same place.
#   2. Move connector headers from include/clink/connectors/<x>_*.hpp
#      into impls/<x>/include/clink/connectors/.
#   3. Move RocksDB backend (header + .cpp) into impls/rocksdb/.
#   4. Move TLS socket (header + .cpp) into impls/tls/.
#   5. Extract per-impl registration from built_in_factories.cpp into
#      a register_factories.cpp inside each impl directory.
#   6. Templatise (or relocate) src/config/pipeline_loader.cpp -
#      currently hardcoded to KafkaMessage.
#   7. Split include/clink/api/builtin_connectors.hpp: file/int64/range
#      builders stay in core; Kafka/Postgres/ClickHouse/S3 builders move
#      into impls/<x>/include/clink/api/<x>_builders.hpp.

add_subdirectory(kafka)
add_subdirectory(postgres)
add_subdirectory(clickhouse)
add_subdirectory(s3)
add_subdirectory(aws)
add_subdirectory(rocksdb)
add_subdirectory(tls)
add_subdirectory(etcd)
add_subdirectory(avro)
add_subdirectory(http_connector)
add_subdirectory(redis)
add_subdirectory(mysql)
add_subdirectory(mqtt)
add_subdirectory(mongodb)
add_subdirectory(iceberg)
# rocksdb-s3 needs both clink::rocksdb and clink::s3, so it is added after them.
add_subdirectory(rocksdb-s3)

# Aggregator: transitively links every impl that defined a target.
# Linking clink::impls_all is equivalent to the legacy
# clink::clink behaviour (kitchen-sink dep on every available impl).
add_library(clink_impls_all INTERFACE)
foreach(_impl kafka postgres clickhouse s3 aws rocksdb tls etcd avro http_connector redis mysql mqtt mongodb iceberg rocksdb_s3)
    if(TARGET clink::${_impl})
        target_link_libraries(clink_impls_all INTERFACE clink::${_impl})
    endif()
endforeach()
add_library(clink::impls_all ALIAS clink_impls_all)
set_target_properties(clink_impls_all PROPERTIES EXPORT_NAME impls_all)

install(TARGETS clink_impls_all
    EXPORT ${CLINK_EXPORT_NAME})
