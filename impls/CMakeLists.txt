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
# Layout per impl: impls/<x>/include/clink/... (public headers),
# impls/<x>/src/ (sources incl. register_factories.cpp), impls/<x>/tests/
# (the per-impl gtest exe, labelled for `ctest -L <x>`). Adding a
# connector means adding a subdirectory in this shape and an
# add_subdirectory line below; docs/connectors/<x>.md documents it.

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
add_subdirectory(rabbitmq)
add_subdirectory(nats)
add_subdirectory(pulsar)
add_subdirectory(cassandra)
add_subdirectory(gcs)
add_subdirectory(azure)
# webhdfs reuses clink::http_connector (added above), so it comes after it.
add_subdirectory(webhdfs)
# rocksdb-s3 needs both clink::rocksdb and clink::s3, so it is added after them.
add_subdirectory(rocksdb-s3)
# forst (ForSt state backend): opt-in (CLINK_WITH_FORST defaults OFF); builds
# the pinned upstream tag via ExternalProject and returns early when disabled.
add_subdirectory(forst)
# vector_search (SQL-native AI KNN): header-only SimSIMD + usearch, needs only
# clink::core (the SQL Row headers it uses are header-only, reachable via core).
add_subdirectory(vector_search)
# onnx (SQL-native AI local inference): the ONNX Runtime ML_PREDICT provider. Opt-in
# (CLINK_WITH_ONNX defaults OFF); returns early without a target when disabled/absent.
add_subdirectory(onnx)

# wasm (WebAssembly scalar UDFs): CREATE FUNCTION ... LANGUAGE wasm. Opt-in
# (OFF default / AUTO local-discovery / ON pinned prebuilt fetch).
add_subdirectory(wasm)

# Aggregator: transitively links every impl that defined a target.
# Linking clink::impls_all is equivalent to the legacy
# clink::clink behaviour (kitchen-sink dep on every available impl).
add_library(clink_impls_all INTERFACE)
foreach(_impl kafka postgres clickhouse s3 aws rocksdb tls etcd avro http_connector redis mysql mqtt mongodb iceberg rabbitmq nats pulsar cassandra gcs azure webhdfs rocksdb_s3 vector_search onnx wasm forst)
    if(TARGET clink::${_impl})
        target_link_libraries(clink_impls_all INTERFACE clink::${_impl})
    endif()
endforeach()
add_library(clink::impls_all ALIAS clink_impls_all)
set_target_properties(clink_impls_all PROPERTIES EXPORT_NAME impls_all)

install(TARGETS clink_impls_all
    EXPORT ${CLINK_EXPORT_NAME})
