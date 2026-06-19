# clink::rocksdb_s3 - remote-state schemes that need BOTH the RocksDB backend
# and the S3 stores: s3+rocksdb://, changelog+s3+rocksdb://, changelog+s3://.
# Kept in its own target so the rocksdb<->arrow-s3 link stays out of clink_core
# and the two base impls. Built only when both clink::rocksdb and clink::s3
# were defined (this subdir is added after both).

if(NOT TARGET clink::rocksdb OR NOT TARGET clink::s3)
    message(STATUS
        "clink::rocksdb_s3 - needs both clink::rocksdb and clink::s3; target not defined")
    return()
endif()

add_library(clink_rocksdb_s3 STATIC
    src/register_state_backend.cpp
)
add_library(clink::rocksdb_s3 ALIAS clink_rocksdb_s3)

target_include_directories(clink_rocksdb_s3 PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_rocksdb_s3 PUBLIC cxx_std_23)
target_compile_definitions(clink_rocksdb_s3 PUBLIC CLINK_HAS_ROCKSDB_S3)
target_link_libraries(clink_rocksdb_s3
    PUBLIC clink::core clink::rocksdb clink::s3
)
clink_set_warnings(clink_rocksdb_s3)

clink_install_impl(
    TARGET clink_rocksdb_s3
    NAME   rocksdb_s3)

message(STATUS "clink::rocksdb_s3 - enabled")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()
