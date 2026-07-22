# clink::forst_s3 - remote-state schemes that need BOTH the ForSt backend
# and the S3 stores: s3+forst://, changelog+s3+forst://. Kept in its own
# target so the forst<->arrow-s3 link stays out of clink_core and the two
# base impls (mirrors clink::rocksdb_s3). Built only when both
# clink::forst and clink::s3 were defined (this subdir is added after
# both; forst is opt-in, so this target usually is too).
#
# No engine headers are needed here: the register file constructs
# ForStStateBackend through its pImpl'd public header, so the include-
# order care that impls/forst takes does not apply.

if(NOT TARGET clink::forst OR NOT TARGET clink::s3)
    message(STATUS
        "clink::forst_s3 - needs both clink::forst and clink::s3; target not defined")
    return()
endif()

add_library(clink_forst_s3 STATIC
    src/register_state_backend.cpp
)
add_library(clink::forst_s3 ALIAS clink_forst_s3)

target_include_directories(clink_forst_s3 PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_forst_s3 PUBLIC cxx_std_23)
target_compile_definitions(clink_forst_s3 PUBLIC CLINK_HAS_FORST_S3)
target_link_libraries(clink_forst_s3
    PUBLIC clink::core clink::forst clink::s3
)
clink_set_warnings(clink_forst_s3)

clink_install_impl(
    TARGET clink_forst_s3
    NAME   forst_s3)

message(STATUS "clink::forst_s3 - enabled")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()
