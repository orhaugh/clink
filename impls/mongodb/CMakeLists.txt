# clink::mongodb - MongoDB connectors (mongo_cdc_source via change streams,
# mongo_sink via bulk upsert) built against mongo-cxx-driver (mongocxx + bsoncxx).
# Dep-gated: builds only when the driver is found.

if(CLINK_WITH_MONGODB STREQUAL "OFF")
    message(STATUS "clink::mongodb - CLINK_WITH_MONGODB=OFF; target not defined")
    return()
endif()

# Discover mongo-cxx-driver three ways, most-portable first:
#   1. pkg-config imported target (apt libmongocxx-dev ships libmongocxx1.pc)
#   2. mongocxx's own CONFIG package (mongo::mongocxx_shared)
#   3. a manual header/lib probe (Homebrew, or a bare install with no pkg-config
#      tool - this dev host's case)
find_package(PkgConfig QUIET)
set(_mongo_target "")
if(PkgConfig_FOUND)
    pkg_check_modules(LIBMONGOCXX QUIET IMPORTED_TARGET libmongocxx1)
    if(LIBMONGOCXX_FOUND)
        set(_mongo_target PkgConfig::LIBMONGOCXX)
    endif()
endif()
if(NOT _mongo_target)
    find_package(mongocxx CONFIG QUIET)
    if(mongocxx_FOUND AND TARGET mongo::mongocxx_shared)
        set(_mongo_target mongo::mongocxx_shared)
    endif()
endif()
if(NOT _mongo_target)
    # mongocxx 4.x puts the stable headers under include/<lib>/v_noabi and ABI
    # redirect headers under include/; both per-lib v_noabi dirs PLUS the prefix
    # include are needed. The libs carry the ABI suffix (mongocxx1 / bsoncxx1).
    find_path(_mongocxx_inc NAMES mongocxx/client.hpp
        HINTS /opt/homebrew/opt/mongo-cxx-driver/include/mongocxx/v_noabi
              /usr/local/include/mongocxx/v_noabi /usr/include/mongocxx/v_noabi)
    find_path(_bsoncxx_inc NAMES bsoncxx/json.hpp
        HINTS /opt/homebrew/opt/mongo-cxx-driver/include/bsoncxx/v_noabi
              /usr/local/include/bsoncxx/v_noabi /usr/include/bsoncxx/v_noabi)
    find_path(_mongo_prefix_inc NAMES mongocxx/v1/client.hpp
        HINTS /opt/homebrew/opt/mongo-cxx-driver/include /usr/local/include /usr/include)
    find_library(_mongocxx_lib NAMES mongocxx1 mongocxx
        HINTS /opt/homebrew/opt/mongo-cxx-driver/lib /opt/homebrew/lib /usr/local/lib /usr/lib)
    find_library(_bsoncxx_lib NAMES bsoncxx1 bsoncxx
        HINTS /opt/homebrew/opt/mongo-cxx-driver/lib /opt/homebrew/lib /usr/local/lib /usr/lib)
    if(_mongocxx_inc AND _bsoncxx_inc AND _mongocxx_lib AND _bsoncxx_lib)
        add_library(clink_mongocxx_external INTERFACE)
        # SYSTEM so clink's -Werror does not fire on the driver's own headers.
        target_include_directories(clink_mongocxx_external SYSTEM INTERFACE
            ${_mongocxx_inc} ${_bsoncxx_inc})
        if(_mongo_prefix_inc)
            target_include_directories(clink_mongocxx_external SYSTEM INTERFACE ${_mongo_prefix_inc})
        endif()
        target_link_libraries(clink_mongocxx_external INTERFACE ${_mongocxx_lib} ${_bsoncxx_lib})
        set(_mongo_target clink_mongocxx_external)
    endif()
endif()
if(NOT _mongo_target)
    if(CLINK_WITH_MONGODB STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_MONGODB=ON but mongo-cxx-driver was not found")
    endif()
    message(STATUS "clink::mongodb - mongo-cxx-driver not found; target not defined")
    return()
endif()

add_library(clink_mongodb STATIC
    src/register_factories.cpp
    src/mongo_cdc_source.cpp
    src/mongo_sink.cpp
)
add_library(clink::mongodb ALIAS clink_mongodb)

target_include_directories(clink_mongodb PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_mongodb PUBLIC cxx_std_23)
target_compile_definitions(clink_mongodb PUBLIC CLINK_HAS_MONGODB)
target_link_libraries(clink_mongodb
    PUBLIC  clink::core
    PRIVATE ${_mongo_target}
)
clink_set_warnings(clink_mongodb)

if(TARGET clink_mongocxx_external)
    install(TARGETS clink_mongocxx_external EXPORT ${CLINK_EXPORT_NAME})
    clink_install_impl(TARGET clink_mongodb NAME mongodb)
else()
    clink_install_impl(
        TARGET   clink_mongodb
        NAME     mongodb
        FIND_DEP "find_dependency(mongocxx CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::mongodb - enabled")
