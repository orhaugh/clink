# clink::mysql - MySQL/MariaDB connectors (mysql_source = incremental cursor
# SELECT via PollingSource, mysql_sink = batched INSERT / upsert) built against
# mariadb-connector-c. Dep-gated: builds only when the client lib is found.

if(CLINK_WITH_MYSQL STREQUAL "OFF")
    message(STATUS "clink::mysql - CLINK_WITH_MYSQL=OFF; target not defined")
    return()
endif()

# Discover mariadb-connector-c three ways, most-portable first:
#   1. pkg-config imported target (apt libmariadb-dev ships libmariadb.pc)
#   2. mariadb's own CONFIG package
#   3. a manual header/lib probe (Homebrew mariadb-connector-c, or a bare install
#      with no pkg-config tool - this dev host's case)
find_package(PkgConfig QUIET)
set(_mysql_target "")
if(PkgConfig_FOUND)
    pkg_check_modules(LIBMARIADB QUIET IMPORTED_TARGET libmariadb)
    if(LIBMARIADB_FOUND)
        set(_mysql_target PkgConfig::LIBMARIADB)
    endif()
endif()
if(NOT _mysql_target)
    find_package(mariadb CONFIG QUIET)
    if(mariadb_FOUND AND TARGET mariadb::mariadb)
        set(_mysql_target mariadb::mariadb)
    endif()
endif()
if(NOT _mysql_target)
    find_path(_mysql_inc NAMES mariadb/mysql.h
        HINTS /opt/homebrew/opt/mariadb-connector-c/include /opt/homebrew/include
              /usr/local/include /usr/include)
    find_library(_mysql_lib NAMES mariadb mariadbclient
        HINTS /opt/homebrew/opt/mariadb-connector-c/lib /opt/homebrew/lib
              /usr/local/lib /usr/lib)
    if(_mysql_inc AND _mysql_lib)
        add_library(clink_mariadb_external INTERFACE)
        target_include_directories(clink_mariadb_external INTERFACE ${_mysql_inc})
        # Homebrew's mariadb-connector-c nests ma_io.h (which mariadb_rpl.h pulls
        # in via <mariadb/ma_io.h>) under include/mariadb/mariadb/, so the binlog
        # CDC header only resolves with include/mariadb also on the search path.
        # Harmless where the layout is flat (apt); the dir always exists.
        if(EXISTS "${_mysql_inc}/mariadb")
            target_include_directories(clink_mariadb_external INTERFACE ${_mysql_inc}/mariadb)
        endif()
        target_link_libraries(clink_mariadb_external INTERFACE ${_mysql_lib})
        set(_mysql_target clink_mariadb_external)
    endif()
endif()
if(NOT _mysql_target)
    if(CLINK_WITH_MYSQL STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_MYSQL=ON but mariadb-connector-c was not found")
    endif()
    message(STATUS "clink::mysql - mariadb-connector-c not found; target not defined")
    return()
endif()

add_library(clink_mysql STATIC
    src/register_factories.cpp
    src/mysql_cdc_source.cpp
)
add_library(clink::mysql ALIAS clink_mysql)

target_include_directories(clink_mysql PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_mysql PUBLIC cxx_std_23)
target_compile_definitions(clink_mysql PUBLIC CLINK_HAS_MYSQL)
target_link_libraries(clink_mysql
    PUBLIC  clink::core
    PRIVATE ${_mysql_target}
)
clink_set_warnings(clink_mysql)

if(TARGET clink_mariadb_external)
    install(TARGETS clink_mariadb_external EXPORT ${CLINK_EXPORT_NAME})
    clink_install_impl(TARGET clink_mysql NAME mysql)
else()
    clink_install_impl(
        TARGET   clink_mysql
        NAME     mysql
        FIND_DEP "find_dependency(mariadb CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::mysql - enabled")
