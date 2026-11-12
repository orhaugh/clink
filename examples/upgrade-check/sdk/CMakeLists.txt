# The upgrade-check job built against an installed clink SDK, as a job's owner
# builds it for each release: a baseline from an earlier release is the same
# source compiled against that release.
#
#   cmake -S examples/upgrade-check/sdk -B <dir> -DCMAKE_PREFIX_PATH=<clink prefix>
#   cmake --build <dir>
#
# The in-tree build (examples/CMakeLists.txt) builds the same module and its
# controlled variants against the tree it is part of.
cmake_minimum_required(VERSION 3.24)
project(clink-upgrade-check-job LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(clink REQUIRED COMPONENTS core)
message(STATUS "clink ${clink_VERSION}")

clink_add_job_module(upgrade_check_job
    SOURCES ${CMAKE_CURRENT_LIST_DIR}/../upgrade_job.cpp
    OUTPUT_NAME upgrade_check_job)
