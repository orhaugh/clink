find_program(CLANG_FORMAT clang-format
        HINTS /Library/Developer/CommandLineTools/usr/bin
              /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin)
find_program(CLANG_TIDY clang-tidy
        HINTS /Library/Developer/CommandLineTools/usr/bin
              /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin)

# clang-format operates on .cpp + .hpp alike - format every owned file.
file(GLOB_RECURSE ALL_FORMAT_FILES
        ${PROJECT_SOURCE_DIR}/src/*.cpp
        ${PROJECT_SOURCE_DIR}/include/clink/*.hpp
        ${PROJECT_SOURCE_DIR}/impls/*/src/*.cpp
        ${PROJECT_SOURCE_DIR}/impls/*/include/clink/*.hpp
        ${PROJECT_SOURCE_DIR}/impls/*/tests/*.cpp
        ${PROJECT_SOURCE_DIR}/tests/*.cpp
        ${PROJECT_SOURCE_DIR}/tools/*.cpp
        ${PROJECT_SOURCE_DIR}/examples/*.cpp)

# clang-tidy only operates on .cpp TUs that the build actually compiles.
# Headers are NOT linted standalone - clang-tidy would have to invent a
# compile command for them, which breaks for private inline-implementation
# headers like include/clink/plugin/plugin_impl.hpp that depend on the
# surrounding context from their public sibling. Headers get linted
# transitively when the consumer .cpp is processed, with warnings filtered
# through HeaderFilterRegex in .clang-tidy.
file(GLOB_RECURSE TIDY_SOURCES
        ${PROJECT_SOURCE_DIR}/src/*.cpp
        ${PROJECT_SOURCE_DIR}/impls/*/src/*.cpp
        ${PROJECT_SOURCE_DIR}/impls/*/tests/*.cpp
        ${PROJECT_SOURCE_DIR}/tests/*.cpp
        ${PROJECT_SOURCE_DIR}/tools/*.cpp
        ${PROJECT_SOURCE_DIR}/examples/*.cpp)

# tests/integration/*.cpp are only compiled when CLINK_INTEGRATION_TESTS
# is ON. Without that, they have no compile_commands.json entry and
# clang-tidy errors out with "file not found" on libpq-fe.h / gtest. Skip
# them in that case so plain `cmake --build ... --target tidy` doesn't
# produce phantom errors.
if(NOT CLINK_INTEGRATION_TESTS)
    list(FILTER TIDY_SOURCES EXCLUDE REGEX "/tests/integration/")
endif()

if (CLANG_FORMAT)
    add_custom_target(format
            COMMAND ${CLANG_FORMAT} -i ${ALL_FORMAT_FILES}
            COMMENT "Running clang-format on all source files")

    add_custom_target(format-check
            COMMAND ${CLANG_FORMAT} --dry-run --Werror ${ALL_FORMAT_FILES}
            COMMENT "Checking formatting with clang-format")
else ()
    message(STATUS "clang-format not found, format/format-check targets will not be available")
endif ()

if (CLANG_TIDY)
    # clang-tidy uses the build dir's compile_commands.json to know how
    # to compile each TU; CMAKE_EXPORT_COMPILE_COMMANDS=ON in the top-
    # level CMakeLists.txt makes that file appear automatically.
    add_custom_target(tidy
            COMMAND ${CLANG_TIDY} -p ${CMAKE_BINARY_DIR} ${TIDY_SOURCES}
            COMMENT "Running clang-tidy on .cpp TUs (headers linted transitively)")
else ()
    message(STATUS "clang-tidy not found, tidy target will not be available")
endif ()
