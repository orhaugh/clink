set(GIT_HOOKS_DIR "${PROJECT_SOURCE_DIR}/.git/hooks")
set(PROJECT_HOOKS_DIR "${PROJECT_SOURCE_DIR}/scripts")

if (EXISTS "${PROJECT_SOURCE_DIR}/.git" AND EXISTS "${PROJECT_HOOKS_DIR}/pre-commit")
    file(COPY "${PROJECT_HOOKS_DIR}/pre-commit"
         DESTINATION "${GIT_HOOKS_DIR}"
         FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                          GROUP_READ GROUP_EXECUTE
                          WORLD_READ WORLD_EXECUTE)
    message(STATUS "Installed pre-commit hook from scripts/pre-commit")
endif ()
