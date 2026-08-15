set(GIT_HOOKS_DIR "${PROJECT_SOURCE_DIR}/.git/hooks")
set(PROJECT_HOOKS_DIR "${PROJECT_SOURCE_DIR}/scripts")

# IS_DIRECTORY, not EXISTS: in a git WORKTREE `.git` is a pointer FILE
# (gitdir: ...), so `.git/hooks` is not a writable path and the copy
# aborted the whole configure - which is how the qualification runner's
# worktree builds failed on their first run. A worktree shares the main
# checkout's hooks anyway, so skipping is correct there, not a loss.
if (IS_DIRECTORY "${PROJECT_SOURCE_DIR}/.git" AND EXISTS "${PROJECT_HOOKS_DIR}/pre-commit")
    file(COPY "${PROJECT_HOOKS_DIR}/pre-commit"
         DESTINATION "${GIT_HOOKS_DIR}"
         FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                          GROUP_READ GROUP_EXECUTE
                          WORLD_READ WORLD_EXECUTE)
    message(STATUS "Installed pre-commit hook from scripts/pre-commit")
endif ()
