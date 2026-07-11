// SQL-off stubs for `clink replay` and `clink replay-diff`. Replay rebuilds
// SQL Row operators via clink::sql::install, so it needs the SQL frontend; a
// SQL-off build links these stubs so the CLI still links and errors with a
// rebuild hint. Both commands the real clink_replay.cpp exports need a stub
// here or the SQL-off `clink` binary fails to link.

#include <iostream>

int clink_cmd_replay(int /*argc*/, char** /*argv*/) {
    std::cerr << "error: this clink build has no SQL support (CLINK_BUILD_SQL=OFF); "
                 "rebuild with -DCLINK_BUILD_SQL=ON to use `clink replay`\n";
    return 2;
}

int clink_cmd_replay_diff(int /*argc*/, char** /*argv*/) {
    std::cerr << "error: this clink build has no SQL support (CLINK_BUILD_SQL=OFF); "
                 "rebuild with -DCLINK_BUILD_SQL=ON to use `clink replay-diff`\n";
    return 2;
}
