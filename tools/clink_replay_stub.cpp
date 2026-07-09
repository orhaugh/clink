// SQL-off stub for `clink replay`. Replay rebuilds SQL Row operators via
// clink::sql::install, so it needs the SQL frontend; a SQL-off build links
// this stub so the CLI still links and errors with a rebuild hint.

#include <iostream>

int clink_cmd_replay(int /*argc*/, char** /*argv*/) {
    std::cerr << "error: this clink build has no SQL support (CLINK_BUILD_SQL=OFF); "
                 "rebuild with -DCLINK_BUILD_SQL=ON to use `clink replay`\n";
    return 2;
}
