// SQL-off stub for the `clink run <file>.sql` dispatch target. Linked in
// place of clink_run_sql.cpp when the build has no SQL frontend, so the
// unified CLI still links and gives an actionable error instead of an
// undefined symbol.

#include <iostream>

int clink_cmd_run_sql(int /*argc*/, char** /*argv*/) {
    std::cerr << "error: this clink build has no SQL support (CLINK_BUILD_SQL=OFF); "
                 "rebuild with -DCLINK_BUILD_SQL=ON to use `clink run <file>.sql`\n";
    return 2;
}
