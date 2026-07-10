// Stub for SQL-off builds: `clink state-query` needs the SQL frontend +
// embedded engine. Mirrors clink_run_sql_stub.cpp.

#include <iostream>

int clink_cmd_state_query(int /*argc*/, char** /*argv*/) {
    std::cerr << "clink state-query requires a build with the SQL frontend "
                 "(CLINK_BUILD_SQL=ON) and the embedded engine.\n";
    return 2;
}
