// Stub for builds whose Arrow ships without Flight SQL: keeps the CLI
// linking and gives an actionable error instead of an unknown command.

#include <iostream>

int clink_cmd_flight_sql(int /*argc*/, char** /*argv*/) {
    std::cerr << "clink flight-sql: this build's Arrow has no Flight SQL "
                 "(install an Arrow with ARROW_FLIGHT_SQL=ON and reconfigure)\n";
    return 2;
}
