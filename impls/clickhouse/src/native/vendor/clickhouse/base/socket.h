#pragma once

// sslsocket.h, copied verbatim, includes "socket.h" from its own directory.
// The installed client carries that header as <clickhouse/base/socket.h>;
// this file forwards to it so the copy can stay byte-for-byte upstream's.
#include <clickhouse/base/socket.h>
