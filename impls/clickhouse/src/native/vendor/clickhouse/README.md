# Vendored clickhouse-cpp header

`base/sslsocket.h` is a verbatim copy of `clickhouse/base/sslsocket.h` from
clickhouse-cpp 2.6.2 (Apache License 2.0, `LICENSE` beside this file). The
library builds and exports `SSLSocketFactory` when it is configured with
OpenSSL, but does not install the header that declares it. The native
ClickHouse sink wraps that factory to count and interrupt its sockets.

`base/socket.h` is not upstream's: it forwards the copy's `#include "socket.h"`
to the installed `<clickhouse/base/socket.h>`.

The translation unit that includes the copy asserts the client version, so a
client bump fails the build until this copy is refreshed from the new release.
