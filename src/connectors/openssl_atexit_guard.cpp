// Before-main installation of the OpenSSL atexit guard (see openssl_atexit_guard.hpp for the
// full rationale: from-source Arrow 24's AWS CRT vs OpenSSL 3 disagree on crypto-allocator
// ownership at shutdown, and OpenSSL's atexit OPENSSL_cleanup then corrupts the heap on Linux).
//
// Suppressing OpenSSL's atexit must happen before the FIRST OpenSSL use in the process. The S3
// lifecycle owner already calls suppress_openssl_atexit() before InitializeS3, which covers the
// common S3-first ordering. This constructor covers the orderings where OpenSSL is brought up
// FIRST by some other subsystem (TLS cluster transport, Postgres-over-SSL, libcurl in the
// iceberg REST catalog) before any S3 sink opens: a before-main constructor beats every such
// lazy, called-from-main init. Suppressing unconditionally is harmless when no S3/AWS path runs
// - it just means OpenSSL's own state is reclaimed by the OS at exit instead of by its handler.
//
// The constructor lives in clink_core. To guarantee the static-lib object file is not dropped by
// the linker (a TU referenced by nothing would be), clink_force_openssl_atexit_guard is an
// exported anchor that clink_node references; see its use in the node entrypoint.

#include "clink/connectors/openssl_atexit_guard.hpp"

namespace clink::connectors {

namespace {
__attribute__((constructor(101))) void install_openssl_atexit_guard() {
    suppress_openssl_atexit();
}
}  // namespace

// Anchor symbol: referencing this from an always-linked TU forces this object file (and thus the
// constructor above) into the final link of a static-clink_core consumer.
int clink_force_openssl_atexit_guard = 0;

}  // namespace clink::connectors
