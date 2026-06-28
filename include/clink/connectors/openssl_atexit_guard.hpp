#pragma once

// Suppress OpenSSL 3's process-exit cleanup (OPENSSL_cleanup, registered via atexit).
//
// WHY. The AWS SDK's CRT (aws-c-cal) brings up system libcrypto during Aws::InitAPI (which
// Arrow's S3 InitializeS3 calls). On the pinned from-source toolchain, OpenSSL's own atexit
// handler OPENSSL_cleanup then runs at exit and CORRUPTS THE HEAP ("malloc_consolidate /
// unaligned fastbin chunk", allocating via CRYPTO_aligned_alloc during teardown): the AWS SDK
// and OpenSSL disagree about who owns the crypto allocator at shutdown. glibc detects the
// corruption and aborts; macOS malloc silently tolerates it, which is why this is Linux-only
// and was masked on the host. Telling OpenSSL not to register its atexit handler removes the
// conflicting teardown entirely; the OS reclaims the memory at process exit anyway - the same
// "let the OS reclaim it" stance clink already takes for the AWS SDK itself.
//
// HOW. We resolve OPENSSL_init_crypto from the already-loaded libcrypto via dlsym(RTLD_DEFAULT)
// rather than linking + including OpenSSL: libcrypto is always present where this matters
// (aws-c-cal / libssl / libcurl pull it in), this keeps the call dependency-free, and it
// degrades to a harmless no-op if the symbol is absent. OPENSSL_INIT_NO_ATEXIT (0x00080000) is
// a stable OpenSSL >=1.1.0 ABI constant. This MUST run before the FIRST OpenSSL use in the
// process, so it is invoked both (a) from a before-main constructor (openssl_atexit_guard.cpp,
// covering jobs that touch OpenSSL via TLS / Postgres-SSL / libcurl before any S3 sink opens)
// and (b) from clink's S3 lifecycle owner before InitializeS3 (covering the common, and only
// tested-in-CI, S3-first ordering and any process that does not link the core guard TU).

#include <cstdint>
#include <dlfcn.h>
#include <mutex>

namespace clink::connectors {

inline void suppress_openssl_atexit() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        using init_crypto_fn = int (*)(std::uint64_t, const void*);
        if (auto* sym = dlsym(RTLD_DEFAULT, "OPENSSL_init_crypto")) {
            constexpr std::uint64_t kNoAtexit = 0x00080000UL;  // OPENSSL_INIT_NO_ATEXIT
            reinterpret_cast<init_crypto_fn>(sym)(kNoAtexit, nullptr);
        }
    });
}

// Anchor for the before-main constructor in openssl_atexit_guard.cpp. Reference this from an
// always-linked TU (clink_node's main) so the static linker keeps that object file - and its
// constructor - in the final binary.
extern int clink_force_openssl_atexit_guard;

}  // namespace clink::connectors
