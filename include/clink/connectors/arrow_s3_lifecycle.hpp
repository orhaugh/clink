#pragma once

// Single owner of the Arrow / AWS-SDK S3 lifecycle for the whole engine.
//
// INIT. Two code paths initialise the AWS C++ SDK: Arrow's S3FileSystem (Parquet-on-S3, S3
// snapshot/CAS stores, the Delta + Iceberg S3 sinks) via arrow::fs::InitializeS3, and clink's
// raw connectors (impls/s3 S3Source/S3Sink, impls/aws Kinesis) which used to call Aws::InitAPI
// directly. We funnel both through ensure_arrow_s3_initialised() so Aws::InitAPI runs exactly
// once under Arrow's guard; arrow::fs::InitializeS3 brings up the whole AWS SDK (memory system,
// HTTP factory, crypto, logging), so a raw Aws::S3 / Aws::Kinesis client built afterwards works
// against that same global state.
//
// EXIT. The pinned from-source Arrow 24 + aws-sdk has TWO distinct, Linux-only exit-time faults
// (macOS's allocator + libc++ teardown masked both, which is why neither showed before the
// toolchain swap), each needing its own fix:
//
//   (a) OpenSSL 3 atexit. The AWS CRT (aws-c-cal) brings up system libcrypto during InitAPI;
//       OpenSSL's atexit OPENSSL_cleanup then corrupts the heap fighting the SDK over the crypto
//       allocator. Fixed with OPENSSL_INIT_NO_ATEXIT (openssl_atexit_guard.hpp), applied on
//       every platform (harmless where unneeded), from a before-main ctor and from init below.
//
//   (b) AWS CRT event-loop threads. The SDK runs background event-loop threads. They MUST be
//       joined (Aws::ShutdownAPI, i.e. arrow::fs::FinalizeS3) before C++ static destruction, or
//       they run canceled tasks mid-teardown and call into half-destroyed statics: "pure virtual
//       method called" on the EvntLoopCleanup thread (Linux), "mutex lock failed" / SIGSEGV
//       (macOS). CRITICALLY, finalising from an atexit handler does NOT work - atexit runs
//       DURING static destruction and races the very teardown it must precede (flaky aborts on
//       both platforms; Arrow's own source calls atexit-FinalizeS3 "a bad idea"). So this header
//       does NOT register any atexit. Instead each entry point that may initialise S3 calls
//       finalize_arrow_s3() explicitly, on the main thread, AFTER its work and BEFORE returning
//       (clink_node's main; the s3_finalizing_gtest_main used by S3-capable test binaries). By
//       then every connector S3FileSystem / Aws client is destroyed and all statics are alive,
//       so ShutdownAPI joins the CRT threads cleanly. finalize_arrow_s3() is a no-op when S3 was
//       never initialised, so calling it unconditionally at an entry point is always safe.
//
// Include only from TUs that carry the Arrow include path (every S3-backed connector, plus
// clink_core, does). The call_once guard keeps init to one InitializeS3.

#include <mutex>
#include <stdexcept>
#include <string>

#include <arrow/filesystem/s3fs.h>

#include "clink/connectors/openssl_atexit_guard.hpp"  // suppress_openssl_atexit

namespace clink::connectors {

// THE canonical "I am about to use Arrow/AWS S3" call. Suppresses OpenSSL's heap-corrupting
// atexit, then initialises Arrow's S3 subsystem (and therefore the AWS SDK) exactly once with a
// quiet log level. Idempotent; robust to Arrow having already initialised S3 behind our back.
inline void ensure_arrow_s3_initialised() {
    static std::once_flag once;
    std::call_once(once, [] {
        suppress_openssl_atexit();  // before InitializeS3 brings up the AWS CRT + OpenSSL
        if (!arrow::fs::IsS3Initialized()) {
            auto opts = arrow::fs::S3GlobalOptions::Defaults();
            opts.log_level = arrow::fs::S3LogLevel::Fatal;  // quiet by default
            auto s = arrow::fs::InitializeS3(opts);
            if (!s.ok()) {
                throw std::runtime_error("Arrow S3 init failed: " + s.ToString());
            }
        }
    });
}

// Finalise Arrow S3 (joins the AWS CRT event-loop threads). Call this exactly once per process,
// on the main thread, AFTER all S3 work and BEFORE returning from main / before static
// destruction - NEVER from an atexit handler or a static destructor (see note (b) above). A
// no-op when S3 was never initialised, so it is safe to call from any entry point unconditionally.
inline void finalize_arrow_s3() {
    (void)arrow::fs::EnsureS3Finalized();
}

}  // namespace clink::connectors
