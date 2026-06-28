#pragma once

// Single owner of the Arrow / AWS-SDK S3 lifecycle for the whole engine.
//
// THE PROBLEM. Two separate code paths used to initialise the AWS C++ SDK:
//   - Arrow's S3FileSystem (Parquet-on-S3, S3 snapshot/CAS stores, the Delta and
//     Iceberg S3 sinks) lazily calls arrow::fs::InitializeS3 -> Aws::InitAPI.
//   - clink's raw connectors (impls/s3 S3Source/S3Sink, impls/aws Kinesis) called
//     Aws::InitAPI directly via aws_sdk_init.hpp.
// Aws::InitAPI / Aws::ShutdownAPI manage GLOBAL, non-ref-counted SDK state, so a
// process that used BOTH paths called InitAPI twice. That was harmless only because
// nobody ever called ShutdownAPI - the "never FinalizeS3" stance papered over it.
//
// It stopped being harmless on the pinned from-source Arrow 24: that build ABORTS at
// process exit if S3 was initialised but never finalised ("arrow::fs::FinalizeS3 was
// not called ..."). Adding a FinalizeS3 atexit then exposed the underlying double-init:
// one ShutdownAPI tore down SDK globals while the other path still held them, giving a
// "corrupted size vs. prev_size" heap abort at exit. (The prior distro Arrow tolerated
// the un-finalised state, which is why none of this surfaced before the toolchain swap.)
//
// THE FIX: a SINGLE owner. Every S3 user - Arrow-based or raw-SDK - initialises through
// arrow::fs (here), so Aws::InitAPI is called exactly once (under Arrow's own guard) and
// a single atexit FinalizeS3 tears it down exactly once. arrow::fs::InitializeS3 brings up
// the entire AWS SDK (memory system, HTTP factory, crypto, logging), so a raw Aws::S3 or
// Aws::Kinesis client constructed afterwards works against the same global SDK state.
//
// WHY atexit (not connector close()): the finalise runs AFTER main returns, by which point
// every connector object holding an S3FileSystem or an Aws client has been destroyed during
// job/test shutdown (those handles are instance-scoped, never static). That ordering is what
// makes finalising safe - the FinalizeS3-vs-live-client race that motivated the original
// "never finalize" stance cannot happen at atexit time.
//
// This header pulls in Arrow's S3 header, so include it only from TUs that carry the Arrow
// include path (every S3-backed connector, plus clink_core, does). The call_once guards keep
// init to a single InitializeS3 and the atexit registration to a single handler no matter how
// many call sites or modules invoke them.

#include <cstdlib>
#include <mutex>
#include <stdexcept>
#include <string>

#include <arrow/filesystem/s3fs.h>

#include "clink/connectors/openssl_atexit_guard.hpp"  // suppress_openssl_atexit

namespace clink::connectors {

// Register arrow::fs::EnsureS3Finalized() to run once at process exit. Use this on paths
// where Arrow itself lazily initialises S3 out of our sight (e.g. an iceberg REST catalog
// resolving an arrow-fs-s3 FileIO internally): we cannot intercept that InitializeS3, but we
// must still pair it with a finalise. EnsureS3Finalized is a no-op when S3 was never
// initialised, so registering this unconditionally is harmless.
inline void ensure_arrow_s3_finalize_registered() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        suppress_openssl_atexit();
        std::atexit([] { (void)arrow::fs::EnsureS3Finalized(); });
    });
}

// THE canonical "I am about to use Arrow/AWS S3" call. Initialises Arrow's S3 subsystem
// (and therefore the AWS SDK) exactly once with a quiet log level, and registers the atexit
// finalise. Idempotent and safe to call from every S3 entry point in the engine; it is also
// robust to Arrow having already initialised S3 behind our back (it skips the InitializeS3 in
// that case and just registers the finalise).
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
        ensure_arrow_s3_finalize_registered();
    });
}

}  // namespace clink::connectors
