// A gtest main() for test binaries that initialise the AWS S3 SDK (Arrow's S3FileSystem
// or the raw impls/s3 + impls/aws connectors). It finalises Arrow S3 AFTER RUN_ALL_TESTS
// but BEFORE static destruction begins.
//
// WHY a custom main instead of the default GTest::gtest_main: the pinned from-source Arrow 24
// + aws-sdk spins up AWS CRT background event-loop threads during InitAPI. If S3 is finalised
// from an atexit handler (or never), Aws::ShutdownAPI runs DURING static destruction and races
// the CRT thread teardown + OpenSSL cleanup, crashing nondeterministically ("pure virtual
// method called" on the EvntLoopCleanup thread on Linux; "mutex lock failed" / SIGSEGV on
// macOS). Arrow's own source notes that calling FinalizeS3 from atexit "is a bad idea". Doing
// it here - on the main thread, after the last test, with all statics still alive - joins the
// CRT threads cleanly. EnsureS3Finalized is a no-op when S3 was never initialised, so this is
// safe for any test binary. See include/clink/connectors/arrow_s3_lifecycle.hpp.

#include <gtest/gtest.h>

#include "clink/connectors/arrow_s3_lifecycle.hpp"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    const int rc = RUN_ALL_TESTS();
    clink::connectors::finalize_arrow_s3();  // before static destruction; see the header
    return rc;
}
