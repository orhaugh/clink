#pragma once

// Shared AWS C++ SDK init guard for ALL AWS-backed connectors that talk to the AWS SDK
// directly (clink::s3's raw S3Source/S3Sink and clink::aws's Kinesis client).
//
// It does NOT call Aws::InitAPI itself. The engine has a SINGLE owner of the AWS SDK
// lifecycle - arrow::fs's S3 init - and this routes through it (see arrow_s3_lifecycle.hpp
// for the full rationale). The short version: Arrow's S3FileSystem already initialises the
// AWS SDK via Aws::InitAPI, and from the pinned Arrow 24 that init MUST be paired with a
// FinalizeS3 at exit. If this guard also called Aws::InitAPI, a process using both an Arrow
// S3 path and a raw connector would init the SDK twice and then corrupt the heap when the
// single FinalizeS3 tore it down. Deferring to the one owner gives exactly one InitAPI and
// one FinalizeS3. arrow::fs::InitializeS3 brings up the whole AWS SDK (memory system, HTTP
// factory, crypto, logging), so a raw Aws::S3 / Aws::Kinesis client constructed afterwards
// works against that same global SDK state.

#include "clink/connectors/arrow_s3_lifecycle.hpp"

namespace clink::aws_sdk {

inline void ensure_initialized() {
    clink::connectors::ensure_arrow_s3_initialised();
}

}  // namespace clink::aws_sdk
