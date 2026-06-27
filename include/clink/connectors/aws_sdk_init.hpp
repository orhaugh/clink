#pragma once

// Shared AWS C++ SDK init guard for ALL AWS-backed connectors (clink::s3 and
// clink::aws). It initialises the SDK exactly ONCE per process via call_once and
// NEVER shuts it down.
//
// Aws::InitAPI / Aws::ShutdownAPI manage GLOBAL, non-ref-counted state. If one
// connector called ShutdownAPI (in close()/dtor) while another still held a
// client, the next SDK call would touch torn-down global state and crash - a
// real hazard for a job that mixes connectors (e.g. read connector='kinesis',
// write connector='s3'). A single shared call_once init + no shutdown removes
// that hazard entirely; the OS reclaims everything at process exit (the same
// "do not FinalizeS3" stance the s3 Parquet path already takes).
//
// This header pulls in the AWS SDK, so it is included ONLY by the .cpp files of
// SDK-linked modules (which carry the SDK include path). It deliberately lives
// in the core include tree (not a per-impl tree) so both clink::s3 and
// clink::aws can include it without depending on each other; the `inline`
// function + variables collapse to ONE instance across the final link, so
// InitAPI runs exactly once no matter how many connector instances or modules
// are present.

#include <mutex>

#include <aws/core/Aws.h>

namespace clink::aws_sdk {

inline void ensure_initialized() {
    static std::once_flag flag;
    static Aws::SDKOptions options;  // static lifetime: must outlive InitAPI + all clients
    std::call_once(flag, [] { Aws::InitAPI(options); });
}

}  // namespace clink::aws_sdk
