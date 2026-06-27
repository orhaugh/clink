#pragma once

// SDK-dependent shared glue for the AWS-family connectors (Kinesis, Firehose,
// DynamoDB). Only included by the impls/aws .cpp files, which are compiled only
// when the AWS SDK is found, so it may include the SDK headers directly.

#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>

namespace clink::aws {

// Initialise the AWS C++ SDK exactly once per process and NEVER shut it down.
// Aws::InitAPI / Aws::ShutdownAPI manage GLOBAL, non-ref-counted state; calling
// ShutdownAPI while another connector still holds a client races its
// destructors (the documented FinalizeS3 hazard the s3 Parquet path also
// avoids). call_once-init + no-shutdown is the safe unilateral choice: the OS
// reclaims everything at process exit.
//
// CAVEAT: the legacy clink::s3 (AWS-SDK) sink/source call InitAPI in open() and
// ShutdownAPI in close(); mixing connector='s3' with the aws-family connectors
// in ONE job can therefore have s3's close() tear the SDK down under a live aws
// client. The s3_parquet path (Arrow S3FileSystem) and all non-AWS connectors
// are unaffected. Documented as a baseline limitation.
inline void ensure_aws_initialized() {
    static std::once_flag flag;
    static Aws::SDKOptions options;  // static lifetime: must outlive InitAPI
    std::call_once(flag, [] { Aws::InitAPI(options); });
}

// Common client config for the AWS-family connectors: region + endpoint
// override (LocalStack / a custom endpoint) + bounded timeouts and retries so a
// write/read fails fast before a checkpoint barrier rather than blocking on the
// SDK's long default backoff.
struct AwsClientOptions {
    std::optional<std::string> region;
    std::optional<std::string> endpoint_override;
    int connect_timeout_ms{3000};
    int request_timeout_ms{10000};
    long max_retries{3};
};

inline Aws::Client::ClientConfiguration make_client_config(const AwsClientOptions& o) {
    Aws::Client::ClientConfiguration cfg;
    if (o.region) {
        cfg.region = *o.region;
    }
    if (o.endpoint_override) {
        cfg.endpointOverride = *o.endpoint_override;
    }
    cfg.connectTimeoutMs = o.connect_timeout_ms;
    cfg.requestTimeoutMs = o.request_timeout_ms;
    cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(o.max_retries);
    return cfg;
}

}  // namespace clink::aws
