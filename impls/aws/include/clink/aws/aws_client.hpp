#pragma once

// SDK-dependent shared glue for the AWS-family connectors (Kinesis, Firehose,
// DynamoDB). Only included by the impls/aws .cpp files, which are compiled only
// when the AWS SDK is found, so it may include the SDK headers directly.

#include <memory>
#include <optional>
#include <string>

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>

#include "clink/connectors/aws_sdk_init.hpp"

namespace clink::aws {

// Initialise the AWS C++ SDK once per process (never shut down). Delegates to
// the SHARED guard so clink::s3 and clink::aws init exactly once between them -
// no module ever calls ShutdownAPI, so a job mixing s3 and aws connectors
// cannot tear the SDK down under a live client. See aws_sdk_init.hpp.
inline void ensure_aws_initialized() {
    clink::aws_sdk::ensure_initialized();
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
