#pragma once

// Test-and-tooling helpers over the S3 API for the exactly-once suites: the
// external observer (every line of every visible object under a prefix), the
// in-doubt witness (multipart uploads staged but not yet completed - the S3
// analogue of pg_prepared_xacts), and idempotent bucket creation for
// throwaway endpoints (LocalStack / MinIO). No AWS SDK types in the
// interface, so callers need only link clink::s3.

#include <cstddef>
#include <string>
#include <vector>

namespace clink::s3 {

// Create the bucket if it does not exist. Retries until the endpoint
// answers (a freshly started LocalStack needs a few seconds), then treats
// already-owned as success. Throws on a real refusal or on timeout.
void ensure_bucket(const std::string& endpoint,
                   const std::string& region,
                   const std::string& bucket);

// Every line of every object under `prefix`, in object-key order. Only
// VISIBLE objects: a multipart upload that has not been completed
// contributes nothing, exactly as a downstream consumer would see it.
std::vector<std::string> read_all_lines(const std::string& endpoint,
                                        const std::string& region,
                                        const std::string& bucket,
                                        const std::string& prefix);

// The number of multipart uploads initiated under `prefix` and neither
// completed nor aborted. Staged-but-uncommitted output sits here; so do
// uploads orphaned by a crash before their checkpoint became durable
// (harmless for correctness, cleaned by a lifecycle rule in production -
// see S3Sink2PC::on_open).
std::size_t pending_multipart_count(const std::string& endpoint,
                                    const std::string& region,
                                    const std::string& bucket,
                                    const std::string& prefix);

}  // namespace clink::s3
