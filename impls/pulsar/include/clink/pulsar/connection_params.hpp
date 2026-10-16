#pragma once

#include <string>

namespace clink::pulsar {

// Apache Pulsar client connection parameters shared by the source and sink. Plain fields - no
// libpulsar types leak into the public headers (the pulsar C API is confined to the .cpp). Auth
// is via an optional JWT token (leave blank for no-auth).
struct PulsarConnParams {
    std::string service_url{"pulsar://localhost:6650"};
    std::string token;            // optional JWT (pulsar_authentication_token)
    int operation_timeout_s{30};  // per-operation timeout
    // TLS, used when service_url is pulsar+ssl://. tls_ca is a PEM file of the CAs to trust in
    // place of the default store. tls_verify_hostname=false keeps the chain check but accepts a
    // broker certificate that names another host.
    std::string tls_ca;
    bool tls_verify_hostname{true};
};

}  // namespace clink::pulsar
