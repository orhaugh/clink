#pragma once

// PRIVATE implementation helper - lives under src/, NOT include/, so the libpulsar C API is
// confined to the .cpp side (the public connector headers stay free of it). We use the C API
// (pulsar/c/), not the C++ API, deliberately: the prebuilt libpulsar .deb / Homebrew bottle is
// built with its own libstdc++/libc++, and passing C++ types (std::string) across that boundary
// risks an ABI mismatch with the image's gcc-14; the C API is C-linkage and ABI-stable.

#include <stdexcept>
#include <string>

#include <pulsar/c/authentication.h>
#include <pulsar/c/client.h>
#include <pulsar/c/client_configuration.h>
#include <pulsar/c/result.h>

#include "clink/pulsar/connection_params.hpp"

namespace clink::pulsar::detail {

inline void check(pulsar_result r, const std::string& ctx) {
    if (r != pulsar_result_Ok) {
        throw std::runtime_error(ctx + ": " + pulsar_result_str(r));
    }
}

// Create a Pulsar client (connection is lazy - it actually connects on create_producer /
// subscribe, where a dead endpoint surfaces as an error). Throws on a bad configuration. The
// caller owns the client and must pulsar_client_close + pulsar_client_free it.
inline pulsar_client_t* connect(const PulsarConnParams& c, const std::string& ctx) {
    pulsar_client_configuration_t* conf = pulsar_client_configuration_create();
    pulsar_client_configuration_set_operation_timeout_seconds(conf, c.operation_timeout_s);
    if (!c.token.empty()) {
        // The configuration takes ownership of the authentication object.
        pulsar_authentication_t* auth = pulsar_authentication_token_create(c.token.c_str());
        pulsar_client_configuration_set_auth(conf, auth);
    }
    pulsar_client_t* client = pulsar_client_create(c.service_url.c_str(), conf);
    pulsar_client_configuration_free(conf);
    if (client == nullptr) {
        throw std::runtime_error(ctx + ": pulsar_client_create failed for " + c.service_url);
    }
    return client;
}

}  // namespace clink::pulsar::detail
