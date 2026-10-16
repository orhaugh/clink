#pragma once

// PRIVATE implementation helper - lives under src/, NOT include/, so the libpulsar C API is
// confined to the .cpp side (the public connector headers stay free of it). We use the C API
// (pulsar/c/), not the C++ API, deliberately: the prebuilt libpulsar .deb / Homebrew bottle is
// built with its own libstdc++/libc++, and passing C++ types (std::string) across that boundary
// risks an ABI mismatch with the image's gcc-14; the C API is C-linkage and ABI-stable.

#include <filesystem>
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

// check, for the call that first reaches a broker (create_producer, subscribe). Over TLS,
// libpulsar reports a broker certificate it refused as a plain ConnectError and names the
// cause only in its log, so the message says how to tell that from an unreachable broker.
inline void check_first_contact(pulsar_result r,
                                const std::string& ctx,
                                const PulsarConnParams& c) {
    if (r == pulsar_result_ConnectError && c.service_url.rfind("pulsar+ssl://", 0) == 0) {
        throw std::runtime_error(
            ctx + ": " + pulsar_result_str(r) +
            ". Over TLS this is also how a refused broker certificate is reported; the "
            "client log then says 'Handshake failed: certificate verify failed'. Check that " +
            (c.tls_ca.empty() ? std::string{"the default CA store"} : "tls_ca " + c.tls_ca) +
            " trusts the broker's CA" +
            (c.tls_verify_hostname
                 ? " and that its certificate names the host in service_url, or turn the "
                   "hostname check off with tls_verify_hostname=false"
                 : std::string{}) +
            ".");
    }
    check(r, ctx);
}

// Create a Pulsar client (connection is lazy - it actually connects on create_producer /
// subscribe, where a dead endpoint surfaces as an error). Throws on a bad configuration. The
// caller owns the client and must pulsar_client_close + pulsar_client_free it.
inline pulsar_client_t* connect(const PulsarConnParams& c, const std::string& ctx) {
    // libpulsar reports a missing trust file only as a failed connect, so name it here.
    if (!c.tls_ca.empty() && !std::filesystem::is_regular_file(c.tls_ca)) {
        throw std::runtime_error(ctx + ": tls_ca " + c.tls_ca + " is not a readable file");
    }
    pulsar_client_configuration_t* conf = pulsar_client_configuration_create();
    pulsar_client_configuration_set_operation_timeout_seconds(conf, c.operation_timeout_s);
    // Over TLS, check that each broker's certificate names the host the client dials it by;
    // libpulsar leaves that off by default and checks only the chain.
    pulsar_client_configuration_set_validate_hostname(conf, c.tls_verify_hostname ? 1 : 0);
    if (!c.tls_ca.empty()) {
        pulsar_client_configuration_set_tls_trust_certs_file_path(conf, c.tls_ca.c_str());
    }
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
