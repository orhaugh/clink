#pragma once

// Maps SASL / TLS WITH-options onto librdkafka config properties for the Kafka
// source and sink factories. Two layers:
//   1. Curated snake_case aliases (SQL-friendly), e.g. sasl_username -> the
//      librdkafka key sasl.username. Covers the common SASL_PLAIN / SCRAM /
//      SSL / mTLS cases.
//   2. A generic escape hatch: any WITH-option keyed `kafka.<prop>` sets the
//      librdkafka property `<prop>` verbatim, so an advanced caller can reach
//      any librdkafka setting the aliases do not cover.
// The result is merged into KafkaSource/Sink::Options.conf, which open()
// applies verbatim (librdkafka validates each key/value and throws on a bad
// one). Secrets (sasl.password, ssl.key.password) are passed through but never
// logged here.

#include <algorithm>
#include <array>
#include <cctype>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "clink/plugin/plugin.hpp"

namespace clink::kafka {

// Refuse a security configuration that would be silently weaker than it
// looks.
//
// librdkafka applies every property it is given and complains about none
// of these combinations, so the failure is invisible:
//
//   * credentials with no security.protocol. The default is plaintext:
//     sasl.username and sasl.password are configured, never presented,
//     and the connection is unauthenticated AND unencrypted. An operator
//     who supplied credentials plainly did not intend that.
//   * TLS material with a non-TLS protocol. A named CA or client
//     certificate that the transport will never use is the same
//     mistake wearing different clothes.
//
// Both are refused at build time - before a byte reaches a broker -
// rather than downgraded, matching the rule the transactional resume
// path already holds (an unusable CA refuses loudly instead of
// downgrading to plaintext). Deliberate plaintext stays available: name
// it, with security_protocol='plaintext'.
inline void assert_security_conf_is_coherent(const std::map<std::string, std::string>& conf) {
    const auto get = [&conf](const char* key) -> std::string {
        auto it = conf.find(key);
        return it == conf.end() ? std::string{} : it->second;
    };
    std::string protocol = get("security.protocol");
    std::transform(protocol.begin(), protocol.end(), protocol.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    const bool has_sasl_credentials =
        !get("sasl.username").empty() || !get("sasl.password").empty();
    const bool has_tls_material = !get("ssl.ca.location").empty() ||
                                  !get("ssl.certificate.location").empty() ||
                                  !get("ssl.key.location").empty();
    // Empty means librdkafka's default, which is plaintext.
    const bool protocol_carries_sasl = protocol == "sasl_plaintext" || protocol == "sasl_ssl";
    const bool protocol_carries_tls = protocol == "ssl" || protocol == "sasl_ssl";

    if (has_sasl_credentials && !protocol_carries_sasl) {
        throw std::runtime_error(
            std::string{"kafka: SASL credentials were configured but security_protocol is "} +
            (protocol.empty() ? "unset, so librdkafka's default (plaintext) applies"
                              : "'" + protocol + "'") +
            " - the credentials would never be presented and the connection would be "
            "unauthenticated and unencrypted. Set security_protocol='sasl_ssl' (or "
            "'sasl_plaintext' on a trusted network), or remove the credentials.");
    }
    if (has_tls_material && !protocol_carries_tls) {
        throw std::runtime_error(
            std::string{"kafka: TLS material (ssl_ca_location / ssl_certificate_location / "
                        "ssl_key_location) was configured but security_protocol is "} +
            (protocol.empty() ? "unset, so librdkafka's default (plaintext) applies"
                              : "'" + protocol + "'") +
            " - the connection would not be encrypted. Set security_protocol='ssl' or "
            "'sasl_ssl'.");
    }
}

// Populate `conf` with the librdkafka security/TLS properties named by the
// build context's WITH-options. Existing entries in `conf` are overwritten by
// a present option.
inline void populate_kafka_security_conf(const clink::plugin::BuildContext& ctx,
                                         std::map<std::string, std::string>& conf) {
    static constexpr std::array<std::pair<std::string_view, std::string_view>, 10> kAliases{{
        {"security_protocol", "security.protocol"},
        {"sasl_mechanism", "sasl.mechanism"},
        {"sasl_username", "sasl.username"},
        {"sasl_password", "sasl.password"},
        {"ssl_ca_location", "ssl.ca.location"},
        {"ssl_certificate_location", "ssl.certificate.location"},
        {"ssl_key_location", "ssl.key.location"},
        {"ssl_key_password", "ssl.key.password"},
        {"ssl_endpoint_identification_algorithm", "ssl.endpoint.identification.algorithm"},
        {"enable_ssl_certificate_verification", "enable.ssl.certificate.verification"},
    }};
    for (const auto& [alias, key] : kAliases) {
        if (const auto v = ctx.param_or(std::string{alias}, ""); !v.empty()) {
            conf[std::string{key}] = v;
        }
    }
    // Generic passthrough: kafka.<prop> -> <prop>. Applied AFTER the
    // aliases, so an explicit kafka.security.protocol wins - deliberate,
    // it is the escape hatch - but the coherence check below runs on the
    // FINAL map, so a passthrough that weakens the transport is caught
    // just the same.
    static constexpr std::string_view kPrefix = "kafka.";
    for (const auto& [k, v] : ctx.params) {
        if (k.size() > kPrefix.size() && k.compare(0, kPrefix.size(), kPrefix) == 0) {
            conf[k.substr(kPrefix.size())] = v;
        }
    }
    assert_security_conf_is_coherent(conf);
}

}  // namespace clink::kafka
