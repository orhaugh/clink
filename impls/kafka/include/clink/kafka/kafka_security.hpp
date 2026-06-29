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

#include <array>
#include <map>
#include <string>
#include <string_view>
#include <utility>

#include "clink/plugin/plugin.hpp"

namespace clink::kafka {

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
    // Generic passthrough: kafka.<prop> -> <prop>.
    static constexpr std::string_view kPrefix = "kafka.";
    for (const auto& [k, v] : ctx.params) {
        if (k.size() > kPrefix.size() && k.compare(0, kPrefix.size(), kPrefix) == 0) {
            conf[k.substr(kPrefix.size())] = v;
        }
    }
}

}  // namespace clink::kafka
