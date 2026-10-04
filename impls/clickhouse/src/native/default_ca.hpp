#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "native/sink_options.hpp"

namespace clink::clickhouse::native {

// What a path is, as the CA lookup sees it. A directory with no entries is
// Missing: it trusts nothing.
enum class PathKind { Missing, File, Directory };

// A CA file or directory trusted on top of OpenSSL's default locations.
struct CaLocation {
    std::string path;
    bool directory{false};
};

// Where the linked OpenSSL looks for trusted CAs, and what the environment
// says instead. An environment variable set to the empty string counts as
// unset.
struct CaEnvironment {
    std::optional<std::string> cert_file_env;  // SSL_CERT_FILE
    std::optional<std::string> cert_dir_env;   // SSL_CERT_DIR
    std::string default_cert_file;             // X509_get_default_cert_file()
    std::string default_cert_dir;              // X509_get_default_cert_dir()
};

// The locations tried, in order: the list the Kafka client probes when its
// OpenSSL is static, so that both connectors in one process trust the same
// store.
[[nodiscard]] const std::vector<std::string>& standard_ca_locations();

// The CA location to add because OpenSSL's own defaults would find nothing.
// None when TLS is off or unverified, when tls_ca_file or tls_ca_dir names a
// CA, when SSL_CERT_FILE or SSL_CERT_DIR is set (OpenSSL reads those itself),
// or when OpenSSL's compiled default file or directory exists. Otherwise the
// first standard location that exists, or none. Pure: the environment and
// the filesystem come in as arguments.
[[nodiscard]] std::optional<CaLocation> fallback_ca_location(
    const TlsOptions& tls,
    const CaEnvironment& env,
    const std::function<PathKind(const std::string&)>& kind_of);

// The same against this process: its environment, the linked OpenSSL's
// defaults and the real filesystem. None on a build without TLS.
[[nodiscard]] std::optional<CaLocation> fallback_ca_location(const TlsOptions& tls);

}  // namespace clink::clickhouse::native
