#include "native/default_ca.hpp"

#include <cstdlib>
#include <filesystem>
#include <system_error>

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
#include <openssl/x509.h>
#endif

namespace clink::clickhouse::native {

namespace {

std::optional<std::string> env_value(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return std::nullopt;
    }
    return std::string(value);
}

PathKind real_kind_of(const std::string& path) {
    std::error_code ec;
    const auto status = std::filesystem::status(path, ec);
    if (ec) {
        return PathKind::Missing;
    }
    if (std::filesystem::is_regular_file(status)) {
        return PathKind::File;
    }
    if (std::filesystem::is_directory(status)) {
        const std::filesystem::directory_iterator it(path, ec);
        if (ec || it == std::filesystem::directory_iterator()) {
            return PathKind::Missing;
        }
        return PathKind::Directory;
    }
    return PathKind::Missing;
}

}  // namespace

const std::vector<std::string>& standard_ca_locations() {
    static const std::vector<std::string> locations = {
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/certs/ca-bundle.crt",
        "/etc/pki/tls/certs/ca-bundle.trust.crt",
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        "/etc/ssl/ca-bundle.pem",
        "/etc/pki/tls/cacert.pem",
        "/etc/ssl/cert.pem",
        "/etc/ssl/cacert.pem",
        "/etc/certs/ca-certificates.crt",
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/ssl/certs",
        "/usr/local/etc/ssl/cert.pem",
        "/usr/local/etc/ssl/cacert.pem",
        "/usr/local/etc/ssl/certs/cert.pem",
        "/usr/local/etc/ssl/certs/cacert.pem",
        "/usr/local/share/certs/ca-root-nss.crt",
        "/etc/openssl/certs/ca-certificates.crt",
#if defined(__APPLE__)
        "/private/etc/ssl/cert.pem",
        "/private/etc/ssl/certs",
        "/usr/local/etc/openssl@1.1/cert.pem",
        "/usr/local/etc/openssl@1.0/cert.pem",
        "/usr/local/etc/openssl/certs",
        "/System/Library/OpenSSL",
#endif
    };
    return locations;
}

std::optional<CaLocation> fallback_ca_location(
    const TlsOptions& tls,
    const CaEnvironment& env,
    const std::function<PathKind(const std::string&)>& kind_of) {
    if (!tls.enabled || !tls.verify || !tls.ca_file.empty() || !tls.ca_dir.empty()) {
        return std::nullopt;
    }
    const auto set = [](const std::optional<std::string>& v) { return v && !v->empty(); };
    if (set(env.cert_file_env) || set(env.cert_dir_env)) {
        return std::nullopt;
    }
    if ((!env.default_cert_file.empty() && kind_of(env.default_cert_file) != PathKind::Missing) ||
        (!env.default_cert_dir.empty() && kind_of(env.default_cert_dir) != PathKind::Missing)) {
        return std::nullopt;
    }
    for (const std::string& path : standard_ca_locations()) {
        const PathKind kind = kind_of(path);
        if (kind != PathKind::Missing) {
            return CaLocation{path, kind == PathKind::Directory};
        }
    }
    return std::nullopt;
}

std::optional<CaLocation> fallback_ca_location(const TlsOptions& tls) {
#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
    CaEnvironment env;
    env.cert_file_env = env_value("SSL_CERT_FILE");
    env.cert_dir_env = env_value("SSL_CERT_DIR");
    env.default_cert_file = X509_get_default_cert_file();
    env.default_cert_dir = X509_get_default_cert_dir();
    return fallback_ca_location(tls, env, real_kind_of);
#else
    (void)tls;
    (void)env_value;
    (void)real_kind_of;
    return std::nullopt;
#endif
}

}  // namespace clink::clickhouse::native
