#pragma once

// The cluster's HTTP API token (CLINK_AUTH_TOKEN), read the same way by a node,
// which requires it on every request, and by every client, which presents it.

#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace clink::http {

// The token in `raw`, without the whitespace around it: a Kubernetes Secret made
// from a file, or `echo token > file`, ends it with a newline, which no request
// header can carry, so a node that kept it would refuse every request, its own
// cluster's included. Blank: no token. Throws std::invalid_argument when what is
// left holds a control character, which no request could present either.
inline std::optional<std::string> auth_token_from(std::string_view raw) {
    constexpr std::string_view kSpace = " \t\r\n\f\v";
    const auto first = raw.find_first_not_of(kSpace);
    if (first == std::string_view::npos) {
        return std::nullopt;
    }
    const auto token = raw.substr(first, raw.find_last_not_of(kSpace) - first + 1);
    for (const char c : token) {
        const auto u = static_cast<unsigned char>(c);
        if (u < 0x20 || u == 0x7f) {
            throw std::invalid_argument(
                "CLINK_AUTH_TOKEN holds a control character, which no HTTP request can carry; "
                "use a token of printable characters");
        }
    }
    return std::string{token};
}

// CLINK_AUTH_TOKEN from this process's environment, as auth_token_from reads it.
inline std::optional<std::string> cluster_auth_token() {
    const char* raw = std::getenv("CLINK_AUTH_TOKEN");
    return raw == nullptr ? std::nullopt : auth_token_from(raw);
}

}  // namespace clink::http
