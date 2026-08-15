#pragma once

// Transport assertion for every libpq connection clink opens.
//
// libpq's default is sslmode=prefer: try TLS, and SILENTLY fall back to
// an unencrypted connection when the server does not offer it. A conninfo
// that names a certificate, or an operator who simply assumed a managed
// database meant an encrypted link, gets plaintext with nothing said.
// That is the one thing a security configuration must never do, and until
// this existed clink handed the raw conninfo to PQconnectdb and asked no
// questions - no sslmode handling anywhere in the connector, and no
// mention of TLS in its documentation.
//
// This does not take the choice away from the operator. Plaintext to a
// Postgres on the same private network is a legitimate deployment, and
// refusing it outright would break every such user to no benefit. What it
// removes is the SILENCE:
//
//   * asked for TLS (sslmode=require / verify-ca / verify-full) and did
//     not get it -> throw. libpq should already have refused; this makes
//     the invariant clink's own rather than a dependency's, so a future
//     libpq default or a build without SSL support cannot quietly weaken
//     it.
//   * asked for nothing, and the connection came up unencrypted -> a
//     loud warning naming the option that fixes it. This is the
//     silent-downgrade case, now stated.
//   * encrypted -> recorded at info, so an operator can prove from the
//     log what the transport actually was.
//
// PQsslInUse is the authority here rather than the requested mode: it
// reports what the established connection IS, which is the only fact
// worth asserting on.

#include <cctype>
#include <libpq-fe.h>
#include <stdexcept>
#include <string>

#include "clink/runtime/log_buffer.hpp"

namespace clink::connectors::pg {

// The sslmode the conninfo asks for, lower-cased, or "" when it names
// none. Only the conninfo is parsed: libpq also reads PGSSLMODE from the
// environment, and a value found there is deliberately treated as "not
// stated in the configuration clink was given", so it lands in the
// warn-if-plaintext path rather than being trusted as an explicit ask.
[[nodiscard]] inline std::string requested_sslmode(const std::string& conninfo) {
    // conninfo is a whitespace-separated list of key=value, values
    // optionally single-quoted. A URI form (postgresql://...) can carry
    // sslmode as a query parameter; both spellings are searched for the
    // same token, which is enough for a mode name that cannot appear as
    // a substring of another key.
    const std::string needle = "sslmode";
    std::size_t pos = 0;
    while ((pos = conninfo.find(needle, pos)) != std::string::npos) {
        // Must be a key: preceded by a delimiter (or start) and followed
        // by '=' after optional spaces.
        const bool key_start = pos == 0 || conninfo[pos - 1] == ' ' || conninfo[pos - 1] == '?' ||
                               conninfo[pos - 1] == '&' || conninfo[pos - 1] == '\t';
        std::size_t after = pos + needle.size();
        while (after < conninfo.size() && conninfo[after] == ' ') {
            ++after;
        }
        if (!key_start || after >= conninfo.size() || conninfo[after] != '=') {
            pos += needle.size();
            continue;
        }
        ++after;
        while (after < conninfo.size() && (conninfo[after] == ' ' || conninfo[after] == '\'')) {
            ++after;
        }
        std::string value;
        while (after < conninfo.size() && conninfo[after] != ' ' && conninfo[after] != '\'' &&
               conninfo[after] != '&') {
            value.push_back(
                static_cast<char>(std::tolower(static_cast<unsigned char>(conninfo[after]))));
            ++after;
        }
        return value;
    }
    return {};
}

// Whether that mode is one that REQUIRES encryption. 'prefer' and
// 'allow' are excluded deliberately: they are the modes that downgrade.
[[nodiscard]] inline bool sslmode_demands_encryption(const std::string& mode) {
    return mode == "require" || mode == "verify-ca" || mode == "verify-full";
}

// Call immediately after a successful PQconnectdb. `who` names the
// connector for the diagnostic ("postgres_sink", "postgres_cdc_source").
inline void assert_no_silent_downgrade(PGconn* conn, const std::string& conninfo, const char* who) {
    if (conn == nullptr) {
        return;
    }
    const auto mode = requested_sslmode(conninfo);
    const bool encrypted = PQsslInUse(conn) != 0;
    if (encrypted) {
        clink::log::info(who,
                         std::string{"connection is TLS-encrypted (sslmode="} +
                             (mode.empty() ? "unset" : mode) + ")");
        return;
    }
    if (sslmode_demands_encryption(mode)) {
        // Unreachable through a correct libpq, which refuses rather than
        // downgrading - which is exactly why it is worth asserting: the
        // day it becomes reachable is the day it must not pass quietly.
        throw std::runtime_error(std::string{who} + ": sslmode=" + mode +
                                 " was requested but the established connection is NOT "
                                 "encrypted; refusing rather than continuing in plaintext");
    }
    clink::log::warn(
        who,
        std::string{"connection is NOT encrypted"} +
            (mode.empty()
                 ? " and the conninfo names no sslmode, so libpq's default (prefer) applied - "
                   "prefer silently falls back to plaintext when the server offers no TLS. "
                   "Set sslmode=require (or verify-full with sslrootcert) in the conninfo to "
                   "require encryption, or sslmode=disable to state plaintext deliberately."
                 : " (sslmode=" + mode + " does not require it)"));
}

}  // namespace clink::connectors::pg
