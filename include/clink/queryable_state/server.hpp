#pragma once

// Server-side HTTP route registration for Queryable State.
//
// Adds two routes to an HttpServer:
//
//   GET /api/v1/queryable_state           -> list registered slot names
//   GET /api/v1/queryable_state/:slot     -> ?key=<hex> lookup
//
// The slot/value bytes are hex-encoded (lowercase) on the wire - the
// simplest scheme that survives URL/JSON transport without escaping
// edge cases. Clients translate between bytes and hex via the
// `hex_encode`/`hex_decode` helpers in this header.
//
// Status codes:
//   200 - slot exists, key found; body = {"value_hex":"..."}
//   404 - slot not registered (body = error) OR key absent (body =
//         {"value_hex":null}) - see `treat_missing_as_404` below
//   400 - malformed key=<hex>
//
// `treat_missing_as_404`: when true (the default) a missing key
// returns HTTP 404 with body {"error":"key not found"}. When false
// the route returns 200 with `value_hex:null` so the caller can
// distinguish "slot exists but key absent" from transport/server
// errors. Pick one and stick with it - clients in this header
// support both.

#include <cstdint>
#include <string>

#include "clink/http/http_server.hpp"
#include "clink/queryable_state/registry.hpp"

namespace clink::queryable_state {

namespace detail {

inline std::string hex_encode(std::span<const std::byte> bytes) {
    static constexpr const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (auto b : bytes) {
        const auto v = static_cast<unsigned char>(b);
        out.push_back(kHex[v >> 4]);
        out.push_back(kHex[v & 0x0F]);
    }
    return out;
}

inline std::optional<std::vector<std::byte>> hex_decode(const std::string& s) {
    if ((s.size() % 2) != 0) {
        return std::nullopt;
    }
    auto from_nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return 10 + (c - 'a');
        }
        if (c >= 'A' && c <= 'F') {
            return 10 + (c - 'A');
        }
        return -1;
    };
    std::vector<std::byte> out;
    out.reserve(s.size() / 2);
    for (std::size_t i = 0; i < s.size(); i += 2) {
        const int hi = from_nibble(s[i]);
        const int lo = from_nibble(s[i + 1]);
        if (hi < 0 || lo < 0) {
            return std::nullopt;
        }
        out.push_back(static_cast<std::byte>((hi << 4) | lo));
    }
    return out;
}

inline std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(c);
        }
    }
    out.push_back('"');
    return out;
}

}  // namespace detail

// Register the queryable-state routes against `server`, backed by
// `registry`. Idempotent for a given (server, registry) pair only in
// the sense that HttpServer::get() replaces a same-path handler.
inline void register_routes(http::HttpServer& server,
                            Registry& registry,
                            bool treat_missing_as_404 = true) {
    server.get("/api/v1/queryable_state",
               [&registry](const http::HttpRequest&) -> http::HttpResponse {
                   http::HttpResponse resp;
                   std::string body = "{\"slots\":[";
                   bool first = true;
                   for (const auto& s : registry.slots()) {
                       if (!first) {
                           body.push_back(',');
                       }
                       first = false;
                       body += detail::json_escape(s);
                   }
                   body += "]}";
                   resp.body = std::move(body);
                   return resp;
               });

    server.get(
        "/api/v1/queryable_state/:slot",
        [&registry, treat_missing_as_404](const http::HttpRequest& req) -> http::HttpResponse {
            http::HttpResponse resp;
            auto slot_it = req.path_params.find("slot");
            if (slot_it == req.path_params.end()) {
                resp.status = 400;
                resp.body = "{\"error\":\"missing slot\"}";
                return resp;
            }
            const std::string& slot = slot_it->second;

            if (!registry.has_slot(slot)) {
                resp.status = 404;
                resp.body = "{\"error\":\"slot not registered\"}";
                return resp;
            }

            auto key_it = req.query.find("key");
            if (key_it == req.query.end()) {
                resp.status = 400;
                resp.body = "{\"error\":\"missing key=<hex> query param\"}";
                return resp;
            }
            auto key_bytes = detail::hex_decode(key_it->second);
            if (!key_bytes.has_value()) {
                resp.status = 400;
                resp.body = "{\"error\":\"malformed key (expected lowercase hex)\"}";
                return resp;
            }

            auto value_bytes = registry.lookup(
                slot, std::span<const std::byte>{key_bytes->data(), key_bytes->size()});
            if (!value_bytes.has_value()) {
                if (treat_missing_as_404) {
                    resp.status = 404;
                    resp.body = "{\"error\":\"key not found\"}";
                } else {
                    resp.body = "{\"value_hex\":null}";
                }
                return resp;
            }
            const auto hex = detail::hex_encode(
                std::span<const std::byte>{value_bytes->data(), value_bytes->size()});
            resp.body = "{\"value_hex\":\"" + hex + "\"}";
            return resp;
        });

    // Subtask-scoped route. Same payload shape as /api/v1/queryable_state/:slot
    // but composes the registry lookup key from (role, subtask_idx, slot)
    // so multiple subtasks of the same op on the same TM don't collide
    // on the same bare slot name.
    server.get(
        "/api/v1/queryable_state/op/:role/subtask/:subtask/slot/:slot",
        [&registry, treat_missing_as_404](const http::HttpRequest& req) -> http::HttpResponse {
            http::HttpResponse resp;
            auto role_it = req.path_params.find("role");
            auto sub_it = req.path_params.find("subtask");
            auto slot_it = req.path_params.find("slot");
            if (role_it == req.path_params.end() || sub_it == req.path_params.end() ||
                slot_it == req.path_params.end()) {
                resp.status = 400;
                resp.body = "{\"error\":\"missing role / subtask / slot\"}";
                return resp;
            }
            std::uint32_t subtask_idx = 0;
            try {
                subtask_idx = static_cast<std::uint32_t>(std::stoul(sub_it->second));
            } catch (...) {
                resp.status = 400;
                resp.body = "{\"error\":\"malformed subtask\"}";
                return resp;
            }
            const std::string composed =
                compose_subtask_slot(role_it->second, subtask_idx, slot_it->second);

            if (!registry.has_slot(composed)) {
                resp.status = 404;
                resp.body = "{\"error\":\"slot not registered\"}";
                return resp;
            }
            auto key_it = req.query.find("key");
            if (key_it == req.query.end()) {
                resp.status = 400;
                resp.body = "{\"error\":\"missing key=<hex> query param\"}";
                return resp;
            }
            auto key_bytes = detail::hex_decode(key_it->second);
            if (!key_bytes.has_value()) {
                resp.status = 400;
                resp.body = "{\"error\":\"malformed key (expected lowercase hex)\"}";
                return resp;
            }
            auto value_bytes = registry.lookup(
                composed, std::span<const std::byte>{key_bytes->data(), key_bytes->size()});
            if (!value_bytes.has_value()) {
                if (treat_missing_as_404) {
                    resp.status = 404;
                    resp.body = "{\"error\":\"key not found\"}";
                } else {
                    resp.body = "{\"value_hex\":null}";
                }
                return resp;
            }
            const auto hex = detail::hex_encode(
                std::span<const std::byte>{value_bytes->data(), value_bytes->size()});
            resp.body = "{\"value_hex\":\"" + hex + "\"}";
            return resp;
        });
}

}  // namespace clink::queryable_state
