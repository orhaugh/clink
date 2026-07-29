#pragma once

// Same-origin static file serving for the coordinator's HTTP server -
// `clink_node --http-static-dir=<dir>` mounts a built single-page console
// (clink-fe's `dist/`, or any static bundle) on `/`, alongside the JSON API.
// Same origin means the console needs no CORS configuration and no separate
// web server: one port serves both the API and the UI.
//
// Semantics, chosen for an SPA with browser-history routing:
//   * a path that resolves to a regular file under the root is served with
//     a content type derived from its extension;
//   * a path with no extension that resolves to nothing falls back to
//     index.html (deep links like /jobs/7 land on the SPA router);
//   * a path with an extension that resolves to nothing is 404 (a missing
//     asset must fail loudly, not serve HTML to a script tag);
//   * anything escaping the root - `..` segments, absolute paths, symlinks
//     pointing outside - is 404, checked against the canonicalised root.
//
// Pure logic + filesystem reads, no server types beyond HttpResponse, so
// the traversal and fallback rules are unit-testable without sockets.

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>

#include "clink/http/http_server.hpp"

namespace clink::http {

inline std::string static_content_type_for(const std::filesystem::path& file) {
    const std::string ext = file.extension().string();
    if (ext == ".html" || ext == ".htm") {
        return "text/html; charset=utf-8";
    }
    if (ext == ".js" || ext == ".mjs") {
        return "text/javascript; charset=utf-8";
    }
    if (ext == ".css") {
        return "text/css; charset=utf-8";
    }
    if (ext == ".json" || ext == ".map") {
        return "application/json";
    }
    if (ext == ".svg") {
        return "image/svg+xml";
    }
    if (ext == ".png") {
        return "image/png";
    }
    if (ext == ".ico") {
        return "image/x-icon";
    }
    if (ext == ".webp") {
        return "image/webp";
    }
    if (ext == ".woff2") {
        return "font/woff2";
    }
    if (ext == ".txt") {
        return "text/plain; charset=utf-8";
    }
    return "application/octet-stream";
}

// Serve `tail` (the request path relative to the mount, no leading slash
// required) from under `root`. `root` must already be canonical - the
// caller canonicalises once at startup, not per request.
inline HttpResponse serve_static_file(const std::filesystem::path& canonical_root,
                                      std::string_view tail) {
    namespace fs = std::filesystem;
    HttpResponse resp;

    // Belt: reject dot-dot segments before touching the filesystem. The
    // canonical-prefix check below is the braces; percent-encoding is
    // already decoded by the HTTP layer, so this sees the real segments.
    if (tail.find("..") != std::string_view::npos) {
        resp.status = 404;
        resp.body = R"({"error":"not found"})";
        return resp;
    }
    while (!tail.empty() && tail.front() == '/') {
        tail.remove_prefix(1);
    }

    const auto read_file = [](const fs::path& p, HttpResponse& out) {
        std::ifstream in(p, std::ios::binary);
        if (!in.is_open()) {
            return false;
        }
        std::ostringstream buf;
        buf << in.rdbuf();
        out.body = buf.str();
        out.content_type = static_content_type_for(p);
        out.status = 200;
        return true;
    };

    std::error_code ec;
    const fs::path candidate =
        tail.empty() ? canonical_root / "index.html" : canonical_root / fs::path{std::string{tail}};
    const fs::path resolved = fs::weakly_canonical(candidate, ec);

    const auto inside_root = [&](const fs::path& p) {
        const std::string rootstr = canonical_root.string();
        const std::string pstr = p.string();
        return pstr == rootstr ||
               (pstr.size() > rootstr.size() && pstr.compare(0, rootstr.size(), rootstr) == 0 &&
                pstr[rootstr.size()] == fs::path::preferred_separator);
    };

    if (!ec && inside_root(resolved) && fs::is_regular_file(resolved, ec) && !ec) {
        if (read_file(resolved, resp)) {
            return resp;
        }
    }

    // SPA fallback: an extensionless miss is a client-side route, and the
    // router in index.html owns it. A missed asset (has an extension) is a
    // real 404.
    const bool looks_like_route = !tail.empty() && fs::path{std::string{tail}}.extension().empty();
    if ((tail.empty() || looks_like_route) && read_file(canonical_root / "index.html", resp)) {
        return resp;
    }

    resp.status = 404;
    resp.content_type = "application/json";
    resp.body = R"({"error":"not found"})";
    return resp;
}

}  // namespace clink::http
