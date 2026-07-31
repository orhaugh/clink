// 10 - the HTTP surface, from a find_package(clink) consumer.
//
// clink embeds an HTTP server and client: the server is how a job serves
// queryable state or a health endpoint from inside an application, and the
// client is how an operator calls an external service (a lookup, a model, a
// feature store) on the data path.
//
// This example exists mainly as a link-time guard. The bundled httplib rides
// clink_core as a BUILD_INTERFACE dependency, so its own usage requirements are
// not automatically part of the installed package, and three separate consumer
// projects have hit the same failure: undefined OpenSSL symbols, then undefined
// inflate/deflate, then undefined _CFArrayGetCount on macOS. Each was a missing
// usage requirement on the exported target, and each was invisible to this
// repository's own tests because they link the build tree, not the install.
// Linking this file against the installed package catches that class of gap.
//
// Build and run:
//
//   cmake -S docs/consumer-examples -B build/examples \
//         -DCMAKE_PREFIX_PATH=/path/to/clink/prefix
//   cmake --build build/examples --target 10_http_surface
//   ./build/examples/10_http_surface

#include <cstdint>
#include <iostream>
#include <string>

#include <clink/http/http_client.hpp>
#include <clink/http/http_server.hpp>

int main() {
    clink::http::HttpServer server;
    server.get("/api/v1/spec", [](const clink::http::HttpRequest& req) {
        clink::http::HttpResponse res;
        res.status = 200;
        res.content_type = "application/json";
        const auto it = req.query.find("commodity");
        const std::string commodity = it == req.query.end() ? "unknown" : it->second;
        res.body = "{\"commodity\":\"" + commodity + "\",\"lo_mc\":2000,\"hi_mc\":8000}";
        return res;
    });

    // Port 0 asks the OS for a free port and start() reports which one, so this
    // example never collides with anything already listening.
    const std::uint16_t port = server.start("127.0.0.1", 0);
    if (port == 0) {
        std::cerr << "could not bind 127.0.0.1\n";
        return 1;
    }

    clink::http::HttpClient client("127.0.0.1", port);
    const auto res = client.get("/api/v1/spec?commodity=vaccine");
    server.stop();

    if (res.status != 200) {
        std::cerr << "GET failed: status=" << res.status << " error=" << res.error << "\n";
        return 1;
    }
    if (res.body.find("\"commodity\":\"vaccine\"") == std::string::npos) {
        std::cerr << "unexpected body: " << res.body << "\n";
        return 1;
    }

    std::cout << "http surface ok: " << res.body << "\n";
    return 0;
}
