#pragma once

// Bearer-token providers for HttpRequest::Options.auth_token_provider. The file provider reads the
// token from a path, re-reading only when the file's modification time changes, so an external
// refresher (a Workload Identity / sidecar / Vault agent that rewrites the file) is picked up
// without restarting the job, while the steady-state cost is a cheap stat. Thread-safe; trailing
// whitespace and newlines are trimmed.

#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

namespace clink::http_connector {

inline std::function<std::string()> make_file_token_provider(std::string path) {
    struct State {
        std::string path;
        std::mutex mu;
        std::filesystem::file_time_type mtime{};
        std::string token;
        bool loaded = false;
    };
    auto st = std::make_shared<State>();
    st->path = std::move(path);
    return [st]() -> std::string {
        std::lock_guard<std::mutex> lk(st->mu);
        std::error_code ec;
        const auto mt = std::filesystem::last_write_time(st->path, ec);
        if (!ec && (!st->loaded || mt != st->mtime)) {
            std::ifstream in(st->path, std::ios::binary);
            if (in) {
                std::ostringstream ss;
                ss << in.rdbuf();
                std::string t = ss.str();
                while (!t.empty() && (t.back() == '\n' || t.back() == '\r' || t.back() == ' ' ||
                                      t.back() == '\t')) {
                    t.pop_back();
                }
                st->token = std::move(t);
                st->mtime = mt;
                st->loaded = true;
            }
        }
        return st->token;
    };
}

}  // namespace clink::http_connector
