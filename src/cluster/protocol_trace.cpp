#include "clink/cluster/protocol_trace.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>

#ifdef _WIN32
#include <process.h>
#define CLINK_GETPID _getpid
#else
#include <unistd.h>
#define CLINK_GETPID getpid
#endif

namespace clink::protocol_trace {
namespace {

// 0 = unresolved, 1 = disabled, 2 = enabled. Relaxed loads on the hot path;
// the resolving store is under the mutex.
std::atomic<int> g_state{0};

struct State {
    std::mutex mu;
    std::string dir;
    std::string role;
    long long pid{0};
    std::ofstream out;
    std::uint64_t seq{0};
};

State& state() {
    static State s;
    return s;
}

void append_json_string(std::string& out, std::string_view v) {
    out.push_back('"');
    for (const unsigned char c : v) {
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
                if (c < 0x20) {
                    static constexpr char hex[] = "0123456789abcdef";
                    out += "\\u00";
                    out.push_back(hex[c >> 4]);
                    out.push_back(hex[c & 0xf]);
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    out.push_back('"');
}

// Resolve the environment once. Called under the state mutex.
void resolve_locked(State& s) {
    const char* dir = std::getenv("CLINK_PROTOCOL_TRACE_DIR");
    if (dir == nullptr || *dir == '\0') {
        g_state.store(1, std::memory_order_release);
        return;
    }
    try {
        std::filesystem::create_directories(dir);
        const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        const auto pid = static_cast<long long>(CLINK_GETPID());
        s.pid = pid;
        const std::string role = s.role.empty() ? "process" : s.role;
        const auto path = std::filesystem::path(dir) / (role + "-" + std::to_string(pid) + "-" +
                                                        std::to_string(now) + ".ndjson");
        s.out.open(path, std::ios::out | std::ios::app);
        if (!s.out.is_open()) {
            g_state.store(1, std::memory_order_release);
            return;
        }
        s.dir = dir;
        s.seq = 0;
        g_state.store(2, std::memory_order_release);
    } catch (const std::exception&) {
        g_state.store(1, std::memory_order_release);
    }
}

bool resolve() {
    auto& s = state();
    std::lock_guard lk(s.mu);
    if (g_state.load(std::memory_order_acquire) == 0) {
        resolve_locked(s);
    }
    return g_state.load(std::memory_order_acquire) == 2;
}

}  // namespace

bool enabled() noexcept {
    const int st = g_state.load(std::memory_order_relaxed);
    if (st != 0) {
        return st == 2;
    }
    try {
        return resolve();
    } catch (...) {
        return false;
    }
}

void set_process_role(std::string_view role) {
    auto& s = state();
    std::lock_guard lk(s.mu);
    if (s.role.empty()) {
        s.role.assign(role.data(), role.size());
    }
}

std::string directory() {
    if (!enabled()) {
        return {};
    }
    auto& s = state();
    std::lock_guard lk(s.mu);
    return s.dir;
}

void reset_for_tests() {
    auto& s = state();
    std::lock_guard lk(s.mu);
    if (s.out.is_open()) {
        s.out.close();
    }
    s.dir.clear();
    s.seq = 0;
    g_state.store(0, std::memory_order_release);
}

Event::Event(std::string_view name) {
    line_.reserve(160);
    line_ += "{\"event\":";
    append_json_string(line_, name);
}

Event& Event::u(std::string_view key, std::uint64_t value) {
    line_.push_back(',');
    append_json_string(line_, key);
    line_.push_back(':');
    line_ += std::to_string(value);
    return *this;
}

Event& Event::i(std::string_view key, std::int64_t value) {
    line_.push_back(',');
    append_json_string(line_, key);
    line_.push_back(':');
    line_ += std::to_string(value);
    return *this;
}

Event& Event::s(std::string_view key, std::string_view value) {
    line_.push_back(',');
    append_json_string(line_, key);
    line_.push_back(':');
    append_json_string(line_, value);
    return *this;
}

Event& Event::b(std::string_view key, bool value) {
    line_.push_back(',');
    append_json_string(line_, key);
    line_ += value ? ":true" : ":false";
    return *this;
}

void Event::emit() {
    if (!enabled()) {
        return;
    }
    auto& s = state();
    std::lock_guard lk(s.mu);
    if (!s.out.is_open()) {
        return;
    }
    const auto ts = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    std::string head =
        "{\"seq\":" + std::to_string(++s.seq) + ",\"ts\":" + std::to_string(ts) + ",\"proc\":";
    // The role may be set after the switch resolved (an in-process cluster
    // that turned tracing on before constructing its coordinator).
    append_json_string(head, (s.role.empty() ? "process" : s.role) + ":" + std::to_string(s.pid));
    head.push_back(',');
    // line_ starts with '{' followed by the event fields; splice the header in.
    s.out << head << std::string_view(line_).substr(1) << "}\n";
    s.out.flush();
}

}  // namespace clink::protocol_trace
