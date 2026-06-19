// Tiny JSON writer used by every HTTP endpoint handler. Builder
// pattern: open objects / arrays, push keys, push values, close.
// Tracks comma separators so callers don't have to.
//
// Why not nlohmann/json or rapidjson: endpoint payloads in clink
// are small (a few KB max), build top-down once per response, and
// never round-trip. The builder costs ~200 LOC and adds zero
// dependencies. If we ever need real JSON parsing on the server
// side, that's the moment to pull a library in.
//
// Output is always compact (no pretty-print, no trailing whitespace)
// - the dashboard SPA does its own formatting client-side.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace clink::http {

class JsonWriter {
public:
    JsonWriter() { buffer_.reserve(256); }

    // Returns the assembled JSON. Caller takes ownership; the writer
    // remains usable but typically isn't reused after str() is called.
    std::string str() const { return buffer_; }

    // --- Container open/close ---

    JsonWriter& begin_object() {
        sep_if_needed();
        buffer_ += '{';
        push_scope(Scope::Object);
        return *this;
    }

    JsonWriter& end_object() {
        pop_scope_expect(Scope::Object);
        buffer_ += '}';
        // After closing a value we're back inside whatever scope
        // contains us; the next call needs a comma if it adds a
        // sibling. mark_value() sets that need_separator state.
        mark_value();
        return *this;
    }

    JsonWriter& begin_array() {
        sep_if_needed();
        buffer_ += '[';
        push_scope(Scope::Array);
        return *this;
    }

    JsonWriter& end_array() {
        pop_scope_expect(Scope::Array);
        buffer_ += ']';
        mark_value();
        return *this;
    }

    // --- Keys (only valid inside an object) ---

    JsonWriter& key(std::string_view k) {
        sep_if_needed();
        buffer_ += '"';
        append_escaped(k);
        buffer_ += "\":";
        // After the colon the next call writes the value WITHOUT a
        // leading comma - clear the separator flag.
        if (!scopes_.empty()) {
            scopes_.back().need_separator = false;
        }
        return *this;
    }

    // --- Primitive values ---

    JsonWriter& string_value(std::string_view v) {
        sep_if_needed();
        buffer_ += '"';
        append_escaped(v);
        buffer_ += '"';
        mark_value();
        return *this;
    }

    JsonWriter& int_value(std::int64_t v) {
        sep_if_needed();
        buffer_ += std::to_string(v);
        mark_value();
        return *this;
    }

    JsonWriter& uint_value(std::uint64_t v) {
        sep_if_needed();
        buffer_ += std::to_string(v);
        mark_value();
        return *this;
    }

    JsonWriter& bool_value(bool v) {
        sep_if_needed();
        buffer_ += v ? "true" : "false";
        mark_value();
        return *this;
    }

    JsonWriter& null_value() {
        sep_if_needed();
        buffer_ += "null";
        mark_value();
        return *this;
    }

    // Convenience: `kv("name", "alice")` is `.key("name").string_value("alice")`.
    JsonWriter& kv(std::string_view k, std::string_view v) { return key(k).string_value(v); }
    JsonWriter& kv(std::string_view k, const char* v) {
        return key(k).string_value(v != nullptr ? std::string_view{v} : std::string_view{});
    }
    JsonWriter& kv(std::string_view k, std::int64_t v) { return key(k).int_value(v); }
    JsonWriter& kv(std::string_view k, int v) {
        return key(k).int_value(static_cast<std::int64_t>(v));
    }
    JsonWriter& kv(std::string_view k, std::uint16_t v) { return key(k).uint_value(v); }
    JsonWriter& kv(std::string_view k, std::uint32_t v) { return key(k).uint_value(v); }
    JsonWriter& kv(std::string_view k, std::uint64_t v) { return key(k).uint_value(v); }
    // size_t is typedef-distinct from uint64_t on Apple LP64 (unsigned
    // long vs unsigned long long) but typedef-identical on most Linux
    // LP64. The requires clause makes this overload exist only on the
    // distinct-typedef platforms; otherwise a concrete size_t overload
    // would be a redefinition of the uint64_t one.
    template <typename T>
        requires std::is_same_v<T, std::size_t> && (!std::is_same_v<std::size_t, std::uint64_t>)
    JsonWriter& kv(std::string_view k, T v) {
        return key(k).uint_value(static_cast<std::uint64_t>(v));
    }
    JsonWriter& kv(std::string_view k, bool v) { return key(k).bool_value(v); }

private:
    enum class Scope : std::uint8_t { Object, Array };
    struct ScopeState {
        Scope kind;
        bool need_separator{false};
    };

    std::string buffer_;
    std::vector<ScopeState> scopes_;

    void push_scope(Scope s) { scopes_.push_back({s, false}); }

    void pop_scope_expect(Scope s) {
        if (scopes_.empty() || scopes_.back().kind != s) {
            // Builder misuse - emit a recoverable marker rather than
            // throw mid-response.
            return;
        }
        scopes_.pop_back();
    }

    void sep_if_needed() {
        if (scopes_.empty()) {
            return;
        }
        auto& top = scopes_.back();
        if (top.need_separator) {
            buffer_ += ',';
        }
    }

    void mark_value() {
        if (scopes_.empty()) {
            return;
        }
        scopes_.back().need_separator = true;
    }

    void append_escaped(std::string_view s) {
        for (char c : s) {
            switch (c) {
                case '"':
                    buffer_ += "\\\"";
                    break;
                case '\\':
                    buffer_ += "\\\\";
                    break;
                case '\n':
                    buffer_ += "\\n";
                    break;
                case '\r':
                    buffer_ += "\\r";
                    break;
                case '\t':
                    buffer_ += "\\t";
                    break;
                case '\b':
                    buffer_ += "\\b";
                    break;
                case '\f':
                    buffer_ += "\\f";
                    break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        char buf[8];
                        std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                        buffer_ += buf;
                    } else {
                        buffer_ += c;
                    }
            }
        }
    }
};

}  // namespace clink::http
