#include "clink/runtime/sink_evidence.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdio>
#include <fstream>
#include <random>
#include <system_error>

#include "clink/config/json.hpp"

namespace clink::capture {

std::string sink_interval_json(const SinkIntervalHeader& header, std::string_view sink_json) {
    config::JsonObject records;
    records["capture"] = config::JsonValue{header.capture};
    records["seen"] = config::JsonValue{static_cast<std::int64_t>(header.records_seen)};
    records["stored"] = config::JsonValue{static_cast<std::int64_t>(header.records_stored)};
    records["truncated"] = config::JsonValue{header.truncated};

    config::JsonObject o;
    o["format"] = config::JsonValue{std::string{kSinkIntervalFormat}};
    o["version"] = config::JsonValue{kSinkIntervalVersion};
    // Text: an operator id is 64 bits, and JSON integers stop at 2^63.
    o["op"] = config::JsonValue{std::to_string(header.op.value())};
    o["subtask"] = config::JsonValue{static_cast<std::int64_t>(header.subtask_idx)};
    o["checkpoint"] = config::JsonValue{static_cast<std::int64_t>(header.checkpoint)};
    o["previous_barrier"] = config::JsonValue{static_cast<std::int64_t>(header.previous_barrier)};
    o["attempt"] = config::JsonValue{header.attempt};
    o["watermark_ms"] = header.watermark_ms.has_value() ? config::JsonValue{*header.watermark_ms}
                                                        : config::JsonValue{};
    o["records"] = config::JsonValue{std::move(records)};
    // The sink's object is parsed so that the file is one valid document; one
    // that does not parse is recorded as such rather than spliced in.
    if (sink_json.empty()) {
        o["sink"] = config::JsonValue{};
    } else {
        try {
            o["sink"] = config::parse(sink_json);
        } catch (const std::exception& e) {
            config::JsonObject bad;
            bad["unreadable"] = config::JsonValue{std::string{e.what()}};
            o["sink"] = config::JsonValue{std::move(bad)};
        }
    }
    return config::JsonValue{std::move(o)}.serialize(2) + "\n";
}

bool write_sink_interval(const std::filesystem::path& dir,
                         const SinkIntervalHeader& header,
                         std::string_view sink_json) noexcept {
    try {
        const auto subdir = dir / ("op-" + std::to_string(header.op.value())) /
                            ("subtask-" + std::to_string(header.subtask_idx));
        std::filesystem::create_directories(subdir);
        const std::string text = sink_interval_json(header, sink_json);
        const auto final_path =
            subdir / ("epoch-" + std::to_string(header.checkpoint) + ".sink.json");
        auto tmp_path = final_path;
        tmp_path += ".tmp";
        {
            std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
            if (!out) {
                return false;
            }
            out.write(text.data(), static_cast<std::streamsize>(text.size()));
            out.close();
            if (!out) {
                std::error_code ec;
                std::filesystem::remove(tmp_path, ec);
                return false;
            }
        }
        std::error_code ec;
        std::filesystem::rename(tmp_path, final_path, ec);
        if (ec) {
            std::filesystem::remove(tmp_path, ec);
            return false;
        }
        return true;
    } catch (...) {
        return false;
    }
}

std::string new_attempt_id() {
    std::random_device rd;
    const std::uint64_t v = (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
    std::array<char, 17> hex{};
    std::snprintf(hex.data(), hex.size(), "%016llx", static_cast<unsigned long long>(v));
    return std::string{hex.data()};
}

bool looks_like_credential(std::string_view name) noexcept {
    std::string lower{name};
    for (auto& c : lower) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    static constexpr std::array<std::string_view, 12> kWords = {"password",
                                                                "passwd",
                                                                "secret",
                                                                "token",
                                                                "credential",
                                                                "api_key",
                                                                "apikey",
                                                                "access_key",
                                                                "private_key",
                                                                "sasl",
                                                                "auth",
                                                                "conninfo"};
    return std::any_of(kWords.begin(), kWords.end(), [&](std::string_view w) {
        return lower.find(w) != std::string::npos;
    });
}

std::map<std::string, std::string> redact_credentials(std::map<std::string, std::string> params) {
    for (auto& [name, value] : params) {
        if (looks_like_credential(name)) {
            value = "<redacted>";
        }
    }
    return params;
}

}  // namespace clink::capture
