#include "clink/cluster/plugin_cache.hpp"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

namespace clink::cluster {

std::uint64_t fnv1a_64(std::span<const std::byte> bytes) noexcept {
    std::uint64_t h = 0xcbf29ce484222325ULL;
    for (auto b : bytes) {
        h ^= static_cast<std::uint8_t>(b);
        h *= 0x100000001b3ULL;
    }
    return h;
}

std::string fnv1a_64_hex(std::span<const std::byte> bytes) {
    const auto h = fnv1a_64(bytes);
    std::ostringstream oss;
    oss << std::hex;
    // Zero-padded 16 chars.
    constexpr int kWidth = 16;
    char buf[kWidth + 1];
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(h));
    return std::string{buf};
}

namespace {

std::string suffix_for_platform() {
#if defined(__APPLE__)
    return ".dylib";
#else
    return ".so";
#endif
}

std::filesystem::path resolve_base_dir(const std::string& explicit_base) {
    if (!explicit_base.empty()) {
        return explicit_base;
    }
    const char* tmp = std::getenv("TMPDIR");
    std::filesystem::path base = (tmp != nullptr && tmp[0] != '\0')
                                     ? std::filesystem::path{tmp}
                                     : std::filesystem::temp_directory_path();
    base /= "clink-plugins";
    base /= std::to_string(::getpid());
    return base;
}

}  // namespace

std::string write_plugin_to_cache(const PluginBinary& blob, const std::string& base_dir) {
    const auto base = resolve_base_dir(base_dir);
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    if (ec) {
        throw std::runtime_error("plugin cache: cannot create " + base.string() + ": " +
                                 ec.message());
    }
    const auto filename = blob.content_hash + suffix_for_platform();
    const auto path = base / filename;
    if (std::filesystem::exists(path)) {
        // Idempotent: same hash, same path, assume same bytes.
        return path.string();
    }
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        throw std::runtime_error("plugin cache: cannot open " + path.string() + " for writing");
    }
    if (!blob.bytes.empty()) {
        out.write(reinterpret_cast<const char*>(blob.bytes.data()),
                  static_cast<std::streamsize>(blob.bytes.size()));
    }
    out.close();
    if (!out.good()) {
        std::filesystem::remove(path);
        throw std::runtime_error("plugin cache: write failed for " + path.string());
    }
    return path.string();
}

PluginBinary make_plugin_binary_from_file(const std::string& path, const std::string& name) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("plugin: cannot open " + path);
    }
    in.seekg(0, std::ios::end);
    const auto size = in.tellg();
    in.seekg(0, std::ios::beg);
    PluginBinary blob;
    blob.bytes.resize(static_cast<std::size_t>(size));
    in.read(reinterpret_cast<char*>(blob.bytes.data()), size);
    if (!in.good()) {
        throw std::runtime_error("plugin: read failed for " + path);
    }
    blob.content_hash = fnv1a_64_hex(blob.bytes);
    blob.name = name.empty() ? std::filesystem::path{path}.filename().string() : name;
    return blob;
}

}  // namespace clink::cluster
