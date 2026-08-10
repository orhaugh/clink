#include "clink/cluster/plugin_cache.hpp"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <span>
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
    // The filename is derived from the BYTES, not from the hash the peer sent.
    //
    // content_hash arrives off the wire and was used verbatim as a path component,
    // unvalidated - so a peer could steer the write outside this directory, and a
    // peer whose declared hash did not match its bytes would have its module cached
    // under a name that means something else. Recomputing costs one pass over
    // bytes that were just received, and makes the cache content-addressed in fact
    // rather than by assertion.
    //
    // A mismatch is not silently corrected: it means the peer and this coordinator
    // disagree about what was sent, which is worth refusing rather than papering
    // over, because every idempotency decision below keys on that name.
    const auto computed = fnv1a_64_hex(std::span<const std::byte>{blob.bytes});
    if (!blob.content_hash.empty() && blob.content_hash != computed) {
        throw std::runtime_error("plugin cache: plugin '" + blob.name + "' declared content_hash " +
                                 blob.content_hash + " but its bytes hash to " + computed +
                                 "; refusing to cache a module under a name that does not "
                                 "describe it");
    }
    const auto filename = computed + suffix_for_platform();
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

std::string find_plugin_in_cache(const std::string& content_hash, const std::string& base_dir) {
    if (content_hash.empty()) {
        return {};
    }
    // Same name derivation as write_plugin_to_cache: the cache is
    // content-addressed by the recomputed hash, so a lookup by a DECLARED
    // hash finds exactly the bytes that hash to it or nothing.
    const auto base = resolve_base_dir(base_dir);
    const auto path = base / (content_hash + suffix_for_platform());
    std::error_code ec;
    if (!std::filesystem::exists(path, ec) || ec) {
        return {};
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
