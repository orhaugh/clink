#include "clink/sql/spill_store.hpp"

#include <array>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <unistd.h>

#include "clink/core/sha256.hpp"

namespace clink::sql {
namespace {
std::string digest(std::string_view key) {
    Sha256 hash;
    hash.update(key.data(), key.size());
    return Sha256::to_hex(hash.finalize());
}
[[noreturn]] void fail(const std::filesystem::path& path) {
    throw std::runtime_error("SQL_SPILL_ERROR: " + path.string());
}
}  // namespace

SpillStore::SpillStore(const std::string& parent) {
    std::string pattern = (std::filesystem::path(parent) / "clink-sql-spill-XXXXXX").string();
    if (!::mkdtemp(pattern.data()))
        fail(parent);
    directory_ = pattern;
}
SpillStore::~SpillStore() {
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
}
std::filesystem::path SpillStore::path_for_(const std::string& key) const {
    const auto hash = digest(key);
    return directory_ / hash.substr(0, 2) / hash;
}

void SpillStore::put(const std::string& key, std::span<const std::byte> value) {
    const auto path = path_for_(key);
    // Verify the full key before replacing an existing hash address.
    const bool existed = std::filesystem::exists(path);
    if (existed && read_(path).first != key)
        fail(path);
    std::filesystem::create_directories(path.parent_path());
    const auto temporary = path.string() + ".tmp";
    try {
        std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
        std::array<unsigned char, 8> length{};
        for (std::size_t i = 0; i < length.size(); ++i)
            length[i] =
                static_cast<unsigned char>(static_cast<std::uint64_t>(key.size()) >> (8 * i));
        Sha256 hash;
        hash.update(length.data(), length.size());
        hash.update(key.data(), key.size());
        hash.update(value.data(), value.size());
        const auto checksum = hash.finalize();
        out.write(reinterpret_cast<const char*>(length.data()), length.size());
        out.write(key.data(), static_cast<std::streamsize>(key.size()));
        out.write(reinterpret_cast<const char*>(value.data()),
                  static_cast<std::streamsize>(value.size()));
        out.write(reinterpret_cast<const char*>(checksum.data()), checksum.size());
        out.close();
        if (!out)
            fail(path);
        std::filesystem::rename(temporary, path);
        if (!existed)
            ++size_;
    } catch (...) {
        std::error_code ignored;
        std::filesystem::remove(temporary, ignored);
        throw;
    }
}

std::pair<std::string, SpillStore::Bytes> SpillStore::read_(const std::filesystem::path& path) {
    const auto size = std::filesystem::file_size(path);
    if (size < 40 ||
        size > static_cast<std::uintmax_t>(std::numeric_limits<std::streamsize>::max()))
        fail(path);
    std::ifstream in(path, std::ios::binary);
    std::array<unsigned char, 8> length{};
    in.read(reinterpret_cast<char*>(length.data()), length.size());
    std::uint64_t key_size = 0;
    for (std::size_t i = 0; i < length.size(); ++i)
        key_size |= static_cast<std::uint64_t>(length[i]) << (8 * i);
    if (!in || key_size > size - 40)
        fail(path);
    std::string key(static_cast<std::size_t>(key_size), '\0');
    Bytes value(static_cast<std::size_t>(size - 40 - key_size));
    in.read(key.data(), static_cast<std::streamsize>(key.size()));
    in.read(reinterpret_cast<char*>(value.data()), static_cast<std::streamsize>(value.size()));
    std::array<std::uint8_t, 32> checksum{};
    in.read(reinterpret_cast<char*>(checksum.data()), checksum.size());
    Sha256 hash;
    hash.update(length.data(), length.size());
    hash.update(key.data(), key.size());
    hash.update(value.data(), value.size());
    if (!in || hash.finalize() != checksum || digest(key) != path.filename().string())
        fail(path);
    return {std::move(key), std::move(value)};
}

std::optional<SpillStore::Bytes> SpillStore::get(const std::string& key) const {
    const auto path = path_for_(key);
    if (!std::filesystem::exists(path))
        return std::nullopt;
    auto [stored_key, value] = read_(path);
    if (stored_key != key)
        fail(path);
    return value;
}
void SpillStore::erase(const std::string& key) {
    const auto path = path_for_(key);
    if (std::filesystem::exists(path)) {
        if (read_(path).first != key)
            fail(path);
        std::filesystem::remove(path);
        --size_;
    }
}
void SpillStore::scan(const std::function<bool(const std::string&, const Bytes&)>& visitor) const {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(directory_)) {
        if (!entry.is_regular_file() || entry.path().extension() == ".tmp")
            continue;
        auto [key, value] = read_(entry.path());
        if (!visitor(key, value))
            break;
    }
}
}  // namespace clink::sql
