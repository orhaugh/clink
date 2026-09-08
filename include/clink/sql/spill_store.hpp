#pragma once

#include <cstddef>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace clink::sql {

// Private execution scratch storage, never a restore point. One hashed file
// per key avoids a resident index proportional to the number of spilled keys.
// Callers serialise access and publish its contents to their StateBackend at
// checkpoints. Successful writes replace whole entries atomically.
class SpillStore {
public:
    using Bytes = std::vector<std::byte>;
    explicit SpillStore(const std::string& parent);
    ~SpillStore();
    SpillStore(const SpillStore&) = delete;
    SpillStore& operator=(const SpillStore&) = delete;

    void put(const std::string& key, std::span<const std::byte> value);
    std::optional<Bytes> get(const std::string& key) const;
    void erase(const std::string& key);
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    // Returning false stops the scan without materialising the remaining keys.
    void scan(const std::function<bool(const std::string&, const Bytes&)>& visitor) const;

private:
    std::filesystem::path path_for_(const std::string& key) const;
    static std::pair<std::string, Bytes> read_(const std::filesystem::path& path);
    std::filesystem::path directory_;
    std::size_t size_{0};
};

}  // namespace clink::sql
