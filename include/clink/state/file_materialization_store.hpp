#pragma once

// FileMaterializationStore - files-on-local-disk implementation of
// ExternalMaterializationStore. Materialization payloads are written
// as `<dir>/mat-<checkpoint_id>.bin`; the handle is the absolute
// path string.
//
// Suitable for: single-node tests, NFS-mounted shared dirs, any
// filesystem the operator already trusts for checkpoint files.
//
// Not suitable for: S3-style object stores (use a real S3-backed
// implementation), heavy multi-writer contention (the file path
// scheme doesn't dedup concurrent writes by different writers
// targeting the same id).

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/state/external_materialization_store.hpp"

namespace clink {

class FileMaterializationStore final : public ExternalMaterializationStore {
public:
    explicit FileMaterializationStore(std::filesystem::path dir) : dir_(std::move(dir)) {
        std::error_code ec;
        std::filesystem::create_directories(dir_, ec);
        // We deliberately don't throw on ec - first write will surface
        // the underlying error with more context, and tests can use a
        // temp dir that already exists.
    }

    std::string write(CheckpointId id, std::span<const std::byte> bytes) override {
        const auto path = dir_ / ("mat-" + std::to_string(id.value()) + ".bin");
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("FileMaterializationStore::write: open failed for " +
                                     path.string());
        }
        if (!bytes.empty()) {
            out.write(reinterpret_cast<const char*>(bytes.data()),
                      static_cast<std::streamsize>(bytes.size()));
            if (!out) {
                throw std::runtime_error("FileMaterializationStore::write: write failed for " +
                                         path.string());
            }
        }
        return path.string();
    }

    std::vector<std::byte> read(const std::string& handle) override {
        std::ifstream in(handle, std::ios::binary);
        if (!in) {
            throw std::runtime_error("FileMaterializationStore::read: open failed for " + handle);
        }
        std::vector<std::byte> out;
        std::istreambuf_iterator<char> it{in}, end;
        for (; it != end; ++it) {
            out.push_back(static_cast<std::byte>(*it));
        }
        return out;
    }

    void erase(const std::string& handle) override {
        std::error_code ec;
        std::filesystem::remove(handle, ec);
    }

    [[nodiscard]] std::string description() const override { return "file://" + dir_.string(); }

    [[nodiscard]] const std::filesystem::path& dir() const noexcept { return dir_; }

private:
    std::filesystem::path dir_;
};

}  // namespace clink
