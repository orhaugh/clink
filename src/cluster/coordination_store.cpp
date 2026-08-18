#include "clink/cluster/coordination_store.hpp"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <sys/file.h>

#include "clink/cluster/fenced_metadata.hpp"
#include "clink/state/durable_file_write.hpp"

namespace clink::cluster {

namespace {

// The filesystem store: the layout, durability and fencing the cluster has
// always used, behind the seam. Keys join onto the root with '/' - the
// resulting paths are byte-identical to the pre-seam helpers, pinned by
// the golden-layout test.
class FilesystemCoordinationStore final : public CoordinationStore {
public:
    explicit FilesystemCoordinationStore(std::filesystem::path root) : root_(std::move(root)) {}

    void put(std::string_view key, std::string_view body) override {
        const auto path = resolve_(key);
        std::error_code ec;
        std::filesystem::create_directories(path.parent_path(), ec);
        clink::state::detail::write_string_fsync_rename(path, body);
    }

    bool put_if_absent(std::string_view key, std::string_view body) override {
        const auto path = resolve_(key);
        std::error_code ec;
        std::filesystem::create_directories(path.parent_path(), ec);
        // Same advisory-lock discipline as the fenced write: exists-then-
        // write without the lock is the classic race, and the lock file is
        // created once and never unlinked (unlink-and-recreate lets a later
        // locker take a fresh inode while an earlier one holds the old).
        const auto lock_path = path.string() + ".wlock";
        const int lock_fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0644);
        if (lock_fd < 0) {
            throw std::runtime_error("coordination store: cannot open lock " + lock_path + ": " +
                                     std::strerror(errno));
        }
        if (::flock(lock_fd, LOCK_EX) != 0) {
            const int e = errno;
            ::close(lock_fd);
            throw std::runtime_error("coordination store: cannot lock " + lock_path + ": " +
                                     std::strerror(e));
        }
        bool wrote = false;
        try {
            if (!std::filesystem::exists(path)) {
                clink::state::detail::write_string_fsync_rename(path, body);
                wrote = true;
            }
        } catch (...) {
            ::flock(lock_fd, LOCK_UN);
            ::close(lock_fd);
            throw;
        }
        ::flock(lock_fd, LOCK_UN);
        ::close(lock_fd);
        return wrote;
    }

    bool fenced_put(std::string_view key,
                    std::string_view body,
                    std::uint64_t writer_epoch,
                    const std::function<std::uint64_t(const std::string&)>& epoch_of,
                    const std::string& caller_context) override {
        const auto path = resolve_(key);
        std::error_code ec;
        std::filesystem::create_directories(path.parent_path(), ec);
        // The store's extractor takes the existing RECORD BODY (an object
        // store fences on what it read back); fenced_metadata_cas_write's
        // extractor takes a path and reads the file itself. Adapt - the
        // contract suite caught the filesystem impl silently fencing
        // against epoch 0 when the body extractor was handed a path.
        return fenced_metadata_cas_write(
            path,
            std::string(body),
            writer_epoch,
            [&epoch_of](const std::string& existing_path) -> std::uint64_t {
                std::ifstream in(existing_path, std::ios::binary);
                if (!in.is_open()) {
                    return 0;  // absent record = epoch 0, extractor not consulted
                }
                const std::string existing((std::istreambuf_iterator<char>(in)),
                                           std::istreambuf_iterator<char>());
                return epoch_of(existing);
            },
            caller_context);
    }

    std::optional<std::string> get(std::string_view key) override {
        std::ifstream in(resolve_(key), std::ios::binary);
        if (!in.is_open()) {
            return std::nullopt;
        }
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    }

    bool exists(std::string_view key) override {
        std::error_code ec;
        return std::filesystem::exists(resolve_(key), ec);
    }

    std::vector<std::string> list(std::string_view prefix) override {
        std::vector<std::string> keys;
        const auto dir = resolve_(prefix);
        std::error_code ec;
        std::filesystem::recursive_directory_iterator it{
            dir, std::filesystem::directory_options::skip_permission_denied, ec};
        if (ec) {
            return keys;
        }
        const std::filesystem::recursive_directory_iterator end;
        const auto root_str = root_.string();
        // Recursive, files only: the object-store shape, where "directories"
        // do not exist and callers derive structure from key paths. The
        // error_code increment throughout: this store is listed while other
        // actors prune (retention) and write, and the throwing iterator
        // turns a vanished entry into a terminated process.
        while (!ec && it != end) {
            std::error_code type_ec;
            if (it->is_regular_file(type_ec) && !type_ec) {
                // Lock files are store mechanism, not records.
                const auto name = it->path().filename().string();
                if (name.size() < 6 || name.compare(name.size() - 6, 6, ".wlock") != 0) {
                    auto rel = it->path().string();
                    if (rel.rfind(root_str, 0) == 0) {
                        rel.erase(0, root_str.size());
                        while (!rel.empty() && rel.front() == '/') {
                            rel.erase(0, 1);
                        }
                        keys.push_back(std::move(rel));
                    }
                }
            }
            it.increment(ec);
        }
        return keys;
    }

    void remove(std::string_view key) override {
        std::error_code ec;
        std::filesystem::remove(resolve_(key), ec);
    }

private:
    [[nodiscard]] std::filesystem::path resolve_(std::string_view key) const {
        return root_ / std::filesystem::path(std::string(key));
    }

    std::filesystem::path root_;
};

struct Registry {
    std::mutex mu;
    std::map<std::string, std::function<std::shared_ptr<CoordinationStore>(const std::string&)>>
        builders;
    // Store instances are cached per root: the filesystem store is
    // stateless-cheap, but an object-store implementation holds a client
    // whose construction is not, and callers resolve stores on hot control
    // paths (every marker write).
    std::unordered_map<std::string, std::shared_ptr<CoordinationStore>> cache;
};

Registry& registry() {
    static Registry r;
    return r;
}

}  // namespace

std::shared_ptr<CoordinationStore> make_coordination_store(const std::string& root_uri) {
    auto& reg = registry();
    std::function<std::shared_ptr<CoordinationStore>(const std::string&)> builder;
    {
        std::lock_guard lock(reg.mu);
        if (const auto it = reg.cache.find(root_uri); it != reg.cache.end()) {
            return it->second;
        }
        const auto scheme_end = root_uri.find("://");
        if (scheme_end == std::string::npos) {
            // The filesystem store constructs trivially and re-enters
            // nothing, so it can be built and cached in one critical section.
            auto store = std::make_shared<FilesystemCoordinationStore>(root_uri);
            reg.cache.emplace(root_uri, store);
            return store;
        }
        const auto scheme = root_uri.substr(0, scheme_end);
        const auto bit = reg.builders.find(scheme);
        if (bit == reg.builders.end()) {
            throw std::runtime_error(
                "coordination store: no implementation registered for scheme '" + scheme +
                "' (root '" + root_uri +
                "'); a cluster must never silently coordinate on the wrong substrate");
        }
        builder = bit->second;
    }
    // Build OUTSIDE the registry lock: a builder may construct real clients
    // and may legitimately compose other stores, which re-enters this
    // factory - held across the call, the mutex self-deadlocks (caught by
    // the contract suite's scheme-dispatch test on its first run).
    auto store = builder(root_uri);
    std::lock_guard lock(reg.mu);
    // A racing resolve of the same root may have inserted meanwhile; first
    // insert wins so every caller still shares one instance per root.
    const auto [it, inserted] = reg.cache.emplace(root_uri, std::move(store));
    return it->second;
}

void register_coordination_store_scheme(
    const std::string& scheme,
    std::function<std::shared_ptr<CoordinationStore>(const std::string&)> builder) {
    auto& reg = registry();
    std::lock_guard lock(reg.mu);
    reg.builders[scheme] = std::move(builder);
    // A re-registration must not serve instances built by the replaced
    // builder.
    for (auto it = reg.cache.begin(); it != reg.cache.end();) {
        if (it->first.rfind(scheme + "://", 0) == 0) {
            it = reg.cache.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace clink::cluster
