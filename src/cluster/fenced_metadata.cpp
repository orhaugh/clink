#include "clink/cluster/fenced_metadata.hpp"

#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>

#include <sys/file.h>

#include "clink/cluster/coordinator.hpp"  // metadata_write_allowed
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/state/durable_file_write.hpp"

namespace clink::cluster {

namespace {

// The lock file is released by close(), on every way out of the critical
// section, the throwing ones included.
struct LockFd {
    int fd{-1};
    ~LockFd() {
        if (fd >= 0) {
            ::close(fd);  // releases the flock
        }
    }
};

// Why a compare-and-set did not land. The two wrappers below report it
// differently: one logs and returns false, the other leaves both to its
// caller.
class CasFailure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The compare-and-set itself. True when `body` landed; false when the fence
// refused it, with the epoch it found in `on_disk`. Throws CasFailure when the
// lock cannot be taken or the durable write fails, with the message the
// logging form has always written for that case.
bool cas_write(const std::filesystem::path& path,
               const std::string& body,
               std::uint64_t writer_epoch,
               const std::function<std::uint64_t(const std::string&)>& epoch_of,
               std::uint64_t& on_disk) {
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);

    const std::string lock_path = path.string() + ".wlock";
    LockFd lock{::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644)};
    if (lock.fd < 0) {
        throw CasFailure("cannot open write lock " + lock_path +
                         "; refusing an unfenced metadata write");
    }
    if (::flock(lock.fd, LOCK_EX) != 0) {
        throw CasFailure("cannot take write lock " + lock_path +
                         "; refusing an unfenced metadata write");
    }
    on_disk = epoch_of(path.string());
    if (!metadata_write_allowed(writer_epoch, on_disk)) {
        return false;
    }
    // Inside the critical section on purpose: a delay armed here holds
    // the historic read-to-rename window open, which is exactly what the
    // CAS race test stretches to prove a concurrent writer can no longer
    // straddle it.
    CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeMetadataWrite);
    try {
        clink::state::detail::write_string_fsync_rename(path, body);
    } catch (const std::exception& e) {
        throw CasFailure("durable write failed for " + path.string() + ": " + e.what());
    }
    return true;
}

}  // namespace

bool fenced_metadata_cas_write(const std::filesystem::path& path,
                               const std::string& body,
                               std::uint64_t writer_epoch,
                               const std::function<std::uint64_t(const std::string&)>& epoch_of,
                               const std::string& caller_context) {
    std::uint64_t on_disk = 0;
    try {
        if (cas_write(path, body, writer_epoch, epoch_of, on_disk)) {
            return true;
        }
    } catch (const CasFailure& e) {
        clink::log::error("coordinator.metadata", e.what());
        return false;
    }
    clink::log::error("coordinator.metadata",
                      "refusing to overwrite " + path.string() + ": it was written by epoch " +
                          std::to_string(on_disk) + " and this coordinator is epoch " +
                          std::to_string(writer_epoch) +
                          ". Leadership has moved; this process is no longer authoritative." +
                          (caller_context.empty() ? std::string{} : " " + caller_context));
    return false;
}

bool fenced_metadata_cas_write_quietly(
    const std::filesystem::path& path,
    const std::string& body,
    std::uint64_t writer_epoch,
    const std::function<std::uint64_t(const std::string&)>& epoch_of) {
    std::uint64_t on_disk = 0;
    return cas_write(path, body, writer_epoch, epoch_of, on_disk);
}

}  // namespace clink::cluster
