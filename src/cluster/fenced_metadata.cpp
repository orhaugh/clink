#include "clink/cluster/fenced_metadata.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <sys/file.h>

#include "clink/cluster/coordinator.hpp"  // metadata_write_allowed
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/state/durable_file_write.hpp"

namespace clink::cluster {

bool fenced_metadata_cas_write(const std::filesystem::path& path,
                               const std::string& body,
                               std::uint64_t writer_epoch,
                               const std::function<std::uint64_t(const std::string&)>& epoch_of,
                               const std::string& caller_context) {
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);

    const std::string lock_path = path.string() + ".wlock";
    const int fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (fd < 0) {
        clink::log::error(
            "coordinator.metadata",
            "cannot open write lock " + lock_path + "; refusing an unfenced metadata write");
        return false;
    }
    if (::flock(fd, LOCK_EX) != 0) {
        ::close(fd);
        clink::log::error(
            "coordinator.metadata",
            "cannot take write lock " + lock_path + "; refusing an unfenced metadata write");
        return false;
    }

    bool ok = false;
    const auto on_disk = epoch_of(path.string());
    if (!metadata_write_allowed(writer_epoch, on_disk)) {
        clink::log::error("coordinator.metadata",
                          "refusing to overwrite " + path.string() + ": it was written by epoch " +
                              std::to_string(on_disk) + " and this coordinator is epoch " +
                              std::to_string(writer_epoch) +
                              ". Leadership has moved; this process is no longer authoritative." +
                              (caller_context.empty() ? std::string{} : " " + caller_context));
    } else {
        // Inside the critical section on purpose: a delay armed here holds
        // the historic read-to-rename window open, which is exactly what the
        // CAS race test stretches to prove a concurrent writer can no longer
        // straddle it.
        CLINK_FAULT_POINT(clink::fault::points::kCoordinatorBeforeMetadataWrite);
        try {
            clink::state::detail::write_string_fsync_rename(path, body);
            ok = true;
        } catch (const std::exception& e) {
            clink::log::error("coordinator.metadata",
                              "durable write failed for " + path.string() + ": " + e.what());
        }
    }
    ::close(fd);  // releases the flock
    return ok;
}

}  // namespace clink::cluster
