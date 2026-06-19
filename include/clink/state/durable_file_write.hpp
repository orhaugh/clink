#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include <sys/types.h>

// Crash-safe, durable file write for state-backend snapshots.
//
// A checkpoint is only honestly "durable before ack" if its bytes are on
// stable storage, not merely in the kernel page cache: a flush()+rename
// survives a process crash but not an OS/power crash. write_fsync_rename
// closes that gap - it fsyncs the file before the rename and fsyncs the
// containing directory after, so a successful return means the bytes (and
// the directory entry that names them) are durable.
//
// This is meant to run OFF the operator thread (on the snapshot worker for
// async-capable backends), so the fsync cost is off the record-processing
// hot path. The synchronous snapshot() path pays it on the calling thread,
// which is correct for the infrequent callers that use it (terminal sink
// commits, the checkpoint coordinator).
//
// POSIX (Linux + macOS). Durability can be disabled with CLINK_STATE_FSYNC=0
// (falls back to flush+rename) for fsync-hostile CI or pure-throughput
// benchmarks where the durability contract is not under test.

namespace clink::state::detail {

// Durability is on unless CLINK_STATE_FSYNC is "0"/"false". Read per call
// (once per snapshot, not per record), so the toggle is dynamic.
inline bool fsync_enabled() {
    const char* p = std::getenv("CLINK_STATE_FSYNC");
    if (p == nullptr) {
        return true;
    }
    return std::strcmp(p, "0") != 0 && std::strcmp(p, "false") != 0;
}

// fsync the directory so a rename into it is durable across a crash.
// Best-effort: if the platform/filesystem rejects fsync on a directory fd
// we do not fail the checkpoint - the file bytes are already durable, and
// the worst case of a lost rename is that recovery falls back to the
// previous (retained) checkpoint, never corruption.
inline void fsync_directory_best_effort(const std::filesystem::path& dir) {
    int fd = ::open(dir.c_str(),
                    O_RDONLY
#ifdef O_DIRECTORY
                        | O_DIRECTORY
#endif
    );
    if (fd < 0) {
        return;
    }
    (void)::fsync(fd);
    ::close(fd);
}

// Write `size` bytes from `data` to `tmp_path`, fsync them, atomically
// rename to `final_path`, then fsync the parent directory. Throws
// std::runtime_error on any I/O failure (so a caller can ack the checkpoint
// as failed). The file fsync is strict - the bytes MUST be durable before
// the rename publishes them - while the directory fsync is best-effort.
inline void write_fsync_rename(const std::filesystem::path& final_path,
                               const std::filesystem::path& tmp_path,
                               const std::byte* data,
                               std::size_t size) {
    const bool do_fsync = fsync_enabled();

    // Write the temp file and fsync it through the SAME descriptor that
    // wrote it. Using one fd for write+fsync is the correct durable
    // pattern: a writeback error is reported once, to an open fd, so an
    // ofstream-write-then-reopen-and-fsync could miss an error the
    // original fd already consumed and return success on lost bytes.
    const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error("durable_write: cannot open " + tmp_path.string() + ": " +
                                 std::strerror(errno));
    }
    std::size_t off = 0;
    while (off < size) {
        const ssize_t n = ::write(fd, data + off, size - off);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            const int e = errno;
            ::close(fd);
            throw std::runtime_error("durable_write: write failed for " + tmp_path.string() + ": " +
                                     std::strerror(e));
        }
        off += static_cast<std::size_t>(n);
    }
    if (do_fsync) {
        if (::fsync(fd) != 0) {
            const int e = errno;
            ::close(fd);
            throw std::runtime_error("durable_write: fsync failed for " + tmp_path.string() + ": " +
                                     std::strerror(e));
        }
    }
    // close is best-effort: durability was already established by the fsync
    // above, so a close error cannot undo it.
    ::close(fd);

    std::error_code ec;
    std::filesystem::rename(tmp_path, final_path, ec);
    if (ec) {
        throw std::runtime_error("durable_write: rename to " + final_path.string() +
                                 " failed: " + ec.message());
    }

    if (do_fsync) {
        fsync_directory_best_effort(final_path.parent_path());
    }
}

}  // namespace clink::state::detail
