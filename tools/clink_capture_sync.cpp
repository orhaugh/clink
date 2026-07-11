// clink capture-push / capture-fetch - capture retention: ship a flight
// recorder's capture tree to object storage beside the checkpoints, and
// pull it back onto any machine for replay.
//
//   clink capture-push  --dir=<capture-root> --to=<uri>   [--epoch=N]
//   clink capture-fetch --from=<uri> --dir=<local dir>    [--epoch=N]
//
// The remote side is any Arrow-filesystem URI (s3://bucket/prefix - the
// usual "beside the checkpoints" home - plus anything else the linked
// Arrow supports, e.g. file:///...). Layout is preserved verbatim, so a
// fetched tree replays exactly like the original:
//
//   clink capture-fetch --from=s3://ckpts/jobs/orders/capture --dir=capture
//   clink replay --capture-dir=capture --checkpoint-dir=... --epoch=9 --verify
//
// --epoch=N transfers only that epoch's .cap files (op.json sidecars
// always ride along, since replay needs them) - the narrow fetch for
// "reproduce THIS incident" and the narrow push for retention policies
// that keep interesting epochs only.
//
// Known cosmetic wart: on builds where the pinned iceberg-cpp statically
// bundles Arrow's s3fs objects, the process carries TWO copies of Arrow's
// S3 lifecycle state (the binary's and libarrow.dylib's). This tool
// initialises and finalises ITS copy correctly; the other copy can still
// print Arrow's "FinalizeS3 was not called" warning at exit. Harmless
// here (verified: transfers complete, exit codes correct, our instance
// finalises clean) - the duplicated-statics ABI wart is tracked with the
// iceberg S3 FileIO notes in impls/iceberg.
//
// Exit codes: 0 = ok, 2 = error.

#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#ifdef CLINK_HAS_ARROW
#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>

#include "clink/connectors/arrow_s3_lifecycle.hpp"
#endif

namespace {

namespace fs = std::filesystem;

std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

void push_usage() {
    std::cerr << "Usage: clink capture-push --dir=<capture-root> --to=<uri> [--epoch=N]\n"
              << "\n"
              << "Upload a flight-recorder capture tree (layout preserved) to an\n"
              << "Arrow-filesystem URI, e.g. s3://bucket/jobs/<job>/capture - the\n"
              << "retention home beside the checkpoints. --epoch=N uploads only that\n"
              << "epoch's .cap files (op.json sidecars always ride along).\n"
              << "\n"
              << "Exit codes: 0 = ok, 2 = error.\n";
}

void fetch_usage() {
    std::cerr << "Usage: clink capture-fetch --from=<uri> --dir=<local dir> [--epoch=N]\n"
              << "\n"
              << "Download a pushed capture tree (layout preserved) so `clink replay`\n"
              << "runs against it locally. --epoch=N fetches only that epoch's .cap\n"
              << "files (op.json sidecars always included) - the narrow 'reproduce\n"
              << "THIS incident' fetch.\n"
              << "\n"
              << "Exit codes: 0 = ok, 2 = error.\n";
}

// Keep a relative capture path when no epoch filter applies, or when it
// is a sidecar / the wanted epoch's .cap.
bool want_file(const std::string& rel, const std::string& epoch) {
    if (epoch.empty()) {
        return true;
    }
    const auto base = fs::path(rel).filename().string();
    if (!base.ends_with(".cap")) {
        return true;  // op.json and any future sidecars always transfer
    }
    return base == ("epoch-" + epoch + ".cap");
}

#ifdef CLINK_HAS_ARROW

std::shared_ptr<arrow::fs::FileSystem> open_remote(const std::string& uri, std::string* prefix) {
    if (uri.starts_with("s3://")) {
        // One init per process, finalised explicitly before returning - the
        // lifecycle every S3-touching piece of clink funnels through.
        clink::connectors::ensure_arrow_s3_initialised();
    }
    auto result = arrow::fs::FileSystemFromUri(uri, prefix);
    if (!result.ok()) {
        throw std::runtime_error("cannot open " + uri + ": " + result.status().ToString());
    }
    return result.MoveValueUnsafe();
}

void copy_local_to_remote(const fs::path& local,
                          arrow::fs::FileSystem& remote,
                          const std::string& remote_path) {
    std::ifstream in(local, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open " + local.string());
    }
    std::string bytes{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    // Object stores ignore this; the local filesystem needs the parent
    // directories to exist before an output stream opens.
    if (const auto slash = remote_path.rfind('/'); slash != std::string::npos) {
        (void)remote.CreateDir(remote_path.substr(0, slash), /*recursive=*/true);
    }
    auto out = remote.OpenOutputStream(remote_path);
    if (!out.ok()) {
        throw std::runtime_error("cannot write " + remote_path + ": " + out.status().ToString());
    }
    auto stream = out.MoveValueUnsafe();
    if (auto st = stream->Write(bytes.data(), static_cast<std::int64_t>(bytes.size())); !st.ok()) {
        throw std::runtime_error("write failed for " + remote_path + ": " + st.ToString());
    }
    if (auto st = stream->Close(); !st.ok()) {
        throw std::runtime_error("close failed for " + remote_path + ": " + st.ToString());
    }
}

void copy_remote_to_local(arrow::fs::FileSystem& remote,
                          const std::string& remote_path,
                          const fs::path& local) {
    auto in = remote.OpenInputStream(remote_path);
    if (!in.ok()) {
        throw std::runtime_error("cannot read " + remote_path + ": " + in.status().ToString());
    }
    auto stream = in.MoveValueUnsafe();
    fs::create_directories(local.parent_path());
    std::ofstream out(local, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("cannot write " + local.string());
    }
    while (true) {
        auto chunk = stream->Read(1 << 20);
        if (!chunk.ok()) {
            throw std::runtime_error("read failed for " + remote_path + ": " +
                                     chunk.status().ToString());
        }
        auto buf = chunk.MoveValueUnsafe();
        if (buf->size() == 0) {
            break;
        }
        out.write(reinterpret_cast<const char*>(buf->data()),
                  static_cast<std::streamsize>(buf->size()));
    }
}

#endif  // CLINK_HAS_ARROW

}  // namespace

int clink_cmd_capture_push(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        push_usage();
        return 0;
    }
    const auto dir = get_arg(argc, argv, "dir");
    const auto to = get_arg(argc, argv, "to");
    const auto epoch = get_arg(argc, argv, "epoch");
    if (dir.empty() || to.empty()) {
        push_usage();
        return 2;
    }
#ifndef CLINK_HAS_ARROW
    std::cerr << "clink capture-push: this build has no Arrow filesystems (CLINK_BUILD_ARROW "
                 "off)\n";
    return 2;
#else
    int rc = 2;
    try {
        // Inner scope so the remote FileSystem is destroyed BEFORE the S3
        // finalise below (the documented lifecycle: finalise on the main
        // thread, after all S3 objects, before static destruction).
        rc = [&] {
            if (!fs::is_directory(dir)) {
                throw std::runtime_error("not a directory: " + dir);
            }
            std::string prefix;
            auto remote = open_remote(to, &prefix);
            std::size_t files = 0;
            std::uintmax_t bytes = 0;
            for (const auto& entry : fs::recursive_directory_iterator(dir)) {
                if (!entry.is_regular_file()) {
                    continue;
                }
                const auto rel = fs::relative(entry.path(), dir).generic_string();
                if (!want_file(rel, epoch)) {
                    continue;
                }
                copy_local_to_remote(entry.path(), *remote, prefix + "/" + rel);
                ++files;
                bytes += entry.file_size();
            }
            if (files == 0) {
                throw std::runtime_error("nothing to push under " + dir +
                                         (epoch.empty() ? "" : " for epoch " + epoch));
            }
            std::cout << "capture-push: " << files << " file(s), " << bytes << " bytes -> " << to
                      << (epoch.empty() ? "" : " (epoch " + epoch + " only)") << "\n";
            return 0;
        }();
    } catch (const std::exception& e) {
        std::cerr << "clink capture-push: " << e.what() << "\n";
        rc = 2;
    }
    clink::connectors::finalize_arrow_s3();  // no-op when S3 was never initialised
    return rc;
#endif
}

int clink_cmd_capture_fetch(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        fetch_usage();
        return 0;
    }
    const auto from = get_arg(argc, argv, "from");
    const auto dir = get_arg(argc, argv, "dir");
    const auto epoch = get_arg(argc, argv, "epoch");
    if (from.empty() || dir.empty()) {
        fetch_usage();
        return 2;
    }
#ifndef CLINK_HAS_ARROW
    std::cerr << "clink capture-fetch: this build has no Arrow filesystems (CLINK_BUILD_ARROW "
                 "off)\n";
    return 2;
#else
    int rc = 2;
    try {
        // Inner scope: remote FileSystem destroyed before the finalise.
        rc = [&] {
            std::string prefix;
            auto remote = open_remote(from, &prefix);
            arrow::fs::FileSelector selector;
            selector.base_dir = prefix;
            selector.recursive = true;
            auto infos = remote->GetFileInfo(selector);
            if (!infos.ok()) {
                throw std::runtime_error("cannot list " + from + ": " + infos.status().ToString());
            }
            std::size_t files = 0;
            for (const auto& info : infos.ValueUnsafe()) {
                if (info.type() != arrow::fs::FileType::File) {
                    continue;
                }
                auto rel = info.path();
                if (rel.starts_with(prefix)) {
                    rel = rel.substr(prefix.size());
                }
                while (!rel.empty() && rel.front() == '/') {
                    rel.erase(rel.begin());
                }
                if (rel.empty() || !want_file(rel, epoch)) {
                    continue;
                }
                copy_remote_to_local(*remote, info.path(), fs::path(dir) / rel);
                ++files;
            }
            if (files == 0) {
                throw std::runtime_error("nothing to fetch under " + from +
                                         (epoch.empty() ? "" : " for epoch " + epoch));
            }
            std::cout << "capture-fetch: " << files << " file(s) <- " << from << " -> " << dir
                      << (epoch.empty() ? "" : " (epoch " + epoch + " only)") << "\n";
            return 0;
        }();
    } catch (const std::exception& e) {
        std::cerr << "clink capture-fetch: " << e.what() << "\n";
        rc = 2;
    }
    clink::connectors::finalize_arrow_s3();  // no-op when S3 was never initialised
    return rc;
#endif
}
