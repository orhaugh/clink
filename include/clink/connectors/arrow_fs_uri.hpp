#pragma once

// Registry-free replacement for arrow::fs::FileSystemFromUri, for the URI
// schemes clink's own tooling forms (file://, s3://, or a bare local path).
//
// WHY THIS EXISTS. arrow::fs::FileSystemFromUri resolves schemes through a
// process-global factory registry. A binary that links libarrow AND a
// dependency that statically bundles Arrow's filesystem objects (the pinned
// iceberg-cpp does) carries TWO copies of Arrow's pending factory
// registrations. Arrow merges the pending lists into the one coalesced
// registry on first use, the duplicate built-in schemes make that merge fail,
// and from then on EVERY FileSystemFromUri call in the process returns
// "Key error: Attempted to register factory for scheme 'file' but that scheme
// is already registered". Constructing the concrete filesystem directly never
// touches the registry, so the same code works in clean and
// duplicated-registration binaries alike. Unknown schemes still fall back to
// FileSystemFromUri; no clink tool forms them today.
//
// Include only from TUs that carry the Arrow include path (the same contract
// as arrow_s3_lifecycle.hpp). For s3:// URIs the helper runs the engine-wide
// S3 lifecycle init itself, so callers need no separate
// ensure_arrow_s3_initialised() call.

#include <filesystem>
#include <memory>
#include <string>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/result.h>
#include <arrow/util/uri.h>

#include "clink/connectors/arrow_s3_lifecycle.hpp"

namespace clink::connectors {

// Open a filesystem for `uri` without consulting Arrow's factory registry
// where possible. On success `*out_path` holds the in-filesystem path (the
// same contract as arrow::fs::FileSystemFromUri). Throws std::runtime_error
// on parse or construction failure.
inline std::shared_ptr<arrow::fs::FileSystem> filesystem_from_uri(const std::string& uri,
                                                                  std::string* out_path) {
    const auto scheme_end = uri.find("://");
    // No scheme: a bare local path.
    if (scheme_end == std::string::npos) {
        *out_path = std::filesystem::absolute(uri).string();
        return std::make_shared<arrow::fs::LocalFileSystem>();
    }
    const std::string scheme = uri.substr(0, scheme_end);
    if (scheme == "file") {
        arrow::util::Uri parsed;
        const auto st = parsed.Parse(uri);
        if (!st.ok()) {
            throw std::runtime_error("cannot parse " + uri + ": " + st.ToString());
        }
        *out_path = parsed.path();
        return std::make_shared<arrow::fs::LocalFileSystem>();
    }
    if (scheme == "s3") {
        // Same lifecycle the registry factory would have required; one init
        // per process, finalised by the entry point (arrow_s3_lifecycle.hpp).
        ensure_arrow_s3_initialised();
        auto options = arrow::fs::S3Options::FromUri(uri, out_path);
        if (!options.ok()) {
            throw std::runtime_error("cannot parse " + uri + ": " + options.status().ToString());
        }
        auto made = arrow::fs::S3FileSystem::Make(*options);
        if (!made.ok()) {
            throw std::runtime_error("cannot open " + uri + ": " + made.status().ToString());
        }
        return made.MoveValueUnsafe();
    }
    // Foreign scheme: fall back to the registry. In a binary afflicted by the
    // duplicated-registration wart this can fail; none of clink's tools form
    // such URIs.
    auto result = arrow::fs::FileSystemFromUri(uri, out_path);
    if (!result.ok()) {
        throw std::runtime_error("cannot open " + uri + ": " + result.status().ToString());
    }
    return result.MoveValueUnsafe();
}

}  // namespace clink::connectors
