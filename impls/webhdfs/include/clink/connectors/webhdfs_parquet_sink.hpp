#pragma once

// WebHdfsParquetSink<T> - writes Parquet to HDFS via the WebHDFS / HttpFS REST API, reusing
// clink's HTTP client (clink::http_connector). No JVM and no libhdfs: unlike the S3/GCS/Azure
// Parquet sinks (which ride a pure-C++/REST client bundled into libarrow), HDFS's native client
// is JNI-based, so this connector speaks the HTTP protocol instead and keeps the engine JVM-free.
//
// The Parquet file is built in memory (Arrow BufferOutputStream + the shared ArrowBatcher<T>
// seam) and uploaded as a single object on close() via WebHDFS's two-step write:
//   1. PUT  <base>/webhdfs/v1<path>?op=CREATE...   (no body) -> 307 Temporary Redirect + Location
//   2. PUT  <Location>                              (the Parquet bytes) -> 201 Created
// A datanode Location returned by a NameNode must be resolvable by this process; an HttpFS gateway
// (single endpoint, no per-datanode redirect target) sidesteps that and is the simplest target.
//
// at-least-once: the file is created+finalised in one shot on close(); a failure mid-upload throws
// and the job replays from the last checkpoint. The whole file is buffered in memory before upload
// (WebHDFS has no incremental Arrow OutputStream), so size is bounded by available memory.

#include <cctype>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

namespace webhdfs_detail {

// Percent-encode a query-parameter value (unreserved set per RFC 3986 kept verbatim).
inline std::string url_encode(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.' || c == '~') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0F]);
        }
    }
    return out;
}

// Percent-encode each path segment, preserving the '/' separators. The HDFS path is user-supplied
// (the SQL 'path=' option); httplib percent-encodes spaces and non-ASCII bytes on the wire but
// leaves the reserved '?', '#' and '%' intact (it has to, since the "?op=..." query is packed into
// the same request string), so those would otherwise corrupt the request target. Encoding here
// composes safely with httplib's pass (it does not re-encode an existing '%').
inline std::string encode_path(const std::string& path) {
    std::string out;
    out.reserve(path.size());
    std::string seg;
    for (char c : path) {
        if (c == '/') {
            out += url_encode(seg);
            seg.clear();
            out.push_back('/');
        } else {
            seg.push_back(c);
        }
    }
    out += url_encode(seg);
    return out;
}

// Build the WebHDFS request path "/webhdfs/v1<path>?op=<op>&k=v..." (empty-valued params dropped).
inline std::string build_webhdfs_path(
    const std::string& hdfs_path,
    const std::string& op,
    const std::vector<std::pair<std::string, std::string>>& params) {
    std::string p = "/webhdfs/v1";
    if (hdfs_path.empty() || hdfs_path.front() != '/') {
        p.push_back('/');
    }
    p += encode_path(hdfs_path);
    p += "?op=" + op;
    for (const auto& [k, v] : params) {
        if (!v.empty()) {
            p += "&" + k + "=" + url_encode(v);
        }
    }
    return p;
}

// Split an absolute http(s) URL (e.g. a 307 Location) into {scheme://authority, /path?query}.
inline std::pair<std::string, std::string> split_url(const std::string& url) {
    const auto scheme = url.find("://");
    if (scheme == std::string::npos) {
        throw std::runtime_error("WebHDFS: malformed redirect URL (no scheme): " + url);
    }
    const auto slash = url.find('/', scheme + 3);
    if (slash == std::string::npos) {
        return {url, "/"};
    }
    return {url.substr(0, slash), url.substr(slash)};
}

inline clink::http_connector::HttpRequest::Options http_opts(const std::string& base_url,
                                                             bool verify_tls,
                                                             int connect_ms,
                                                             int rw_ms) {
    clink::http_connector::HttpRequest::Options o;
    o.base_url = base_url;
    o.verify_tls = verify_tls;
    o.connect_timeout_ms = connect_ms;
    o.rw_timeout_ms = rw_ms;
    return o;
}

}  // namespace webhdfs_detail

template <typename T>
class WebHdfsParquetSink final : public Sink<T> {
public:
    struct Options {
        std::string base_url;  // WebHDFS/HttpFS root, e.g. http://nn:9870 or http://httpfs:14000
        std::string path;      // HDFS file path, e.g. /clink/out.parquet
        std::optional<std::string> user;              // -> &user.name=
        std::optional<std::string> delegation_token;  // -> &delegation=
        std::optional<std::string> permission;        // -> &permission= (octal, e.g. "644")
        bool overwrite{true};
        bool verify_tls{true};  // https only; false skips server-cert verification
        int connect_timeout_ms{5000};
        int rw_timeout_ms{30000};
        parquet::Compression::type compression{parquet::Compression::ZSTD};
    };

    WebHdfsParquetSink(Options opts,
                       ArrowBatcher<T> batcher,
                       std::string name = "webhdfs_parquet_sink")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.base_url.empty()) {
            throw std::invalid_argument("WebHdfsParquetSink: base_url is required");
        }
        if (opts_.path.empty()) {
            throw std::invalid_argument("WebHdfsParquetSink: path is required");
        }
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "WebHdfsParquetSink: ArrowBatcher must have schema and build set");
        }
        if (opts_.base_url.rfind("https://", 0) == 0 &&
            !clink::http_connector::HttpRequest::tls_supported()) {
            throw std::invalid_argument(
                "WebHdfsParquetSink: base_url is https:// but this build of clink::http_connector "
                "has no TLS support; rebuild the http_connector module with OpenSSL");
        }
    }

    void open() override {
        // Build the Parquet file in memory; the upload happens in close(). No network here.
        auto out = arrow::io::BufferOutputStream::Create();
        if (!out.ok()) {
            throw std::runtime_error("WebHdfsParquetSink: BufferOutputStream: " +
                                     out.status().ToString());
        }
        out_stream_ = *out;
        auto props = parquet::WriterProperties::Builder().compression(opts_.compression)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto schema = batcher_.schema();
        auto writer = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), out_stream_, props, arrow_props);
        if (!writer.ok()) {
            throw std::runtime_error("WebHdfsParquetSink: FileWriter::Open: " +
                                     writer.status().ToString());
        }
        writer_ = std::move(*writer);
    }

    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        auto record_batch = batcher_.build(batch);
        if (!record_batch) {
            throw std::runtime_error("WebHdfsParquetSink: ArrowBatcher.build returned null");
        }
        if (auto s = writer_->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("WebHdfsParquetSink: WriteRecordBatch: " + s.ToString());
        }
    }

    void close() override {
        if (!writer_) {
            return;  // never opened, or already finalised
        }
        const auto close_status = writer_->Close();
        writer_.reset();
        if (!close_status.ok()) {
            out_stream_.reset();
            throw std::runtime_error("WebHdfsParquetSink: writer close: " +
                                     close_status.ToString());
        }
        auto buffer = out_stream_->Finish();
        out_stream_.reset();
        if (!buffer.ok()) {
            throw std::runtime_error("WebHdfsParquetSink: buffer finish: " +
                                     buffer.status().ToString());
        }
        upload_(*buffer);
    }

    std::string name() const override { return name_; }

private:
    std::vector<std::pair<std::string, std::string>> create_params_() const {
        std::vector<std::pair<std::string, std::string>> p;
        p.emplace_back("overwrite", opts_.overwrite ? "true" : "false");
        if (opts_.user) {
            p.emplace_back("user.name", *opts_.user);
        }
        if (opts_.delegation_token) {
            p.emplace_back("delegation", *opts_.delegation_token);
        }
        if (opts_.permission) {
            p.emplace_back("permission", *opts_.permission);
        }
        return p;
    }

    void upload_(const std::shared_ptr<arrow::Buffer>& buffer) {
        // Step 1: CREATE on the NameNode/HttpFS -> 307 Temporary Redirect with a datanode Location.
        clink::http_connector::HttpRequest nn(webhdfs_detail::http_opts(
            opts_.base_url, opts_.verify_tls, opts_.connect_timeout_ms, opts_.rw_timeout_ms));
        const auto create_path =
            webhdfs_detail::build_webhdfs_path(opts_.path, "CREATE", create_params_());
        auto r1 = nn.put(create_path, /*body=*/"", "application/octet-stream");
        if (r1.status == 0) {
            throw std::runtime_error("WebHdfsParquetSink: CREATE transport error: " + r1.error);
        }
        if (r1.status < 300 || r1.status >= 400) {
            // CREATE carries no body, so a non-redirect 2xx would create an empty file rather than
            // upload the Parquet bytes: fail loudly instead. A gateway that only does
            // single-request inline writes (no datanode redirect) is not supported.
            throw std::runtime_error(
                "WebHdfsParquetSink: CREATE expected a 307 redirect to a datanode, got HTTP " +
                std::to_string(r1.status) + ": " + r1.body);
        }
        const auto loc = r1.headers.find("location");
        if (loc == r1.headers.end()) {
            throw std::runtime_error("WebHdfsParquetSink: CREATE 307 missing Location header");
        }

        // Step 2: PUT the Parquet bytes to the datanode Location.
        const auto [dn_base, dn_rest] = webhdfs_detail::split_url(loc->second);
        clink::http_connector::HttpRequest dn(webhdfs_detail::http_opts(
            dn_base, opts_.verify_tls, opts_.connect_timeout_ms, opts_.rw_timeout_ms));
        std::string body(reinterpret_cast<const char*>(buffer->data()),
                         static_cast<std::size_t>(buffer->size()));
        auto r2 = dn.put(dn_rest, body, "application/octet-stream");
        if (r2.status == 0) {
            throw std::runtime_error("WebHdfsParquetSink: datanode PUT transport error: " +
                                     r2.error);
        }
        if (r2.status < 200 || r2.status >= 300) {
            throw std::runtime_error("WebHdfsParquetSink: datanode PUT failed: HTTP " +
                                     std::to_string(r2.status) + ": " + r2.body);
        }
    }

    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::io::BufferOutputStream> out_stream_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
};

}  // namespace clink
