#pragma once

// WebHdfsParquetSource<T> - reads a Parquet file from HDFS via the WebHDFS / HttpFS REST API,
// the symmetric counterpart to WebHdfsParquetSink<T>. Reuses clink's HTTP client (no JVM/libhdfs).
// The file is fetched whole into memory and parsed with Arrow's Parquet reader through the shared
// ArrowBatcher<T> seam. Single-object source; WebHdfsMultiObjectParquetSource (below) reads a whole
// directory by delegating to this class per file.
//
// WebHDFS read is a two-step GET: OPEN on the NameNode/HttpFS returns a 307 redirect to a datanode
// (or, on some gateways, the bytes directly with a 200); we follow the Location and GET the data.

#include <algorithm>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "clink/config/json.hpp"                      // LISTSTATUS response parsing
#include "clink/connectors/webhdfs_parquet_sink.hpp"  // webhdfs_detail helpers
#include "clink/core/arrow_batcher.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class WebHdfsParquetSource final : public Source<T> {
public:
    struct Options {
        std::string base_url;  // WebHDFS/HttpFS root
        std::string path;      // HDFS file path
        std::optional<std::string> user;
        std::optional<std::string> delegation_token;
        bool verify_tls{true};
        int connect_timeout_ms{5000};
        int rw_timeout_ms{30000};
    };

    WebHdfsParquetSource(Options opts,
                         ArrowBatcher<T> batcher,
                         std::string name = "webhdfs_parquet_source")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.base_url.empty()) {
            throw std::invalid_argument("WebHdfsParquetSource: base_url is required");
        }
        if (opts_.path.empty()) {
            throw std::invalid_argument("WebHdfsParquetSource: path is required");
        }
        if (!batcher_.schema || !batcher_.parse) {
            throw std::invalid_argument(
                "WebHdfsParquetSource: ArrowBatcher must have schema and parse set");
        }
        if (opts_.base_url.rfind("https://", 0) == 0 &&
            !clink::http_connector::HttpRequest::tls_supported()) {
            throw std::invalid_argument(
                "WebHdfsParquetSource: base_url is https:// but this build of "
                "clink::http_connector has no TLS support; rebuild it with OpenSSL");
        }
    }

    // Reading a single Parquet object to its last row group is finite.
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        // FromString takes ownership of the moved string without copying its payload, so feed the
        // fetched bytes straight in rather than copying a named lvalue (avoids a whole-file copy).
        auto buffer = arrow::Buffer::FromString(fetch_());
        in_ = std::make_shared<arrow::io::BufferReader>(buffer);

        auto reader_result = parquet::arrow::OpenFile(in_, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            throw std::runtime_error("WebHdfsParquetSource: OpenFile: " +
                                     reader_result.status().ToString());
        }
        reader_ = std::move(*reader_result);

        std::shared_ptr<arrow::Schema> file_schema;
        if (auto s = reader_->GetSchema(&file_schema); !s.ok()) {
            throw std::runtime_error("WebHdfsParquetSource: GetSchema: " + s.ToString());
        }
        auto expected = batcher_.schema();
        if (!file_schema->Equals(*expected, /*check_metadata=*/false)) {
            throw std::runtime_error("WebHdfsParquetSource: schema mismatch - file has " +
                                     file_schema->ToString() + "; ArrowBatcher expects " +
                                     expected->ToString());
        }

        auto br_result = reader_->GetRecordBatchReader();
        if (!br_result.ok()) {
            throw std::runtime_error("WebHdfsParquetSource: GetRecordBatchReader: " +
                                     br_result.status().ToString());
        }
        batch_reader_ = std::move(*br_result);
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled() || !batch_reader_) {
            return false;
        }
        std::shared_ptr<arrow::RecordBatch> rb;
        if (auto s = batch_reader_->ReadNext(&rb); !s.ok()) {
            throw std::runtime_error("WebHdfsParquetSource: ReadNext: " + s.ToString());
        }
        if (!rb) {
            return false;
        }
        auto parsed = batcher_.parse(*rb);
        if (!parsed.has_value()) {
            throw std::runtime_error("WebHdfsParquetSource: ArrowBatcher.parse returned nullopt");
        }
        if (!parsed->empty()) {
            out.emit_data(std::move(*parsed));
        }
        return true;
    }

    void close() override {
        batch_reader_.reset();
        reader_.reset();
        in_.reset();
    }

    std::string name() const override { return name_; }

private:
    std::vector<std::pair<std::string, std::string>> open_params_() const {
        std::vector<std::pair<std::string, std::string>> p;
        if (opts_.user) {
            p.emplace_back("user.name", *opts_.user);
        }
        if (opts_.delegation_token) {
            p.emplace_back("delegation", *opts_.delegation_token);
        }
        return p;
    }

    // OPEN -> (307 -> follow Location ->) GET the file bytes.
    std::string fetch_() {
        clink::http_connector::HttpRequest nn(webhdfs_detail::http_opts(
            opts_.base_url, opts_.verify_tls, opts_.connect_timeout_ms, opts_.rw_timeout_ms));
        const auto open_path =
            webhdfs_detail::build_webhdfs_path(opts_.path, "OPEN", open_params_());
        auto r1 = nn.get(open_path);
        if (r1.status == 0) {
            throw std::runtime_error("WebHdfsParquetSource: OPEN transport error: " + r1.error);
        }
        if (r1.status >= 300 && r1.status < 400) {
            const auto loc = r1.headers.find("location");
            if (loc == r1.headers.end()) {
                throw std::runtime_error("WebHdfsParquetSource: OPEN 307 missing Location header");
            }
            const auto [dn_base, dn_rest] = webhdfs_detail::split_url(loc->second);
            clink::http_connector::HttpRequest dn(webhdfs_detail::http_opts(
                dn_base, opts_.verify_tls, opts_.connect_timeout_ms, opts_.rw_timeout_ms));
            auto r2 = dn.get(dn_rest);
            if (r2.status == 0) {
                throw std::runtime_error("WebHdfsParquetSource: datanode GET transport error: " +
                                         r2.error);
            }
            if (r2.status < 200 || r2.status >= 300) {
                throw std::runtime_error("WebHdfsParquetSource: datanode GET failed: HTTP " +
                                         std::to_string(r2.status) + ": " + r2.body);
            }
            return std::move(r2.body);
        }
        if (r1.status >= 200 && r1.status < 300) {
            return std::move(r1.body);  // gateway returned the bytes directly
        }
        throw std::runtime_error("WebHdfsParquetSource: OPEN failed: HTTP " +
                                 std::to_string(r1.status) + ": " + r1.body);
    }

    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::io::RandomAccessFile> in_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
};

// WebHdfsMultiObjectParquetSource<T> - reads every Parquet object under an HDFS directory via the
// WebHDFS / HttpFS REST API, the multi-object counterpart to WebHdfsParquetSource<T>. WebHDFS is
// not an Arrow filesystem, so it cannot use the shared MultiObjectParquetSource; instead it lists
// the directory with a LISTSTATUS call, shards the file list round-robin across subtasks (file i is
// read by subtask i % parallelism), and reads each assigned file by delegating to a single-object
// WebHdfsParquetSource (so the two-step OPEN, schema check and Parquet decode are reused verbatim).
// Bounded; at-least-once, with no cross-file replay (matching the single-object WebHDFS source).
template <typename T>
class WebHdfsMultiObjectParquetSource final : public Source<T> {
public:
    struct Options {
        std::string base_url;  // WebHDFS/HttpFS root
        std::string dir;       // HDFS directory to list
        std::optional<std::string> user;
        std::optional<std::string> delegation_token;
        bool verify_tls{true};
        int connect_timeout_ms{5000};
        int rw_timeout_ms{30000};
        std::string suffix{".parquet"};  // only files whose name ends with this are read
        int subtask_idx{0};
        int parallelism{1};
    };

    WebHdfsMultiObjectParquetSource(Options opts,
                                    ArrowBatcher<T> batcher,
                                    std::string name = "webhdfs_parquet_dir_source")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.base_url.empty()) {
            throw std::invalid_argument("WebHdfsMultiObjectParquetSource: base_url is required");
        }
        if (opts_.dir.empty()) {
            throw std::invalid_argument("WebHdfsMultiObjectParquetSource: dir is required");
        }
        if (opts_.parallelism < 1 || opts_.subtask_idx < 0 ||
            opts_.subtask_idx >= opts_.parallelism) {
            throw std::invalid_argument(
                "WebHdfsMultiObjectParquetSource: invalid subtask_idx/parallelism");
        }
        if (!batcher_.schema || !batcher_.parse) {
            throw std::invalid_argument(
                "WebHdfsMultiObjectParquetSource: ArrowBatcher must have schema and parse set");
        }
        if (opts_.base_url.rfind("https://", 0) == 0 &&
            !clink::http_connector::HttpRequest::tls_supported()) {
            throw std::invalid_argument(
                "WebHdfsMultiObjectParquetSource: base_url is https:// but this build of "
                "clink::http_connector has no TLS support; rebuild it with OpenSSL");
        }
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        files_ = list_assigned_files_();
        next_file_idx_ = 0;
        open_next_inner_();
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        while (inner_) {
            if (inner_->produce(out)) {
                return true;  // current file emitted (or had an empty batch) and may have more
            }
            inner_->close();  // current file drained
            inner_.reset();
            open_next_inner_();
        }
        return false;
    }

    void close() override {
        if (inner_) {
            inner_->close();
            inner_.reset();
        }
    }

    std::string name() const override { return name_; }

private:
    std::vector<std::string> list_assigned_files_() {
        std::vector<std::pair<std::string, std::string>> params;
        if (opts_.user) {
            params.emplace_back("user.name", *opts_.user);
        }
        if (opts_.delegation_token) {
            params.emplace_back("delegation", *opts_.delegation_token);
        }
        clink::http_connector::HttpRequest nn(webhdfs_detail::http_opts(
            opts_.base_url, opts_.verify_tls, opts_.connect_timeout_ms, opts_.rw_timeout_ms));
        const auto path = webhdfs_detail::build_webhdfs_path(opts_.dir, "LISTSTATUS", params);
        auto r = nn.get(path);
        if (r.status == 0) {
            throw std::runtime_error(
                "WebHdfsMultiObjectParquetSource: LISTSTATUS transport error: " + r.error);
        }
        if (r.status < 200 || r.status >= 300) {
            throw std::runtime_error("WebHdfsMultiObjectParquetSource: LISTSTATUS failed: HTTP " +
                                     std::to_string(r.status) + ": " + r.body);
        }

        std::vector<std::string> all;
        const clink::config::JsonValue root = clink::config::parse(r.body);
        if (root.is_object() && root.contains("FileStatuses")) {
            const auto& statuses = root.at("FileStatuses");
            if (statuses.is_object() && statuses.contains("FileStatus")) {
                const auto& arr = statuses.at("FileStatus");
                if (arr.is_array()) {
                    for (const auto& entry : arr.as_array()) {
                        if (!entry.is_object()) {
                            continue;
                        }
                        const auto type = entry.string_or("type", "");
                        const auto suffix = entry.string_or("pathSuffix", "");
                        if (type == "FILE" && !suffix.empty() && suffix.ends_with(opts_.suffix)) {
                            all.push_back(join_path_(opts_.dir, suffix));
                        }
                    }
                }
            }
        }
        std::sort(all.begin(), all.end());

        const auto par = static_cast<std::size_t>(opts_.parallelism);
        const auto me = static_cast<std::size_t>(opts_.subtask_idx);
        std::vector<std::string> mine;
        for (std::size_t i = 0; i < all.size(); ++i) {
            if (i % par == me) {
                mine.push_back(all[i]);
            }
        }
        return mine;
    }

    static std::string join_path_(const std::string& dir, const std::string& name) {
        if (!dir.empty() && dir.back() == '/') {
            return dir + name;
        }
        return dir + "/" + name;
    }

    void open_next_inner_() {
        while (next_file_idx_ < files_.size()) {
            typename WebHdfsParquetSource<T>::Options o;
            o.base_url = opts_.base_url;
            o.path = files_[next_file_idx_++];
            o.user = opts_.user;
            o.delegation_token = opts_.delegation_token;
            o.verify_tls = opts_.verify_tls;
            o.connect_timeout_ms = opts_.connect_timeout_ms;
            o.rw_timeout_ms = opts_.rw_timeout_ms;
            inner_ = std::make_shared<WebHdfsParquetSource<T>>(std::move(o), batcher_);
            inner_->open();
            return;
        }
        inner_.reset();  // no more files
    }

    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::vector<std::string> files_;
    std::size_t next_file_idx_ = 0;
    std::shared_ptr<WebHdfsParquetSource<T>> inner_;
};

}  // namespace clink
