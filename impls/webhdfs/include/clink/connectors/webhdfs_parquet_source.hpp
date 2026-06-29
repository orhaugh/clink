#pragma once

// WebHdfsParquetSource<T> - reads a Parquet file from HDFS via the WebHDFS / HttpFS REST API,
// the symmetric counterpart to WebHdfsParquetSink<T>. Reuses clink's HTTP client (no JVM/libhdfs).
// The file is fetched whole into memory and parsed with Arrow's Parquet reader through the shared
// ArrowBatcher<T> seam. Single-object source (directory/glob reads are the obvious follow-up).
//
// WebHDFS read is a two-step GET: OPEN on the NameNode/HttpFS returns a 307 redirect to a datanode
// (or, on some gateways, the bytes directly with a 200); we follow the Location and GET the data.

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

}  // namespace clink
