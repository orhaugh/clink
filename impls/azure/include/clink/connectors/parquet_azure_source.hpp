#pragma once

// ParquetAzureSource<T> - reads Parquet from an Azure Blob Storage container via Arrow's
// AzureFileSystem. Symmetric counterpart to ParquetAzureSink<T> and a direct analogue of
// ParquetGcsSource<T>. Single-object source (multi-object / partitioned dataset reads are the
// obvious follow-up).

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/filesystem/azurefs.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "clink/connectors/parquet_azure_sink.hpp"  // azure_detail::make_azure_options
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class ParquetAzureSource final : public Source<T> {
public:
    struct Options {
        std::string container;     // required
        std::string key;           // required
        std::string account_name;  // required
        bool anonymous{false};
        std::optional<std::string> account_key;
        std::optional<std::string> sas_token;
        bool use_default_credential{false};
        std::optional<std::string> blob_storage_authority;
        std::optional<std::string> blob_storage_scheme;
    };

    ParquetAzureSource(Options opts,
                       ArrowBatcher<T> batcher,
                       std::string name = "parquet_azure_source")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.container.empty()) {
            throw std::invalid_argument("ParquetAzureSource: container is required");
        }
        if (opts_.key.empty()) {
            throw std::invalid_argument("ParquetAzureSource: key is required");
        }
        if (opts_.account_name.empty()) {
            throw std::invalid_argument("ParquetAzureSource: account_name is required");
        }
        if (!batcher_.schema || !batcher_.parse) {
            throw std::invalid_argument(
                "ParquetAzureSource: ArrowBatcher must have schema and parse set");
        }
    }

    // Reading a single Parquet object to its last row group is finite.
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        auto azure_opts = azure_detail::make_azure_options(opts_.account_name,
                                                           opts_.anonymous,
                                                           opts_.account_key,
                                                           opts_.sas_token,
                                                           opts_.use_default_credential,
                                                           opts_.blob_storage_authority,
                                                           opts_.blob_storage_scheme);
        auto fs_result = arrow::fs::AzureFileSystem::Make(azure_opts);
        if (!fs_result.ok()) {
            throw std::runtime_error("ParquetAzureSource: AzureFileSystem::Make: " +
                                     fs_result.status().ToString());
        }
        fs_ = *fs_result;

        const auto path = opts_.container + "/" + opts_.key;
        auto in_result = fs_->OpenInputFile(path);
        if (!in_result.ok()) {
            throw std::runtime_error("ParquetAzureSource: OpenInputFile(" + path +
                                     "): " + in_result.status().ToString());
        }
        in_ = *in_result;

        auto reader_result = parquet::arrow::OpenFile(in_, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            throw std::runtime_error("ParquetAzureSource: OpenFile: " +
                                     reader_result.status().ToString());
        }
        reader_ = std::move(*reader_result);

        std::shared_ptr<arrow::Schema> file_schema;
        if (auto s = reader_->GetSchema(&file_schema); !s.ok()) {
            throw std::runtime_error("ParquetAzureSource: GetSchema: " + s.ToString());
        }
        auto expected = batcher_.schema();
        if (!file_schema->Equals(*expected, /*check_metadata=*/false)) {
            throw std::runtime_error("ParquetAzureSource: schema mismatch - file has " +
                                     file_schema->ToString() + "; ArrowBatcher expects " +
                                     expected->ToString());
        }

        auto br_result = reader_->GetRecordBatchReader();
        if (!br_result.ok()) {
            throw std::runtime_error("ParquetAzureSource: GetRecordBatchReader: " +
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
            throw std::runtime_error("ParquetAzureSource: ReadNext: " + s.ToString());
        }
        if (!rb) {
            return false;
        }
        auto parsed = batcher_.parse(*rb);
        if (!parsed.has_value()) {
            throw std::runtime_error("ParquetAzureSource: ArrowBatcher.parse returned nullopt");
        }
        if (!parsed->empty()) {
            out.emit_data(std::move(*parsed));
        }
        return true;
    }

    void close() override {
        batch_reader_.reset();
        reader_.reset();
        if (in_) {
            (void)in_->Close();
            in_.reset();
        }
    }

    std::string name() const override { return name_; }

private:
    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::AzureFileSystem> fs_;
    std::shared_ptr<arrow::io::RandomAccessFile> in_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
};

}  // namespace clink
