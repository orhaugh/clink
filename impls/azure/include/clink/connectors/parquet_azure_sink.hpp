#pragma once

// ParquetAzureSink<T> - writes Parquet to an Azure Blob Storage container using Arrow's
// AzureFileSystem as the transport. The Azure analogue of ParquetS3Sink<T> / ParquetGcsSink<T>:
// same ArrowBatcher<T> seam, same per-key writer model. Like GCS (and unlike S3) it needs no
// process-wide init/finalise, and OpenSSL's atexit is already suppressed engine-wide
// (openssl_atexit_guard), so there is no exit-teardown hook here.
//
// Auth (in precedence order): anonymous (a public container), an account shared key (also how the
// Azurite emulator authenticates), a SAS token, or - by default - the DefaultAzureCredential chain
// (environment, workload identity, managed identity, Azure CLI). anonymous=true takes precedence
// and ignores any account_key/sas_token also supplied. blob_storage_authority + blob_storage_scheme
// target an emulator such as Azurite.
//
// Paths are "<container>/<blob>"; the storage account is named separately (account_name).

#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <arrow/api.h>
#include <arrow/filesystem/azurefs.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

namespace azure_detail {

// Build AzureOptions from the connector's auth/endpoint settings (shared by the sink + source).
// Throws std::runtime_error if the chosen credential fails to configure.
inline arrow::fs::AzureOptions make_azure_options(
    const std::string& account_name,
    bool anonymous,
    const std::optional<std::string>& account_key,
    const std::optional<std::string>& sas_token,
    bool use_default_credential,
    const std::optional<std::string>& blob_storage_authority,
    const std::optional<std::string>& blob_storage_scheme) {
    arrow::fs::AzureOptions opts;
    opts.account_name = account_name;
    // An emulator (Azurite) is reached by overriding the authority/scheme. A non-dotted
    // authority puts the account in the URL path (http://host:port/<account>/...), which is
    // exactly Azurite's layout. We mirror the override onto the DFS endpoint too: Azurite
    // serves ADLS Gen2 calls through the same host, and a flat-namespace account never needs
    // the real *.dfs.core.windows.net.
    if (blob_storage_authority) {
        opts.blob_storage_authority = *blob_storage_authority;
        opts.dfs_storage_authority = *blob_storage_authority;
    }
    if (blob_storage_scheme) {
        opts.blob_storage_scheme = *blob_storage_scheme;
        opts.dfs_storage_scheme = *blob_storage_scheme;
    }

    arrow::Status st;
    if (anonymous) {
        st = opts.ConfigureAnonymousCredential();
    } else if (account_key) {
        st = opts.ConfigureAccountKeyCredential(*account_key);
    } else if (sas_token) {
        st = opts.ConfigureSASCredential(*sas_token);
    } else if (use_default_credential) {
        st = opts.ConfigureDefaultCredential();
    } else {
        // No explicit credential: the DefaultAzureCredential chain (env / workload identity /
        // managed identity / CLI), matching the ambient-identity default of the other cloud sinks.
        st = opts.ConfigureDefaultCredential();
    }
    if (!st.ok()) {
        throw std::runtime_error("ParquetAzure: credential config: " + st.ToString());
    }
    return opts;
}

}  // namespace azure_detail

template <typename T>
class ParquetAzureSink final : public Sink<T> {
public:
    struct Options {
        std::string container;                   // required
        std::string key;                         // required when `bucket_assigner` is unset
        std::string account_name;                // required (the storage account)
        bool anonymous{false};                   // public container / emulator
        std::optional<std::string> account_key;  // shared-key auth
        std::optional<std::string> sas_token;    // SAS-token auth
        bool use_default_credential{false};      // force the DefaultAzureCredential chain
        std::optional<std::string> blob_storage_authority;  // emulator host:port (e.g. Azurite)
        std::optional<std::string> blob_storage_scheme;     // "http" for the emulator; else https
        parquet::Compression::type compression{parquet::Compression::ZSTD};
        // Optional per-record blob-key assigner (Hive-style partitioning). Empty = single object.
        std::function<std::string(const T&)> bucket_assigner;
    };

    ParquetAzureSink(Options opts, ArrowBatcher<T> batcher, std::string name = "parquet_azure_sink")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.container.empty()) {
            throw std::invalid_argument("ParquetAzureSink: container is required");
        }
        if (opts_.account_name.empty()) {
            throw std::invalid_argument("ParquetAzureSink: account_name is required");
        }
        if (!opts_.bucket_assigner && opts_.key.empty()) {
            throw std::invalid_argument(
                "ParquetAzureSink: key is required when no bucket_assigner is set");
        }
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "ParquetAzureSink: ArrowBatcher must have schema and build set");
        }
    }

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
            throw std::runtime_error("ParquetAzureSink: AzureFileSystem::Make: " +
                                     fs_result.status().ToString());
        }
        fs_ = *fs_result;
        if (!opts_.bucket_assigner) {
            ensure_writer_for_key_(opts_.key);
        }
    }

    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        if (!opts_.bucket_assigner) {
            write_batch_to_writer_(opts_.key, batch);
            return;
        }
        std::unordered_map<std::string, Batch<T>> by_key;
        for (const auto& rec : batch) {
            const auto k = opts_.bucket_assigner(rec.value());
            auto& sub = by_key[k];
            if (rec.event_time()) {
                sub.emplace(rec.value(), *rec.event_time());
            } else {
                sub.emplace(rec.value());
            }
        }
        for (auto& [k, sub_batch] : by_key) {
            write_batch_to_writer_(k, sub_batch);
        }
    }

    void close() override {
        // Close + release EVERY writer/stream before reporting an error. AzureFileSystem
        // buffers writes in the background, so a failure commonly surfaces here at Close()
        // time; a mid-loop throw would strand the other keys' streams un-Closed (a partial
        // or zero-length blob) and leave writers_ populated for an unsafe retry. Capture the
        // first error, finish the teardown, then rethrow.
        std::string first_err;
        for (auto& [k, entry] : writers_) {
            if (entry.writer) {
                if (auto s = entry.writer->Close(); !s.ok() && first_err.empty()) {
                    first_err = "ParquetAzureSink: writer close (" + k + "): " + s.ToString();
                }
                entry.writer.reset();
            }
            if (entry.out) {
                if (auto s = entry.out->Close(); !s.ok() && first_err.empty()) {
                    first_err = "ParquetAzureSink: stream close (" + k + "): " + s.ToString();
                }
                entry.out.reset();
            }
        }
        writers_.clear();
        if (!first_err.empty()) {
            throw std::runtime_error(first_err);
        }
    }

    std::string name() const override { return name_; }

private:
    struct WriterEntry {
        std::shared_ptr<arrow::io::OutputStream> out;
        std::unique_ptr<parquet::arrow::FileWriter> writer;
    };

    WriterEntry& ensure_writer_for_key_(const std::string& key) {
        auto it = writers_.find(key);
        if (it != writers_.end()) {
            return it->second;
        }
        const auto path = opts_.container + "/" + key;
        auto out_result = fs_->OpenOutputStream(path, /*metadata=*/{});
        if (!out_result.ok()) {
            throw std::runtime_error("ParquetAzureSink: OpenOutputStream(" + path +
                                     "): " + out_result.status().ToString());
        }
        auto props = parquet::WriterProperties::Builder().compression(opts_.compression)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto schema = batcher_.schema();
        auto writer_result = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), *out_result, props, arrow_props);
        if (!writer_result.ok()) {
            throw std::runtime_error("ParquetAzureSink: FileWriter::Open(" + path +
                                     "): " + writer_result.status().ToString());
        }
        WriterEntry entry;
        entry.out = *out_result;
        entry.writer = std::move(*writer_result);
        auto [ins, _] = writers_.emplace(key, std::move(entry));
        return ins->second;
    }

    void write_batch_to_writer_(const std::string& key, const Batch<T>& batch) {
        auto& entry = ensure_writer_for_key_(key);
        auto record_batch = batcher_.build(batch);
        if (!record_batch) {
            throw std::runtime_error("ParquetAzureSink: ArrowBatcher.build returned null");
        }
        if (auto s = entry.writer->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("ParquetAzureSink: WriteRecordBatch (" + key +
                                     "): " + s.ToString());
        }
    }

    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::AzureFileSystem> fs_;
    std::unordered_map<std::string, WriterEntry> writers_;
};

}  // namespace clink
