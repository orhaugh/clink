// Fluent builders for clink's built-in (core, no external deps)
// connector factories. Each builder is a thin typed wrapper around the
// registered factory name + param map; .build() returns a Source/Sink
// descriptor the Pipeline knows how to insert into
// the graph.
//
// Vendor-specific builders (Kafka, Postgres, ClickHouse, S3) moved into
// per-impl headers under clink/api/<vendor>_builders.hpp during the
// impls split. This header transitively re-includes them so existing
// `#include <clink/api/builtin_connectors.hpp>` callsites keep
// resolving the vendor builder names. Eventually those re-includes get
// dropped and callers must `#include <clink/api/kafka_builders.hpp>`
// (etc.) explicitly.
//
// User-defined connectors live in plugins; their builders are
// generated alongside the plugin's register_source / register_sink
// calls. The pattern here is the template for those.

#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"

// Transitional vendor re-exports for back-compat with code that used to
// pull all builders through this single umbrella header. Each include
// is guarded by __has_include so callers can build core-only without
// the impl include dirs on the search path. A later change removes these
// re-exports entirely - callers will need to include the matching
// clink/api/<vendor>_builders.hpp explicitly.
#if __has_include("clink/api/kafka_builders.hpp")
#include "clink/api/kafka_builders.hpp"
#endif
#if __has_include("clink/api/postgres_builders.hpp")
#include "clink/api/postgres_builders.hpp"
#endif
#if __has_include("clink/api/clickhouse_builders.hpp")
#include "clink/api/clickhouse_builders.hpp"
#endif
#if __has_include("clink/api/s3_builders.hpp")
#include "clink/api/s3_builders.hpp"
#endif

namespace clink::api {

// ----- int64_range_source ---------------------------------------------
//
// Emits the integer sequence start, start+step, ..., start+(count-1)*step.
// At parallelism > 1, subtask i emits the strided slice {i, i+N, i+2N, ...}
// so every record appears exactly once across N parallel subtasks.
class IntRangeSource {
public:
    class Builder {
    public:
        Builder& count(std::int64_t v) {
            count_ = v;
            return *this;
        }
        Builder& start(std::int64_t v) {
            start_ = v;
            return *this;
        }
        Builder& step(std::int64_t v) {
            step_ = v;
            return *this;
        }
        Builder& parallelism(std::uint32_t p) {
            parallelism_ = p;
            return *this;
        }

        SourceDescriptor build() const {
            SourceDescriptor d;
            d.op_type = "int64_range_source";
            d.channel_type = "int64";
            d.params["count"] = std::to_string(count_);
            d.params["start"] = std::to_string(start_);
            d.params["step"] = std::to_string(step_);
            d.parallelism = parallelism_;
            return d;
        }

    private:
        std::int64_t count_{0};
        std::int64_t start_{1};
        std::int64_t step_{1};
        std::uint32_t parallelism_{1};
    };

    static Builder builder() { return Builder{}; }
};

// ----- string_lines_source --------------------------------------------
//
// Emits each entry of `lines` as a record. v1 uses a comma-separated
// concatenation (no escaping).
class StringLinesSource {
public:
    class Builder {
    public:
        Builder& lines(std::string csv) {
            lines_ = std::move(csv);
            return *this;
        }
        Builder& parallelism(std::uint32_t p) {
            parallelism_ = p;
            return *this;
        }

        SourceDescriptor build() const {
            SourceDescriptor d;
            d.op_type = "string_lines_source";
            d.channel_type = "string";
            d.params["lines"] = lines_;
            d.parallelism = parallelism_;
            return d;
        }

    private:
        std::string lines_;
        std::uint32_t parallelism_{1};
    };

    static Builder builder() { return Builder{}; }
};

// ----- file_text_source -----------------------------------------------
//
// Reads newline-delimited text from a path. Each line is emitted as a
// std::string record.
class FileTextSource {
public:
    class Builder {
    public:
        Builder& path(std::filesystem::path p) {
            path_ = std::move(p);
            return *this;
        }
        Builder& batch_size(std::int64_t n) {
            batch_size_ = n;
            return *this;
        }

        SourceDescriptor build() const {
            SourceDescriptor d;
            d.op_type = "file_text_source";
            d.channel_type = "string";
            d.params["path"] = path_.string();
            d.params["batch_size"] = std::to_string(batch_size_);
            return d;
        }

    private:
        std::filesystem::path path_;
        std::int64_t batch_size_{256};
    };

    static Builder builder() { return Builder{}; }
};

// ----- file_text_sink -------------------------------------------------
//
// Writes one std::string per line. With parallelism > 1 the per-subtask
// output is suffixed by subtask_idx (path.0, path.1, ...).
class FileTextSink {
public:
    class Builder {
    public:
        Builder& path(std::filesystem::path p) {
            path_ = std::move(p);
            return *this;
        }
        Builder& append(bool v) {
            append_ = v;
            return *this;
        }
        Builder& parallelism(std::uint32_t p) {
            parallelism_ = p;
            return *this;
        }

        SinkDescriptor build() const {
            SinkDescriptor d;
            d.op_type = "file_text_sink";
            d.channel_type = "string";
            d.params["path"] = path_.string();
            d.params["append"] = append_ ? "true" : "false";
            d.parallelism = parallelism_;
            return d;
        }

    private:
        std::filesystem::path path_;
        bool append_{false};
        std::uint32_t parallelism_{1};
    };

    static Builder builder() { return Builder{}; }
};

// ----- file_int64_sink ------------------------------------------------
//
// Writes one std::int64_t per line. With parallelism > 1 the per-subtask
// output is suffixed by subtask_idx.
class FileInt64Sink {
public:
    class Builder {
    public:
        Builder& path(std::filesystem::path p) {
            path_ = std::move(p);
            return *this;
        }
        Builder& parallelism(std::uint32_t p) {
            parallelism_ = p;
            return *this;
        }

        SinkDescriptor build() const {
            SinkDescriptor d;
            d.op_type = "file_int64_sink";
            d.channel_type = "int64";
            d.params["path"] = path_.string();
            d.parallelism = parallelism_;
            return d;
        }

    private:
        std::filesystem::path path_;
        std::uint32_t parallelism_{1};
    };

    static Builder builder() { return Builder{}; }
};

}  // namespace clink::api
