// Stable tier: connector authoring bases and the built-in connectors.
// Compile-only; frozen (see README.md). Additions only.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/filesystem/localfs.h>

#include "clink/connectors/capability.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/delivery_guarantee.hpp"
#include "clink/connectors/directory_file_source.hpp"
#include "clink/connectors/file_2pc_sink.hpp"
#include "clink/connectors/file_sink.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_2pc_sink.hpp"
#include "clink/connectors/parquet_sink.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/connectors/polling_source.hpp"
#include "clink/connectors/text_format.hpp"

namespace {

using namespace std::chrono_literals;

[[maybe_unused]] void file_connectors() {
    const std::filesystem::path p{"/tmp/conformance"};
    clink::TextFormat<std::string> fmt = clink::string_text_format();
    (void)fmt.decode;
    (void)fmt.encode;
    clink::TextFormat<std::int64_t> ints{
        .decode = [](std::string_view s) -> std::optional<std::int64_t> {
            return std::stoll(std::string{s});
        },
        .encode = [](const std::int64_t& v) { return std::to_string(v); },
    };
    (void)std::make_shared<clink::FileSource<std::string>>(p, fmt);
    (void)std::make_shared<clink::FileSource<std::string>>(p, fmt, /*batch_size=*/256, "named");
    (void)std::make_shared<clink::DirectoryFileSource<std::string>>(p, fmt);
    (void)std::make_shared<clink::FileSink<std::string>>(p, fmt);
    (void)std::make_shared<clink::FileSink<std::int64_t>>(
        p, ints, /*append=*/true, "named", /*overwrite=*/false);
    (void)std::make_shared<clink::FileSink2PC<std::string>>(p, fmt, /*subtask_idx=*/0);
}

[[maybe_unused]] void parquet_connectors() {
    const std::filesystem::path p{"/tmp/conformance.parquet"};
    (void)std::make_shared<clink::ParquetSource<std::int64_t>>(p, clink::int64_arrow_batcher());
    (void)std::make_shared<clink::ParquetSink<std::int64_t>>(p, clink::int64_arrow_batcher());
    (void)std::make_shared<clink::ParquetSink<std::int64_t>>(
        p, clink::int64_arrow_batcher(), parquet::Compression::SNAPPY, "named");
    (void)std::make_shared<clink::ParquetSink2PC<std::int64_t>>(
        p, clink::int64_arrow_batcher(), /*subtask_idx=*/0);

    clink::MultiObjectParquetSource<std::int64_t>::Options opts;
    opts.prefix = "/tmp/objects";
    opts.recursive = true;
    opts.suffix = ".parquet";
    opts.subtask_idx = 0;
    opts.parallelism = 1;
    opts.require_match = false;
    (void)std::make_shared<clink::MultiObjectParquetSource<std::int64_t>>(
        []() -> std::shared_ptr<arrow::fs::FileSystem> {
            return std::make_shared<arrow::fs::LocalFileSystem>();
        },
        opts,
        clink::int64_arrow_batcher());
}

[[maybe_unused]] void polling_source() {
    clink::PollingSource<std::string>::Options opts;
    opts.interval = 1s;
    opts.initial_cursor = "";
    opts.jitter_frac = 0.1;
    opts.bounded = false;
    opts.name = "poller";
    (void)std::make_shared<clink::PollingSource<std::string>>(opts, [](const std::string& cursor) {
        return clink::PollingSource<std::string>::PollResult{.records = {}, .next_cursor = cursor};
    });
}

[[maybe_unused]] void capabilities_and_guarantees() {
    clink::connectors::ConnectorCapabilities caps{
        .name = "conformance",
        .version = "1",
        .is_source = true,
        .is_sink = false,
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::LogOffset,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .available_in_sql = true,
    };
    (void)caps.self_check();
    clink::connectors::declare_connector(caps);
    (void)clink::connectors::CapabilityRegistry::instance().all();
    (void)clink::connectors::CapabilityRegistry::instance().size();
    clink::connectors::CapabilityRegistry::instance().undeclare("conformance");
    (void)clink::connectors::to_string(clink::connectors::Boundedness::Bounded);

    clink::connectors::PipelineFacts facts;
    facts.checkpointing_enabled = true;
    facts.durable_state_backend = true;
    facts.connectors.push_back(clink::connectors::PipelineConnector{
        .op_type = "conformance_source", .connector_name = "conformance", .is_source = true});
    facts.determinism.reads_wall_clock = false;
    clink::connectors::GuaranteeReport report = clink::connectors::analyse_pipeline(facts);
    (void)report.level;
    (void)report.limiting_factor;
    (void)report.reasons;
    (void)report.warnings;
    (void)report.rejections;
    (void)clink::connectors::to_string(report.level);
}

[[maybe_unused]] void cdc_events() {
    clink::CdcEvent ev;
    ev.op = clink::CdcEvent::Op::Insert;
    ev.table = "public.users";
    ev.lsn = "0/16E2A38";
    ev.xid = 42;
    ev.values.push_back(
        clink::CdcField{.name = "id", .value = "1", .type = "bigint", .is_null = false});
    (void)ev;
}

}  // namespace
