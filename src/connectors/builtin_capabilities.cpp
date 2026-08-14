// Capability declarations for the connectors that ship in clink_core.
//
// Every field here was read out of the implementation it describes, not
// inferred from what the underlying technology can do. Where the code and
// an optimistic reading of the docs disagree, the code wins and the
// difference goes in `limitations`.

#include "clink/connectors/builtin_capabilities.hpp"

#include "clink/connectors/capability.hpp"

namespace clink::connectors {

void declare_builtin_capabilities() {
    // ---- file (line/int64 source + sink) --------------------------------
    //
    // Source: include/clink/connectors/file_source.hpp keeps a byte offset
    // and restores it via restore_offset() before open(), so it replays.
    // Sink: include/clink/connectors/file_sink.hpp writes straight to the
    // target stream, so a restart re-emits whatever the last checkpoint
    // did not cover. That is at-least-once, and calling it anything
    // stronger would be wrong - use file_2pc for exactly-once.
    declare_connector(ConnectorCapabilities{
        .name = "file",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {},
        .runtime_dependencies = {},
        .formats = {"text/lines", "int64"},
        .boundedness = Boundedness::Bounded,
        .replayable = true,
        .offset_model = OffsetModel::FileOffset,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"filesystem-permissions"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"the plain sink appends as it goes, so a restart re-emits "
                        "post-checkpoint records; use file_2pc for exactly-once output",
                        "one file per subtask - parallelism > 1 suffixes the path"},
    });

    // ---- file_2pc -------------------------------------------------------
    //
    // include/clink/connectors/file_2pc_sink.hpp: stages to a temp file,
    // persists the handle at the barrier, atomic-renames on the commit
    // broadcast that follows the COMPLETED-N marker. Genuine two-phase
    // commit against the filesystem.
    declare_connector(ConnectorCapabilities{
        .name = "file_2pc",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .formats = {"text/lines"},
        .boundedness = Boundedness::Either,
        .replayable = false,
        .offset_model = OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::ExactlyOnceTwoPhaseCommit,
        .transactional = true,
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"filesystem-permissions"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"a staging file for a checkpoint that never completed is left in "
                        "staging/ for manual cleanup - recovery will not auto-abort it, "
                        "because a half-written file cannot be told apart from a "
                        "commit that failed"},
        // dir OR path: the C++ factory takes `dir`, the SQL DDL supplies
        // `path`. Naming only one spelling would reject valid SQL jobs.
        .required_options_for_exactly_once = {"dir|path"},
    });

    // ---- parquet --------------------------------------------------------
    //
    // ParquetSink writes a whole file and closes it; the source reads
    // files or a prefix. No per-record offset state on the source, so a
    // restart re-reads from the top of its assigned files.
    declare_connector(ConnectorCapabilities{
        .name = "parquet",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"arrow", "parquet"},
        .formats = {"parquet"},
        .boundedness = Boundedness::Bounded,
        // The source snapshots its batch index (batches_emitted_) between
        // produce() calls and restore seeks past the already-emitted
        // batches - see ParquetSource::snapshot_offset/restore_offset.
        // This record used to claim no position was kept; that was stale
        // (written before the offset support landed), and it made the
        // guarantee analyser cap every parquet-source pipeline at
        // at-most-once and reject exactly-once jobs the engine supports.
        // The claim is now held by the source contract suite
        // (tests/test_source_contract.cpp), which replays every produce
        // boundary.
        .replayable = true,
        .offset_model = OffsetModel::FileOffset,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .schema_evolution = true,
        .partition_discovery = true,
        .auth_methods = {"filesystem-permissions"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"offset granularity is the record batch: a restore resumes at the "
                        "next unread batch",
                        "use parquet_2pc for checkpoint-tied output"},
    });

    // ---- parquet_2pc ----------------------------------------------------
    declare_connector(ConnectorCapabilities{
        .name = "parquet_2pc",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"arrow", "parquet"},
        .formats = {"parquet"},
        .boundedness = Boundedness::Either,
        .replayable = false,
        .offset_model = OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::ExactlyOnceTwoPhaseCommit,
        .transactional = true,
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"filesystem-permissions"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"same staging-file cleanup caveat as file_2pc"},
        // dir OR path: the C++ factory takes `dir`, the SQL DDL supplies
        // `path`. Naming only one spelling would reject valid SQL jobs.
        .required_options_for_exactly_once = {"dir|path"},
    });

    // ---- generator / range sources --------------------------------------
    //
    // Deterministic, fully re-derivable from their params, so a restart
    // reproduces the same records: replayable in the strict sense even
    // though nothing is persisted.
    declare_connector(ConnectorCapabilities{
        .name = "generator",
        .version = "1",
        .is_source = true,
        .is_sink = false,
        .formats = {"int64", "text/lines"},
        .boundedness = Boundedness::Bounded,
        .replayable = true,
        .offset_model = OffsetModel::FileOffset,
        .checkpoint_integrated = true,
        .delivery = DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .backpressure = true,
        .available_in_sql = true,
        .limitations = {"a test and benchmark source; the record set is derived from the "
                        "op params, not read from anywhere"},
    });

    // ---- blackhole / print ----------------------------------------------
    //
    // Discards or prints. There is no external system to be exactly-once
    // against, which is why this is NoDurableRestartGuarantee rather than
    // a delivery level - the question does not apply.
    declare_connector(ConnectorCapabilities{
        .name = "blackhole",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .formats = {"any"},
        .boundedness = Boundedness::Either,
        .replayable = false,
        .offset_model = OffsetModel::None,
        .checkpoint_integrated = false,
        .delivery = DeliveryGuarantee::NoDurableRestartGuarantee,
        .transactional = false,
        .backpressure = true,
        .available_in_sql = true,
        .limitations = {"output is discarded; present for benchmarking and for draining a "
                        "branch whose results are not wanted"},
    });
}

}  // namespace clink::connectors
