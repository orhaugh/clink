#pragma once

// Apache Iceberg table-format SOURCE for the SQL Row channel, built on the
// native Apache iceberg-cpp library - the read half of the iceberg
// connector: `connector='iceberg'` tables are queryable, not just writable.
//
// SEMANTICS: a BOUNDED scan of one table snapshot. open() loads the table
// through the catalog (SQLite or REST - same selection rules as the sink),
// pins the snapshot, plans the file-scan tasks and streams every data
// file's Arrow batches as Rows. Column projection pushes down twice: the
// scan plans only the selected columns, and each Parquet data file is read
// with that projection (Iceberg column pruning).
//
// EXACTLY-ONCE REPLAY: the source checkpoints (snapshot id, task index,
// batch index) through the standard snapshot_offset/restore_offset hooks.
// A restore re-plans the SAME pinned snapshot (task order is deterministic
// for a fixed snapshot) and skips to the recorded position, so recovery
// re-emits nothing and skips nothing even if the table gained snapshots
// meanwhile.
//
// NOT in v1: delete-file application (a table with positional/equality
// deletes in the scanned snapshot is rejected loudly rather than read
// wrong), incremental/changelog scans, and residual filter pushdown.
//
// This header is driver-free (no iceberg-cpp includes) so factory
// registration and tests do not need the iceberg-cpp headers; the
// implementation lives in the connector's .cpp.

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"  // RowColumn

namespace clink::iceberg {

struct IcebergRowSourceOptions {
    // Catalog warehouse location: a local path or "s3://bucket/prefix".
    std::string warehouse;
    std::vector<std::string> namespace_levels{"default"};
    std::string table;  // table name (required)
    // Catalog selector - same rules as the sink: default SQLite at
    // "<warehouse>/catalog.db"; http(s):// selects the REST catalog.
    std::string catalog_uri;
    std::string rest_auth_token;
    // S3 FileIO properties for an s3:// warehouse (iceberg "s3.*" keys).
    std::unordered_map<std::string, std::string> file_io_props;
    // The declared columns to read (name + Arrow type), possibly already
    // narrowed by the planner's projection pushdown. The scan selects
    // exactly these columns; a name missing from the table errors.
    std::vector<clink::sql::RowColumn> columns;
    std::string name{"iceberg_row_source"};
};

// Build the bounded Iceberg snapshot-scan source.
std::shared_ptr<Source<clink::sql::Row>> make_iceberg_row_source(IcebergRowSourceOptions opts);

}  // namespace clink::iceberg
