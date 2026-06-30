#pragma once

// Data-lineage model: a small graph of the external datasets a job reads
// from and writes to, derived from the submitted JobGraphSpec.
//
// The model is deliberately a graph of CONNECTORS and DATASETS, not of
// physical operators. Vertices are sources and sinks; datasets are the
// external things they touch; edges are coarse source -> sink
// dependencies (real reachability over the DAG, not all-to-all). This is
// what an external lineage system (a catalog, a metadata store) wants to
// ingest, and it is stable across operator-level topology edits.
//
// Identity is shaped after the OpenLineage data model (namespace + name +
// open facets) so the built-in exporter and any third-party listener map
// it onto their own catalog with little translation. Nothing here does
// I/O; extract_lineage is a pure function of the spec.

#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace clink::cluster {
struct JobGraphSpec;
}

namespace clink::lineage {

// One source column that contributes to a sink output column. Identifies
// the source dataset by the same (ns, name) the source vertex carries, plus
// the source field name.
struct ColumnInputField {
    std::string ns;
    std::string name;
    std::string field;
};

// Column-level lineage for one sink output column: the source columns it is
// derived from, and how. `transformation` is "IDENTITY" (a straight copy),
// "TRANSFORMATION" (a computed expression) or "AGGREGATION" (through a GROUP
// BY aggregate). Empty inputs means no traceable source (e.g. COUNT(*), a
// window bound, or an opaque async lookup).
struct ColumnLineageField {
    std::string output;
    std::string transformation;
    std::vector<ColumnInputField> inputs;
};

// One external data entity a connector reads from or writes to. Identity
// is the (ns, name) pair:
//   * ns   - the storage system's address, e.g. "kafka://broker:9092",
//            "postgres://db:5432", "s3://bucket", "file".
//   * name - the entity within it, e.g. a topic, a table, an object key,
//            a path.
//   * facets - open, additive metadata (connector family, record format,
//            element-schema hint, delivery mode). New keys never break a
//            consumer.
struct LineageDataset {
    std::string ns;
    std::string name;
    std::map<std::string, std::string> facets;
    // Per-output-column lineage. Populated on SINK datasets only, when the
    // job came from SQL and column lineage was captured. Empty otherwise.
    std::vector<ColumnLineageField> column_lineage;
};

// One connector in the lineage graph - a source or a sink - keyed by the
// graph-local operator id. uid carries the stable identifier when set
// (preferred join key for state / metrics / savepoints).
struct LineageVertex {
    std::string id;       // graph-local operator id
    std::string uid;      // stable uid (may be empty)
    std::string name;     // display name (falls back to op_type)
    std::string op_type;  // connector factory type
    // Sources only: "bounded" | "unbounded" | "unknown". Best-effort,
    // classified from the connector family + mode. Empty on sinks.
    std::string boundedness;
    std::vector<LineageDataset> datasets;
};

// A coarse source-vertex -> sink-vertex dependency: the source's records
// reach the sink through the job DAG. Endpoints reference vertex ids.
struct LineageEdge {
    std::string from;  // source vertex id
    std::string to;    // sink vertex id
};

struct LineageGraph {
    std::vector<LineageVertex> sources;
    std::vector<LineageVertex> sinks;
    std::vector<LineageEdge> edges;

    bool empty() const { return sources.empty() && sinks.empty(); }

    // Canonical JSON:
    //   {"sources":[<vertex>...],"sinks":[<vertex>...],
    //    "edges":[{"from":"<id>","to":"<id>"}...]}
    // where <vertex> is
    //   {"id","uid","name","op_type"[,"boundedness"],
    //    "datasets":[{"namespace","name","facets":{...}}...]}
    std::string to_json() const;
    // Reconstruct from to_json() output. Unknown keys are ignored, so a
    // payload that wraps the graph with extra fields parses cleanly.
    static LineageGraph from_json(std::string_view json_text);
};

// Derive the lineage graph from a submitted job spec. Sources are ops
// with no inputs; sinks are ops nothing reads from; edges are real
// source -> sink reachability over the DAG. Dataset identity is read
// from each op's `type` (connector family) and `params` (locator).
// Pure; no I/O.
LineageGraph extract_lineage(const cluster::JobGraphSpec& spec);

// Map a connector factory type plus its params to one external dataset
// identity. Exposed for testing. `out_channel` is the operator's element
// type, recorded as a coarse schema-hint facet.
LineageDataset dataset_for(const std::string& op_type,
                           const std::map<std::string, std::string>& params,
                           const std::string& out_channel);

// Build a dataset identity from an already-resolved connector family (rather
// than a factory type) plus its option map. dataset_for() is exactly
// connector_family(op_type) fed into this. Exposed so the SQL column-lineage
// capture can compute a source table's (ns, name) the SAME way the lowered-op
// path does, keeping column-lineage input refs aligned with source vertices.
LineageDataset dataset_from_family(const std::string& family,
                                   const std::map<std::string, std::string>& params,
                                   const std::string& out_channel);

// Serialise / parse a column-lineage field list as a JSON array. Shared by
// the lineage model (sink dataset round-trip) and the SQL capture carrier.
std::string column_lineage_to_json(const std::vector<ColumnLineageField>& fields);
std::vector<ColumnLineageField> column_lineage_from_json(std::string_view json_array);

// Parse the connector family out of a factory type string, e.g.
// "kafka_2pc_sink_string" -> "kafka", "s3_parquet_string_source" ->
// "s3_parquet", "postgres_cdc_source" -> "postgres". Exposed for testing.
std::string connector_family(const std::string& op_type);

}  // namespace clink::lineage
