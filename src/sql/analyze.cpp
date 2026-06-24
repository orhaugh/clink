#include "clink/sql/analyze.hpp"

#include <memory>
#include <string>
#include <vector>

#include "clink/cluster/operator_registry.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/parser.hpp"         // TranslationError
#include "clink/sql/physical_plan.hpp"  // ScanSourceSpec, row_scan_source_spec
#include "clink/sql/row.hpp"

namespace clink::sql {

void analyze_table(Catalog& catalog,
                   const std::string& name,
                   const std::vector<std::string>& columns) {
    const TableDef* table = catalog.get_table(name);
    if (table == nullptr) {
        throw TranslationError("ANALYZE: unknown table '" + name + "'", 0);
    }
    // Row source factory + params (throws for connectors that are not a direct
    // bounded Row source - e.g. a Kafka string stream needing a bridge).
    const ScanSourceSpec spec = row_scan_source_spec(*table);

    auto& registry = cluster::OperatorRegistry::default_instance();
    const auto* sf = registry.find_source(spec.type, std::string{"row"});
    if (sf == nullptr) {
        throw TranslationError("ANALYZE: source factory '" + spec.type +
                                   "' is not registered (is the connector implementation linked?)",
                               0);
    }
    cluster::OperatorBuildContext bctx;
    bctx.params = spec.params;
    auto source = std::static_pointer_cast<Source<Row>>(sf->build(bctx));
    if (!source) {
        throw TranslationError("ANALYZE: '" + spec.type + "' did not build a Row source", 0);
    }
    if (!source->is_bounded()) {
        throw TranslationError(
            "ANALYZE requires a bounded table; '" + name + "' is an unbounded stream", 0);
    }

    std::vector<std::string> cols = columns;
    if (cols.empty()) {
        cols.reserve(table->columns.size());
        for (const auto& c : table->columns) {
            cols.push_back(c.name);
        }
    }
    StatsCollector collector(cols);

    // A one-shot in-process scan: the bounded Row source feeds a sink that folds
    // each row into the collector. run() returns when the source exhausts and the
    // channels close.
    Dag dag;
    auto handle = dag.add_source<Row>(source);
    auto sink = std::make_shared<FunctionSink<Row>>(
        [&collector](const Row& r) { collector.observe(r); }, "analyze_stats_sink");
    dag.add_sink<Row>(handle, sink);

    JobConfig cfg;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    for (const auto& [op, msg] : exec.operator_errors()) {
        throw TranslationError("ANALYZE scan of '" + name + "' failed in " + op + ": " + msg, 0);
    }

    catalog.merge_table_stats(name, collector.to_with_options());
}

}  // namespace clink::sql
