#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"
#include "clink/vector_search/install.hpp"
#include "clink/vector_search/knn_index.hpp"
#include "clink/vector_search/vector_search_operator.hpp"

namespace clink::vector_search {

namespace {

std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start <= s.size()) {
        const auto comma = s.find(',', start);
        const auto end = comma == std::string::npos ? s.size() : comma;
        if (end > start) {
            out.push_back(s.substr(start, end - start));
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
}

Metric parse_metric(const std::string& m) {
    if (m == "l2") {
        return Metric::L2;
    }
    if (m == "dot") {
        return Metric::Dot;
    }
    return Metric::Cosine;
}

std::size_t param_size(const clink::plugin::BuildContext& ctx,
                       const std::string& key,
                       std::size_t fallback) {
    const std::string v = ctx.param_or(key, "");
    if (v.empty()) {
        return fallback;
    }
    try {
        return static_cast<std::size_t>(std::stoull(v));
    } catch (...) {
        return fallback;
    }
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    reg.register_operator<clink::sql::Row, clink::sql::Row>(
        "vector_search_row",
        [](const clink::plugin::BuildContext& ctx)
            -> std::shared_ptr<clink::Operator<clink::sql::Row, clink::sql::Row>> {
            VectorSearchOperator::Config cfg;
            cfg.source_factory = ctx.param_or("vector_source_factory", "");
            // Reconstruct the corpus source build params from the namespaced
            // vector_table.* keys the physical planner emitted.
            const std::string prefix = "vector_table.";
            for (const auto& [k, v] : ctx.params) {
                if (k.rfind(prefix, 0) == 0) {
                    cfg.source_params[k.substr(prefix.size())] = v;
                }
            }
            cfg.query_column = ctx.param_or("query_column", "");
            cfg.index_column = ctx.param_or("index_column", "");
            cfg.vector_columns = split_csv(ctx.param_or("vector_columns", ""));
            cfg.score_column = ctx.param_or("score_column", "score");
            cfg.top_k = param_size(ctx, "top_k", 10);
            cfg.index.metric = parse_metric(ctx.param_or("metric", "cosine"));
            cfg.index.kind = ctx.param_or("index", "auto");
            cfg.index.dim = param_size(ctx, "dim", 0);
            cfg.index.m = param_size(ctx, "hnsw_m", 16);
            cfg.index.ef_construction = param_size(ctx, "hnsw_ef_construction", 128);
            cfg.index.ef_search = param_size(ctx, "hnsw_ef_search", 64);
            cfg.index.hnsw_auto_threshold = param_size(ctx, "hnsw_auto_threshold", 50000);
            return std::make_shared<VectorSearchOperator>(std::move(cfg));
        });
}

}  // namespace clink::vector_search
