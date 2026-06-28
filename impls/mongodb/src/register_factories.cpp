// MongoDB connector factory registration (mongo_cdc_source, mongo_sink). Driver-free
// (the make_* factories that pull mongocxx are defined in the per-connector .cpp).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/mongodb/install.hpp"
#include "clink/mongodb/mongo_cdc_source.hpp"
#include "clink/mongodb/mongo_sink.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::mongodb {

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // mongo_sink: bulk-write JSON-object records into a collection. At-least-once
    // (effectively-once with on_duplicate='replace' + a stable key). Params:
    //   uri (mongodb://localhost:27017), database (required), collection (required)
    //   on_duplicate ('replace' -> replace_one upsert by key_field; default = insert)
    //   key_field (default "_id"), batch_records (1000), linger_ms (0)
    reg.register_sink<std::string>(
        "mongo_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            MongoSinkOptions o;
            o.uri = ctx.param_or("uri", "mongodb://localhost:27017");
            o.database = ctx.param_or("database", "");
            o.collection = ctx.param_or("collection", "");
            o.upsert = ctx.param_or("on_duplicate", "") == "replace";
            o.key_field = ctx.param_or("key_field", "_id");
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "mongo_sink";
            return make_mongo_sink(o);
        });

    // mongo_cdc_source: change-streams CDC. Emits insert/update/replace/delete as a
    // flat JSON object with __op/__ns/__id. At-least-once (resume-token checkpoint).
    // REQUIRES a replica set. Params:
    //   uri, database (watch scope; empty = whole deployment),
    //   collection (empty = the whole database)
    //   full_document ('false' -> updates carry only updatedFields; default = lookup
    //       so updates carry the full post-image)
    //   max_await_ms (default 1000) - change-stream maxAwaitTime / cancel latency
    reg.register_source<std::string>(
        "mongo_cdc_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            MongoCdcOptions o;
            o.uri = ctx.param_or("uri", "mongodb://localhost:27017");
            o.database = ctx.param_or("database", "");
            o.collection = ctx.param_or("collection", "");
            o.full_document_lookup = ctx.param_or("full_document", "lookup") != "false";
            o.max_await = std::chrono::milliseconds{ctx.param_int64_or("max_await_ms", 1000)};
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = "mongo_cdc_source";
            return make_mongo_cdc_source(o);
        });
}

}  // namespace clink::mongodb
