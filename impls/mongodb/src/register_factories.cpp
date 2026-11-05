// MongoDB connector factory registration (mongo_cdc_source, mongo_sink,
// mongo_upsert_sink). Driver-free (the make_* factories that pull mongocxx are
// defined in the per-connector .cpp).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include "clink/connectors/capability.hpp"
#include "clink/mongodb/install.hpp"
#include "clink/mongodb/mongo_cdc_source.hpp"
#include "clink/mongodb/mongo_event.hpp"
#include "clink/mongodb/mongo_sink.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::mongodb {

namespace {

using clink::plugin::BuildContext;

std::int64_t positive(const BuildContext& ctx,
                      const char* factory,
                      const char* key,
                      std::int64_t fallback) {
    const auto v = ctx.param_int64_or(key, fallback);
    if (v < 1) {
        throw std::runtime_error(std::string(factory) + ": '" + key + "' must be at least 1");
    }
    return v;
}

MongoSinkOptions sink_options(const BuildContext& ctx, const char* factory) {
    MongoSinkOptions o;
    o.uri = ctx.param_or("uri", "mongodb://localhost:27017");
    o.database = ctx.param_or("database", "");
    o.collection = ctx.param_or("collection", "");
    o.key_field = ctx.param_or("key_field", "_id");
    o.batch_records = static_cast<std::size_t>(positive(ctx, factory, "batch_records", 1000));
    o.max_bytes = static_cast<std::size_t>(positive(ctx, factory, "max_bytes", 16 * 1024 * 1024));
    const auto linger = ctx.param_int64_or("linger_ms", 0);
    if (linger < 0) {
        throw std::runtime_error(std::string(factory) + ": 'linger_ms' must not be negative");
    }
    o.max_age = std::chrono::milliseconds{linger};
    o.write_timeout = std::chrono::milliseconds{positive(ctx, factory, "write_timeout_ms", 30000)};
    o.name = factory;
    return o;
}

FullDocument full_document_of(const std::string& v) {
    if (v == "updateLookup" || v == "lookup") {
        return FullDocument::UpdateLookup;
    }
    if (v == "whenAvailable") {
        return FullDocument::WhenAvailable;
    }
    if (v == "required") {
        return FullDocument::Required;
    }
    if (v == "off" || v == "false") {
        return FullDocument::Off;
    }
    throw std::runtime_error(
        "mongo_cdc_source: 'full_document' must be 'updateLookup', 'whenAvailable', 'required' "
        "or 'off', not '" +
        v + "'");
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    // One record per mode, as the analyser reads a record by op type: the source,
    // the insert sink, and the sink that replaces by key.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mongo_cdc",
        .version = "1",
        .is_source = true,
        .is_sink = false,
        .build_dependencies = {"mongo-cxx-driver"},
        .runtime_dependencies = {"mongodb >= 4.0.7, a replica set or sharded cluster"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // The checkpoint holds the change-stream resume token, and the stream
        // resumes after it: events after the checkpoint come again.
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::OpaqueToken,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "scram"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"max_await_ms"},
        .available_in_sql = false,  // no connector='mongodb' binding in the SQL planner
        .limitations = {"changes only: no snapshot of what the collection holds when the "
                        "source first opens",
                        "a single reader: only subtask 0 watches",
                        "a resume token the oplog no longer covers cannot resume, and the "
                        "job fails rather than start at the current time",
                        "drop, rename, dropDatabase and invalidate events stop the source",
                        "full_document='updateLookup' reads an update's document after the "
                        "event, so it can carry later changes, or none once deleted",
                        "a document field named __op, __ns, __id or __image stops the source"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mongo",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"mongo-cxx-driver"},
        .runtime_dependencies = {"mongodb >= 4"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        // Every barrier's flush is majority-acknowledged before the checkpoint
        // completes. An insert is at-least-once; mongo_sink's legacy
        // on_duplicate='replace' writes as mongo_upsert_sink does, but this record
        // credits no more than at-least-once for it.
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "scram"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"write_timeout_ms"},
        .available_in_sql = false,
        .limitations = {"a replayed record without an _id is inserted again",
                        "a URI asking for a write concern below w=majority is refused",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "mongo_upsert",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"mongo-cxx-driver"},
        .runtime_dependencies = {"mongodb >= 4"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        // A replay writes each key again, in order, so duplicates collapse onto
        // the key's latest record: the key must be present and of a type that
        // names one value, and a key other than _id must carry a unique index,
        // all of which the sink checks.
        .delivery = clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent,
        .transactional = false,
        .idempotency_key_option = "key_field",
        .auth_methods = {"none", "scram"},
        .tls = true,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"write_timeout_ms"},
        .available_in_sql = false,
        .limitations = {"a record without key_field, or with a key that is not a string, an "
                        "integer or an ObjectId, fails the flush",
                        "a key_field other than _id needs a unique index on it alone",
                        "a key converges to its latest record only within one sink subtask, or "
                        "across subtasks when the stream is partitioned by key_field",
                        "a replay rewrites keys to earlier records before the replay reaches "
                        "their latest ones",
                        "a URI asking for a write concern below w=majority is refused",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });

    // mongo_sink: bulk-write JSON-object records into a collection. At-least-once.
    // Params:
    //   uri (mongodb://localhost:27017), database (required), collection (required)
    //   on_duplicate ('' or 'insert' = insert; 'replace' = replace_one upsert by
    //       key_field, as mongo_upsert_sink writes)
    //   key_field (default "_id"), batch_records (1000), max_bytes (16 MiB),
    //   linger_ms (0), write_timeout_ms (30000)
    reg.register_sink<std::string>(
        "mongo_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            auto o = sink_options(ctx, "mongo_sink");
            const auto mode = ctx.param_or("on_duplicate", "");
            if (mode == "replace") {
                o.upsert = true;
            } else if (!mode.empty() && mode != "insert") {
                throw std::runtime_error(
                    "mongo_sink: 'on_duplicate' must be 'insert' or "
                    "'replace', not '" +
                    mode + "'");
            } else if (!ctx.param_or("key_field", "").empty()) {
                throw std::runtime_error(
                    "mongo_sink: 'key_field' applies only with "
                    "on_duplicate='replace'; an insert matches nothing");
            }
            return make_mongo_sink(o);
        });

    // mongo_upsert_sink: replace_one(filter={key_field: value}).upsert(true) per
    // record, ordered. Effectively once on the key under the conditions in
    // mongo_sink.hpp. Same params as mongo_sink but on_duplicate.
    reg.register_sink<std::string>(
        "mongo_upsert_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            auto o = sink_options(ctx, "mongo_upsert_sink");
            o.upsert = true;
            return make_mongo_sink(o);
        });

    // mongo_cdc_source: change streams. Emits insert/update/replace/delete as a
    // flat JSON object with __op/__ns/__id/__image. At-least-once (resume-token
    // checkpoint). REQUIRES a replica set. Params:
    //   uri, database (watch scope; empty = whole deployment),
    //   collection (empty = the whole database)
    //   full_document ('updateLookup' (default; 'lookup' too), 'whenAvailable',
    //       'required', or 'off' ('false' too))
    //   max_await_ms (default 1000) - change-stream maxAwaitTime / cancel latency
    reg.register_source<std::string>(
        "mongo_cdc_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            MongoCdcOptions o;
            o.uri = ctx.param_or("uri", "mongodb://localhost:27017");
            o.database = ctx.param_or("database", "");
            o.collection = ctx.param_or("collection", "");
            o.full_document = full_document_of(ctx.param_or("full_document", "updateLookup"));
            o.max_await =
                std::chrono::milliseconds{positive(ctx, "mongo_cdc_source", "max_await_ms", 1000)};
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.name = "mongo_cdc_source";
            return make_mongo_cdc_source(o);
        });
}

}  // namespace clink::mongodb
