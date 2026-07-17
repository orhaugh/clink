// Typed Postgres CDC source helper. Produces a `DataStream<CdcEvent>`
// from a logical replication slot - same wire as
// `postgres_cdc_text_source` but skips the JSON-string round trip so
// downstream operators see the full typed CdcEvent (op/table/lsn/xid +
// CdcFields with per-column type metadata).
//
// The underlying `postgres_cdc_event_source` factory is already
// registered by `clink::postgres::install(reg)`; this helper just
// constructs the matching `SourceDescriptor`. It's a free function
// (not a fluent Builder) so the descriptor stays pure and the helper
// can return a `DataStream<CdcEvent>` directly - sidesteps the
// "builder returns SourceDescriptor but env.source<T>() needs T" hop
// users would otherwise write at every call site.

#pragma once

#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/postgres/cdc_event_codec.hpp"

namespace clink::postgres {

struct CdcEventSourceOptions {
    std::string conninfo;
    std::string slot_name;
    std::string plugin = "test_decoding";
    std::string publication_names;
    bool create_slot{true};
    bool drop_slot_on_close{false};
};

inline clink::api::DataStream<clink::CdcEvent> cdc_event_source(clink::api::Pipeline& env,
                                                                const CdcEventSourceOptions& opts,
                                                                std::string id = {}) {
    clink::api::SourceDescriptor d;
    d.op_type = "postgres_cdc_event_source";
    d.channel_type = std::string{kChannelPostgresCdcEvent};
    d.params["conninfo"] = opts.conninfo;
    d.params["slot_name"] = opts.slot_name;
    if (!opts.plugin.empty()) {
        d.params["plugin"] = opts.plugin;
    }
    if (!opts.publication_names.empty()) {
        d.params["publication_names"] = opts.publication_names;
    }
    d.params["create_slot"] = opts.create_slot ? "true" : "false";
    d.params["drop_slot_on_close"] = opts.drop_slot_on_close ? "true" : "false";
    return env.template source<clink::CdcEvent>(std::move(d), std::move(id));
}

}  // namespace clink::postgres
