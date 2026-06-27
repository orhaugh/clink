#pragma once

// CdcEvent -> flat JSON row (M5), for the Row-path postgres_cdc_source. Pure (no
// libpq) so it is unit-testable without a live replication slot. A data-change
// event (insert/update/delete) becomes one JSON object with the changed columns
// at the top level (NULL cells -> JSON null, from CdcField::is_null) plus CDC
// metadata under reserved __op / __table / __lsn / __xid keys, so json_string_to_
// row maps the data columns by name. Transaction markers (begin/commit/truncate/
// unknown) carry no row to map and return std::nullopt (the caller skips them).
//
// RESERVED-KEY PRECEDENCE: data columns are written first, then the __-prefixed
// metadata is added only if that key is free (no-overwrite emplace). So a
// (pathological) data column literally named __op / __table / __lsn / __xid keeps
// its data value and the corresponding metadata is omitted for that row - data is
// never silently overwritten.
// CAVEAT (inherited from the CDC layer): an unchanged-TOASTed column has an empty
// value with is_null=false, so it emits "" - it cannot be distinguished here from
// a real empty string (see cdc_event.hpp).

#include <cstdint>
#include <optional>
#include <string>

#include "clink/config/json.hpp"
#include "clink/connectors/cdc_event.hpp"

namespace clink::pgcdc {

inline std::string cdc_op_name(CdcEvent::Op op) {
    switch (op) {
        case CdcEvent::Op::Begin:
            return "begin";
        case CdcEvent::Op::Insert:
            return "insert";
        case CdcEvent::Op::Update:
            return "update";
        case CdcEvent::Op::Delete:
            return "delete";
        case CdcEvent::Op::Truncate:
            return "truncate";
        case CdcEvent::Op::Commit:
            return "commit";
        case CdcEvent::Op::Unknown:
            return "unknown";
    }
    return "unknown";
}

inline std::optional<std::string> cdc_event_to_json_row(const CdcEvent& ev) {
    if (ev.op != CdcEvent::Op::Insert && ev.op != CdcEvent::Op::Update &&
        ev.op != CdcEvent::Op::Delete) {
        return std::nullopt;  // transaction marker: no row to map
    }
    clink::config::JsonObject obj;
    for (const auto& f : ev.values) {
        obj[f.name] = f.is_null ? clink::config::JsonValue{} : clink::config::JsonValue{f.value};
    }
    // emplace = no-overwrite: a data column already holding one of these keys wins.
    obj.emplace("__op", clink::config::JsonValue{cdc_op_name(ev.op)});
    obj.emplace("__table", clink::config::JsonValue{ev.table});
    obj.emplace("__lsn", clink::config::JsonValue{ev.lsn});
    obj.emplace("__xid", clink::config::JsonValue{static_cast<std::int64_t>(ev.xid)});
    return clink::config::JsonValue{std::move(obj)}.serialize(0);
}

}  // namespace clink::pgcdc

namespace clink {
// Connector-neutral alias: this CdcEvent -> flat-JSON helper is shared by every
// CDC source (Postgres, MySQL binlog, ...). New callers should use clink::cdc;
// the historical clink::pgcdc name is retained for the Postgres source's
// existing references.
namespace cdc = pgcdc;
}  // namespace clink
