#pragma once

// CdcEvent -> flat JSON row (M5), for the Row-path postgres_cdc_source. Pure (no
// libpq) so it is unit-testable without a live replication slot. A data-change
// event (insert/update/delete) becomes one JSON object with the changed columns
// at the top level (NULL cells -> JSON null, from CdcField::is_null) plus CDC
// metadata under reserved __op / __table / __lsn / __xid keys, so json_string_to_
// row maps the data columns by name. Transaction markers (begin/commit/truncate/
// unknown) carry no row to map and return std::nullopt (the caller skips them).

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
    obj["__op"] = clink::config::JsonValue{cdc_op_name(ev.op)};
    obj["__table"] = clink::config::JsonValue{ev.table};
    obj["__lsn"] = clink::config::JsonValue{ev.lsn};
    obj["__xid"] = clink::config::JsonValue{static_cast<std::int64_t>(ev.xid)};
    return clink::config::JsonValue{std::move(obj)}.serialize(0);
}

}  // namespace clink::pgcdc
