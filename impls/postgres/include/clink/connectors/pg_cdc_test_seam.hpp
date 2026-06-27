#pragma once

// Test-only seam for the pgoutput decoder.
//
// PgOutputState (the pgoutput binary-protocol decoder) lives in an anonymous
// namespace inside postgres_cdc_source.cpp, so it cannot be unit-tested directly.
// This seam exposes ONE narrow entry point - feed raw pgoutput message payloads
// (the bytes AFTER the XLogData header, i.e. exactly what PgOutputState sees) to
// a fresh decoder and report the decoded change events plus how many I/U/D events
// were DROPPED as undecodable (unknown relation / truncated tuple).
//
// This lets the F1 silent-data-loss accounting (the drop counter + the truncation
// guard, including the UPDATE old-image case) be tested deterministically without
// a live Postgres. Compiled into clink::postgres only when libpq is available
// (CLINK_HAS_POSTGRES); not part of the production API surface.

#include <cstdint>
#include <string>
#include <vector>

#include "clink/connectors/cdc_event.hpp"

namespace clink::pg_cdc_testing {

struct PgDecodeResult {
    std::vector<CdcEvent> events;  // events the decoder emitted (nullopt skipped)
    std::uint64_t dropped{0};      // I/U/D change events dropped as undecodable
};

// Decode a sequence of raw pgoutput message payloads through one decoder
// instance (so a Relation message can register a relation that a later
// Insert/Update/Delete references). Each element is one message payload.
PgDecodeResult decode_pgoutput_messages(const std::vector<std::string>& messages);

}  // namespace clink::pg_cdc_testing
