#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace clink::network {

// Wire protocol for clink's TCP transport.
//
// Stream format:
//   [u32 frame_length_be][frame_payload]
// where frame_payload is:
//   [u8 kind][kind-specific bytes]
//
// Kinds:
//   0  Data         - LEGACY (pre-Arrow migration). The sender no
//                      longer emits this; the receiver treats any
//                      arriving Kind::Data as a stale-peer hard
//                      error and closes the connection. The enum
//                      value is reserved so packet captures can
//                      still name the byte. Historical format:
//                      [u32 nrecords][record0][record1]...
//                      each record: [u8 has_time][i64 time_be][u32 value_len][value_bytes]
//   1  Watermark    - [i64 timestamp_be]
//   2  Barrier      - [u64 ck_id_be]
//   3  Close        - empty payload; signals end-of-stream from the sender
//   4  CreditUpdate - [u32 delta_be]; reverse-direction frame (receiver → sender)
//                      that grants `delta` additional records' worth of send
//                      credit. The sender's push pauses on cv when credit
//                      hits 0 and resumes on receipt. Initial credit is
//                      bootstrapped by the receiver sending one
//                      CreditUpdate(INITIAL_CREDIT) right after accept().
//   5  Terminal     - [u64 ck_id_be]; terminal barrier (id is the sentinel
//                      from the source). Same payload shape as Barrier but
//                      flag-distinguished so receivers know to commit
//                      locally on 2PC sinks.
//   6  WatermarkIdle - [i64 timestamp_be]; idle-watermark variant
//                      (withIdleness analogue). The
//                      timestamp records where the assigner was
//                      when it went idle; downstream multi-input
//                      alignment excludes idle inputs from the
//                      running min.
//   7  ArrowBatch   - **The data carrier on the new wire.** Payload
//                      is a complete Arrow IPC stream (schema +
//                      record-batch + EOS) carrying the records of
//                      one Batch<T>. Schema varies by ArrowBatcher<T>:
//                      built-in types get columnar schemas (int64 →
//                      {event_time:int64(null), value:int64}; string
//                      → {event_time:int64(null), value:utf8});
//                      unknown user types get a binary fallback
//                      ({event_time:int64(null), value_bytes:binary})
//                      wrapping the existing per-record Codec<T>
//                      bytes. Either way the wire kind is the same
//                      and the receiver dispatches on the embedded
//                      Arrow schema. Receiver validates the schema
//                      against its registered batcher before
//                      parsing; mismatch → frame rejected.
//
// All multi-byte integers are big-endian on the wire (network byte order),
// except the inner Arrow IPC payload (Kind::ArrowBatch) which uses
// Arrow's own little-endian-default IPC encoding with an embedded
// endianness flag.

enum class Kind : std::uint8_t {
    Data = 0,
    Watermark = 1,
    Barrier = 2,
    Close = 3,
    CreditUpdate = 4,
    Terminal = 5,
    // Idle-watermark variant. Payload: [i64 timestamp_be]. The
    // timestamp records where the assigner was when it went idle
    // (informational); downstream multi-input alignment uses the
    // is_idle flag to skip this input from min-watermark
    // computation. Active watermarks continue to use Kind::Watermark
    // for wire-format backward compatibility - peers that don't know
    // about idle watermarks ignore frames with this kind by default
    // (length-prefixed framing). New peers decode it into a Watermark
    // with idle=true.
    WatermarkIdle = 6,
    // Arrow-IPC-framed data batch. Payload is a complete Arrow IPC
    // stream (schema + record-batch + EOS) carrying the records of
    // one Batch<T>. The schema varies by ArrowBatcher<T>: built-in
    // types get columnar schemas (e.g. int64 → {event_time, value}),
    // unknown types get a binary fallback ({event_time, value_bytes})
    // wrapping the existing per-record Codec<T> bytes. Either way
    // the wire kind is the same and downstream code dispatches on
    // the embedded schema, not on a separate Kind byte.
    //
    // Kind::Data above is kept as dead-code documentation of the
    // historic per-record framing; the runtime stopped emitting it
    // when the wire migrated to Arrow IPC.
    ArrowBatch = 7,
    // Phase 29b: drain marker. Payload:
    //   [u32 subtask_idx_be][u32 target_parallelism_be]
    // Announces "this upstream subtask is winding down; routing for
    // its key-groups is moving to peer subtasks." Downstream
    // operators consume it to know a fresh stream is incoming from
    // the new subtask set. The actual rescale choreography (when to
    // emit drain, when to shut the upstream down) is owned by the
    // JM's RescaleCoordinator (29c/d); this kind is the wire bit.
    Drain = 8,
};

// Sender's starting credit budget. The receiver bootstraps a fresh
// connection with one CreditUpdate(INITIAL_CREDIT) right after accept,
// then tops up incrementally as it consumes batches. INITIAL_CREDIT is
// sized so single-batch jobs and unit tests with no replenishment can
// still finish; tune via JobConfig if a workload genuinely needs less
// memory pressure tolerance.
inline constexpr std::uint32_t kInitialNetworkCredit = 2048;

inline void put_u32_be(std::vector<std::byte>& buf, std::uint32_t v) {
    for (int i = 3; i >= 0; --i) {
        buf.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void put_u64_be(std::vector<std::byte>& buf, std::uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        buf.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void put_i64_be(std::vector<std::byte>& buf, std::int64_t v) {
    put_u64_be(buf, static_cast<std::uint64_t>(v));
}

inline void put_bytes(std::vector<std::byte>& buf, const std::vector<std::byte>& src) {
    buf.insert(buf.end(), src.begin(), src.end());
}

inline std::uint32_t read_u32_be(const std::byte* p) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

inline std::uint64_t read_u64_be(const std::byte* p) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

inline std::int64_t read_i64_be(const std::byte* p) {
    return static_cast<std::int64_t>(read_u64_be(p));
}

}  // namespace clink::network
