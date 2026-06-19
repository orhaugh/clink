#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"

namespace clink {

// Per-window state carried by event-time window operators. Holds the
// aggregate plus a `fired` flag so allowed_lateness can distinguish
// the on-time fire from a late-record-driven re-fire.
//
// Lifecycle of a window in operator state:
//
//   * Created in the OPEN state (fired = false) when the first record
//     for that key+window_start arrives.
//   * Stays OPEN until the watermark crosses window_end. At that point
//     the operator emits the aggregate (on-time pane) and flips fired
//     to true.
//   * Subsequent late records arriving within allowed_lateness update
//     the aggregate AND immediately re-emit (late pane).
//   * Once watermark > window_end + allowed_lateness, the entry is
//     purged from state. Late records after that are dropped.
//
template <typename Agg>
struct WindowEntry {
    Agg agg{};
    bool fired{false};
    // Monotonic counter that PaneInfo.pane_index reads from. Survives
    // snapshot/restore because the codec encodes it. Bumped each time
    // the operator emits a pane for this window.
    std::int64_t next_pane_index{0};
};

// Codec for WindowEntry derived from the user's Agg codec. Wire format:
// [u8 fired][i64 next_pane_index, big-endian][agg bytes]. Persistent
// state survives version skews as long as the layout is honoured.
template <typename Agg>
inline Codec<WindowEntry<Agg>> window_entry_codec(Codec<Agg> agg_codec) {
    return Codec<WindowEntry<Agg>>{
        .encode =
            [agg_codec](const WindowEntry<Agg>& we) {
                std::vector<std::byte> out;
                out.reserve(1 + 8);
                out.push_back(static_cast<std::byte>(we.fired ? 1 : 0));
                const auto pi = static_cast<std::uint64_t>(we.next_pane_index);
                for (int i = 7; i >= 0; --i) {
                    out.push_back(static_cast<std::byte>((pi >> (i * 8)) & 0xFF));
                }
                auto agg_bytes = agg_codec.encode(we.agg);
                out.insert(out.end(), agg_bytes.begin(), agg_bytes.end());
                return out;
            },
        .decode = [agg_codec](typename Codec<WindowEntry<Agg>>::BytesView b)
            -> std::optional<WindowEntry<Agg>> {
            if (b.size() < 1 + 8) {
                return std::nullopt;
            }
            WindowEntry<Agg> we;
            we.fired = (static_cast<unsigned char>(b[0]) != 0);
            std::uint64_t pi = 0;
            for (int i = 0; i < 8; ++i) {
                pi = (pi << 8) | static_cast<unsigned char>(b[1 + static_cast<std::size_t>(i)]);
            }
            we.next_pane_index = static_cast<std::int64_t>(pi);
            auto agg = agg_codec.decode(b.subspan(1 + 8));
            if (!agg.has_value()) {
                return std::nullopt;
            }
            we.agg = std::move(*agg);
            return we;
        }};
}

// --- Durable session windows (FOUND-2) ----------------------------------
//
// A session's durable form. The in-memory SessionWindowOperator keeps a sorted
// std::map<session_start, Session> per key; the durable mirror is ONE
// KeyedState row per key holding vector<SessionRow> (so a merge is a single
// atomic put of the whole vector - no multi-row erase/put sequence to tear).
// `start` is carried explicitly because the durable vector is flat (the map key
// is gone); the in-memory map is rebuilt by re-sorting on `start`.
template <typename Agg>
struct SessionRow {
    std::int64_t start{};
    std::int64_t end{};
    bool fired{false};
    Agg agg{};
};

// Wire: [u8 version=1][i64 start BE][i64 end BE][u8 fired][agg bytes]. The agg
// is the remainder (vector_codec frames each element), mirroring
// window_entry_codec.
template <typename Agg>
inline Codec<SessionRow<Agg>> session_row_codec(Codec<Agg> agg_codec) {
    constexpr std::size_t kHeader = 1 + 8 + 8 + 1;
    return Codec<SessionRow<Agg>>{
        .encode =
            [agg_codec](const SessionRow<Agg>& sr) {
                std::vector<std::byte> out;
                out.reserve(kHeader);
                out.push_back(static_cast<std::byte>(1));  // version
                const auto put_be = [&out](std::int64_t v) {
                    const auto u = static_cast<std::uint64_t>(v);
                    for (int i = 7; i >= 0; --i) {
                        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
                    }
                };
                put_be(sr.start);
                put_be(sr.end);
                out.push_back(static_cast<std::byte>(sr.fired ? 1 : 0));
                auto agg_bytes = agg_codec.encode(sr.agg);
                out.insert(out.end(), agg_bytes.begin(), agg_bytes.end());
                return out;
            },
        .decode = [agg_codec](typename Codec<SessionRow<Agg>>::BytesView b)
            -> std::optional<SessionRow<Agg>> {
            if (b.size() < kHeader || static_cast<unsigned char>(b[0]) != 1) {
                return std::nullopt;
            }
            const auto get_be = [&b](std::size_t off) {
                std::uint64_t u = 0;
                for (int i = 0; i < 8; ++i) {
                    u = (u << 8) | static_cast<unsigned char>(b[off + static_cast<std::size_t>(i)]);
                }
                return static_cast<std::int64_t>(u);
            };
            SessionRow<Agg> sr;
            sr.start = get_be(1);
            sr.end = get_be(9);
            sr.fired = (static_cast<unsigned char>(b[17]) != 0);
            auto agg = agg_codec.decode(b.subspan(kHeader));
            if (!agg.has_value()) {
                return std::nullopt;
            }
            sr.agg = std::move(*agg);
            return sr;
        }};
}

// --- Durable evicting windows (FOUND-2) ---------------------------------
//
// Codec for a raw Record<Value> as buffered by the evicting window. Wire:
// [u8 has_event_time][i64 event_time_ms BE if present][value bytes]. PaneInfo is
// deliberately NOT serialised (engine-only; the evictor + process fn read only
// value + event_time). The value is the remainder (vector_codec frames each
// element).
template <typename Value>
inline Codec<Record<Value>> record_codec(Codec<Value> value_codec) {
    return Codec<Record<Value>>{
        .encode =
            [value_codec](const Record<Value>& r) {
                std::vector<std::byte> out;
                const bool has_et = r.event_time().has_value();
                out.push_back(static_cast<std::byte>(has_et ? 1 : 0));
                if (has_et) {
                    const auto u = static_cast<std::uint64_t>(r.event_time()->millis());
                    for (int i = 7; i >= 0; --i) {
                        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
                    }
                }
                auto vb = value_codec.encode(r.value());
                out.insert(out.end(), vb.begin(), vb.end());
                return out;
            },
        .decode = [value_codec](
                      typename Codec<Record<Value>>::BytesView b) -> std::optional<Record<Value>> {
            if (b.empty()) {
                return std::nullopt;
            }
            const bool has_et = (static_cast<unsigned char>(b[0]) != 0);
            std::size_t off = 1;
            std::optional<EventTime> et;
            if (has_et) {
                if (b.size() < 1 + 8) {
                    return std::nullopt;
                }
                std::uint64_t u = 0;
                for (int i = 0; i < 8; ++i) {
                    u = (u << 8) | static_cast<unsigned char>(b[1 + static_cast<std::size_t>(i)]);
                }
                et = EventTime{static_cast<std::int64_t>(u)};
                off = 1 + 8;
            }
            auto v = value_codec.decode(b.subspan(off));
            if (!v.has_value()) {
                return std::nullopt;
            }
            Record<Value> r{std::move(*v)};
            if (et.has_value()) {
                r.set_event_time(*et);
            }
            return r;
        }};
}

// The durable form of an evicting window's per-(window,key) buffer.
template <typename Value>
struct BufferEntry {
    std::vector<Record<Value>> records;
    bool fired{false};
    std::int64_t next_pane_index{0};
};

// Wire: [u8 version=1][u8 fired][i64 next_pane_index BE][vector_codec<Record>
// bytes]. The framed record vector is the remainder.
template <typename Value>
inline Codec<BufferEntry<Value>> buffer_entry_codec(Codec<Value> value_codec) {
    constexpr std::size_t kHeader = 1 + 1 + 8;
    auto vec = vector_codec<Record<Value>>(record_codec<Value>(std::move(value_codec)));
    return Codec<BufferEntry<Value>>{
        .encode =
            [vec](const BufferEntry<Value>& be) {
                std::vector<std::byte> out;
                out.reserve(kHeader);
                out.push_back(static_cast<std::byte>(1));  // version
                out.push_back(static_cast<std::byte>(be.fired ? 1 : 0));
                const auto u = static_cast<std::uint64_t>(be.next_pane_index);
                for (int i = 7; i >= 0; --i) {
                    out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
                }
                auto rb = vec.encode(be.records);
                out.insert(out.end(), rb.begin(), rb.end());
                return out;
            },
        .decode = [vec](typename Codec<BufferEntry<Value>>::BytesView b)
            -> std::optional<BufferEntry<Value>> {
            if (b.size() < kHeader || static_cast<unsigned char>(b[0]) != 1) {
                return std::nullopt;
            }
            BufferEntry<Value> be;
            be.fired = (static_cast<unsigned char>(b[1]) != 0);
            std::uint64_t u = 0;
            for (int i = 0; i < 8; ++i) {
                u = (u << 8) | static_cast<unsigned char>(b[2 + static_cast<std::size_t>(i)]);
            }
            be.next_pane_index = static_cast<std::int64_t>(u);
            auto recs = vec.decode(b.subspan(kHeader));
            if (!recs.has_value()) {
                return std::nullopt;
            }
            be.records = std::move(*recs);
            return be;
        }};
}

}  // namespace clink
