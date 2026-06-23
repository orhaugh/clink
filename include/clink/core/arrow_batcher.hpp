#pragma once

// ArrowBatcher<T> - the wire-format seam for Batch<T>.
//
// Built atop Apache Arrow's RecordBatch + IPC, ArrowBatcher converts
// `std::vector<Record<T>>` to and from a `shared_ptr<arrow::RecordBatch>`
// with a documented schema. The NetworkChannelSink emits Arrow IPC
// stream bytes for every data frame; the NetworkChannelSource decodes
// them. There is one wire format for all data - `Kind::ArrowBatch` -
// whether T has a specialised columnar batcher or falls back to
// `make_default_arrow_batcher<T>(Codec<T>)`.
//
// Construction:
//   * For arbitrary T, call `make_default_arrow_batcher<T>(codec)` -
//     the resulting batcher uses a 2-column schema {event_time:int64
//     (nullable), value_bytes:binary} wrapping the existing per-record
//     Codec<T> output. No columnar win, but every type rides Arrow
//     framing automatically.
//   * For built-in types, the same header provides specialised
//     factories - `int64_arrow_batcher()`, `string_arrow_batcher()`,
//     etc. - that emit rich columnar schemas (e.g. {event_time:int64
//     (nullable), value:int64}). These produce ~5-9× wire-throughput
//     wins per the project_arrow_perf_baseline bench.
//
// Implementation note: like Codec<T>, ArrowBatcher<T> is a pair of
// std::function closures so call sites can supply lambdas without
// writing class hierarchies. The closures are stored on TypeRegistry
// alongside Codec<T> and threaded through bridge construction.

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#ifdef CLINK_HAS_ARROW
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#endif

#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"

namespace clink {

#ifdef CLINK_HAS_ARROW

// Build/parse an Arrow RecordBatch of Record<T>s. Schema is supplied
// by the implementation; the wire format is the Arrow IPC stream
// bytes serialised from the produced batch.
template <typename T>
struct ArrowBatcher {
    using BuildFn = std::function<std::shared_ptr<arrow::RecordBatch>(const Batch<T>&)>;
    using ParseFn = std::function<std::optional<Batch<T>>(const arrow::RecordBatch&)>;
    using SchemaFn = std::function<std::shared_ptr<arrow::Schema>()>;

    SchemaFn schema;
    BuildFn build;
    ParseFn parse;
};

// Helpers for the event-time column shared by all built-in batchers.
// We represent event-time as a single nullable int64 column. Null
// means "no event-time" (the historic has_time=false case).
inline std::shared_ptr<arrow::Field> arrow_event_time_field() {
    return arrow::field("event_time", arrow::int64(), /*nullable=*/true);
}

namespace detail {

// Build (event-time array, value array(s)) helpers. These are factored
// out so every built-in batcher gets identical event-time handling.
inline arrow::Status append_event_time(arrow::Int64Builder& b, const std::optional<EventTime>& t) {
    if (t.has_value()) {
        return b.Append(t->millis());
    }
    return b.AppendNull();
}

inline std::optional<EventTime> read_event_time(const arrow::Int64Array& arr, int64_t i) {
    if (arr.IsNull(i))
        return std::nullopt;
    return EventTime{arr.Value(i)};
}

}  // namespace detail

// Universal fallback. Schema is {event_time:int64(null),
// value_bytes:binary}. value_bytes column carries the per-record
// Codec<T>::encode output verbatim. parse() decodes it via
// Codec<T>::decode. No columnar win but the wire framing is unified
// Arrow IPC.
template <typename T>
inline ArrowBatcher<T> make_default_arrow_batcher(Codec<T> codec) {
    auto schema_fn = [] {
        return arrow::schema({
            arrow_event_time_field(),
            arrow::field("value_bytes", arrow::binary(), /*nullable=*/false),
        });
    };
    auto build = [codec, schema_fn](const Batch<T>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        arrow::Int64Builder t_b;
        arrow::BinaryBuilder v_b;
        const auto n = static_cast<int64_t>(batch.size());
        if (auto s = t_b.Reserve(n); !s.ok())
            return nullptr;
        if (auto s = v_b.Reserve(n); !s.ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (auto s = detail::append_event_time(t_b, rec.event_time()); !s.ok())
                return nullptr;
            auto enc = codec.encode(rec.value());
            if (auto s = v_b.Append(reinterpret_cast<const uint8_t*>(enc.data()),
                                    static_cast<int32_t>(enc.size()));
                !s.ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (auto s = t_b.Finish(&t_arr); !s.ok())
            return nullptr;
        if (auto s = v_b.Finish(&v_arr); !s.ok())
            return nullptr;
        return arrow::RecordBatch::Make(schema_fn(), n, {t_arr, v_arr});
    };
    auto parse = [codec](const arrow::RecordBatch& batch) -> std::optional<Batch<T>> {
        if (batch.num_columns() < 2)
            return std::nullopt;
        const auto* t_arr = static_cast<const arrow::Int64Array*>(batch.column(0).get());
        const auto* v_arr = static_cast<const arrow::BinaryArray*>(batch.column(1).get());
        Batch<T> out;
        out.reserve(static_cast<std::size_t>(batch.num_rows()));
        for (int64_t i = 0; i < batch.num_rows(); ++i) {
            int32_t vlen = 0;
            const uint8_t* vptr = v_arr->GetValue(i, &vlen);
            auto val = codec.decode(std::span<const std::byte>{
                reinterpret_cast<const std::byte*>(vptr), static_cast<std::size_t>(vlen)});
            if (!val.has_value())
                return std::nullopt;
            const auto ts = detail::read_event_time(*t_arr, i);
            if (ts.has_value()) {
                out.emplace(std::move(*val), *ts);
            } else {
                out.emplace(std::move(*val));
            }
        }
        return out;
    };
    return ArrowBatcher<T>{
        .schema = std::move(schema_fn),
        .build = std::move(build),
        .parse = std::move(parse),
    };
}

// ---------------------------------------------------------------------
// Specialised columnar batchers for built-in types
// ---------------------------------------------------------------------

// int64 - schema {event_time:int64(null), value:int64}.
inline ArrowBatcher<std::int64_t> int64_arrow_batcher() {
    auto schema_fn = [] {
        return arrow::schema({
            arrow_event_time_field(),
            arrow::field("value", arrow::int64(), /*nullable=*/false),
        });
    };
    auto build =
        [schema_fn](const Batch<std::int64_t>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        arrow::Int64Builder t_b;
        arrow::Int64Builder v_b;
        const auto n = static_cast<int64_t>(batch.size());
        if (auto s = t_b.Reserve(n); !s.ok())
            return nullptr;
        if (auto s = v_b.Reserve(n); !s.ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (auto s = detail::append_event_time(t_b, rec.event_time()); !s.ok())
                return nullptr;
            if (auto s = v_b.Append(rec.value()); !s.ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (auto s = t_b.Finish(&t_arr); !s.ok())
            return nullptr;
        if (auto s = v_b.Finish(&v_arr); !s.ok())
            return nullptr;
        return arrow::RecordBatch::Make(schema_fn(), n, {t_arr, v_arr});
    };
    auto parse = [](const arrow::RecordBatch& batch) -> std::optional<Batch<std::int64_t>> {
        if (batch.num_columns() < 2)
            return std::nullopt;
        const auto* t_arr = static_cast<const arrow::Int64Array*>(batch.column(0).get());
        const auto* v_arr = static_cast<const arrow::Int64Array*>(batch.column(1).get());
        Batch<std::int64_t> out;
        out.reserve(static_cast<std::size_t>(batch.num_rows()));
        for (int64_t i = 0; i < batch.num_rows(); ++i) {
            const auto val = v_arr->Value(i);
            const auto ts = detail::read_event_time(*t_arr, i);
            if (ts.has_value()) {
                out.emplace(val, *ts);
            } else {
                out.emplace(val);
            }
        }
        return out;
    };
    return ArrowBatcher<std::int64_t>{schema_fn, build, parse};
}

// int64 keyed - schema {event_time:int64(null), key:int64, value:int64}.
// Backs the columnar keyed-aggregation path (increment 2): the key column
// drives grouping and the value column is aggregated, both read straight from
// the Arrow buffers with no row materialization. parse rebuilds the row form
// (Batch<pair<key,value>>) for a row-only downstream / lazy materialization.
inline ArrowBatcher<std::pair<std::int64_t, std::int64_t>> int64_keyed_arrow_batcher() {
    using KV = std::pair<std::int64_t, std::int64_t>;
    auto schema_fn = [] {
        return arrow::schema({
            arrow_event_time_field(),
            arrow::field("key", arrow::int64(), /*nullable=*/false),
            arrow::field("value", arrow::int64(), /*nullable=*/false),
        });
    };
    auto build = [schema_fn](const Batch<KV>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        arrow::Int64Builder t_b;
        arrow::Int64Builder k_b;
        arrow::Int64Builder v_b;
        const auto n = static_cast<int64_t>(batch.size());
        if (!t_b.Reserve(n).ok() || !k_b.Reserve(n).ok() || !v_b.Reserve(n).ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (!detail::append_event_time(t_b, rec.event_time()).ok())
                return nullptr;
            if (!k_b.Append(rec.value().first).ok())
                return nullptr;
            if (!v_b.Append(rec.value().second).ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        std::shared_ptr<arrow::Array> k_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (!t_b.Finish(&t_arr).ok() || !k_b.Finish(&k_arr).ok() || !v_b.Finish(&v_arr).ok())
            return nullptr;
        return arrow::RecordBatch::Make(schema_fn(), n, {t_arr, k_arr, v_arr});
    };
    auto parse = [](const arrow::RecordBatch& batch) -> std::optional<Batch<KV>> {
        if (batch.num_columns() < 3)
            return std::nullopt;
        const auto* t_arr = static_cast<const arrow::Int64Array*>(batch.column(0).get());
        const auto* k_arr = static_cast<const arrow::Int64Array*>(batch.column(1).get());
        const auto* v_arr = static_cast<const arrow::Int64Array*>(batch.column(2).get());
        Batch<KV> out;
        out.reserve(static_cast<std::size_t>(batch.num_rows()));
        for (int64_t i = 0; i < batch.num_rows(); ++i) {
            const KV kv{k_arr->Value(i), v_arr->Value(i)};
            const auto ts = detail::read_event_time(*t_arr, i);
            if (ts.has_value()) {
                out.emplace(kv, *ts);
            } else {
                out.emplace(kv);
            }
        }
        return out;
    };
    return ArrowBatcher<KV>{schema_fn, build, parse};
}

// string-keyed - schema {event_time:int64(null), key:utf8, value:int64}.
// Backs the columnar string-keyed aggregation path (increment 7b): the utf8 key
// column drives grouping and the int64 value column is aggregated. parse rebuilds
// the row form (Batch<pair<string,int64>>) for a row-only downstream / lazy
// materialization - constructing a std::string per row, the cost the columnar
// path avoids via string_view lookups.
inline ArrowBatcher<std::pair<std::string, std::int64_t>> string_keyed_arrow_batcher() {
    using KV = std::pair<std::string, std::int64_t>;
    auto schema_fn = [] {
        return arrow::schema({
            arrow_event_time_field(),
            arrow::field("key", arrow::utf8(), /*nullable=*/false),
            arrow::field("value", arrow::int64(), /*nullable=*/false),
        });
    };
    auto build = [schema_fn](const Batch<KV>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        arrow::Int64Builder t_b;
        arrow::StringBuilder k_b;
        arrow::Int64Builder v_b;
        const auto n = static_cast<int64_t>(batch.size());
        if (!t_b.Reserve(n).ok() || !k_b.Reserve(n).ok() || !v_b.Reserve(n).ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (!detail::append_event_time(t_b, rec.event_time()).ok())
                return nullptr;
            if (!k_b.Append(rec.value().first).ok())
                return nullptr;
            if (!v_b.Append(rec.value().second).ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        std::shared_ptr<arrow::Array> k_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (!t_b.Finish(&t_arr).ok() || !k_b.Finish(&k_arr).ok() || !v_b.Finish(&v_arr).ok())
            return nullptr;
        return arrow::RecordBatch::Make(schema_fn(), n, {t_arr, k_arr, v_arr});
    };
    auto parse = [](const arrow::RecordBatch& batch) -> std::optional<Batch<KV>> {
        if (batch.num_columns() < 3)
            return std::nullopt;
        const auto* t_arr = static_cast<const arrow::Int64Array*>(batch.column(0).get());
        const auto* k_arr = static_cast<const arrow::StringArray*>(batch.column(1).get());
        const auto* v_arr = static_cast<const arrow::Int64Array*>(batch.column(2).get());
        Batch<KV> out;
        out.reserve(static_cast<std::size_t>(batch.num_rows()));
        for (int64_t i = 0; i < batch.num_rows(); ++i) {
            KV kv{std::string(k_arr->GetView(i)), v_arr->Value(i)};
            const auto ts = detail::read_event_time(*t_arr, i);
            if (ts.has_value()) {
                out.emplace(std::move(kv), *ts);
            } else {
                out.emplace(std::move(kv));
            }
        }
        return out;
    };
    return ArrowBatcher<KV>{schema_fn, build, parse};
}

namespace detail {

// Generic primitive batcher template - instantiated for int32, uint32,
// uint64, etc. by the typed factories below. Builder/array types are
// supplied as template parameters.
template <typename T, typename ArrowType, typename Builder, typename Array>
inline ArrowBatcher<T> primitive_arrow_batcher(std::shared_ptr<arrow::DataType> dtype) {
    auto schema_fn = [dtype] {
        return arrow::schema({
            arrow_event_time_field(),
            arrow::field("value", dtype, /*nullable=*/false),
        });
    };
    auto build = [schema_fn](const Batch<T>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        arrow::Int64Builder t_b;
        Builder v_b;
        const auto n = static_cast<int64_t>(batch.size());
        if (auto s = t_b.Reserve(n); !s.ok())
            return nullptr;
        if (auto s = v_b.Reserve(n); !s.ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (auto s = append_event_time(t_b, rec.event_time()); !s.ok())
                return nullptr;
            if (auto s = v_b.Append(rec.value()); !s.ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (auto s = t_b.Finish(&t_arr); !s.ok())
            return nullptr;
        if (auto s = v_b.Finish(&v_arr); !s.ok())
            return nullptr;
        return arrow::RecordBatch::Make(schema_fn(), n, {t_arr, v_arr});
    };
    auto parse = [](const arrow::RecordBatch& batch) -> std::optional<Batch<T>> {
        if (batch.num_columns() < 2)
            return std::nullopt;
        const auto* t_arr = static_cast<const arrow::Int64Array*>(batch.column(0).get());
        const auto* v_arr = static_cast<const Array*>(batch.column(1).get());
        Batch<T> out;
        out.reserve(static_cast<std::size_t>(batch.num_rows()));
        for (int64_t i = 0; i < batch.num_rows(); ++i) {
            const auto val = static_cast<T>(v_arr->Value(i));
            const auto ts = read_event_time(*t_arr, i);
            if (ts.has_value()) {
                out.emplace(val, *ts);
            } else {
                out.emplace(val);
            }
        }
        return out;
    };
    (void)dtype;  // captured by schema_fn
    return ArrowBatcher<T>{schema_fn, build, parse};
}

}  // namespace detail

inline ArrowBatcher<std::int32_t> int32_arrow_batcher() {
    return detail::primitive_arrow_batcher<std::int32_t,
                                           arrow::Int32Type,
                                           arrow::Int32Builder,
                                           arrow::Int32Array>(arrow::int32());
}

inline ArrowBatcher<std::uint32_t> uint32_arrow_batcher() {
    return detail::primitive_arrow_batcher<std::uint32_t,
                                           arrow::UInt32Type,
                                           arrow::UInt32Builder,
                                           arrow::UInt32Array>(arrow::uint32());
}

inline ArrowBatcher<std::uint64_t> uint64_arrow_batcher() {
    return detail::primitive_arrow_batcher<std::uint64_t,
                                           arrow::UInt64Type,
                                           arrow::UInt64Builder,
                                           arrow::UInt64Array>(arrow::uint64());
}

// string - schema {event_time:int64(null), value:utf8}.
inline ArrowBatcher<std::string> string_arrow_batcher() {
    auto schema_fn = [] {
        return arrow::schema({
            arrow_event_time_field(),
            arrow::field("value", arrow::utf8(), /*nullable=*/false),
        });
    };
    auto build =
        [schema_fn](const Batch<std::string>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        arrow::Int64Builder t_b;
        arrow::StringBuilder v_b;
        const auto n = static_cast<int64_t>(batch.size());
        if (auto s = t_b.Reserve(n); !s.ok())
            return nullptr;
        if (auto s = v_b.Reserve(n); !s.ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (auto s = detail::append_event_time(t_b, rec.event_time()); !s.ok())
                return nullptr;
            if (auto s = v_b.Append(rec.value()); !s.ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        std::shared_ptr<arrow::Array> v_arr;
        if (auto s = t_b.Finish(&t_arr); !s.ok())
            return nullptr;
        if (auto s = v_b.Finish(&v_arr); !s.ok())
            return nullptr;
        return arrow::RecordBatch::Make(schema_fn(), n, {t_arr, v_arr});
    };
    auto parse = [](const arrow::RecordBatch& batch) -> std::optional<Batch<std::string>> {
        if (batch.num_columns() < 2)
            return std::nullopt;
        const auto* t_arr = static_cast<const arrow::Int64Array*>(batch.column(0).get());
        const auto* v_arr = static_cast<const arrow::StringArray*>(batch.column(1).get());
        Batch<std::string> out;
        out.reserve(static_cast<std::size_t>(batch.num_rows()));
        for (int64_t i = 0; i < batch.num_rows(); ++i) {
            int32_t vlen = 0;
            const uint8_t* vptr = v_arr->GetValue(i, &vlen);
            std::string s(reinterpret_cast<const char*>(vptr), static_cast<std::size_t>(vlen));
            const auto ts = detail::read_event_time(*t_arr, i);
            if (ts.has_value()) {
                out.emplace(std::move(s), *ts);
            } else {
                out.emplace(std::move(s));
            }
        }
        return out;
    };
    return ArrowBatcher<std::string>{schema_fn, build, parse};
}

// ---------------------------------------------------------------------
// IPC serialise / deserialise - used by NetworkChannel and tests
// ---------------------------------------------------------------------

// Serialise a single RecordBatch to an Arrow IPC stream byte buffer.
// Returns empty vector on failure (callers should propagate).
inline std::vector<std::byte> arrow_batch_to_ipc(const arrow::RecordBatch& batch) {
    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok())
        return {};
    const auto& sink = *sink_result;
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, batch.schema());
    if (!writer_result.ok())
        return {};
    const auto& writer = *writer_result;
    if (auto s = writer->WriteRecordBatch(batch); !s.ok())
        return {};
    if (auto s = writer->Close(); !s.ok())
        return {};
    auto buf_result = sink->Finish();
    if (!buf_result.ok())
        return {};
    const auto& buf = *buf_result;
    std::vector<std::byte> out(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(out.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    return out;
}

// Read a single RecordBatch from an Arrow IPC stream blob.
//
// The bytes are copied into an Arrow-OWNED buffer first: the decoded
// RecordBatch's column arrays are zero-copy slices of the reader's buffer, so a
// consumer that RETAINS the batch (the sidecar-preserving Row wire keeps the
// columnar batch lazily downstream) must not depend on the caller's transient
// wire bytes outliving it. The copy is one memcpy per frame (a whole batch), so
// it is amortized; eager-decode batchers that copy values out during parse are
// unaffected by the extra owned buffer.
inline std::shared_ptr<arrow::RecordBatch> arrow_batch_from_ipc(const std::byte* data,
                                                                std::size_t size) {
    if (size == 0)
        return nullptr;
    auto buf_result = arrow::AllocateBuffer(static_cast<int64_t>(size));
    if (!buf_result.ok())
        return nullptr;
    std::shared_ptr<arrow::Buffer> buffer = std::move(*buf_result);
    std::memcpy(buffer->mutable_data(), data, size);
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
    if (!reader_result.ok())
        return nullptr;
    const auto& reader = *reader_result;
    std::shared_ptr<arrow::RecordBatch> batch;
    if (auto s = reader->ReadNext(&batch); !s.ok())
        return nullptr;
    return batch;
}

#endif  // CLINK_HAS_ARROW

}  // namespace clink
