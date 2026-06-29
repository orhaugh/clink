#pragma once

// Generic columnar ArrowBatcher<T> for user-defined aggregate types.
//
// The built-in batchers in arrow_batcher.hpp (int64_arrow_batcher,
// string_arrow_batcher, ...) are hand-written, one per concrete shape.
// Everything else falls back to make_default_arrow_batcher<T>(codec),
// which encodes the whole record into a single value_bytes:binary
// column - correct, but opaque (no per-column typing, no external
// readability, per-record encode on the hot path).
//
// This header closes that gap WITHOUT hand-writing a batcher per type.
// A user describes a plain struct's fields once:
//
//     struct Trade { std::int64_t id; std::string symbol; double px; };
//     CLINK_ARROW_FIELDS(Trade, id, symbol, px)   // at namespace scope
//
// and make_columnar_arrow_batcher<Trade>() folds over that description
// to synthesise the schema/build/parse closures, emitting one typed
// Arrow column per field:
//
//     {event_time:int64(null), id:int64, symbol:utf8, px:float64}
//
// The resulting ArrowBatcher<Trade> is byte-for-byte a normal batcher
// and plugs straight into the 3-arg registration overload:
//
//     reg.register_typed<Trade>("Trade", trade_codec(),
//                               make_columnar_arrow_batcher<Trade>());
//
// Constraints on T:
//   * default-constructible (parse default-constructs then assigns)
//   * public, assignable data members named in CLINK_ARROW_FIELDS
//   * each named field's type has an ArrowColumnTraits mapping (the
//     fixed-width integer types, float, double, bool, std::string;
//     extend ArrowColumnTraits for more)
//
// Scope of this prototype: the wire/Parquet layout becomes genuinely
// columnar and externally typed. It does NOT auto-vectorise row
// operators - a process_columnar() operator that understands the
// schema is still bespoke. See arrow_batcher.hpp for the built-in
// columnar operators that path is modelled on.
//
// When a shipping compiler gains C++26 static reflection, the only
// piece that changes is the field enumeration: CLINK_ARROW_FIELDS is
// replaced by reflecting over T's members. The ArrowColumnTraits table
// and the generator below stay exactly as they are.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "clink/core/arrow_batcher.hpp"

#ifdef CLINK_HAS_ARROW

namespace clink {

// ---------------------------------------------------------------------
// Field type -> Arrow column mapping
// ---------------------------------------------------------------------
//
// One specialisation per supported C++ field type. Each provides the
// Arrow builder/array pair, the logical DataType, and append/read.
// The primary template is a hard error so an unsupported field type
// fails at the point of use with a readable message instead of deep
// inside the generator.
template <typename F>
struct ArrowColumnTraits {
    static_assert(sizeof(F) == 0,
                  "clink: no Arrow column mapping for this field type. Supported out of the "
                  "box: fixed-width integers (8/16/32/64, signed + unsigned), float, double, "
                  "bool, std::string. Either store the field as one of those, or add an "
                  "ArrowColumnTraits<> specialisation for it.");
};

#define CLINK_DEFINE_ARROW_COLUMN(CppT, BuilderT, ArrayT, DT_FACTORY) \
    template <>                                                       \
    struct ArrowColumnTraits<CppT> {                                  \
        using Builder = BuilderT;                                     \
        using Array = ArrayT;                                         \
        static std::shared_ptr<arrow::DataType> datatype() {          \
            return DT_FACTORY();                                      \
        }                                                             \
        static arrow::Status append(Builder& b, CppT v) {             \
            return b.Append(v);                                       \
        }                                                             \
        static CppT read(const Array& a, std::int64_t i) {            \
            return static_cast<CppT>(a.Value(i));                     \
        }                                                             \
    }

CLINK_DEFINE_ARROW_COLUMN(std::int8_t, arrow::Int8Builder, arrow::Int8Array, arrow::int8);
CLINK_DEFINE_ARROW_COLUMN(std::int16_t, arrow::Int16Builder, arrow::Int16Array, arrow::int16);
CLINK_DEFINE_ARROW_COLUMN(std::int32_t, arrow::Int32Builder, arrow::Int32Array, arrow::int32);
CLINK_DEFINE_ARROW_COLUMN(std::int64_t, arrow::Int64Builder, arrow::Int64Array, arrow::int64);
CLINK_DEFINE_ARROW_COLUMN(std::uint8_t, arrow::UInt8Builder, arrow::UInt8Array, arrow::uint8);
CLINK_DEFINE_ARROW_COLUMN(std::uint16_t, arrow::UInt16Builder, arrow::UInt16Array, arrow::uint16);
CLINK_DEFINE_ARROW_COLUMN(std::uint32_t, arrow::UInt32Builder, arrow::UInt32Array, arrow::uint32);
CLINK_DEFINE_ARROW_COLUMN(std::uint64_t, arrow::UInt64Builder, arrow::UInt64Array, arrow::uint64);
CLINK_DEFINE_ARROW_COLUMN(float, arrow::FloatBuilder, arrow::FloatArray, arrow::float32);
CLINK_DEFINE_ARROW_COLUMN(double, arrow::DoubleBuilder, arrow::DoubleArray, arrow::float64);
CLINK_DEFINE_ARROW_COLUMN(bool, arrow::BooleanBuilder, arrow::BooleanArray, arrow::boolean);

#undef CLINK_DEFINE_ARROW_COLUMN

// std::string maps to utf8; read returns a fresh std::string (the row
// cost the columnar path otherwise avoids), append takes a const ref.
template <>
struct ArrowColumnTraits<std::string> {
    using Builder = arrow::StringBuilder;
    using Array = arrow::StringArray;
    static std::shared_ptr<arrow::DataType> datatype() { return arrow::utf8(); }
    static arrow::Status append(Builder& b, const std::string& v) { return b.Append(v); }
    static std::string read(const Array& a, std::int64_t i) { return a.GetString(i); }
};

// ---------------------------------------------------------------------
// Field descriptor + the per-type field list trait
// ---------------------------------------------------------------------
//
// A descriptor binds a column name to a pointer-to-member. The
// CLINK_ARROW_FIELDS macro specialises ArrowFields<T> to return a
// std::tuple of these.
template <typename T, typename F>
struct ArrowFieldDescriptor {
    using owner_type = T;
    using field_type = F;
    const char* name;
    F T::* member;
};

template <typename T, typename F>
constexpr ArrowFieldDescriptor<T, F> make_arrow_field_descriptor(const char* name, F T::* member) {
    return ArrowFieldDescriptor<T, F>{name, member};
}

// Primary template: T has no field description. Specialised by
// CLINK_ARROW_FIELDS. `registered` gates the generator below.
template <typename T>
struct ArrowFields {
    static constexpr bool registered = false;
};

template <typename T>
concept HasArrowFields = ArrowFields<T>::registered;

namespace detail {

// Build one Arrow column from field `d` across the whole batch.
template <typename T, typename Desc>
inline std::shared_ptr<arrow::Array> build_field_column(const Batch<T>& batch, const Desc& d) {
    using F = typename Desc::field_type;
    using Traits = ArrowColumnTraits<F>;
    typename Traits::Builder b;
    if (auto s = b.Reserve(static_cast<std::int64_t>(batch.size())); !s.ok())
        return nullptr;
    for (const auto& rec : batch) {
        if (auto s = Traits::append(b, rec.value().*(d.member)); !s.ok())
            return nullptr;
    }
    std::shared_ptr<arrow::Array> arr;
    if (auto s = b.Finish(&arr); !s.ok())
        return nullptr;
    return arr;
}

// Read column `col_index` back into field `d` of every row. dynamic_cast
// guards against a mismatched incoming schema (returns false -> parse
// rejects the batch, matching the built-in batchers' contract).
template <typename T, typename Desc>
inline bool read_field_column(const arrow::RecordBatch& batch,
                              int col_index,
                              const Desc& d,
                              std::vector<T>& rows) {
    using F = typename Desc::field_type;
    using Traits = ArrowColumnTraits<F>;
    const auto* col = dynamic_cast<const typename Traits::Array*>(batch.column(col_index).get());
    if (col == nullptr)
        return false;
    for (std::int64_t i = 0; i < batch.num_rows(); ++i) {
        rows[static_cast<std::size_t>(i)].*(d.member) = Traits::read(*col, i);
    }
    return true;
}

}  // namespace detail

// ---------------------------------------------------------------------
// The generator
// ---------------------------------------------------------------------
//
// Synthesises an ArrowBatcher<T> with one typed column per described
// field, plus the shared nullable event_time column at index 0. The
// schema/build/parse closures fold over ArrowFields<T>::descriptors()
// at compile time, so there is no per-type hand-written code.
template <HasArrowFields T>
inline ArrowBatcher<T> make_columnar_arrow_batcher() {
    auto schema_fn = [] {
        const auto descs = ArrowFields<T>::descriptors();
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.push_back(arrow_event_time_field());
        std::apply(
            [&](auto const&... d) {
                (fields.push_back(arrow::field(
                     d.name,
                     ArrowColumnTraits<typename std::decay_t<decltype(d)>::field_type>::datatype(),
                     /*nullable=*/false)),
                 ...);
            },
            descs);
        return arrow::schema(fields);
    };

    auto build = [schema_fn](const Batch<T>& batch) -> std::shared_ptr<arrow::RecordBatch> {
        const auto descs = ArrowFields<T>::descriptors();
        constexpr std::size_t nfields = std::tuple_size_v<std::decay_t<decltype(descs)>>;
        const auto n = static_cast<std::int64_t>(batch.size());

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(nfields + 1);

        arrow::Int64Builder t_b;
        if (auto s = t_b.Reserve(n); !s.ok())
            return nullptr;
        for (const auto& rec : batch) {
            if (auto s = detail::append_event_time(t_b, rec.event_time()); !s.ok())
                return nullptr;
        }
        std::shared_ptr<arrow::Array> t_arr;
        if (auto s = t_b.Finish(&t_arr); !s.ok())
            return nullptr;
        arrays.push_back(std::move(t_arr));

        bool ok = true;
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (
                [&] {
                    if (!ok)
                        return;
                    auto arr = detail::build_field_column<T>(batch, std::get<I>(descs));
                    if (arr == nullptr) {
                        ok = false;
                        return;
                    }
                    arrays.push_back(std::move(arr));
                }(),
                ...);
        }(std::make_index_sequence<nfields>{});
        if (!ok)
            return nullptr;

        return arrow::RecordBatch::Make(schema_fn(), n, arrays);
    };

    auto parse = [](const arrow::RecordBatch& batch) -> std::optional<Batch<T>> {
        const auto descs = ArrowFields<T>::descriptors();
        constexpr std::size_t nfields = std::tuple_size_v<std::decay_t<decltype(descs)>>;
        if (batch.num_columns() < static_cast<int>(nfields) + 1)
            return std::nullopt;
        const auto* t_arr = dynamic_cast<const arrow::Int64Array*>(batch.column(0).get());
        if (t_arr == nullptr)
            return std::nullopt;

        const auto n = batch.num_rows();
        std::vector<T> rows(static_cast<std::size_t>(n));

        bool ok = true;
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (
                [&] {
                    if (!ok)
                        return;
                    if (!detail::read_field_column<T>(
                            batch, static_cast<int>(I) + 1, std::get<I>(descs), rows))
                        ok = false;
                }(),
                ...);
        }(std::make_index_sequence<nfields>{});
        if (!ok)
            return std::nullopt;

        Batch<T> out;
        out.reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i < n; ++i) {
            const auto ts = detail::read_event_time(*t_arr, i);
            if (ts.has_value()) {
                out.emplace(std::move(rows[static_cast<std::size_t>(i)]), *ts);
            } else {
                out.emplace(std::move(rows[static_cast<std::size_t>(i)]));
            }
        }
        return out;
    };

    return ArrowBatcher<T>{std::move(schema_fn), std::move(build), std::move(parse)};
}

// Auto-select the on-wire / Parquet ArrowBatcher for T. When T opted in to
// a typed columnar layout via CLINK_ARROW_FIELDS, return the generated typed
// batcher; otherwise the binary-fallback default. This is the policy the
// codec-only register_* overloads and codec-only channel constructors use,
// so a described struct gets typed Arrow columns through the ordinary API
// with no separate columnar-register call, while undescribed types keep the
// unified Arrow-IPC binary framing. The codec is consumed only by the
// fallback (the typed layout is driven entirely by the field descriptors).
template <typename T>
inline ArrowBatcher<T> make_auto_arrow_batcher(Codec<T> codec) {
    if constexpr (HasArrowFields<T>) {
        (void)codec;
        return make_columnar_arrow_batcher<T>();
    } else {
        return make_default_arrow_batcher<T>(std::move(codec));
    }
}

}  // namespace clink

// ---------------------------------------------------------------------
// CLINK_ARROW_FIELDS - describe a struct's fields once
// ---------------------------------------------------------------------
//
// Usage (at namespace scope, after T is fully defined):
//
//     struct Trade { std::int64_t id; std::string symbol; double px; };
//     CLINK_ARROW_FIELDS(Trade, id, symbol, px)
//
// Expands to an explicit specialisation of clink::ArrowFields<T> whose
// descriptors() returns a tuple of (name, &T::field) pairs. Supports up
// to 16 fields; extend the FE_/PICK lists below for more.

#define CLINK_ARROW_PP_EXPAND(...) __VA_ARGS__

#define CLINK_ARROW_FIELD_ONE(T, field) ::clink::make_arrow_field_descriptor(#field, &T::field)

#define CLINK_ARROW_FE_1(T, a) CLINK_ARROW_FIELD_ONE(T, a)
#define CLINK_ARROW_FE_2(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_1(T, __VA_ARGS__))
#define CLINK_ARROW_FE_3(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_2(T, __VA_ARGS__))
#define CLINK_ARROW_FE_4(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_3(T, __VA_ARGS__))
#define CLINK_ARROW_FE_5(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_4(T, __VA_ARGS__))
#define CLINK_ARROW_FE_6(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_5(T, __VA_ARGS__))
#define CLINK_ARROW_FE_7(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_6(T, __VA_ARGS__))
#define CLINK_ARROW_FE_8(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_7(T, __VA_ARGS__))
#define CLINK_ARROW_FE_9(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_8(T, __VA_ARGS__))
#define CLINK_ARROW_FE_10(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_9(T, __VA_ARGS__))
#define CLINK_ARROW_FE_11(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_10(T, __VA_ARGS__))
#define CLINK_ARROW_FE_12(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_11(T, __VA_ARGS__))
#define CLINK_ARROW_FE_13(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_12(T, __VA_ARGS__))
#define CLINK_ARROW_FE_14(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_13(T, __VA_ARGS__))
#define CLINK_ARROW_FE_15(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_14(T, __VA_ARGS__))
#define CLINK_ARROW_FE_16(T, a, ...) \
    CLINK_ARROW_FIELD_ONE(T, a), CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_15(T, __VA_ARGS__))

#define CLINK_ARROW_FE_PICK(                                                          \
    _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, NAME, ...) \
    NAME

#define CLINK_ARROW_FOR_EACH(T, ...)                             \
    CLINK_ARROW_PP_EXPAND(CLINK_ARROW_FE_PICK(__VA_ARGS__,       \
                                              CLINK_ARROW_FE_16, \
                                              CLINK_ARROW_FE_15, \
                                              CLINK_ARROW_FE_14, \
                                              CLINK_ARROW_FE_13, \
                                              CLINK_ARROW_FE_12, \
                                              CLINK_ARROW_FE_11, \
                                              CLINK_ARROW_FE_10, \
                                              CLINK_ARROW_FE_9,  \
                                              CLINK_ARROW_FE_8,  \
                                              CLINK_ARROW_FE_7,  \
                                              CLINK_ARROW_FE_6,  \
                                              CLINK_ARROW_FE_5,  \
                                              CLINK_ARROW_FE_4,  \
                                              CLINK_ARROW_FE_3,  \
                                              CLINK_ARROW_FE_2,  \
                                              CLINK_ARROW_FE_1)(T, __VA_ARGS__))

#define CLINK_ARROW_FIELDS(T, ...)                                          \
    template <>                                                             \
    struct clink::ArrowFields<T> {                                          \
        static constexpr bool registered = true;                            \
        static auto descriptors() {                                         \
            return ::std::make_tuple(CLINK_ARROW_FOR_EACH(T, __VA_ARGS__)); \
        }                                                                   \
    }

#endif  // CLINK_HAS_ARROW
