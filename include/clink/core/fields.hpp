#pragma once

// Field metadata for user-defined record types: the one declaration the
// engine derives everything else from.
//
// A descriptor binds a field name to a pointer-to-member; the
// CLINK_ARROW_FIELDS macro specialises ArrowFields<T> to return a tuple
// of them, and HasArrowFields gates every generator that consumes the
// description (the columnar batcher in columnar_batcher.hpp today;
// design record 009 adds the derived codec, registration defaults and
// the shape fingerprint).
//
// Nothing here needs Arrow - this header is the Arrow-free seam split
// out of columnar_batcher.hpp so consumers that only want the metadata
// (a codec, a fingerprint) do not drag arrow/api.h into their TU. The
// names keep their historical spellings; columnar_batcher.hpp includes
// this header, so every existing include site is unaffected.
//
// When a shipping compiler gains C++26 static reflection, a reflecting
// frontend populates this same trait and the macro becomes optional;
// nothing downstream changes.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

namespace clink {

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

}  // namespace clink

namespace clink {

namespace fields_detail {

template <typename>
struct fp_is_vector : std::false_type {};
template <typename E, typename A>
struct fp_is_vector<std::vector<E, A>> : std::true_type {};
template <typename>
struct fp_is_optional : std::false_type {};
template <typename E>
struct fp_is_optional<std::optional<E>> : std::true_type {};
template <typename>
struct fp_is_map : std::false_type {};
template <typename K, typename V, typename C, typename A>
struct fp_is_map<std::map<K, V, C, A>> : std::true_type {};

// The FROZEN kind-tag table. These bytes are hashed into every shape
// fingerprint that snapshots persist, so a value here may NEVER change
// or be reused; new kinds append new values. The golden static_asserts
// in tests/test_state_fingerprint_gate.cpp pin the derivation.
enum class FpKind : std::uint8_t {
    kI8 = 0x01,
    kI16 = 0x02,
    kI32 = 0x03,
    kI64 = 0x04,
    kU8 = 0x05,
    kU16 = 0x06,
    kU32 = 0x07,
    kU64 = 0x08,
    kF32 = 0x09,
    kF64 = 0x0A,
    kBool = 0x0B,
    kString = 0x0C,
    kOptional = 0x0D,
    kVector = 0x0E,
    kMap = 0x0F,
    kNestedBegin = 0x10,
    kNestedEnd = 0x11,
};

constexpr std::uint64_t kFpOffset = 14695981039346656037ULL;  // FNV-1a 64
constexpr std::uint64_t kFpPrime = 1099511628211ULL;

constexpr std::uint64_t fp_byte(std::uint64_t h, std::uint8_t b) {
    return (h ^ b) * kFpPrime;
}
constexpr std::uint64_t fp_cstr(std::uint64_t h, const char* s) {
    for (; *s != '\0'; ++s) {
        h = fp_byte(h, static_cast<std::uint8_t>(*s));
    }
    return fp_byte(h, 0);  // terminator, so "ab"+"c" never equals "a"+"bc"
}

template <typename T>
constexpr std::uint64_t fp_record(std::uint64_t h);

template <typename F>
constexpr std::uint64_t fp_kind(std::uint64_t h) {
    if constexpr (std::is_same_v<F, bool>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kBool));
    } else if constexpr (std::is_same_v<F, std::int8_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kI8));
    } else if constexpr (std::is_same_v<F, std::int16_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kI16));
    } else if constexpr (std::is_same_v<F, std::int32_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kI32));
    } else if constexpr (std::is_same_v<F, std::int64_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kI64));
    } else if constexpr (std::is_same_v<F, std::uint8_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kU8));
    } else if constexpr (std::is_same_v<F, std::uint16_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kU16));
    } else if constexpr (std::is_same_v<F, std::uint32_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kU32));
    } else if constexpr (std::is_same_v<F, std::uint64_t>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kU64));
    } else if constexpr (std::is_same_v<F, float>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kF32));
    } else if constexpr (std::is_same_v<F, double>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kF64));
    } else if constexpr (std::is_same_v<F, std::string>) {
        return fp_byte(h, static_cast<std::uint8_t>(FpKind::kString));
    } else if constexpr (fp_is_optional<F>::value) {
        return fp_kind<typename F::value_type>(
            fp_byte(h, static_cast<std::uint8_t>(FpKind::kOptional)));
    } else if constexpr (fp_is_vector<F>::value) {
        return fp_kind<typename F::value_type>(
            fp_byte(h, static_cast<std::uint8_t>(FpKind::kVector)));
    } else if constexpr (fp_is_map<F>::value) {
        return fp_kind<typename F::mapped_type>(
            fp_kind<typename F::key_type>(fp_byte(h, static_cast<std::uint8_t>(FpKind::kMap))));
    } else if constexpr (HasArrowFields<F>) {
        return fp_byte(fp_record<F>(fp_byte(h, static_cast<std::uint8_t>(FpKind::kNestedBegin))),
                       static_cast<std::uint8_t>(FpKind::kNestedEnd));
    } else {
        static_assert(sizeof(F) == 0,
                      "clink: no shape-fingerprint mapping for this field type (the supported "
                      "set matches the derived codec's)");
        return h;
    }
}

template <typename T>
constexpr std::uint64_t fp_record(std::uint64_t h) {
    return std::apply(
        [&](const auto&... d) {
            ((h = fp_kind<typename std::remove_cvref_t<decltype(d)>::field_type>(
                  fp_cstr(h, d.name))),
             ...);
            return h;
        },
        ArrowFields<T>::descriptors());
}

}  // namespace fields_detail

// The shape fingerprint of a described type: a stable 64-bit hash over the
// ordered (field name, wire kind) sequence, recursive into composites. Two
// declarations produce the same fingerprint exactly when the derived codec
// would produce interchangeable bytes for them, so a snapshot stamped with
// one fingerprint and bound against a type with another is a shape change:
// the restore gate refuses it unless a schema-version bump and migration
// intervened. Collision quality is gate-quality, not identity (64-bit
// FNV-1a); deliberate.
template <typename T>
    requires HasArrowFields<T>
inline constexpr std::uint64_t fields_fingerprint_v =
    fields_detail::fp_record<T>(fields_detail::kFpOffset);

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
        /* The declared spelling, the default channel name at registration  \
           (a namespaced type invoked as CLINK_ARROW_FIELDS(ns::T, ...)     \
           registers as "ns::T"). */                                        \
        static constexpr const char* name = #T;                             \
        static constexpr auto descriptors() {                               \
            return ::std::make_tuple(CLINK_ARROW_FOR_EACH(T, __VA_ARGS__)); \
        }                                                                   \
    }
