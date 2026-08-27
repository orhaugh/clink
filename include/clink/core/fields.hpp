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

#include <tuple>

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
