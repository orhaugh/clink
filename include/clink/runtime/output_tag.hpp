#pragma once

#include <string>
#include <string_view>
#include <utility>

namespace clink {

// OutputTag<T> names a side output channel of an operator. Each tag is
// keyed by its string id; the type parameter T is the side channel's
// element type and is enforced at registration / emit time so the
// emitter the user gets back is typed correctly.
//
// Same id with different T is a programmer error - the side output
// registry will reject the typed lookup on the cast.
template <typename T>
struct OutputTag {
    using element_type = T;

    std::string id;

    OutputTag() = default;
    explicit OutputTag(std::string tag_id) : id(std::move(tag_id)) {}

    [[nodiscard]] std::string_view name() const noexcept { return id; }
};

}  // namespace clink
