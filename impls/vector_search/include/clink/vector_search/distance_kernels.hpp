#pragma once

#include <cstddef>

// Vector distance kernels for VECTOR_SEARCH. The SimSIMD-backed implementation
// (compiled when CLINK_VECTOR_SIMSIMD is defined) does its own runtime CPU-feature
// dispatch (AVX2/AVX-512/NEON), so it delivers real SIMD without clink changing its
// build arch flags. A scalar C++ fallback compiles when SimSIMD is absent, so the
// feature degrades to correct-but-slower rather than breaking. SimSIMD headers stay
// PRIVATE to the impl .cpp, so this header carries no third-party include and does
// not touch the plugin ABI.

namespace clink::vector_search {

enum class Metric { Dot, Cosine, L2 };

// The canonical metric value between two float32 vectors of length dim:
//  - Dot    -> dot product        (higher = nearer)
//  - Cosine -> cosine similarity  (higher = nearer)
//  - L2     -> squared euclidean  (lower  = nearer)
// The operator emits this verbatim as the `score` column and ranks with metric_nearer
// below (so it does not need to know each metric's direction).
[[nodiscard]] float distance(Metric m, const float* a, const float* b, std::size_t dim) noexcept;

// True iff a is a nearer neighbour than b under metric m (Dot/Cosine: larger score;
// L2: smaller score). Used to build the top-k.
[[nodiscard]] inline bool metric_nearer(Metric m, float a, float b) noexcept {
    return m == Metric::L2 ? a < b : a > b;
}

// True iff this build was compiled against SimSIMD (real runtime SIMD dispatch),
// false when only the scalar fallback is present. Logged once at operator open().
[[nodiscard]] bool simd_enabled() noexcept;

}  // namespace clink::vector_search
