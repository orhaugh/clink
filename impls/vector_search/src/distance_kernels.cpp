#include "clink/vector_search/distance_kernels.hpp"

#include <cmath>

#if defined(CLINK_VECTOR_SIMSIMD)
#include <simsimd/simsimd.h>
#endif

namespace clink::vector_search {

namespace {

// The scalar fallbacks are used only when SimSIMD is absent; [[maybe_unused]] keeps
// the SimSIMD build (where the #else branch is dead) warning-clean.
[[maybe_unused]] float scalar_dot(const float* a, const float* b, std::size_t n) {
    float s = 0.0F;
    for (std::size_t i = 0; i < n; ++i) {
        s += a[i] * b[i];
    }
    return s;
}

[[maybe_unused]] float scalar_l2sq(const float* a, const float* b, std::size_t n) {
    float s = 0.0F;
    for (std::size_t i = 0; i < n; ++i) {
        const float d = a[i] - b[i];
        s += d * d;
    }
    return s;
}

// Cosine SIMILARITY (higher = nearer). Zero-norm vectors yield 0.
[[maybe_unused]] float scalar_cosine(const float* a, const float* b, std::size_t n) {
    float dot = 0.0F;
    float na = 0.0F;
    float nb = 0.0F;
    for (std::size_t i = 0; i < n; ++i) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    const float denom = std::sqrt(na) * std::sqrt(nb);
    return denom > 0.0F ? dot / denom : 0.0F;
}

}  // namespace

float distance(Metric m, const float* a, const float* b, std::size_t dim) noexcept {
#if defined(CLINK_VECTOR_SIMSIMD)
    // SimSIMD runtime-dispatches to the best ISA the CPU supports. It returns a
    // DISTANCE for cosine (1 - similarity); we convert back to similarity so the
    // metric value matches the scalar path's convention (higher = nearer).
    simsimd_distance_t d = 0;
    switch (m) {
        case Metric::Dot:
            simsimd_dot_f32(a, b, dim, &d);
            return static_cast<float>(d);
        case Metric::Cosine:
            simsimd_cos_f32(a, b, dim, &d);
            return static_cast<float>(1.0 - d);
        case Metric::L2:
            simsimd_l2sq_f32(a, b, dim, &d);
            return static_cast<float>(d);
    }
    return 0.0F;
#else
    switch (m) {
        case Metric::Dot:
            return scalar_dot(a, b, dim);
        case Metric::Cosine:
            return scalar_cosine(a, b, dim);
        case Metric::L2:
            return scalar_l2sq(a, b, dim);
    }
    return 0.0F;
#endif
}

bool simd_enabled() noexcept {
#if defined(CLINK_VECTOR_SIMSIMD)
    return true;
#else
    return false;
#endif
}

}  // namespace clink::vector_search
