#pragma once

// Access to Arrow's compute kernels (comparison, arithmetic, boolean, and the rest).
//
// WHY THIS EXISTS AT ALL. From Arrow 15 the kernel set moved out of libarrow into
// libarrow_compute, and registration became explicit rather than happening at load. A
// build that links only libarrow gets a function registry holding 13 entries - `filter`
// and a few other selection kernels - and `greater_equal` is not among them. Calling
// arrow::compute::Initialize() once takes that registry from 13 functions to 305.
//
// The engine previously recorded this as a dead end. A comment in
// columnar_filter_operator.hpp stated that Initialize() "is not an exported symbol in
// this package (verified against Arrow::arrow_shared)", and the comparison masks were
// hand-rolled because of it. The verification had looked in libarrow; the symbol is
// exported from libarrow_compute. Measured against both the Homebrew and the pinned
// Arrow 24 prefixes: link the compute library, call Initialize(), and greater_equal
// answers correctly.
//
// USE IT LIKE THIS. Ask before calling a kernel, and keep the fallback:
//
//     if (clink::arrow_compute_available()) {
//         auto mask = arrow::compute::CallFunction("greater_equal", {col, threshold});
//         ...
//     }
//     // otherwise the hand-rolled path
//
// The fallback is not ceremony. A build against an Arrow without the compute library is
// supported, kernel semantics for nulls and type promotion do not always match a
// hand-rolled scan, and clink's own measurements put the arithmetic in a Kafka JSON
// pipeline at about 4% of worker CPU - so a kernel is a correctness and capability
// improvement far more often than it is a throughput one.

#include <mutex>

#ifdef CLINK_HAS_ARROW_COMPUTE
#include <arrow/compute/api.h>
#endif

namespace clink {

// True when Arrow's full kernel set is linked AND successfully registered.
//
// Registers on the first call, once per process, via std::call_once: Initialize() walks
// 300-odd kernels into the registry, which is worth doing lazily rather than at every
// process start, and it must not race between operator threads. A failed Initialize()
// reports false rather than throwing, so a caller takes its fallback path instead of
// failing a job.
[[nodiscard]] inline bool arrow_compute_available() noexcept {
#ifdef CLINK_HAS_ARROW_COMPUTE
    static bool ok = false;
    static std::once_flag once;
    std::call_once(once, [] { ok = arrow::compute::Initialize().ok(); });
    return ok;
#else
    return false;
#endif
}

// How many functions the registry holds, after initialisation. Exists so a test can
// distinguish "linked" from "actually registered": the difference between the two is 13
// functions and 305, and nothing else observable.
[[nodiscard]] inline std::size_t arrow_compute_function_count() noexcept {
#ifdef CLINK_HAS_ARROW_COMPUTE
    if (!arrow_compute_available()) {
        return 0;
    }
    return arrow::compute::GetFunctionRegistry()->GetFunctionNames().size();
#else
    return 0;
#endif
}

}  // namespace clink
