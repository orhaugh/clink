// Arrow's compute kernels must be genuinely reachable, not merely linked.
//
// The engine carried a comment for months stating that arrow::compute::Initialize() was
// not exported by the Arrow package it links, and hand-rolled its comparison masks
// because of it. The check had looked in libarrow; the symbol is exported from
// libarrow_compute, which is a separate library from Arrow 15 onwards. These tests pin
// the corrected fact so it cannot quietly regress into folklore again, and so a build
// against an Arrow without the compute library still reports honestly.

#include <cstdint>
#include <memory>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_compute.hpp"

#ifdef CLINK_HAS_ARROW_COMPUTE
#include <arrow/compute/api.h>
#endif

// The distinction that matters: linking the library is not the same as registering the
// kernels. Without Initialize() the registry holds 13 functions; with it, ~305.
TEST(ArrowCompute, KernelsAreRegisteredNotJustLinked) {
    if (!clink::arrow_compute_available()) {
        GTEST_SKIP() << "built against an Arrow without libarrow_compute";
    }
    // 13 is the un-initialised count. Anything near it means Initialize() did not take.
    EXPECT_GT(clink::arrow_compute_function_count(), 100u)
        << "compute reports available but the registry is nearly empty, so the kernels "
           "were linked without being registered";
}

// Idempotent and thread-safe by contract: operators call it per batch.
TEST(ArrowCompute, AvailabilityIsStableAcrossCalls) {
    const bool first = clink::arrow_compute_available();
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(clink::arrow_compute_available(), first);
    }
}

#ifdef CLINK_HAS_ARROW_COMPUTE
// The kernel that the hand-rolled comparison mask exists to replace must be present AND
// produce the right answer - a registered name that computes the wrong thing would be
// worse than an absent one.
TEST(ArrowCompute, GreaterEqualKernelIsPresentAndCorrect) {
    if (!clink::arrow_compute_available()) {
        GTEST_SKIP() << "built against an Arrow without libarrow_compute";
    }
    arrow::Int64Builder b;
    ASSERT_TRUE(b.AppendValues({1, 5, 9}).ok());
    std::shared_ptr<arrow::Array> col;
    ASSERT_TRUE(b.Finish(&col).ok());

    auto out = arrow::compute::CallFunction("greater_equal",
                                            {col, arrow::Datum(static_cast<std::int64_t>(5))});
    ASSERT_TRUE(out.ok()) << out.status().ToString();
    auto mask = std::static_pointer_cast<arrow::BooleanArray>(out->make_array());
    ASSERT_EQ(mask->length(), 3);
    EXPECT_FALSE(mask->Value(0));  // 1 >= 5
    EXPECT_TRUE(mask->Value(1));   // 5 >= 5
    EXPECT_TRUE(mask->Value(2));   // 9 >= 5
}

// A spread of the kernel families the hand-rolled paths would otherwise need.
TEST(ArrowCompute, ArithmeticAndBooleanFamiliesAreReachable) {
    if (!clink::arrow_compute_available()) {
        GTEST_SKIP() << "built against an Arrow without libarrow_compute";
    }
    auto* reg = arrow::compute::GetFunctionRegistry();
    for (const char* name : {"greater_equal", "less", "equal", "add", "multiply", "and_kleene"}) {
        EXPECT_TRUE(reg->GetFunction(name).ok()) << name << " is not registered";
    }
}
#endif
