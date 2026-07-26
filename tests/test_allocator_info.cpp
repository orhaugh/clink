// The reported allocator must agree with how the build was configured.
//
// CLINK_WITH_JEMALLOC is opt-in and OFF by default, and every part of making it real is
// invisible from outside: jemalloc has to be found, linked, and actually intercepting
// malloc. A build that reported success while leaving the system allocator in charge
// would look identical to one that worked. That class of silent no-op has bitten this
// codebase twice - an in-process data-plane fast path that was dead in every container
// deployment, and a duplicate registration that replaced an optimised sink - so the
// reported name is pinned to the build flag here rather than trusted.

#include <string>

#include <gtest/gtest.h>

#include "clink/core/allocator_info.hpp"

// Without CLINK_HAS_JEMALLOC the name is exactly "system": no build can report a
// jemalloc it was not compiled against.
TEST(AllocatorInfo, NameAgreesWithTheBuildConfiguration) {
    const std::string name = clink::allocator_name();
    if (clink::built_with_jemalloc()) {
        EXPECT_EQ(name.rfind("jemalloc", 0), 0u)
            << "built with jemalloc but reporting '" << name << "'";
    } else {
        EXPECT_EQ(name, "system") << "built without jemalloc but reporting '" << name << "'";
    }
}

// When jemalloc IS linked, mallctl must answer - that is the difference between "the
// library is on the link line" and "the library is in charge". A linked-but-mute
// jemalloc reports itself as such rather than claiming a version it cannot read.
TEST(AllocatorInfo, JemallocBuildReportsAVersionFromTheLibrary) {
    if (!clink::built_with_jemalloc()) {
        GTEST_SKIP() << "not built with CLINK_WITH_JEMALLOC";
    }
    const std::string name = clink::allocator_name();
    EXPECT_EQ(name.find("version unavailable"), std::string::npos)
        << "jemalloc is linked but mallctl did not answer, so it may not be intercepting "
           "malloc: "
        << name;
    // "jemalloc " plus a version string of some kind.
    EXPECT_GT(name.size(), std::string{"jemalloc "}.size());
}
