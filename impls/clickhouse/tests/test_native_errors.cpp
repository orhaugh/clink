#include <string>

#include <gtest/gtest.h>

#include "native/errors.hpp"

namespace clink::clickhouse::native {
namespace {

TEST(NativeErrors, WhatCarriesTheCodeInBrackets) {
    const NativeSinkError e(code::kOptionInvalid, "batch_rows must be a positive integer");
    EXPECT_EQ(e.code(), "clickhouse.option_invalid");
    EXPECT_STREQ(e.what(), "[clickhouse.option_invalid] batch_rows must be a positive integer");
}

TEST(NativeErrors, AConversionErrorNamesTheColumnAndTheRow) {
    const ConversionError e("amount", 7, "value out of range for Int16");
    EXPECT_EQ(e.code(), code::kConversionFailed);
    EXPECT_EQ(e.column(), "amount");
    EXPECT_EQ(e.row(), 7);
    const std::string what = e.what();
    EXPECT_NE(what.find("`amount`"), std::string::npos) << what;
    EXPECT_NE(what.find("row 7"), std::string::npos) << what;
    EXPECT_NE(what.find("value out of range for Int16"), std::string::npos) << what;
}

}  // namespace
}  // namespace clink::clickhouse::native
