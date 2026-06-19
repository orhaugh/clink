// Factory-string registration for custom-struct Parquet: register a
// CLINK_ARROW_FIELDS type's connectors under op-type strings and confirm
//   1. the type + all three factories resolve in the registries,
//   2. the plain sink/source factories build working connectors that
//      round-trip via the "path" param (the job-spec config vehicle),
//   3. the 2PC factory builds a ParquetSink2PC, and a missing 'path'
//      param is a clean error.

#ifndef CLINK_HAS_PARQUET
#error "test_columnar_parquet_factories requires Parquet (ships with Arrow)"
#endif

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/connectors/columnar_parquet_factories.hpp"
#include "clink/connectors/parquet_2pc_sink.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

using namespace clink;

struct ColPqFacTrade {
    std::int64_t id;
    std::string symbol;
    double price;
    std::int32_t qty;
    bool buy;
};

CLINK_ARROW_FIELDS(ColPqFacTrade, id, symbol, price, qty, buy);

namespace {

// Minimal codec (register_type requires one for state + bridges; Parquet
// itself uses only the batcher).
clink::Codec<ColPqFacTrade> fac_codec() {
    clink::Codec<ColPqFacTrade> c;
    c.encode = [](const ColPqFacTrade& t) {
        std::vector<std::byte> out;
        auto put_u64 = [&](std::uint64_t v) {
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xff));
        };
        put_u64(static_cast<std::uint64_t>(t.id));
        std::uint64_t pbits = 0;
        std::memcpy(&pbits, &t.price, 8);
        put_u64(pbits);
        put_u64(static_cast<std::uint64_t>(static_cast<std::uint32_t>(t.qty)));
        out.push_back(static_cast<std::byte>(t.buy ? 1 : 0));
        for (char ch : t.symbol)
            out.push_back(static_cast<std::byte>(static_cast<unsigned char>(ch)));
        return out;
    };
    c.decode = [](std::span<const std::byte>) -> std::optional<ColPqFacTrade> {
        return ColPqFacTrade{};  // not exercised by these tests
    };
    return c;
}

std::filesystem::path tmp_parquet(const std::string& tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    return std::filesystem::temp_directory_path() /
           ("clink_pqfac_" + tag + "_" + std::to_string(rng()) + ".parquet");
}

bool trade_eq(const ColPqFacTrade& a, const ColPqFacTrade& b) {
    return a.id == b.id && a.symbol == b.symbol && a.price == b.price && a.qty == b.qty &&
           a.buy == b.buy;
}

}  // namespace

TEST(ColumnarParquetFactories, RegistersTypeAndAllThreeFactories) {
    clink::cluster::TypeRegistry tr;
    clink::cluster::RunnerRegistry rr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, rr, sr);

    clink::plugin::register_columnar_parquet<ColPqFacTrade>(reg, "factrade", fac_codec());

    EXPECT_NE(tr.find("factrade"), nullptr);
    EXPECT_NE(rr.find_sink("factrade_parquet_sink", "factrade"), nullptr);
    EXPECT_NE(rr.find_source("factrade_parquet_source", "factrade"), nullptr);
    EXPECT_NE(rr.find_sink("factrade_parquet_2pc_sink", "factrade"), nullptr);
}

TEST(ColumnarParquetFactories, FactoriesBuildWorkingConnectorsViaPathParam) {
    const auto path = tmp_parquet("roundtrip");

    const std::vector<ColPqFacTrade> rows = {
        {1, "AAPL", 191.25, 100, true},
        {2, "MSFT", 410.10, 50, false},
    };

    clink::plugin::BuildContext ctx;
    ctx.params["path"] = path.string();

    {
        auto sink = clink::plugin::columnar_parquet_sink_factory<ColPqFacTrade>(
            "factrade_parquet_sink")(ctx);
        ASSERT_NE(sink, nullptr);
        sink->open();
        Batch<ColPqFacTrade> b;
        b.emplace(rows[0], EventTime{1});
        b.emplace(rows[1]);
        sink->on_data(b);
        sink->close();
    }

    {
        auto source = clink::plugin::columnar_parquet_source_factory<ColPqFacTrade>(
            "factrade_parquet_source")(ctx);
        ASSERT_NE(source, nullptr);
        source->open();
        std::vector<ColPqFacTrade> got;
        Emitter<ColPqFacTrade> em([&](StreamElement<ColPqFacTrade> e) {
            if (e.is_data())
                for (const auto& r : e.as_data())
                    got.push_back(r.value());
            return true;
        });
        while (source->produce(em)) {
        }
        source->close();

        ASSERT_EQ(got.size(), rows.size());
        for (std::size_t i = 0; i < rows.size(); ++i)
            EXPECT_TRUE(trade_eq(got[i], rows[i]));
    }

    std::filesystem::remove(path);
}

TEST(ColumnarParquetFactories, TwoPcFactoryBuildsParquetSink2PC) {
    clink::plugin::BuildContext ctx;
    ctx.params["path"] = tmp_parquet("2pc").parent_path().string();
    auto sink = clink::plugin::columnar_parquet_2pc_sink_factory<ColPqFacTrade>(
        "factrade_parquet_2pc_sink")(ctx);
    ASSERT_NE(sink, nullptr);
    EXPECT_NE(dynamic_cast<ParquetSink2PC<ColPqFacTrade>*>(sink.get()), nullptr);
}

TEST(ColumnarParquetFactories, MissingPathParamThrows) {
    clink::plugin::BuildContext ctx;  // no "path"
    auto sink_fac = clink::plugin::columnar_parquet_sink_factory<ColPqFacTrade>("factrade_sink");
    EXPECT_THROW(sink_fac(ctx), std::runtime_error);
    auto src_fac = clink::plugin::columnar_parquet_source_factory<ColPqFacTrade>("factrade_src");
    EXPECT_THROW(src_fac(ctx), std::runtime_error);
}
