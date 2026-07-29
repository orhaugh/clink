// Per-retained-row memory cost of the SQL top-N / dedup operator, measured
// against the REAL operator.
//
// WHY. On the two-node rig, nexmark q18 (latest bid per (bidder, auction) - a
// ROW_NUMBER dedup) held 3.7 GB for 9,193,877 retained rows: about 428 bytes per
// row whose serialized payload is 124 bytes, or ~3.4x representation overhead.
// Dedup retains nearly the whole input on that dataset, so the representation is
// the memory bill. TopNPerKeyRowOp originally stored each retained row as a
// materialized Row (a FlatMap of interned-name -> JsonValue), which pays the
// column-map array, per-string heap blocks and allocator rounding per row; the
// encoded representation stores the row as one JSON string plus its decoded sort
// keys, and this bench exists to price the difference and to catch it regressing.
//
// METHOD. Same shape as clink_window_state_bench: run the same query over the
// same records twice through the EMBEDDED engine - once where nearly every record
// opens a new (bidder, auction) partition (retains ~everything), once with a tiny
// key space (retains almost nothing) - and difference the peak RSS in separate
// child processes (ru_maxrss is a high-water mark and never falls). Everything
// that does not scale with the retained-row count cancels, leaving bytes per
// retained row.
//
//   clink_topn_state_bench [records] [groups]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include <sys/resource.h>

#include "clink/embed/embedded_engine.hpp"

namespace {

std::uint64_t peak_rss_bytes() {
    struct rusage ru{};
    if (getrusage(RUSAGE_SELF, &ru) != 0) {
        return 0;
    }
#ifdef __APPLE__
    return static_cast<std::uint64_t>(ru.ru_maxrss);  // bytes on macOS
#else
    return static_cast<std::uint64_t>(ru.ru_maxrss) * 1024ULL;  // kilobytes on Linux
#endif
}

// One NDJSON file of bid-shaped records with a controlled number of distinct
// (bidder, auction) pairs. The payload shape (a ~25-char URL, an 8-char channel)
// matches what the rig measured, so bytes-per-row here is comparable to the rig's
// arithmetic rather than to a toy row.
std::string write_input(const std::filesystem::path& path,
                        std::int64_t records,
                        std::int64_t groups) {
    std::ofstream f(path);
    for (std::int64_t i = 0; i < records; ++i) {
        const std::int64_t pair = i % groups;
        f << R"({"auction":)" << (pair % 100'000) << R"(,"bidder":)" << (pair / 100'000)
          << R"(,"price":)" << (i % 99'991) << R"(,"channel":"ch)" << (i % 8)
          << R"(","url":"https://nexmark.dev/)" << (i % 6000) << R"(","datetime":)"
          << (1'000'000'000'000LL + i) << "}\n";
    }
    return path.string();
}

int run_query(const std::string& input, std::int64_t groups) {
    std::ostringstream sink;
    clink::embed::EngineOptions opts;
    opts.out = &sink;
    opts.err = &sink;
    clink::embed::EmbeddedEngine engine(std::move(opts));

    // The q18 shape: ROW_NUMBER dedup, one retained row per (bidder, auction).
    std::ostringstream sql;
    sql << "CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, "
           "url VARCHAR, datetime BIGINT) WITH (connector='file', format='json', path='"
        << input << "', event_time_column='datetime', watermark_lag_ms='4000');\n"
        << "CREATE TABLE out_t (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, "
           "url VARCHAR, datetime BIGINT) WITH (connector='blackhole');\n"
        << "INSERT INTO out_t SELECT auction, bidder, price, channel, url, datetime FROM "
           "(SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY datetime DESC) "
           "AS rn FROM bid) AS R WHERE rn = 1;\n";
    if (engine.execute_script(sql.str()) != 0) {
        std::fprintf(stderr,
                     "query failed for groups=%lld:\n%s\n",
                     static_cast<long long>(groups),
                     sink.str().c_str());
        return 1;
    }
    engine.await_all();
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const std::int64_t records = argc > 1 ? std::strtoll(argv[1], nullptr, 10) : 2'000'000;
    const std::int64_t groups = argc > 2 ? std::strtoll(argv[2], nullptr, 10) : records;

    if (argc > 3 && std::string(argv[3]) == "--child") {
        const std::int64_t g = std::strtoll(argv[2], nullptr, 10);
        const auto input = write_input(std::filesystem::temp_directory_path() /
                                           ("clink-tnb-" + std::to_string(g) + ".ndjson"),
                                       records,
                                       g);
        if (run_query(input, g) != 0) {
            return 1;
        }
        std::printf("%llu\n", static_cast<unsigned long long>(peak_rss_bytes()));
        return 0;
    }

    std::printf("top-n state bench: records=%lld, comparing %lld retained rows against 1000\n\n",
                static_cast<long long>(records),
                static_cast<long long>(groups));

    const std::string self = argv[0];
    const auto measure = [&](std::int64_t g) -> std::uint64_t {
        const std::string cmd =
            self + " " + std::to_string(records) + " " + std::to_string(g) + " --child";
        FILE* p = popen(cmd.c_str(), "r");
        if (p == nullptr) {
            return 0;
        }
        char buf[64] = {0};
        if (std::fgets(buf, sizeof(buf), p) == nullptr) {
            pclose(p);
            return 0;
        }
        pclose(p);
        return std::strtoull(buf, nullptr, 10);
    };

    const auto many = measure(groups);
    const auto few = measure(1000);
    if (many == 0 || few == 0) {
        std::fprintf(stderr, "a child run produced no measurement\n");
        return 1;
    }

    std::printf("  %10lld retained: peak RSS %7.1f MB\n",
                static_cast<long long>(groups),
                static_cast<double>(many) / 1e6);
    std::printf("  %10d retained: peak RSS %7.1f MB\n", 1000, static_cast<double>(few) / 1e6);
    if (many > few && groups > 1000) {
        const double per = static_cast<double>(many - few) / static_cast<double>(groups - 1000);
        std::printf("\n  attributable to retained rows: %.1f MB over %lld rows\n",
                    static_cast<double>(many - few) / 1e6,
                    static_cast<long long>(groups - 1000));
        std::printf("  => %.0f BYTES PER RETAINED ROW (serialized payload ~130 B)\n", per);
    } else {
        std::printf("\n  no measurable per-row cost at this scale\n");
    }
    return 0;
}
