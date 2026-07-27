// Per-group memory cost of a windowed aggregate, measured against the REAL operator.
//
// WHY. On the two-node rig, nexmark q12 held 1,407 MB for 195,710 groups of
// COUNT(*) keyed by an int64. Varying only the group count (195,710 groups vs 5, same
// records, same window) attributed 766 MB of that to grouping: about 3.9 KB per group,
// to store an int64 key and a counter. Two orders of magnitude more than the data.
//
// This reproduces that locally so a fix can be iterated on in seconds instead of on a
// cloud rig, and it drives the SQL runtime through the embedded engine rather than a
// hand-written model - the hot-path bench's q12 shape is a MODEL of the state, so it
// cannot validate a change to the operator that actually runs.
//
// METHOD. Run the same query twice over the same records, once with many distinct group
// keys and once with one, and difference the peak RSS. Everything that does not scale
// with the group count - the runtime, the source, buffered batches - cancels, so what is
// left is the per-group cost. Reported as bytes per group.
//
//   clink_window_state_bench [records] [groups]
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

// Peak resident set for this process, in bytes. ru_maxrss is a HIGH-WATER mark and never
// falls, which is what makes the two-run difference meaningful: the second run's peak
// includes the first run's, so the runs are done in separate processes (see main).
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

// One NDJSON file of bid-shaped records with a controlled number of distinct keys, and
// all timestamps inside a single tumbling window so the run holds every group at once
// (which is the state-heavy case the rig measured).
std::string write_input(const std::filesystem::path& path,
                        std::int64_t records,
                        std::int64_t groups) {
    std::ofstream f(path);
    for (std::int64_t i = 0; i < records; ++i) {
        f << R"({"auction":)" << (i % 1000) << R"(,"bidder":)" << (i % groups) << R"(,"price":)"
          << (i % 997) << R"(,"channel":"ch)" << (i % 8) << R"(","url":"https://x.test/)"
          << (i % 500) << R"(","datetime":)" << (1'000'000'000'000LL + (i % 1000)) << "}\n";
    }
    return path.string();
}

int run_query(const std::string& input, std::int64_t groups) {
    std::ostringstream sink;  // swallow statement output; it is not what is measured
    clink::embed::EngineOptions opts;
    opts.out = &sink;
    opts.err = &sink;
    clink::embed::EmbeddedEngine engine(std::move(opts));

    std::ostringstream sql;
    sql << "CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, "
           "url VARCHAR, datetime BIGINT) WITH (connector='file', format='json', path='"
        << input << "', event_time_column='datetime', watermark_lag_ms='4000');\n"
        << "CREATE TABLE out_t (bidder BIGINT, bid_count BIGINT) WITH (connector='blackhole');\n"
        << "INSERT INTO out_t SELECT bidder, COUNT(*) AS bid_count FROM bid "
           "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder;\n";
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
    const std::int64_t records = argc > 1 ? std::strtoll(argv[1], nullptr, 10) : 400000;
    const std::int64_t groups = argc > 2 ? std::strtoll(argv[2], nullptr, 10) : 100000;

    // Child mode: one run, print the peak RSS, exit. Each measurement needs its own
    // process because ru_maxrss is a high-water mark that never falls, so two runs in one
    // process would report the larger of the two twice.
    if (argc > 3 && std::string(argv[3]) == "--child") {
        const std::int64_t g = std::strtoll(argv[2], nullptr, 10);
        const auto input = write_input(
            std::filesystem::temp_directory_path() / ("clink-wsb-" + std::to_string(g) + ".ndjson"),
            records,
            g);
        if (run_query(input, g) != 0) {
            return 1;
        }
        std::printf("%llu\n", static_cast<unsigned long long>(peak_rss_bytes()));
        return 0;
    }

    std::printf("window state bench: records=%lld, comparing %lld groups against 1 group\n\n",
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
    const auto one = measure(1);
    if (many == 0 || one == 0) {
        std::fprintf(stderr, "a child run produced no measurement\n");
        return 1;
    }

    std::printf("  %10lld groups: peak RSS %7.1f MB\n",
                static_cast<long long>(groups),
                static_cast<double>(many) / 1e6);
    std::printf("  %10d group : peak RSS %7.1f MB\n", 1, static_cast<double>(one) / 1e6);
    if (many > one && groups > 1) {
        const double per = static_cast<double>(many - one) / static_cast<double>(groups - 1);
        std::printf("\n  attributable to grouping: %.1f MB over %lld groups\n",
                    static_cast<double>(many - one) / 1e6,
                    static_cast<long long>(groups - 1));
        std::printf("  => %.0f BYTES PER GROUP (an int64 key and a COUNT)\n", per);
    } else {
        std::printf("\n  no measurable per-group cost at this scale\n");
    }
    return 0;
}
