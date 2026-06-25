// Shared-input producer for the cross-engine Nexmark comparison (INC 2).
//
// Runs clink's deterministic NexmarkGenerator and writes ONE canonical dataset,
// partitioned by type, as NDJSON: <out-dir>/{person,auction,bid}.ndjson, one JSON
// object per line - the SAME JSON shape clink's nexmark_source emits and Flink's
// JSON format decodes. The driver then loads each file into its Kafka topic
// (nx-person/nx-auction/nx-bid), so BOTH engines read identical bytes and neither
// re-derives the stream (see pipeline.md).
//
// Each type is produced by a type-filtered generator over the SAME seed/event
// count: all three advance over the identical stream deterministically, so the
// three files together are the single canonical Nexmark stream split by type, with
// foreign keys consistent across them. Generation is off the engines' timed path.
//
//   nexmark_dump --events 1000000 --tps 1000000 --out-dir /tmp/nx
//
// Determinism is the load-bearing property: two runs at the same --events/--tps
// (fixed seed) produce byte-identical files, so per-query EXPECTED output-row
// counts are reproducible. Verify with: run twice, diff the files.

#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

#include "clink/config/json.hpp"
#include "clink/nexmark/generator.hpp"
#include "clink/sql/row.hpp"

using namespace clink;

namespace {

// Dump every event of one type to <dir>/<name>.ndjson; return the line count.
std::int64_t dump_type(const std::string& path,
                       int type_filter,
                       std::int64_t events,
                       std::int64_t tps) {
    nexmark::NexmarkConfig cfg;
    cfg.events_num = events;
    cfg.tps = tps;
    cfg.type_filter = type_filter;
    nexmark::NexmarkGenerator gen(cfg);

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("nexmark_dump: cannot open " + path);
    }
    std::int64_t n = 0;
    while (auto rec = gen.next()) {
        const std::string line = clink::config::serialize_output(
            clink::config::JsonValue{clink::config::JsonObject{rec->value().values}});
        out.write(line.data(), static_cast<std::streamsize>(line.size()));
        out.put('\n');
        ++n;
    }
    return n;
}

}  // namespace

int main(int argc, char** argv) {
    std::int64_t events = 1'000'000;
    std::int64_t tps = 1'000'000;
    std::string out_dir = "/tmp/nx";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() { return i + 1 < argc ? std::string(argv[++i]) : std::string(); };
        if (a == "--events")
            events = std::stoll(next());
        else if (a == "--tps")
            tps = std::stoll(next());
        else if (a == "--out-dir")
            out_dir = next();
    }

    try {
        const std::int64_t n_person = dump_type(out_dir + "/person.ndjson", 0, events, tps);
        const std::int64_t n_auction = dump_type(out_dir + "/auction.ndjson", 1, events, tps);
        const std::int64_t n_bid = dump_type(out_dir + "/bid.ndjson", 2, events, tps);
        const std::int64_t total = n_person + n_auction + n_bid;
        std::cout << "{\"events\":" << events << ",\"person\":" << n_person
                  << ",\"auction\":" << n_auction << ",\"bid\":" << n_bid << ",\"total\":" << total
                  << ",\"out_dir\":\"" << out_dir << "\"}\n";
    } catch (const std::exception& e) {
        std::cerr << "nexmark_dump failed: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
