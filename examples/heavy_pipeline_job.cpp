// heavy_pipeline_job - heavy-duty integration target.
//
// Pipeline: from_elements<Customer>(99 deterministic customers) ->
//           map<Order>(c -> Order{region, amount, count=1}) ->
//           key_by(hash(region)) ->
//           reduce((a,b) -> per-region accumulator) ->
//           map<string>(serialize Order as "<region>|<sum>|<count>") ->
//           sink(file_text_sink, parallelism=3).
//
// The test wraps this .so via clink_submit_job against a coordinator + 3 workers
// running as separate processes. Records cross the wire from the
// reduce subtask (par=1) to three sink subtasks (par=3, Rebalance).
// Each sink writes its records to <out>.<subtask_idx>.
//
// Verification: the test collects all 99 lines across the 3 sink
// files, groups by region, and checks the highest-count record per
// region matches the expected total. Reduce emits the running
// accumulator after each input, so the highest-count row per region
// is the final sum for that region.
//
// Output path is set via CLINK_HEAVY_OUT_BASE (the sink appends
// .0, .1, .2 for each subtask).

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/core/fields.hpp"
#include "clink/job/register_job.hpp"

namespace heavy {

struct Customer {
    std::int64_t id{0};
    std::string region;   // "NA", "EU", "ASIA", "SA"
    std::string product;  // "widget", "gadget", "gizmo"
    std::int64_t amount{0};
};

struct Order {
    std::string region;
    std::int64_t total_amount{0};
    std::int64_t count{0};
};

}  // namespace heavy

// One declaration per type (design record 009): everything the two hand
// codecs, the explicit channel names and the Arrow descriptions used to
// say separately now derives from these. At global scope because the
// macro specialises clink::ArrowFields<T>.
CLINK_FIELDS(heavy::Customer, id, region, product, amount);
CLINK_FIELDS(heavy::Order, region, total_amount, count);

namespace heavy {

// Build the 99-customer input deterministically. The test relies on
// these counts and per-region totals (see test_heavy_pipeline_e2e.cpp).
//
//   NA   : ids 0..29  -> amount = id*10        -> sum 4350, count 30
//   EU   : ids 30..59 -> amount = (id-30)*20   -> sum 8700, count 30
//   ASIA : ids 60..84 -> amount = (id-60)*5+100 -> sum 4000, count 25
//   SA   : ids 85..98 -> amount = (id-85)*7+50  -> sum 1337, count 14
inline std::vector<Customer> make_customers() {
    std::vector<Customer> out;
    out.reserve(99);
    for (std::int64_t id = 0; id < 30; ++id) {
        out.push_back(Customer{id, "NA", "widget", id * 10});
    }
    for (std::int64_t id = 30; id < 60; ++id) {
        out.push_back(Customer{id, "EU", "gadget", (id - 30) * 20});
    }
    for (std::int64_t id = 60; id < 85; ++id) {
        out.push_back(Customer{id, "ASIA", "gizmo", (id - 60) * 5 + 100});
    }
    for (std::int64_t id = 85; id < 99; ++id) {
        out.push_back(Customer{id, "SA", "widget", (id - 85) * 7 + 50});
    }
    return out;
}

inline std::string output_base_path() {
    if (const char* p = std::getenv("CLINK_HEAVY_OUT_BASE"); p != nullptr && *p != '\0') {
        return p;
    }
    return "/tmp/clink_heavy_pipeline_out";
}

inline void define_job(clink::api::Pipeline& pipeline) {
    // Register the custom typed channels from their declarations alone:
    // channel name = the declared type name, codec = the derived codec,
    // batcher auto-selected. Still through pipeline.registry() so the
    // registrations land in the per-job bundle AND the .so's local
    // default-instance (the mirror in plugin_impl.hpp::register_type).
    pipeline.registry().register_type<Customer>();
    pipeline.registry().register_type<Order>();

    pipeline.from_elements<Customer>(make_customers())
        .map<Order>([](const Customer& c) { return Order{c.region, c.amount, 1}; })
        .key_by([](const Order& o) -> std::int64_t {
            return static_cast<std::int64_t>(std::hash<std::string>{}(o.region));
        })
        .reduce([](const Order& a, const Order& b) {
            return Order{a.region, a.total_amount + b.total_amount, a.count + b.count};
        })
        .map<std::string>([](const Order& o) {
            return o.region + "|" + std::to_string(o.total_amount) + "|" + std::to_string(o.count);
        })
        .sink(clink::api::FileTextSink::builder().path(output_base_path()).parallelism(3).build());
}

}  // namespace heavy

CLINK_REGISTER_JOB("heavy-pipeline",
                   "1.0",
                   "99 customers -> map -> keyBy region -> reduce -> sink across 3 workers",
                   heavy::define_job);
