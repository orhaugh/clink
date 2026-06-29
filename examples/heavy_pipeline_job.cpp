// heavy_pipeline_job - heavy-duty integration target.
//
// Pipeline: from_elements<Customer>(99 deterministic customers) ->
//           map<Order>(c -> Order{region, amount, count=1}) ->
//           key_by(hash(region)) ->
//           reduce((a,b) -> per-region accumulator) ->
//           map<string>(serialize Order as "<region>|<sum>|<count>") ->
//           sink(file_text_sink, parallelism=3).
//
// The test wraps this .so via clink_submit_job against a JM + 3 TMs
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
#include "clink/api/stream_execution_environment.hpp"
#include "clink/core/codec.hpp"
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

// ---------- byte codec helpers ----------

inline void put_u32(std::vector<std::byte>& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void put_i64(std::vector<std::byte>& out, std::int64_t v) {
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
    }
}

inline void put_str(std::vector<std::byte>& out, const std::string& s) {
    put_u32(out, static_cast<std::uint32_t>(s.size()));
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    out.insert(out.end(), p, p + s.size());
}

inline bool read_u32(std::span<const std::byte> b, std::size_t& pos, std::uint32_t& v) {
    if (pos + 4 > b.size()) {
        return false;
    }
    v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[pos + i])) << (i * 8);
    }
    pos += 4;
    return true;
}

inline bool read_i64(std::span<const std::byte> b, std::size_t& pos, std::int64_t& v) {
    if (pos + 8 > b.size()) {
        return false;
    }
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[pos + i])) << (i * 8);
    }
    pos += 8;
    v = static_cast<std::int64_t>(u);
    return true;
}

inline bool read_str(std::span<const std::byte> b, std::size_t& pos, std::string& s) {
    std::uint32_t len = 0;
    if (!read_u32(b, pos, len)) {
        return false;
    }
    if (pos + len > b.size()) {
        return false;
    }
    s.assign(reinterpret_cast<const char*>(b.data() + pos), len);
    pos += len;
    return true;
}

inline clink::Codec<Customer> customer_codec() {
    return clink::Codec<Customer>{
        .encode =
            [](const Customer& c) {
                std::vector<std::byte> out;
                out.reserve(64);
                put_i64(out, c.id);
                put_str(out, c.region);
                put_str(out, c.product);
                put_i64(out, c.amount);
                return out;
            },
        .decode = [](std::span<const std::byte> b) -> std::optional<Customer> {
            Customer c;
            std::size_t pos = 0;
            if (!read_i64(b, pos, c.id) || !read_str(b, pos, c.region) ||
                !read_str(b, pos, c.product) || !read_i64(b, pos, c.amount)) {
                return std::nullopt;
            }
            return c;
        }};
}

inline clink::Codec<Order> order_codec() {
    return clink::Codec<Order>{.encode =
                                   [](const Order& o) {
                                       std::vector<std::byte> out;
                                       out.reserve(32);
                                       put_str(out, o.region);
                                       put_i64(out, o.total_amount);
                                       put_i64(out, o.count);
                                       return out;
                                   },
                               .decode = [](std::span<const std::byte> b) -> std::optional<Order> {
                                   Order o;
                                   std::size_t pos = 0;
                                   if (!read_str(b, pos, o.region) ||
                                       !read_i64(b, pos, o.total_amount) ||
                                       !read_i64(b, pos, o.count)) {
                                       return std::nullopt;
                                   }
                                   return o;
                               }};
}

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

inline void define_job(clink::api::StreamExecutionEnvironment& env) {
    // Register the custom typed channels. Goes through env.registry()
    // so the registrations land in the per-job bundle on the JM/TM
    // AND the .so's local default-instance (via the mirror
    // in plugin_impl.hpp::register_type) so the .so's runtime
    // template instantiations resolve them too.
    env.registry().register_type<Customer>("heavy.customer", customer_codec());
    env.registry().register_type<Order>("heavy.order", order_codec());

    env.from_elements<Customer>(make_customers())
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
                   "99 customers -> map -> keyBy region -> reduce -> sink across 3 TMs",
                   heavy::define_job);
