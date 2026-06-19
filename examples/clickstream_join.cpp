// Clickstream join example.
//
// Reads two TSV files (clicks.tsv, orders.tsv), assigns event-time from each
// record's embedded timestamp via a WatermarkAssignerOperator, then joins
// them by user_id within a [-50ms, +500ms] interval. Output is JSON-per-line.
//
// Pipeline shape:
//
//   FileSource<Click>  -> WatermarkAssigner<Click>  ----.
//                                                        \
//                                                         interval_join (key=user_id)
//                                                        /
//   FileSource<Order>  -> WatermarkAssigner<Order>  ----'
//
//                    -> Map<Joined, std::string>  (format as JSON)
//                    -> FileSink<std::string>
//
// File formats (tab-separated):
//   clicks.tsv:  user_id   event_time_ms   url
//   orders.tsv:  user_id   event_time_ms   sku
//
// Default uses inner join. Pass `--full-outer` to switch.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/file_sink.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/time/watermark_strategy.hpp"

namespace {

struct Click {
    std::string user_id;
    std::int64_t timestamp_ms;
    std::string url;
};
struct Order {
    std::string user_id;
    std::int64_t timestamp_ms;
    std::string sku;
};
struct Joined {
    std::optional<Click> click;
    std::optional<Order> order;
};

std::vector<std::string> split_tsv(std::string_view line) {
    std::vector<std::string> out;
    std::size_t start = 0;
    for (std::size_t i = 0; i < line.size(); ++i) {
        if (line[i] == '\t') {
            out.emplace_back(line.substr(start, i - start));
            start = i + 1;
        }
    }
    out.emplace_back(line.substr(start));
    return out;
}

clink::TextFormat<Click> click_format() {
    return clink::TextFormat<Click>{.decode = [](std::string_view line) -> std::optional<Click> {
                                        auto parts = split_tsv(line);
                                        if (parts.size() != 3) {
                                            return std::nullopt;
                                        }
                                        try {
                                            return Click{parts[0], std::stoll(parts[1]), parts[2]};
                                        } catch (...) {
                                            return std::nullopt;
                                        }
                                    },
                                    .encode = [](const Click&) { return std::string{}; }};
}

clink::TextFormat<Order> order_format() {
    return clink::TextFormat<Order>{.decode = [](std::string_view line) -> std::optional<Order> {
                                        auto parts = split_tsv(line);
                                        if (parts.size() != 3) {
                                            return std::nullopt;
                                        }
                                        try {
                                            return Order{parts[0], std::stoll(parts[1]), parts[2]};
                                        } catch (...) {
                                            return std::nullopt;
                                        }
                                    },
                                    .encode = [](const Order&) { return std::string{}; }};
}

std::string format_json(const Joined& j) {
    std::ostringstream o;
    o << "{";
    bool has_content = false;
    auto sep = [&](std::ostream& os) -> std::ostream& {
        if (has_content) {
            os << ",";
        }
        has_content = true;
        return os;
    };
    if (j.click.has_value()) {
        sep(o) << R"("user":")" << j.click->user_id << R"(","url":")" << j.click->url
               << R"(","click_t":)" << j.click->timestamp_ms;
    } else if (j.order.has_value()) {
        sep(o) << R"("user":")" << j.order->user_id << R"(")";
    }
    if (j.order.has_value()) {
        sep(o) << R"("sku":")" << j.order->sku << R"(","order_t":)" << j.order->timestamp_ms;
    }
    if (!j.click.has_value()) {
        sep(o) << R"("unmatched":"left_only")";
    } else if (!j.order.has_value()) {
        sep(o) << R"("unmatched":"right_only")";
    }
    o << "}";
    return o.str();
}

std::filesystem::path write_default(std::string_view name, std::string_view contents) {
    auto p = std::filesystem::temp_directory_path() / name;
    std::ofstream o(p, std::ios::trunc);
    o << contents;
    return p;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace clink;
    using namespace std::chrono_literals;

    bool full_outer = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == "--full-outer") {
            full_outer = true;
        }
    }

    const auto clicks_path = write_default("clink_clicks.tsv",
                                           "alice\t1000\t/landing\n"
                                           "bob\t1500\t/home\n"
                                           "alice\t2000\t/product/shoe\n"
                                           "carol\t2500\t/about\n");
    const auto orders_path =
        write_default("clink_orders.tsv",
                      "alice\t1100\tshoe\n"        // joins click@1000 (delta=100)
                      "alice\t2050\tshoe-laces\n"  // joins click@2000 (delta=50)
                      "dave\t3000\twrench\n"       // unmatched; carol/bob have clicks but no orders
        );
    const auto out_path = std::filesystem::temp_directory_path() / "clink_join_output.jsonl";

    Dag dag;

    auto src_clicks = std::make_shared<FileSource<Click>>(clicks_path, click_format());
    auto src_orders = std::make_shared<FileSource<Order>>(orders_path, order_format());

    // Each source's records have no inherent event-time; assign one from the
    // record's embedded timestamp_ms field. The assigner also emits
    // monotonic watermarks, which is what drives interval_join eviction.
    auto stamp_clicks = std::make_shared<WatermarkAssignerOperator<Click>>(
        [](const Click& c) { return EventTime{c.timestamp_ms}; },
        std::make_unique<MonotonicWatermarkStrategy<Click>>(),
        "stamp_clicks");
    auto stamp_orders = std::make_shared<WatermarkAssignerOperator<Order>>(
        [](const Order& o) { return EventTime{o.timestamp_ms}; },
        std::make_unique<MonotonicWatermarkStrategy<Order>>(),
        "stamp_orders");

    auto h_clicks_raw = dag.add_source<Click>(src_clicks);
    auto h_orders_raw = dag.add_source<Order>(src_orders);
    auto h_clicks = dag.add_operator<Click, Click>(h_clicks_raw, stamp_clicks);
    auto h_orders = dag.add_operator<Order, Order>(h_orders_raw, stamp_orders);

    auto h_j = dag.interval_join<Click, Order, std::string, Joined>(
        h_clicks,
        h_orders,
        [](const Click& c) { return c.user_id; },
        [](const Order& o) { return o.user_id; },
        50ms,
        500ms,
        [](const std::optional<Click>& c, const std::optional<Order>& o) { return Joined{c, o}; },
        full_outer ? Dag::JoinType::FullOuter : Dag::JoinType::Inner);

    auto fmt = std::make_shared<MapOperator<Joined, std::string>>(format_json, "to_json");
    auto h_fmt = dag.add_operator<Joined, std::string>(h_j, fmt);

    auto sink = std::make_shared<FileSink<std::string>>(out_path, string_text_format());
    dag.add_sink<std::string>(h_fmt, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    std::cout << "Read clicks from " << clicks_path << "\n"
              << "Read orders from " << orders_path << "\n"
              << "Wrote " << (full_outer ? "full-outer" : "inner") << " join output to " << out_path
              << "\n";
    return EXIT_SUCCESS;
}
