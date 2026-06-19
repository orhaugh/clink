// SplitByVariantOperator tests. Contract:
//   * Each variant alternative lands in its matching typed side output,
//     and nowhere else.
//   * The main output passes the full variant stream through unchanged
//     so consumers can attach diagnostic taps.
//
// The point of this operator is to give clink users a generic
// fan-out-by-tag building block - every codebase doing typed routing
// (typed CDC dispatch, typed event-class routing, etc.) ends up
// hand-rolling something like this. v1 routes only on compile-time
// variant alternative; routing on a runtime discriminator (e.g. the
// CDC `event.table` field) is a separate operator class.

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/split_by_variant_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/output_tag.hpp"

using namespace clink;

namespace {

using V = std::variant<std::int64_t, std::string>;

class VariantSource final : public Source<V> {
public:
    explicit VariantSource(std::vector<V> items) : items_(std::move(items)) {}
    bool produce(Emitter<V>& out) override {
        if (emitted_) {
            return false;
        }
        Batch<V> b;
        for (auto& v : items_) {
            b.emplace(v);
        }
        out.emit_data(std::move(b));
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "variant_source"; }

private:
    std::vector<V> items_;
    bool emitted_{false};
};

template <typename T>
class LocalCollectingSink final : public Sink<T> {
public:
    void on_data(const Batch<T>& batch) override {
        for (const auto& r : batch) {
            received_.push_back(r.value());
        }
    }
    std::string name() const override { return "collecting_sink"; }
    std::vector<T> received_;
};

}  // namespace

TEST(SplitByVariantOperator, EachAlternativeReachesMatchingSideOutputOnly) {
    OutputTag<std::int64_t> tag_int("ints");
    OutputTag<std::string> tag_str("strs");

    Dag dag;
    auto src = std::make_shared<VariantSource>(std::vector<V>{
        V{std::int64_t{1}},
        V{std::string{"a"}},
        V{std::int64_t{2}},
        V{std::string{"b"}},
        V{std::int64_t{3}},
    });
    auto split = std::make_shared<SplitByVariantOperator<V, std::int64_t, std::string>>(
        std::make_tuple(tag_int, tag_str));
    auto main_sink = std::make_shared<LocalCollectingSink<V>>();
    auto int_sink = std::make_shared<LocalCollectingSink<std::int64_t>>();
    auto str_sink = std::make_shared<LocalCollectingSink<std::string>>();

    auto h_src = dag.add_source<V>(src);
    auto h_op = dag.add_operator<V, V>(h_src, split);
    auto h_int = dag.side_output<std::int64_t>(h_op, tag_int);
    auto h_str = dag.side_output<std::string>(h_op, tag_str);
    dag.add_sink<V>(h_op, main_sink);
    dag.add_sink<std::int64_t>(h_int, int_sink);
    dag.add_sink<std::string>(h_str, str_sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(int_sink->received_, (std::vector<std::int64_t>{1, 2, 3}));
    EXPECT_EQ(str_sink->received_, (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(main_sink->received_.size(), 5u);
}
