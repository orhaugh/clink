// Unit tests for OutputTag-based side outputs.
//
// Verifies the wiring path Dag::side_output<T>() + RuntimeContext::
// side_output<T>(tag) + an operator's emit-to-side-output behaviour
// when run end-to-end via LocalExecutor.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/output_tag.hpp"

namespace {

using namespace clink;

// Validating operator: forwards even values to the main output and
// odd values to a side output channel typed as std::string ("odd-<v>").
class ValidatingOperator final : public Operator<std::int64_t, std::int64_t> {
public:
    explicit ValidatingOperator(OutputTag<std::string> odd_tag) : odd_tag_(std::move(odd_tag)) {}

    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            if (el.is_watermark()) {
                out.emit_watermark(el.as_watermark());
            } else {
                out.emit_barrier(el.as_barrier());
            }
            return;
        }
        Batch<std::int64_t> main_batch;
        Batch<std::string> side_batch;
        for (const auto& r : el.as_data()) {
            if (r.value() % 2 == 0) {
                main_batch.emplace(r.value());
            } else {
                side_batch.emplace("odd-" + std::to_string(r.value()));
            }
        }
        if (!main_batch.empty()) {
            out.emit_data(std::move(main_batch));
        }
        if (!side_batch.empty()) {
            auto side = this->runtime()->side_output<std::string>(odd_tag_);
            side.emit_data(std::move(side_batch));
        }
    }
    std::string name() const override { return "validating_op"; }

private:
    OutputTag<std::string> odd_tag_;
};

class VectorSource final : public Source<std::int64_t> {
public:
    explicit VectorSource(std::vector<std::int64_t> items) : items_(std::move(items)) {}
    bool produce(Emitter<std::int64_t>& out) override {
        if (emitted_) {
            return false;
        }
        Batch<std::int64_t> b;
        for (auto v : items_) {
            b.emplace(v);
        }
        out.emit_data(std::move(b));
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "vector_src"; }

private:
    std::vector<std::int64_t> items_;
    bool emitted_{false};
};

template <typename T>
class CollectingSink final : public Sink<T> {
public:
    void on_data(const Batch<T>& batch) override {
        for (const auto& r : batch) {
            received_.push_back(r.value());
        }
    }
    std::string name() const override { return "collecting_sink"; }
    std::vector<T> received_;
};

TEST(SideOutput, MainAndSideStreamsReachTheirOwnSinks) {
    OutputTag<std::string> odd_tag("odd");
    Dag dag;
    auto src = std::make_shared<VectorSource>(std::vector<std::int64_t>{1, 2, 3, 4, 5, 6});
    auto op = std::make_shared<ValidatingOperator>(odd_tag);
    auto main_sink = std::make_shared<CollectingSink<std::int64_t>>();
    auto side_sink = std::make_shared<CollectingSink<std::string>>();

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
    auto h_side = dag.side_output<std::string>(h1, odd_tag);
    dag.add_sink<std::int64_t>(h1, main_sink);
    dag.add_sink<std::string>(h_side, side_sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(main_sink->received_, (std::vector<std::int64_t>{2, 4, 6}));
    EXPECT_EQ(side_sink->received_, (std::vector<std::string>{"odd-1", "odd-3", "odd-5"}));
}

// A sink that records the control elements it saw, not just the data.
// The question below is not "did records arrive" - the test above answers
// that - but "did the CONTROL stream arrive too".
template <typename T>
class ControlAwareSink final : public Sink<T> {
public:
    void on_data(const Batch<T>& batch) override {
        for (const auto& r : batch) {
            received_.push_back(r.value());
        }
    }
    void on_watermark(Watermark wm) override { watermarks_.push_back(wm); }
    void on_barrier(CheckpointBarrier b) override { barriers_.push_back(b.id()); }
    std::string name() const override { return "control_aware_sink"; }

    std::vector<T> received_;
    std::vector<Watermark> watermarks_;
    std::vector<CheckpointId> barriers_;
};

// Emits a barrier and a watermark, then data, then closes.
class ControlThenDataSource final : public Source<std::int64_t> {
public:
    bool produce(Emitter<std::int64_t>& out) override {
        if (done_) {
            return false;
        }
        out.emit_barrier(CheckpointBarrier(CheckpointId{7}));
        out.emit_watermark(Watermark{EventTime{1234}});
        Batch<std::int64_t> b;
        b.emplace(1);  // odd -> side output
        b.emplace(2);  // even -> main output
        out.emit_data(std::move(b));
        done_ = true;
        return false;
    }
    std::string name() const override { return "control_then_data_src"; }

private:
    bool done_{false};
};

TEST(SideOutput, BarriersAndWatermarksReachASideOutputSinkToo) {
    // A side output is not a lesser output. A 2PC sink on a side branch
    // needs the barrier to stage and the commit to publish; a windowed
    // operator on one needs the watermark to fire. If the control stream
    // only follows the main edge, a side branch silently never
    // checkpoints and never advances time - and every data-routing test,
    // including the one above, still passes.
    OutputTag<std::string> odd_tag("odd");
    Dag dag;
    auto src = std::make_shared<ControlThenDataSource>();
    auto op = std::make_shared<ValidatingOperator>(odd_tag);
    auto main_sink = std::make_shared<ControlAwareSink<std::int64_t>>();
    auto side_sink = std::make_shared<ControlAwareSink<std::string>>();

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
    auto h_side = dag.side_output<std::string>(h1, odd_tag);
    dag.add_sink<std::int64_t>(h1, main_sink);
    dag.add_sink<std::string>(h_side, side_sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // The main branch is the control: if IT missed them, the test is
    // wrong rather than the engine.
    ASSERT_EQ(main_sink->barriers_.size(), 1U) << "the main sink did not see the barrier either";
    EXPECT_EQ(main_sink->barriers_[0], CheckpointId{7});
    ASSERT_FALSE(main_sink->watermarks_.empty());

    EXPECT_EQ(side_sink->barriers_.size(), 1U)
        << "the side-output sink never saw the checkpoint barrier, so a 2PC sink on a side "
           "branch would stage nothing and commit nothing";
    if (!side_sink->barriers_.empty()) {
        EXPECT_EQ(side_sink->barriers_[0], CheckpointId{7});
    }
    EXPECT_FALSE(side_sink->watermarks_.empty())
        << "the side-output sink never saw a watermark, so an event-time operator on a side "
           "branch would never fire";

    // And the data still routed as before, so nothing above passed by
    // virtue of the branch being dead.
    EXPECT_EQ(main_sink->received_, (std::vector<std::int64_t>{2}));
    EXPECT_EQ(side_sink->received_, (std::vector<std::string>{"odd-1"}));
}

TEST(SideOutput, UnregisteredTagThrows) {
    OutputTag<std::string> bad_tag("nope");
    Dag dag;
    auto src = std::make_shared<VectorSource>(std::vector<std::int64_t>{1});
    auto op = std::make_shared<ValidatingOperator>(bad_tag);
    auto main_sink = std::make_shared<CollectingSink<std::int64_t>>();

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
    // Note: NOT registering side_output<std::string>(h1, bad_tag).
    dag.add_sink<std::int64_t>(h1, main_sink);

    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto errors = exec.operator_errors();
    ASSERT_FALSE(errors.empty());
    bool found = false;
    for (const auto& [op_name, msg] : errors) {
        if (msg.find("side_output") != std::string::npos && msg.find("nope") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "expected a side_output 'nope' error in operator errors";
}

TEST(SideOutput, RegisteringSameTagTwiceThrows) {
    OutputTag<std::string> tag("dup");
    Dag dag;
    auto src = std::make_shared<VectorSource>(std::vector<std::int64_t>{1});
    auto op = std::make_shared<ValidatingOperator>(tag);
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
    (void)dag.side_output<std::string>(h1, tag);
    EXPECT_THROW(dag.side_output<std::string>(h1, tag), std::runtime_error);
}

// A no-op identity operator used as the second op in the chain. Its
// only job in this test is to prove that wrapping ValidatingOperator
// inside ChainedOperator doesn't disturb its side-output behaviour.
class IdentityOperator final : public Operator<std::int64_t, std::int64_t> {
public:
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (el.is_data()) {
            out.emit_data(el.as_data());
        } else if (el.is_watermark()) {
            out.emit_watermark(el.as_watermark());
        } else {
            out.emit_barrier(el.as_barrier());
        }
    }
    std::string name() const override { return "identity"; }
};

// Regression test for the fix in operator_base.hpp:ChainedOperator::
// open(): without the side-output-channel propagation, an inner op
// emitting via runtime()->side_output<T>(tag) throws because its
// freshly-minted RC has an empty channel map. With the propagation,
// the chained op behaves identically to the non-chained case.
TEST(SideOutput, InnerOpInChainedOperatorReachesSideOutputSink) {
    OutputTag<std::string> odd_tag("odd-from-chained");
    Dag dag;
    auto src = std::make_shared<VectorSource>(std::vector<std::int64_t>{1, 2, 3, 4, 5});
    auto inner_a = std::make_shared<ValidatingOperator>(odd_tag);
    auto inner_b = std::make_shared<IdentityOperator>();
    auto chained = std::make_shared<ChainedOperator<std::int64_t, std::int64_t, std::int64_t>>(
        inner_a, inner_b);
    auto main_sink = std::make_shared<CollectingSink<std::int64_t>>();
    auto side_sink = std::make_shared<CollectingSink<std::string>>();

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, chained);
    auto h_side = dag.side_output<std::string>(h1, odd_tag);
    dag.add_sink<std::int64_t>(h1, main_sink);
    dag.add_sink<std::string>(h_side, side_sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(main_sink->received_, (std::vector<std::int64_t>{2, 4}));
    EXPECT_EQ(side_sink->received_, (std::vector<std::string>{"odd-1", "odd-3", "odd-5"}));
}

}  // namespace
