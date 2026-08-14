// In-tree instantiations of the public sink contract suite
// (clink/test/sink_contract.hpp): the file and Parquet 2PC sinks. The
// suite drives the real CommittingSink choreography against a shared
// InMemoryStateBackend, so the crash cases are the actual
// prepared-handle-persist / recover-at-open protocol, not a re-enactment.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/file_2pc_sink.hpp"
#include "clink/connectors/parquet_2pc_sink.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/test/output_capture.hpp"
#include "clink/test/sink_contract.hpp"

namespace {

using clink::test::SinkContractFixture;
using clink::test::SinkContractSuite;

struct RegisterBuiltInsForSinks : ::testing::Environment {
    void SetUp() override { clink::cluster::ensure_built_ins_registered(); }
};
const auto* const kSinkEnv = ::testing::AddGlobalTestEnvironment(new RegisterBuiltInsForSinks);

// --- file_2pc -----------------------------------------------------------------

struct FileSink2PCContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "file_2pc";

    static SinkContractFixture<std::string> make(const std::filesystem::path& dir) {
        const auto out = dir / "out";
        SinkContractFixture<std::string> fx;
        fx.records = {"alpha", "beta", "gamma", "delta"};
        fx.fresh = [out] {
            return std::make_shared<clink::FileSink2PC<std::string>>(
                out, clink::string_text_format(), /*subtask_idx=*/0, "file_2pc_sink_string");
        };
        fx.committed = [out] {
            std::vector<std::string> lines;
            std::error_code ec;
            for (const auto& e : std::filesystem::directory_iterator(out / "committed", ec)) {
                if (!e.is_regular_file()) {
                    continue;
                }
                std::ifstream in(e.path());
                std::string line;
                while (std::getline(in, line)) {
                    if (!line.empty()) {
                        lines.push_back(line);
                    }
                }
            }
            return lines;
        };
        return fx;
    }
};

// --- parquet_2pc --------------------------------------------------------------

struct ParquetSink2PCContract {
    using Value = std::int64_t;
    static constexpr std::string_view kCapabilityName = "parquet_2pc";

    static SinkContractFixture<std::int64_t> make(const std::filesystem::path& dir) {
        const auto out = dir / "out";
        SinkContractFixture<std::int64_t> fx;
        fx.records = {11, 22, 33, 44};
        fx.fresh = [out] {
            return std::make_shared<clink::ParquetSink2PC<std::int64_t>>(
                out, clink::int64_arrow_batcher(), /*subtask_idx=*/0);
        };
        fx.committed = [out] {
            std::vector<std::int64_t> values;
            std::error_code ec;
            for (const auto& e : std::filesystem::directory_iterator(out / "committed", ec)) {
                if (!e.is_regular_file()) {
                    continue;
                }
                clink::ParquetSource<std::int64_t> reader(e.path(), clink::int64_arrow_batcher());
                reader.open();
                clink::test::OutputCapture<std::int64_t> cap;
                while (reader.produce(cap.emitter())) {
                }
                reader.close();
                for (auto& v : cap.values()) {
                    values.push_back(v);
                }
            }
            return values;
        };
        return fx;
    }
};

}  // namespace

namespace clink::test {

INSTANTIATE_TYPED_TEST_SUITE_P(FileSink2PC, SinkContractSuite, FileSink2PCContract);
INSTANTIATE_TYPED_TEST_SUITE_P(ParquetSink2PC, SinkContractSuite, ParquetSink2PCContract);

}  // namespace clink::test
