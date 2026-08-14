// In-tree instantiations of the public source contract suite
// (clink/test/source_contract.hpp): the file family and Parquet. Each
// adapter is a worked example of the shape a consumer writes for their own
// source; the suite itself derives the replay obligations from the
// connector's ConnectorCapabilities record, so these instantiations are
// also what keeps those records honest.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/directory_file_source.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/connectors/parquet_sink.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/test/source_contract.hpp"

namespace {

using clink::test::MalformedInputPolicy;
using clink::test::SourceContractFixture;
using clink::test::SourceContractSuite;

// Built-ins declare their capability records from the same call that
// registers their factories; the suite reads those records.
struct RegisterBuiltIns : ::testing::Environment {
    void SetUp() override { clink::cluster::ensure_built_ins_registered(); }
};
const auto* const kEnv = ::testing::AddGlobalTestEnvironment(new RegisterBuiltIns);

void write_lines(const std::filesystem::path& p, const std::vector<std::string>& lines) {
    std::ofstream out(p, std::ios::trunc);
    for (const auto& l : lines) {
        out << l << "\n";
    }
}

// A decoding format with a genuine malformed case: a line is a record iff
// it starts with "ok:". string_text_format() accepts every line, which
// would make "malformed" unrepresentable and the policy case vacuous.
clink::TextFormat<std::string> ok_prefixed_format() {
    clink::TextFormat<std::string> f;
    f.decode = [](std::string_view line) -> std::optional<std::string> {
        if (line.rfind("ok:", 0) != 0) {
            return std::nullopt;
        }
        return std::string{line.substr(3)};
    };
    f.encode = [](const std::string& v) { return "ok:" + v; };
    return f;
}

std::vector<std::string> values(std::size_t count) {
    std::vector<std::string> out;
    out.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        out.push_back("v" + std::to_string(i));
    }
    return out;
}

// --- FileSource --------------------------------------------------------------

struct FileSourceContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "file";
    // TextFormat::decode returning nullopt is the documented filter
    // semantic: an unparseable line is skipped, the rest of the file is
    // still good.
    static constexpr MalformedInputPolicy kMalformedPolicy = MalformedInputPolicy::Skip;

    static SourceContractFixture<std::string> fixture_over(const std::filesystem::path& file,
                                                           std::vector<std::string> expected) {
        SourceContractFixture<std::string> fx;
        fx.expected = std::move(expected);
        // batch_size 2 on purpose: produce boundaries then differ from
        // record boundaries, so the replay case cuts mid-batch-run rather
        // than at the trivially easy per-record points.
        fx.fresh = [file] {
            return std::make_unique<clink::FileSource<std::string>>(
                file, ok_prefixed_format(), /*batch_size=*/2);
        };
        return fx;
    }

    static SourceContractFixture<std::string> make(const std::filesystem::path& dir,
                                                   std::size_t count) {
        const auto file = dir / "input.txt";
        const auto vals = values(count);
        std::vector<std::string> lines;
        lines.reserve(vals.size());
        for (const auto& v : vals) {
            lines.push_back("ok:" + v);
        }
        write_lines(file, lines);
        return fixture_over(file, vals);
    }

    static std::optional<SourceContractFixture<std::string>> make_with_malformed(
        const std::filesystem::path& dir) {
        const auto file = dir / "with_malformed.txt";
        write_lines(file, {"ok:a", "### not a record ###", "ok:b"});
        return fixture_over(file, {"a", "b"});
    }

    static std::optional<SourceContractFixture<std::string>> make_oversized(
        const std::filesystem::path& dir) {
        const auto file = dir / "oversized.txt";
        // 4 MiB single record: far past the source's internal read chunk,
        // so delivery requires the line assembly to be genuinely unbounded.
        const std::string huge(4u * 1024 * 1024, 'x');
        write_lines(file, {"ok:a", "ok:" + huge, "ok:b"});
        return fixture_over(file, {"a", huge, "b"});
    }
};

// --- DirectoryFileSource ------------------------------------------------------

struct DirectoryFileSourceContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "file";
    static constexpr MalformedInputPolicy kMalformedPolicy = MalformedInputPolicy::Skip;

    static SourceContractFixture<std::string> fixture_over(const std::filesystem::path& dir,
                                                           std::vector<std::string> expected) {
        SourceContractFixture<std::string> fx;
        fx.expected = std::move(expected);
        fx.fresh = [dir] {
            return std::make_unique<clink::DirectoryFileSource<std::string>>(
                dir, ok_prefixed_format(), /*batch_size=*/2);
        };
        return fx;
    }

    // Records spread over two files, so the replay cuts cross the
    // file-to-file transition - the offset here is (file, line) shaped and
    // the transition is exactly where a naive one loses or repeats.
    static SourceContractFixture<std::string> make(const std::filesystem::path& dir,
                                                   std::size_t count) {
        const auto in = dir / "input_dir";
        std::filesystem::create_directories(in);
        const auto vals = values(count);
        std::vector<std::string> first;
        std::vector<std::string> second;
        for (std::size_t i = 0; i < vals.size(); ++i) {
            (i < vals.size() / 2 ? first : second).push_back("ok:" + vals[i]);
        }
        write_lines(in / "a.txt", first);
        write_lines(in / "b.txt", second);
        return fixture_over(in, vals);
    }

    static std::optional<SourceContractFixture<std::string>> make_with_malformed(
        const std::filesystem::path& dir) {
        const auto in = dir / "malformed_dir";
        std::filesystem::create_directories(in);
        write_lines(in / "a.txt", {"ok:a", "### not a record ###"});
        write_lines(in / "b.txt", {"ok:b"});
        return fixture_over(in, {"a", "b"});
    }

    static std::optional<SourceContractFixture<std::string>> make_oversized(
        const std::filesystem::path& dir) {
        const auto in = dir / "oversized_dir";
        std::filesystem::create_directories(in);
        const std::string huge(4u * 1024 * 1024, 'x');
        write_lines(in / "a.txt", {"ok:a", "ok:" + huge});
        write_lines(in / "b.txt", {"ok:b"});
        return fixture_over(in, {"a", huge, "b"});
    }
};

// --- ParquetSource ------------------------------------------------------------

struct ParquetSourceContract {
    using Value = std::int64_t;
    static constexpr std::string_view kCapabilityName = "parquet";
    // Parquet framing is structural: a file that does not parse has no
    // safe remainder to continue with, so the source refuses it.
    static constexpr MalformedInputPolicy kMalformedPolicy = MalformedInputPolicy::Refuse;

    static SourceContractFixture<std::int64_t> fixture_over(const std::filesystem::path& file,
                                                            std::vector<std::int64_t> expected) {
        SourceContractFixture<std::int64_t> fx;
        fx.expected = std::move(expected);
        fx.fresh = [file] {
            return std::make_unique<clink::ParquetSource<std::int64_t>>(
                file, clink::int64_arrow_batcher());
        };
        return fx;
    }

    static SourceContractFixture<std::int64_t> make(const std::filesystem::path& dir,
                                                    std::size_t count) {
        const auto file = dir / "input.parquet";
        std::vector<std::int64_t> vals;
        clink::ParquetSink<std::int64_t> sink(file, clink::int64_arrow_batcher());
        sink.open();
        // One batch per row group. Note what the suite then reports:
        // ParquetSource::produce drains every row group in a single call,
        // so the only produce boundaries are start and end - the replay
        // case still holds at both (and the mutation check proved it
        // catches a no-op restore there), but parquet has no mid-stream
        // cut for the runtime to snapshot at within one file.
        for (std::size_t i = 0; i < count; i += 2) {
            clink::Batch<std::int64_t> batch;
            for (std::size_t j = i; j < std::min(count, i + 2); ++j) {
                const auto v = static_cast<std::int64_t>(j * 10);
                batch.push(clink::Record<std::int64_t>{v});
                vals.push_back(v);
            }
            sink.on_data(batch);
        }
        sink.close();
        return fixture_over(file, vals);
    }

    static std::optional<SourceContractFixture<std::int64_t>> make_with_malformed(
        const std::filesystem::path& dir) {
        const auto file = dir / "corrupt.parquet";
        write_lines(file, {"this is not a parquet file"});
        auto fx = fixture_over(file, {});
        return fx;
    }

    static std::optional<SourceContractFixture<std::int64_t>> make_oversized(
        const std::filesystem::path&) {
        // Fixed-width int64 records cannot be oversized.
        return std::nullopt;
    }
};

}  // namespace

// The INSTANTIATE macro references the suite's registration symbols
// unqualified, so it must live in the suite's own namespace; the adapters
// in the anonymous namespace above are still found by enclosing-scope
// lookup.
namespace clink::test {

INSTANTIATE_TYPED_TEST_SUITE_P(FileSource, SourceContractSuite, FileSourceContract);
INSTANTIATE_TYPED_TEST_SUITE_P(DirectoryFileSource,
                               SourceContractSuite,
                               DirectoryFileSourceContract);
INSTANTIATE_TYPED_TEST_SUITE_P(ParquetSource, SourceContractSuite, ParquetSourceContract);

}  // namespace clink::test
