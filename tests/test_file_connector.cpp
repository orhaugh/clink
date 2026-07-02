#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/directory_file_source.hpp"
#include "clink/connectors/file_sink.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;

namespace {

std::filesystem::path tmp_path(std::string_view name) {
    return std::filesystem::temp_directory_path() / name;
}

void write_lines(const std::filesystem::path& p, const std::vector<std::string>& lines) {
    std::ofstream o(p, std::ios::trunc);
    for (const auto& l : lines) {
        o << l << '\n';
    }
}

std::vector<std::string> read_lines(const std::filesystem::path& p) {
    std::ifstream in(p);
    std::vector<std::string> out;
    std::string line;
    while (std::getline(in, line)) {
        out.push_back(std::move(line));
    }
    return out;
}

}  // namespace

TEST(FileConnector, SourceReadsLinesIntoCollectingSink) {
    const auto path = tmp_path("clink_file_source.txt");
    write_lines(path, {"alpha", "beta", "gamma", "delta"});

    Dag dag;
    auto src = std::make_shared<FileSource<std::string>>(path,
                                                         string_text_format(),
                                                         /*batch_size*/ 2);
    auto sink = std::make_shared<CollectingSink<std::string>>();

    auto h0 = dag.add_source<std::string>(src);
    dag.add_sink<std::string>(h0, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto got = sink->collected();
    EXPECT_EQ(got, (std::vector<std::string>{"alpha", "beta", "gamma", "delta"}));

    std::filesystem::remove(path);
}

TEST(FileConnector, DirectorySourceReadsAllFilesInSortedOrder) {
    // A directory of files is read wholesale, files traversed in filename order
    // regardless of creation order, so a partitioned backing reads back deterministically.
    const auto dir = std::filesystem::temp_directory_path() / "clink_dir_src";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    write_lines(dir / "c.ndjson", {"delta"});
    write_lines(dir / "a.ndjson", {"alpha", "beta"});
    write_lines(dir / "b.ndjson", {"gamma"});

    Dag dag;
    auto src = std::make_shared<DirectoryFileSource<std::string>>(dir,
                                                                  string_text_format(),
                                                                  /*batch_size*/ 2);
    auto sink = std::make_shared<CollectingSink<std::string>>();
    auto h0 = dag.add_source<std::string>(src);
    dag.add_sink<std::string>(h0, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<std::string>{"alpha", "beta", "gamma", "delta"}));
    std::filesystem::remove_all(dir);
}

TEST(FileConnector, DirectorySourceEmptyDirProducesNoRecords) {
    const auto dir = std::filesystem::temp_directory_path() / "clink_dir_src_empty";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Dag dag;
    auto src = std::make_shared<DirectoryFileSource<std::string>>(dir, string_text_format());
    auto sink = std::make_shared<CollectingSink<std::string>>();
    auto h0 = dag.add_source<std::string>(src);
    dag.add_sink<std::string>(h0, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_TRUE(sink->collected().empty());
    std::filesystem::remove_all(dir);
}

TEST(FileConnector, DirectorySourceRejectsNonDirectory) {
    // Pointed at a plain file, DirectoryFileSource fails open() with a clear message
    // (recorded as an operator error, like the missing-file case).
    const auto path = tmp_path("clink_dir_src_not_a_dir.txt");
    write_lines(path, {"x"});

    auto src = std::make_shared<DirectoryFileSource<std::string>>(path, string_text_format());
    Dag dag;
    auto h0 = dag.add_source<std::string>(src);
    auto sink = std::make_shared<CollectingSink<std::string>>();
    dag.add_sink<std::string>(h0, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    const auto errors = exec.operator_errors();
    bool found = false;
    for (const auto& [op_name, msg] : errors) {
        if (msg.find("not a directory") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
    std::filesystem::remove(path);
}

TEST(FileConnector, SinkWritesLinesUppercase) {
    const auto in_path = tmp_path("clink_file_in.txt");
    const auto out_path = tmp_path("clink_file_out.txt");
    write_lines(in_path, {"foo", "bar", "baz"});

    Dag dag;
    auto src = std::make_shared<FileSource<std::string>>(in_path, string_text_format());
    auto upper = std::make_shared<MapOperator<std::string, std::string>>([](const std::string& s) {
        std::string out = s;
        std::transform(
            out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::toupper(c); });
        return out;
    });
    auto sink = std::make_shared<FileSink<std::string>>(out_path, string_text_format());

    auto h0 = dag.add_source<std::string>(src);
    auto h1 = dag.add_operator<std::string, std::string>(h0, upper);
    dag.add_sink<std::string>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto got = read_lines(out_path);
    EXPECT_EQ(got, (std::vector<std::string>{"FOO", "BAR", "BAZ"}));

    std::filesystem::remove(in_path);
    std::filesystem::remove(out_path);
}

TEST(FileConnector, MissingFileIsRecordedAsOperatorError) {
    // Engine swallows operator open() failures into operator_errors()
    // rather than propagating from run(). Pin this contract for the
    // file-source missing-file case.
    const auto path = tmp_path("clink_does_not_exist_XYZ.txt");
    std::filesystem::remove(path);  // ensure absent

    auto src = std::make_shared<FileSource<std::string>>(path, string_text_format());

    Dag dag;
    auto h0 = dag.add_source<std::string>(src);
    auto sink = std::make_shared<CollectingSink<std::string>>();
    dag.add_sink<std::string>(h0, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    const auto errors = exec.operator_errors();
    ASSERT_FALSE(errors.empty());
    bool found = false;
    for (const auto& [op_name, msg] : errors) {
        if (msg.find("cannot open") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
    EXPECT_TRUE(sink->collected().empty());
}

TEST(FileConnector, EmptyFileProducesNoRecords) {
    const auto path = tmp_path("clink_empty.txt");
    write_lines(path, {});

    Dag dag;
    auto src = std::make_shared<FileSource<std::string>>(path, string_text_format());
    auto sink = std::make_shared<CollectingSink<std::string>>();
    auto h0 = dag.add_source<std::string>(src);
    dag.add_sink<std::string>(h0, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_TRUE(sink->collected().empty());
    std::filesystem::remove(path);
}

TEST(FileConnector, SourceConstructorRejectsMissingDecoder) {
    const auto path = tmp_path("clink_x.txt");
    write_lines(path, {"line"});
    TextFormat<int> bad_format;  // no decode set
    bad_format.encode = [](const int& v) { return std::to_string(v); };

    EXPECT_THROW(FileSource<int>(path, bad_format), std::invalid_argument);
    std::filesystem::remove(path);
}

TEST(FileConnector, DecoderCanFilterAndParse) {
    const auto path = tmp_path("clink_file_int.txt");
    write_lines(path, {"1", "2", "not-a-number", "4"});

    TextFormat<int> int_format;
    int_format.decode = [](std::string_view sv) -> std::optional<int> {
        try {
            return std::stoi(std::string{sv});
        } catch (...) {
            return std::nullopt;  // skip malformed lines
        }
    };
    int_format.encode = [](const int& v) { return std::to_string(v); };

    Dag dag;
    auto src = std::make_shared<FileSource<int>>(path, int_format);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    dag.add_sink<int>(h0, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<int>{1, 2, 4}));

    std::filesystem::remove(path);
}
