#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>
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
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "src/cluster/built_in_file_sinks.hpp"

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

namespace {

// A path of this test's own, so parallel ctest processes never share a file.
std::filesystem::path own_path(std::string_view leaf) {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::filesystem::temp_directory_path() /
           ("clink_" + std::string{info->name()} + "_" + std::to_string(::getpid()) + "_" +
            std::string{leaf});
}

std::string file_text(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

Batch<std::string> line_batch(const std::vector<std::string>& lines) {
    Batch<std::string> b;
    for (const auto& l : lines) {
        b.emplace(l);
    }
    return b;
}

}  // namespace

// Every line before a barrier is in the file once the barrier returns. The
// lines used to wait in the stream's buffer until it filled or the input
// ended, so a checkpoint could complete over lines that a killed process then
// lost, while the restored run kept the file and resumed after them.
TEST(FileConnector, ABarrierWritesOutEveryLineBeforeIt) {
    const auto path = own_path("out.txt");
    FileSink<std::string> sink(path, string_text_format());
    EXPECT_TRUE(sink.gates_checkpoint_ack());
    sink.open();
    sink.on_data(line_batch({"a", "b"}));
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_EQ(file_text(path), "a\nb\n");
    sink.close();
    std::filesystem::remove(path);
}

// A restored run cuts the partial last line a kill left behind before it
// appends. It used to append straight after the fragment, gluing the replayed
// line to it, so both were corrupt.
TEST(FileConnector, ARestoredRunCutsAPartialLastLineBeforeAppending) {
    const auto path = own_path("out.txt");
    {
        std::ofstream o(path, std::ios::binary);
        o << "a\nb\npartial-ag";
    }
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{7}, "file_sink", &state, nullptr);
    rctx.set_restore_from_checkpoint_id(1);
    FileSink<std::string> sink(path, string_text_format());
    sink.attach_runtime(&rctx);
    sink.open();
    sink.on_data(line_batch({"partial-again", "c"}));
    sink.flush();
    sink.close();
    EXPECT_EQ(file_text(path), "a\nb\npartial-again\nc\n");
    std::filesystem::remove(path);
}

// A write the disk refuses fails the sink at the barrier, instead of leaving a
// gap in the file that the checkpoint would then cover. /dev/full refuses
// every write.
TEST(FileConnector, AWriteTheDiskRefusesFailsTheBarrier) {
    if (!std::filesystem::exists("/dev/full")) {
        GTEST_SKIP() << "needs /dev/full, which Linux provides";
    }
    FileSink<std::string> sink("/dev/full", string_text_format());
    sink.open();
    sink.on_data(line_batch({"a"}));
    EXPECT_THROW(sink.on_barrier(CheckpointBarrier{CheckpointId{1}}), std::runtime_error);
}

// The built-in line sinks (file_line_sink, file_int64_sink, registered into
// both registries from these classes) keep their file in a restored run, as
// the at-least-once "file" capability they are analysed under requires. They
// used to truncate it on every open, so a restore erased every line before
// its restore point, which the sources do not replay.
TEST(FileConnector, TheBuiltInLineSinksKeepTheirFileInARestoredRun) {
    const auto lines = own_path("lines.txt");
    const auto ints = own_path("ints.txt");
    {
        std::ofstream(lines, std::ios::binary) << "a\nb\nhal";
        std::ofstream(ints, std::ios::binary) << "1\n2\n";
    }
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{8}, "line_sinks", &state, nullptr);
    rctx.set_restore_from_checkpoint_id(1);

    clink::cluster::detail::FileLineSink line_sink(lines.string());
    EXPECT_TRUE(line_sink.gates_checkpoint_ack());
    line_sink.attach_runtime(&rctx);
    line_sink.open();
    line_sink.on_data(line_batch({"c"}));
    line_sink.on_barrier(CheckpointBarrier{CheckpointId{2}});
    EXPECT_EQ(file_text(lines), "a\nb\nc\n") << "kept, torn tail cut, flushed at the barrier";
    line_sink.close();

    clink::cluster::detail::FileInt64Sink int_sink(ints.string());
    int_sink.attach_runtime(&rctx);
    int_sink.open();
    Batch<std::int64_t> more;
    more.emplace(std::int64_t{3});
    int_sink.on_data(more);
    int_sink.flush();
    int_sink.close();
    EXPECT_EQ(file_text(ints), "1\n2\n3\n");

    // A fresh run still starts the file over.
    clink::cluster::detail::FileLineSink fresh(lines.string());
    fresh.open();
    fresh.on_data(line_batch({"z"}));
    fresh.flush();
    fresh.close();
    EXPECT_EQ(file_text(lines), "z\n");
    std::filesystem::remove(lines);
    std::filesystem::remove(ints);
}
