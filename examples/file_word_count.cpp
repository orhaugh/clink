// File-driven word count.
//
// Reads newline-delimited text from a file (path passed on argv, or a default
// embedded sample), tokenizes each line into words, counts per word, and
// writes "word\tcount" lines to a sink file. Demonstrates:
//
//   FileSource<string>  -> FlatMap<string, string>   (tokenize)
//                        -> KeyBy<string, string>
//                        -> TumblingWindow<...>      (24h window, fires at EOS)
//                        -> Map<pair, string>        (format as TSV)
//                        -> FileSink<string>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/file_sink.hpp"
#include "clink/connectors/file_source.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/operators/flat_map_operator.hpp"
#include "clink/operators/key_by_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

std::vector<std::string> tokenize(const std::string& line) {
    std::vector<std::string> out;
    std::string current;
    for (char c : line) {
        if (std::isalpha(static_cast<unsigned char>(c)) != 0) {
            current.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        } else if (!current.empty()) {
            out.push_back(std::move(current));
            current.clear();
        }
    }
    if (!current.empty()) {
        out.push_back(std::move(current));
    }
    return out;
}

std::filesystem::path write_default_input() {
    auto p = std::filesystem::temp_directory_path() / "clink_word_count_input.txt";
    std::ofstream o(p, std::ios::trunc);
    o << "the quick brown fox jumps over the lazy dog\n"
      << "the quick blue cat sleeps under the lazy dog\n"
      << "every good boy deserves fudge\n";
    return p;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace clink;
    using namespace std::chrono_literals;

    const std::filesystem::path in_path =
        (argc >= 2) ? std::filesystem::path{argv[1]} : write_default_input();
    const std::filesystem::path out_path =
        (argc >= 3) ? std::filesystem::path{argv[2]}
                    : std::filesystem::temp_directory_path() / "clink_word_count_output.txt";

    Dag dag;

    auto src = std::make_shared<FileSource<std::string>>(in_path,
                                                         string_text_format(),
                                                         /*batch_size*/ 256);
    auto tokenizer = std::make_shared<FlatMapOperator<std::string, std::string>>(
        [](const std::string& line) { return tokenize(line); });
    auto key_by = std::make_shared<KeyByOperator<std::string, std::string>>(
        [](const std::string& w) { return w; });
    auto window = std::make_shared<TumblingWindowOperator<std::string, std::string, std::uint64_t>>(
        std::chrono::hours{24},
        []() -> std::uint64_t { return 0; },
        [](const std::uint64_t& acc, const std::string& /*v*/) { return acc + 1; });
    auto formatter =
        std::make_shared<MapOperator<std::pair<std::string, std::uint64_t>, std::string>>(
            [](const std::pair<std::string, std::uint64_t>& kv) {
                return kv.first + "\t" + std::to_string(kv.second);
            });
    auto sink = std::make_shared<FileSink<std::string>>(out_path, string_text_format());

    auto h0 = dag.add_source<std::string>(src);
    auto h1 = dag.add_operator<std::string, std::string>(h0, tokenizer);
    auto h2 = dag.add_operator<std::string, std::pair<std::string, std::string>>(h1, key_by);
    auto h3 = dag.add_operator<std::pair<std::string, std::string>,
                               std::pair<std::string, std::uint64_t>>(h2, window);
    auto h4 = dag.add_operator<std::pair<std::string, std::uint64_t>, std::string>(h3, formatter);
    dag.add_sink<std::string>(h4, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    std::cout << "wrote counts to " << out_path << '\n';
    return EXIT_SUCCESS;
}
