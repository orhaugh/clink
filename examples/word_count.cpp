// Word count over a bounded synthetic input.
//
// This example wires:
//   VectorSource<std::string>
//     -> MapOperator<std::string, std::string>   (lowercase + strip punctuation)
//     -> KeyByOperator<std::string, std::string> (key = the word itself)
//     -> TumblingWindowOperator<...>             (count occurrences per word)
//     -> FunctionSink<...>                       (stdout)
//
// Because all inputs share an event time of 0 and the source emits
// Watermark::max() at end-of-stream, the tumbling window fires exactly once at
// stream completion, producing a single (word, count) pair per word.

#include <chrono>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "clink/operators/key_by_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

std::string normalize(const std::string& word) {
    std::string out;
    out.reserve(word.size());
    for (char c : word) {
        if (std::isalpha(static_cast<unsigned char>(c))) {
            out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
    }
    return out;
}

std::vector<std::string> tokenize(const std::string& text) {
    std::vector<std::string> tokens;
    std::istringstream is(text);
    std::string token;
    while (is >> token) {
        std::string norm = normalize(token);
        if (!norm.empty()) {
            tokens.push_back(std::move(norm));
        }
    }
    return tokens;
}

}  // namespace

int main() {
    using namespace clink;
    using namespace std::chrono_literals;

    const std::string text =
        "the quick brown fox jumps over the lazy dog "
        "the quick blue cat sleeps under the lazy dog";

    auto tokens = tokenize(text);

    std::vector<Record<std::string>> input;
    input.reserve(tokens.size());
    for (auto& t : tokens) {
        input.emplace_back(Record<std::string>{std::move(t), EventTime{0}});
    }

    Dag dag;
    auto src = std::make_shared<VectorSource<std::string>>(std::move(input));
    auto key_by = std::make_shared<KeyByOperator<std::string, std::string>>(
        [](const std::string& w) { return w; });
    auto window = std::make_shared<TumblingWindowOperator<std::string, std::string, std::uint64_t>>(
        std::chrono::hours{24},
        []() -> std::uint64_t { return 0; },
        [](const std::uint64_t& acc, const std::string& /*v*/) -> std::uint64_t {
            return acc + 1;
        });

    auto sink = std::make_shared<FunctionSink<std::pair<std::string, std::uint64_t>>>(
        [](const std::pair<std::string, std::uint64_t>& kv) {
            std::cout << kv.first << " : " << kv.second << '\n';
        });

    auto h0 = dag.add_source<std::string>(src);
    auto h1 = dag.add_operator<std::string, std::pair<std::string, std::string>>(h0, key_by);
    auto h2 = dag.add_operator<std::pair<std::string, std::string>,
                               std::pair<std::string, std::uint64_t>>(h1, window);
    dag.add_sink<std::pair<std::string, std::uint64_t>>(h2, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    return 0;
}
