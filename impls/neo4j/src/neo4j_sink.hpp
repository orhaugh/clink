#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"
#include "reply.hpp"

namespace clink::neo4j {

// Ends the request in flight when the task is cancelled or the attempt's
// deadline passes. One thread per sink, asleep unless a request is in flight;
// it touches nothing but HttpRequest::interrupt(), so it can hide no error and
// holds no data. Joined by stop() and the destructor.
class RequestWatch {
public:
    enum class Why : std::uint8_t { None, Cancelled, Deadline };

    explicit RequestWatch(clink::http_connector::HttpRequest& http);
    ~RequestWatch();
    RequestWatch(const RequestWatch&) = delete;
    RequestWatch& operator=(const RequestWatch&) = delete;

    void arm(std::chrono::steady_clock::time_point deadline, CancelSignal cancel);
    // Why the watch interrupted the request armed last, if it did.
    Why disarm();
    void stop();

private:
    void run();

    clink::http_connector::HttpRequest& http_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool armed_{false};
    bool stop_{false};
    std::chrono::steady_clock::time_point deadline_{};
    CancelSignal cancel_;
    Why why_{Why::None};
    std::thread thread_;
};

// Writes each batch of rows to Neo4j as one implicit transaction of the
// configured statement, through the Query API, with the rows as the $rows
// parameter. At least once: a checkpoint barrier returns only after every row
// received before it is in a transaction the server reported committed.
class Neo4jSink final : public Sink<clink::sql::Row> {
public:
    explicit Neo4jSink(SinkOptions options);
    ~Neo4jSink() override;

    void open() override;
    void on_data(const Batch<clink::sql::Row>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    void flush() override;
    void close() override;
    void close_cancelled() override;
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "neo4j_sink"; }

    // What the sink has done, for its summary line and for tests.
    struct Counts {
        std::uint64_t batches_committed{0};
        std::uint64_t rows_committed{0};
        std::uint64_t attempts_failed{0};
        std::uint64_t retries{0};
        std::uint64_t outcome_unknown{0};
        std::uint64_t rows_maybe_applied_twice{0};
    };
    [[nodiscard]] const Counts& counts() const noexcept { return counts_; }
    [[nodiscard]] std::uint64_t buffered_rows() const noexcept { return pending_rows_; }

private:
    void add_row_(const clink::sql::Row& row);
    void commit_pending_(const char* why);
    void check_statement_();
    [[nodiscard]] std::string body_() const;
    [[nodiscard]] std::string where_() const;
    void publish_buffer_gauges_();
    void summary_(const char* how) const;
    [[noreturn]] void fail_(const char* code, const std::string& message);

    SinkOptions opts_;
    std::unique_ptr<clink::http_connector::HttpRequest> http_;
    std::unique_ptr<RequestWatch> watch_;
    clink::http_connector::BoundedRequest request_;
    CancelSignal cancel_;
    std::string path_;
    std::string body_prefix_;  // {"statement":...,"parameters":{"rows":{"$type":"List","_value":[
    std::string rows_;         // the encoded rows, comma-separated
    std::string scratch_;      // one row while it is encoded
    std::uint64_t pending_rows_{0};
    std::chrono::steady_clock::time_point first_pending_{};
    std::uint64_t batch_seq_{0};
    std::string server_version_;
    Counts counts_;
    bool opened_{false};
    bool closed_{false};
};

}  // namespace clink::neo4j
