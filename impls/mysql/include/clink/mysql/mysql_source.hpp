#pragma once

// MySQL incremental cursor source: runs `SELECT * FROM <table> WHERE
// <cursor_column> > <cursor> ORDER BY <cursor_column> ASC LIMIT <n>` on a poll
// interval, emitting one JSON-object string per row (keyed by column name) for
// the json_string_to_row bridge. Built on PollingSource<std::string>, so the
// cursor is checkpointed as operator state and replayed on restart =
// AT-LEAST-ONCE. The cursor is EXCLUSIVE (>) so the boundary row is never
// re-emitted. Not CDC.
//
// TYPE NOTE: the text protocol returns every cell as a string, so emitted JSON
// values are strings; the downstream Row bridge coerces them per the declared
// column types. Requires an ascending, monotonic cursor_column (e.g. an
// AUTO_INCREMENT id or an updated_at) - out-of-order inserts below the cursor are
// skipped.

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/connectors/polling_source.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_sql.hpp"

namespace clink::mysql {

struct MysqlPollOptions {
    ConnectOptions conn;
    std::string table;          // required
    std::string cursor_column;  // required; ascending + monotonic
    std::string initial_cursor;
    int batch_size{1000};
    std::chrono::milliseconds interval{1000};
    double jitter_frac{0.0};
    std::string name{"mysql_source"};
};

namespace detail {
struct MysqlPollState {
    ConnectOptions conn;
    std::string table;
    std::string cursor_column;
    int batch_size{1000};
    std::unique_ptr<Connection> client;  // lazily connected on first poll
};
}  // namespace detail

// Build a PollingSource<std::string> that tails a MySQL table by cursor. Throws
// on missing/invalid table or cursor_column (fail fast at build).
inline std::shared_ptr<PollingSource<std::string>> make_mysql_poll_source(
    const MysqlPollOptions& o) {
    if (o.table.empty()) {
        throw std::runtime_error(o.name + ": 'table' is required");
    }
    if (o.cursor_column.empty()) {
        throw std::runtime_error(o.name + ": 'cursor_column' is required");
    }
    (void)quote_ident(o.table);  // validate identifiers early
    (void)quote_ident(o.cursor_column);

    auto state = std::make_shared<detail::MysqlPollState>();
    state->conn = o.conn;
    state->table = o.table;
    state->cursor_column = o.cursor_column;
    state->batch_size = o.batch_size > 0 ? o.batch_size : 1000;

    PollingSource<std::string>::Options popts;
    popts.interval = o.interval;
    popts.initial_cursor = o.initial_cursor;
    popts.jitter_frac = o.jitter_frac;
    popts.name = o.name;

    auto poll = [state](const std::string& cursor) -> PollingSource<std::string>::PollResult {
        PollingSource<std::string>::PollResult out;
        try {
            if (!state->client) {
                state->client = std::make_unique<Connection>(state->conn);
            }
            const std::string sql =
                build_select_sql(state->table,
                                 state->cursor_column,
                                 cursor,
                                 state->batch_size,
                                 [&](std::string_view s) { return state->client->escape(s); });
            Result res = state->client->query(sql);
            if (!res) {
                return out;
            }
            const unsigned int nf = res.num_fields();
            MYSQL_FIELD* fields = res.fields();
            int cursor_idx = -1;
            for (unsigned int i = 0; i < nf; ++i) {
                if (state->cursor_column == fields[i].name) {
                    cursor_idx = static_cast<int>(i);
                    break;
                }
            }
            std::string last_cursor;
            std::uint64_t bytes = 0;
            MYSQL_ROW row;
            while ((row = res.fetch_row()) != nullptr) {
                unsigned long* lens = res.lengths();
                clink::config::JsonObject obj;
                for (unsigned int i = 0; i < nf; ++i) {
                    std::string key(fields[i].name, fields[i].name_length);
                    if (row[i] == nullptr) {
                        obj[key] = clink::config::JsonValue{};  // SQL NULL -> JSON null
                    } else {
                        obj[key] = clink::config::JsonValue{std::string(row[i], lens[i])};
                    }
                }
                std::string json = clink::config::JsonValue{std::move(obj)}.serialize(0);
                bytes += json.size();
                out.records.push_back(std::move(json));
                if (cursor_idx >= 0 && row[cursor_idx] != nullptr) {
                    last_cursor.assign(row[cursor_idx],
                                       lens[static_cast<unsigned int>(cursor_idx)]);
                }
            }
            if (!out.records.empty()) {
                clink::metrics::connector::records_in_inc("mysql", out.records.size());
                clink::metrics::connector::bytes_in_inc("mysql", bytes);
            }
            if (!last_cursor.empty()) {
                out.next_cursor = std::move(last_cursor);
            }
            return out;
        } catch (...) {
            clink::metrics::connector::error_inc("mysql", "source");
            state->client.reset();  // drop a broken connection; replay reconnects
            throw;                  // subtask fails -> job replays from checkpointed cursor
        }
    };

    return std::make_shared<PollingSource<std::string>>(std::move(popts), std::move(poll));
}

}  // namespace clink::mysql
