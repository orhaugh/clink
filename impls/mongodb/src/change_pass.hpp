#pragma once

// One produce() pass of the MongoDB change-stream source, over any cursor that
// hands out change events one at a time, so the checkpoint-token rule is
// testable without a server.
//
// The rule: the resume token a checkpoint may record covers only events the
// pass handed over. The cursor never reads ahead of what next() returned, so
// its resume_token() after the pass is the token of the last event returned,
// or, once next() found nothing more, the stream's position after the batch it
// read, which covers no event the pass did not see. An earlier version advanced
// the driver's iterator past the last event it emitted when it stopped at the
// per-pass cap, which read the next event into the driver; the token taken
// then covered an event no record carried, and a restore from it skipped that
// event.
//
// A pass also ends once its time budget is spent (see read_pass), so a steady
// flow of changes cannot hold back the checkpoint barriers that go in between
// passes until the cap fills.
//
// Cursor requirements:
//   std::optional<std::string> next();          the next event as JSON, or
//                                               nullopt when none is ready
//   std::optional<std::string> resume_token();  the token covering exactly what
//                                               next() returned

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/mongodb/mongo_event.hpp"

namespace clink::mongodb::detail {

// The record one change event becomes: change_event_to_json's envelope, or a
// typed SQL row. Throws for an event the source cannot continue past.
using EventMapper = std::function<std::string(const clink::config::JsonValue&)>;

struct Pass {
    std::vector<std::string> rows;  // the records the pass hands over, in event order
    std::uint64_t bytes{0};
    std::optional<std::string> token;  // what a checkpoint after the pass may record
    bool stopped{false};               // stop() ended it before the cap or an idle cursor
};

// Reads at most `max_events` events. Stops early when `stop()` is true, checked
// before each read so no event is read and dropped, or when the cursor has
// nothing ready. The source's stop is a cancel or the pass's time budget spent:
// while changes keep arriving the cursor always has one more, and a pass bound
// only by its cap held back checkpoint barriers, which go in between passes, for
// as long as the cap took to fill. Throws what `map` throws, before the pass's
// token is taken, so nothing of a failed pass is ever checkpointed.
template <typename Cursor>
Pass read_pass(Cursor& cursor,
               const EventMapper& map,
               std::size_t max_events,
               const std::function<bool()>& stop) {
    Pass pass;
    while (pass.rows.size() < max_events) {
        if (stop()) {
            pass.stopped = true;
            break;
        }
        auto event = cursor.next();
        if (!event) {
            break;
        }
        auto row = map(clink::config::parse(*event));
        pass.bytes += row.size();
        pass.rows.push_back(std::move(row));
    }
    pass.token = cursor.resume_token();
    return pass;
}

}  // namespace clink::mongodb::detail
