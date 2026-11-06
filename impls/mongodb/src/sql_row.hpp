#pragma once

// The typed SQL row of a MongoDB change event, for a connector='mongodb' table.
//
// The source reads each event as canonical Extended JSON, which keeps every
// BSON type apart, and converts each declared column to the value the engine
// holds for its SQL type. A value the type cannot hold exactly is refused,
// naming the field, its BSON type and the column, rather than rounded,
// truncated or carried as the wrong kind. The supported pairs:
//
//   declared SQL type     BSON types                       row value
//   BIGINT                int32, int64                      integer
//   INTEGER               int32; int64 within range         integer
//   SMALLINT              int32, int64 within range         integer
//   DOUBLE                double; int32; int64 within 2^53  number (NaN, Infinity refused)
//   REAL                  double a float holds exactly;     number
//                         int32 within 2^24
//   DECIMAL(p, s)         decimal128, int32, int64          exact decimal; more than p - s
//                                                           integer digits or s fraction
//                                                           digits refused, as are NaN and
//                                                           Infinity
//   BOOLEAN               bool                              true / false
//   TIMESTAMP(p) [WITH    date                              epoch milliseconds (BSON dates
//   TIME ZONE]                                              are UTC instants to the ms)
//   DATE                  date at UTC midnight              'YYYY-MM-DD'
//   VARCHAR               string                            the string
//                         ObjectId                          its 24 hex digits
//                         int32, int64                      decimal digits
//                         embedded document, array          canonical Extended JSON text
//
// Every other pair is refused: a double into DECIMAL (binary fractions are not
// decimal ones), a decimal or a double into an integer or VARCHAR, a date into
// VARCHAR, and binary data, timestamps, regular expressions, code and the
// other BSON types into anything. A null, or a field the document does not
// have, is SQL NULL for any declared type; an update read without a full
// document (__image 'partial') has only the fields it set, so the rest are
// NULL there, except _id, which cannot change and is taken from the event's key.
//
// Change metadata is available as declared columns with reserved names:
//   __op     VARCHAR: insert, update, replace or delete
//   __ns     VARCHAR: "<database>.<collection>"
//   __image  VARCHAR: full, lookup, partial or key (mongo_event.hpp)
//   __id     the document's _id, converted by the rules above for its declared
//            type, as _id is
// Any other declared name starting with __ is refused, and so is a document
// field named like the metadata (change_event_to_json's rule).

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/mongodb/mongo_event.hpp"

namespace clink::mongodb::sql {

enum class Kind {
    BigInt,
    Integer,
    SmallInt,
    Double,
    Real,
    Decimal,
    Boolean,
    Timestamp,
    Date,
    Varchar,
    MetaOp,
    MetaNs,
    MetaImage,
};

struct Column {
    std::string name;
    std::string sql_type;  // as the planner rendered it, for messages
    Kind kind{Kind::Varchar};
    bool is_id{false};  // __id: the _id's value, converted for kind
    int precision{0};
    int scale{0};
};

// A declared column or value the source cannot fill.
class ConversionError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The planner's declared_types ("name:TYPE;name:TYPE"). Throws ConversionError
// for a SQL type the source cannot fill or a reserved name it does not know.
std::vector<Column> parse_declared_types(std::string_view text);

// The row for one change event read as canonical Extended JSON: every declared
// column, in declared order, from the metadata or the document's top-level
// field of that name. Throws ConversionError for a value its column cannot
// hold, and what decompose_change throws.
std::string change_event_to_sql_row(const clink::config::JsonValue& canonical_event,
                                    FullDocument mode,
                                    const std::vector<Column>& columns);

// The sink side, for a connector='mongodb' sink table. Each declared column is
// written as the BSON type its SQL type maps to, the reverse of the table
// above:
//
//   BIGINT                 int64
//   INTEGER, SMALLINT      int32
//   DOUBLE, REAL           double (NaN and the infinities kept)
//   DECIMAL(p, s)          decimal128, with the row's digits
//   BOOLEAN                bool
//   TIMESTAMP(p) [WITH     date (the row's epoch milliseconds)
//   TIME ZONE]
//   DATE                   date at UTC midnight
//   VARCHAR                string
//
// SQL NULL is written as null. A name starting with __ is an ordinary field
// here, except __row_kind, which SQL reserves for the change kind.
std::vector<Column> parse_sink_columns(std::string_view text);

struct SinkDocument {
    // The row's change kind (__row_kind): insert when unmarked, update_before,
    // update_after or delete.
    std::string kind;
    // The declared columns in declared order, as canonical Extended JSON.
    std::string json;
};

// A row as row_to_json_string writes it, with non-finite doubles in the tagged
// form, as the document the sink writes. Throws ConversionError for a value its
// column's BSON type cannot hold.
SinkDocument sql_row_to_document(std::string_view row_json, const std::vector<Column>& columns);

}  // namespace clink::mongodb::sql
