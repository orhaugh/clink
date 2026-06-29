#include "clink/sql/type.hpp"

#include <string>
#include <variant>

#include "clink/sql/parser.hpp"

#include "arrow/api.h"

namespace clink::sql {

namespace {

[[noreturn]] void reject(const ast::TypeName& type, const std::string& reason) {
    std::string spelling = type.schema.empty() ? type.name : (type.schema + "." + type.name);
    throw TranslationError("SQL type " + spelling + ": " + reason, type.loc.pos);
}

arrow::TimeUnit::type timestamp_unit_from_precision(int precision, const ast::TypeName& type) {
    // SQL TIMESTAMP(p) -> precision = number of fractional-second
    // digits. We map to Arrow's coarsest unit that retains the digits:
    //   p == 0     -> SECOND
    //   p in 1..3  -> MILLI
    //   p in 4..6  -> MICRO
    //   p in 7..9  -> NANO
    if (precision == 0)
        return arrow::TimeUnit::SECOND;
    if (precision <= 3)
        return arrow::TimeUnit::MILLI;
    if (precision <= 6)
        return arrow::TimeUnit::MICRO;
    if (precision <= 9)
        return arrow::TimeUnit::NANO;
    reject(type, "TIMESTAMP precision must be in [0, 9]");
}

}  // namespace

std::shared_ptr<arrow::DataType> sql_type_to_arrow(const ast::TypeName& type) {
    // PG canonical names are lowercase. Schema is either empty (TEXT,
    // VARCHAR) or "pg_catalog" (most others). We match on name only;
    // a non-pg_catalog schema we don't recognize is rejected.
    if (!type.schema.empty() && type.schema != "pg_catalog") {
        reject(type, "unknown type schema");
    }
    // Array column types (Wave 5): `elem[]...[]` -> nested arrow::list.
    // Resolve the scalar element type from a copy with the array
    // dimensions stripped, then wrap once per dimension.
    if (type.array_ndims > 0) {
        ast::TypeName element = type;
        element.array_ndims = 0;
        auto inner = sql_type_to_arrow(element);
        for (int d = 0; d < type.array_ndims; ++d) {
            inner = arrow::list(inner);
        }
        return inner;
    }
    const std::string& n = type.name;

    if (n == "int8" || n == "bigint")
        return arrow::int64();
    if (n == "int4" || n == "integer" || n == "int")
        return arrow::int32();
    if (n == "int2" || n == "smallint")
        return arrow::int16();
    if (n == "bool" || n == "boolean")
        return arrow::boolean();
    if (n == "float4" || n == "real")
        return arrow::float32();
    if (n == "float8" || n == "double")
        return arrow::float64();

    // PG normalizes VARCHAR and CHAR to bpchar / varchar internally;
    // PG also keeps "text" without a schema. All three map to Arrow
    // utf8 for streaming purposes - fixed-width semantics aren't
    // useful on a stream and would just complicate buffering.
    if (n == "text" || n == "varchar" || n == "bpchar" || n == "string") {
        return arrow::utf8();
    }

    if (n == "timestamp") {
        int precision = type.typmods.empty() ? 6 : type.typmods[0];
        return arrow::timestamp(timestamp_unit_from_precision(precision, type));
    }
    if (n == "timestamptz") {
        int precision = type.typmods.empty() ? 6 : type.typmods[0];
        return arrow::timestamp(timestamp_unit_from_precision(precision, type), "UTC");
    }
    if (n == "date")
        return arrow::date32();
    if (n == "time")
        return arrow::time64(arrow::TimeUnit::MICRO);

    if (n == "numeric") {
        // PG NUMERIC without precision defaults to "any" - we pin to
        // decimal128(38, 9) so we have a concrete on-wire shape.
        // Decimal arithmetic itself is out of scope for now.
        int precision = type.typmods.size() > 0 ? type.typmods[0] : 38;
        int scale = type.typmods.size() > 1 ? type.typmods[1] : 9;
        if (precision < 1 || precision > 38)
            reject(type, "DECIMAL precision must be in [1, 38]");
        if (scale < 0 || scale > precision)
            reject(type, "DECIMAL scale must be in [0, precision]");
        return arrow::decimal128(precision, scale);
    }

    if (n == "bytea")
        return arrow::binary();

    // Composite types (#61): the pre-parser shim populates name + params for
    // the PG-ungrammatical MAP<k,v> / ROW<f t, ...> / MULTISET<t> spellings.
    if (n == "map") {
        if (type.params.size() != 2) {
            reject(type, "MAP requires a key and a value type");
        }
        return arrow::map(sql_type_to_arrow(type.params[0]), sql_type_to_arrow(type.params[1]));
    }
    if (n == "row") {
        if (type.params.empty() || type.params.size() != type.field_names.size()) {
            reject(type, "ROW requires named fields");
        }
        arrow::FieldVector fields;
        fields.reserve(type.params.size());
        for (std::size_t f = 0; f < type.params.size(); ++f) {
            fields.push_back(arrow::field(type.field_names[f], sql_type_to_arrow(type.params[f])));
        }
        return arrow::struct_(fields);
    }
    if (n == "multiset") {
        // Arrow has no distinct multiset type; represent as a list (same shape
        // as ARRAY - bag-vs-ordered semantics are not modelled on the wire).
        if (type.params.size() != 1) {
            reject(type, "MULTISET requires exactly one element type");
        }
        return arrow::list(sql_type_to_arrow(type.params[0]));
    }
    // A composite placeholder that escaped the reattach pass (e.g. a composite
    // type used outside a CREATE TABLE column) - composites are v1-supported
    // only as column types.
    if (n.rfind("__clink_ctype_", 0) == 0) {
        reject(type, "composite types (MAP/ROW/MULTISET) are only supported as column types");
    }

    reject(type, "type not supported");
}

std::string arrow_to_sql_type_string(const arrow::DataType& type) {
    switch (type.id()) {
        case arrow::Type::INT64:
            return "BIGINT";
        case arrow::Type::INT32:
            return "INTEGER";
        case arrow::Type::INT16:
            return "SMALLINT";
        case arrow::Type::INT8:
            return "TINYINT";
        case arrow::Type::BOOL:
            return "BOOLEAN";
        case arrow::Type::FLOAT:
            return "REAL";
        case arrow::Type::DOUBLE:
            return "DOUBLE";
        case arrow::Type::STRING:
            return "VARCHAR";
        case arrow::Type::BINARY:
            return "BYTEA";
        case arrow::Type::DATE32:
            return "DATE";
        case arrow::Type::TIME64:
            return "TIME";
        case arrow::Type::TIMESTAMP: {
            const auto& ts = static_cast<const arrow::TimestampType&>(type);
            int p = 6;
            switch (ts.unit()) {
                case arrow::TimeUnit::SECOND:
                    p = 0;
                    break;
                case arrow::TimeUnit::MILLI:
                    p = 3;
                    break;
                case arrow::TimeUnit::MICRO:
                    p = 6;
                    break;
                case arrow::TimeUnit::NANO:
                    p = 9;
                    break;
            }
            std::string base = "TIMESTAMP(" + std::to_string(p) + ")";
            if (!ts.timezone().empty())
                base += " WITH TIME ZONE";
            return base;
        }
        case arrow::Type::DECIMAL128: {
            const auto& d = static_cast<const arrow::Decimal128Type&>(type);
            return "DECIMAL(" + std::to_string(d.precision()) + ", " + std::to_string(d.scale()) +
                   ")";
        }
        case arrow::Type::LIST: {
            // Wave 5: arrow::list -> `<elem> ARRAY` (re-parses to the
            // same type). Nested lists chain the suffix.
            const auto& l = static_cast<const arrow::ListType&>(type);
            return arrow_to_sql_type_string(*l.value_type()) + " ARRAY";
        }
        case arrow::Type::STRUCT: {
            // Wave 5c: arrow::struct_ -> `ROW<name type, ...>`. Rendered in
            // SQL terms for EXPLAIN and the sink-compat rejection message
            // (PG DDL cannot declare this type, so it never re-parses).
            const auto& st = static_cast<const arrow::StructType&>(type);
            std::string s = "ROW<";
            for (int i = 0; i < st.num_fields(); ++i) {
                if (i != 0)
                    s += ", ";
                s += st.field(i)->name() + " " + arrow_to_sql_type_string(*st.field(i)->type());
            }
            s += ">";
            return s;
        }
        case arrow::Type::MAP: {
            // Wave 5b: arrow::map -> `MAP<k, v>` (SQL terms; not PG-DDL
            // declarable).
            const auto& m = static_cast<const arrow::MapType&>(type);
            return "MAP<" + arrow_to_sql_type_string(*m.key_type()) + ", " +
                   arrow_to_sql_type_string(*m.item_type()) + ">";
        }
        default:
            return type.ToString();
    }
}

ast::TypeName parse_sql_type_expression(const std::string& expr) {
    // PG accepts a type expression only as part of a column / cast /
    // similar context. Synthesize a single-column CREATE TABLE so the
    // existing parser path can decode the type into ast::TypeName.
    std::string synth = "CREATE TABLE _clink_type_probe (_c " + expr + ")";
    auto script = parse(synth);
    if (script.statements.size() != 1 ||
        !std::holds_alternative<ast::CreateTableStmt>(script.statements[0])) {
        throw TranslationError("could not parse SQL type expression: " + expr, 0);
    }
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);
    if (create.columns.size() != 1) {
        throw TranslationError("could not parse SQL type expression: " + expr, 0);
    }
    return create.columns[0].type;
}

}  // namespace clink::sql
