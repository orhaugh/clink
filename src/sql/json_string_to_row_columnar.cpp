// Out-of-line half of JsonStringToRowColumnarOperator: the simdjson on-demand
// decoder.
//
// It lives here rather than in the header because simdjson is a PRIVATE
// implementation detail of the engine. Including <simdjson.h> from a public
// header puts it on the include path of everything that consumes clink's
// headers - which compiled fine locally, where the SQL target already had
// simdjson's include directory, and broke the container build with
// "fatal error: simdjson.h: No such file or directory" the moment a translation
// unit without that path compiled it.

#include "clink/sql/json_string_to_row_columnar.hpp"

#include <algorithm>
#include <bit>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <simdjson.h>
#include <string_view>
#include <vector>

#include "clink/config/decimal.hpp"

namespace clink::sql {

// Parser + padded scratch buffer, reused across records so the fast path
// allocates nothing per line. On-demand requires SIMDJSON_PADDING readable bytes
// past the end of the document, which is why the line is copied into a
// grown-once buffer rather than parsed in place.
struct JsonStringToRowColumnarOperator::Ondemand {
    simdjson::ondemand::parser parser;
    std::vector<char> pad;
};

JsonStringToRowColumnarOperator::JsonStringToRowColumnarOperator(std::vector<RowColumn> columns,
                                                                 std::vector<std::string> projected,
                                                                 std::string name)
    // The row fallback MUST use the same schema-aware decode the columnar arms
    // are written against, or this operator would disagree with itself depending on which
    // carrier a batch took - so it takes the SAME projection. row_json_text_format_projected
    // already preserves the synthetic __row_kind changelog marker, which is why the keep-list
    // goes through it rather than being applied by hand.
    : fmt_(row_json_text_format_for_columns_projected(columns, projected)), name_(std::move(name)) {
    // Empty keep-list means keep everything.
    const bool project = !projected.empty();
    const std::set<std::string> keep(projected.begin(), projected.end());
    resolved_.reserve(columns.size());
    data_fields_.reserve(columns.size() + 1);
    data_fields_.push_back(clink::arrow_event_time_field());  // sidecar column 0
    std::set<std::string> seen;
    for (const auto& c : columns) {
        auto eff = row_columnar_detail::effective_type(c.type);
        if (!columnar_capable_type_(eff->id())) {
            schema_capable_ = false;
        }
        // A declared column in the engine-reserved "__" namespace would
        // collide with the partition sidecar column this operator appends
        // (a duplicate name makes GetFieldIndex ambiguous -> the partition
        // reader silently yields nothing -> per-partition watermarking
        // collapses to a global watermark). Refuse the columnar path for
        // such a schema; the row fallback is always correct.
        if (c.name.rfind("__", 0) == 0) {
            schema_capable_ = false;
        }
        // A backslash in a declared name is the one case where comparing a RAW JSON
        // key against it could match while the key's UNESCAPED form differs, which
        // would put a field in the wrong column. Vanishingly rare and refused rather
        // than reasoned about; the row fallback handles such a schema correctly.
        if (c.name.find('\\') != std::string::npos) {
            schema_capable_ = false;
        }
        // A duplicated declared name defeats the count+per-key-find
        // faithfulness gate (obj.size() matches the inflated column count
        // and the duplicate key is found twice, so an undeclared field can
        // slip through and be silently dropped). The catalog does not reject
        // duplicate column names, so guard here: force the always-correct
        // row fallback for such a (malformed) schema.
        if (!seen.insert(c.name).second) {
            schema_capable_ = false;
        }
        if (eff->id() == arrow::Type::DECIMAL128) {
            // Needed per line to recover exact digits (see the parse loop).
            decimal_scales_[c.name] = static_cast<const arrow::Decimal128Type&>(*eff).scale();
        }
        const auto tid = eff->id();
        const std::int32_t sc = tid == arrow::Type::DECIMAL128
                                    ? static_cast<const arrow::Decimal128Type&>(*eff).scale()
                                    : 0;
        const bool want = !project || keep.count(c.name) > 0;
        // Only projected columns appear in the emitted Arrow schema. An unprojected one
        // still gets a Resolved entry, because the field walk must recognise it as
        // DECLARED - and read its value as far as the row decode's parse would - rather
        // than treat it as undeclared and bail the whole batch.
        if (want) {
            data_fields_.push_back(arrow::field(c.name, eff, /*nullable=*/true));
        }
        resolved_.push_back(
            {c.name, std::move(eff), tid, sc, want, static_cast<std::uint32_t>(c.name.size())});
    }
    // Identity is the right initial guess: a producer emitting fields in declared
    // order is the common case, so the first record already hits.
    pos_hint_.resize(resolved_.size());
    for (std::size_t i = 0; i < pos_hint_.size(); ++i) {
        pos_hint_[i] = static_cast<std::uint32_t>(i);
    }
}

JsonStringToRowColumnarOperator::~JsonStringToRowColumnarOperator() = default;

namespace {

// An integer token outside 64 bits: more digits than any 64-bit integer, a
// negative below INT64_MIN, or a positive at or above 2^64. The row decode's
// DOM parse refuses the whole line for one, so the record never reaches the
// row path, while get_double reads any digit count, so a column read through
// it must refuse one. Peeks only: the value is left for the caller to read.
bool integer_token_past_64_bits(simdjson::ondemand::value& v) {
    simdjson::ondemand::number_type t{};
    return v.get_number_type().get(t) == simdjson::SUCCESS &&
           t == simdjson::ondemand::number_type::big_integer;
}

// The byte after a scalar token, as the DOM parse takes one: whitespace, or the
// comma, brace or bracket that ends the value. simdjson also lets a colon or an
// opening brace or bracket end a scalar, but its structure check then refuses
// the line, so refusing them here changes no answer.
[[gnu::always_inline]] inline bool ends_value(char c) {
    switch (c) {
        case ' ':
        case '\t':
        case '\n':
        case '\r':
        case ',':
        case '}':
        case ']':
            return true;
        default:
            return false;
    }
}

enum class NumberVerdict { accept, refuse, undecided };

// Whether the `n` bytes at `p`, one to eight of them, are all ASCII digits, in a
// handful of word operations rather than a loop whose exit depends on the data.
// Reads a whole word, which the padded line buffer allows, and fills the bytes
// past `n` with '0' so that they pass.
[[gnu::always_inline]] inline bool all_digits(const char* p, std::size_t n) noexcept {
    std::uint64_t word{};
    std::memcpy(&word, p, sizeof word);
    const std::uint64_t keep =
        n >= sizeof word ? ~std::uint64_t{0} : (std::uint64_t{1} << (8 * n)) - 1;
    word = (word & keep) | (0x3030303030303030ULL & ~keep);
    // Every byte has the high nibble 3, and adding 6 keeps it there: 0x30 to 0x39.
    return ((word & 0xF0F0F0F0F0F0F0F0ULL) |
            (((word + 0x0606060606060606ULL) & 0xF0F0F0F0F0F0F0F0ULL) >> 4)) ==
           0x3333333333333333ULL;
}

// The common number token, settled inline: an integer of at most 16 digits, which
// no 64-bit check can refuse, with no leading zero and nothing after it but the
// byte that ends the value. The token runs to the next structural character, so
// a numeral followed by whitespace, a fraction or an exponent fails this and
// goes to number_parses_slow, which decides it. False never means refused.
[[gnu::always_inline]] inline bool short_integer(std::string_view tok) noexcept {
    const char* p = tok.data();
    const std::size_t sign = p[0] == '-' ? 1 : 0;
    const char* const digits = p + sign;
    const std::size_t count = tok.size() - sign;
    return count - 1 < 16 && ends_value(p[tok.size()]) && (digits[0] != '0' || count == 1) &&
           all_digits(digits, count < 8 ? count : 8) &&
           (count <= 8 || all_digits(digits + 8, count - 8));
}

// The DOM parse's verdict on the number token `tok`, where the text alone
// settles it: the JSON number grammar, then a byte that ends the value. A token
// the grammar admits is undecided when it could fall outside what the parse
// holds: an integer of more than 18 digits, which may pass 64 bits, or a
// fraction or exponent form whose integer digits and exponent together pass
// 308, which may pass the largest double. Anything smaller is certainly inside,
// and an exponent far below zero only underflows to zero, which the parse
// takes. Reads no further than the byte after the token, which the document's
// closing brace bounds.
NumberVerdict number_verdict(std::string_view tok) noexcept {
    const char* p = tok.data();
    const auto digit = [](char c) { return c >= '0' && c <= '9'; };
    if (*p == '-') {
        ++p;
    }
    const char* const integer_start = p;
    if (*p == '0') {
        ++p;
    } else if (digit(*p)) {
        while (digit(*p)) {
            ++p;
        }
    } else {
        return NumberVerdict::refuse;
    }
    const auto integer_digits = static_cast<std::int64_t>(p - integer_start);
    bool integer = true;
    if (*p == '.') {
        ++p;
        if (!digit(*p)) {
            return NumberVerdict::refuse;
        }
        while (digit(*p)) {
            ++p;
        }
        integer = false;
    }
    std::int64_t exponent = 0;
    if (*p == 'e' || *p == 'E') {
        ++p;
        const bool negative = *p == '-';
        if (*p == '-' || *p == '+') {
            ++p;
        }
        if (!digit(*p)) {
            return NumberVerdict::refuse;
        }
        // Saturates well past any decision below rather than overflowing.
        constexpr std::int64_t kExponentCap = 100000;
        while (digit(*p)) {
            if (exponent < kExponentCap) {
                exponent = exponent * 10 + (*p - '0');
            }
            ++p;
        }
        exponent = negative ? -exponent : exponent;
        integer = false;
    }
    if (!ends_value(*p)) {
        return NumberVerdict::refuse;
    }
    if (integer) {
        return integer_digits <= 18 ? NumberVerdict::accept : NumberVerdict::undecided;
    }
    return integer_digits + exponent <= 308 ? NumberVerdict::accept : NumberVerdict::undecided;
}

// Whether the DOM parse accepts the number token `v`, whose text is `tok`, for a
// token short_integer could not settle: from the text where number_verdict
// settles it, and otherwise from simdjson's own number parser, the routine the
// DOM parse runs. Out of line and cold, so the walk's hot loop stays small.
// Peeks only.
[[gnu::noinline, gnu::cold]] bool number_parses_slow(simdjson::ondemand::value& v,
                                                     std::string_view tok) {
    switch (number_verdict(tok)) {
        case NumberVerdict::accept:
            return true;
        case NumberVerdict::refuse:
            return false;
        case NumberVerdict::undecided:
            break;
    }
    simdjson::ondemand::number n;
    return v.get_number().get(n) == simdjson::SUCCESS;
}

// Whether the DOM parse accepts the number token `v`, whose text is `tok`. Peeks
// only.
[[gnu::always_inline]] inline bool number_parses(simdjson::ondemand::value& v,
                                                 std::string_view tok) {
    return short_integer(tok) || number_parses_slow(v, tok);
}

// How deep below a field a value outside the projection is walked. Deeper than
// this the walk declines, and the DOM arm, whose parse is the row decode's own,
// decides the line.
constexpr int kMaxWalkedNesting = 32;

// Whether a string token holds a backslash, eight bytes at a time. The token
// lies in the padded line buffer, which has SIMDJSON_PADDING readable bytes past
// the line, so a word may run up to seven bytes past the token; those bytes are
// masked off. A flagged byte above the first true match can be a false flag,
// but never one below it, so any flag inside the token means a match.
[[gnu::always_inline]] inline bool has_backslash(std::string_view tok) noexcept {
    static_assert(std::endian::native == std::endian::little,
                  "the tail mask keeps the low-order bytes of each word");
    constexpr std::uint64_t kOnes = 0x0101010101010101ULL;
    constexpr std::uint64_t kHighs = 0x8080808080808080ULL;
    constexpr std::uint64_t kBackslashes = kOnes * static_cast<unsigned char>('\\');
    const char* p = tok.data();
    for (std::size_t left = tok.size(); left > 0;) {
        std::uint64_t word{};
        std::memcpy(&word, p, sizeof word);
        const std::uint64_t x = word ^ kBackslashes;
        std::uint64_t hits = (x - kOnes) & ~x & kHighs;
        if (left < sizeof word) {
            hits &= (std::uint64_t{1} << (8 * left)) - 1;
        }
        if (hits != 0) {
            return true;
        }
        p += sizeof word;
        left = left > sizeof word ? left - sizeof word : 0;
    }
    return false;
}

// A string with an escape in it: unescaped, as the DOM parse unescapes it, to
// learn whether that parse takes it. Out of line and cold.
[[gnu::noinline, gnu::cold]] bool escaped_string_parses(simdjson::ondemand::value& v) {
    std::string_view s;
    return v.get_string().get(s) == simdjson::SUCCESS;
}

bool container_parses(simdjson::ondemand::value& v, char open, int depth);

// Whether the DOM parse the row decode runs would accept `v`, a value this arm
// does not build. The row decode refuses the WHOLE line over any value it cannot
// parse, so a value outside the projection still has to be read as far as that
// parse reads it: skipped unread, an integer past 64 bits, a numeral the grammar
// does not have, a bad escape, a misspelt atom or a malformed array would ride
// columnar with its record while the row decode emptied it. Stage 1 has already
// checked the whole line's UTF-8 and unescaped control characters, so a string
// without a backslash parses; anything else is checked by the routine the DOM
// parse uses for it. A true return is never wrong: false only sends the batch to
// the DOM arm. The common scalars are settled inline in the field walk; the rest
// take an out-of-line path.
[[gnu::always_inline]] inline bool value_parses(simdjson::ondemand::value& v, int depth) {
    const std::string_view tok = v.raw_json_token();
    if (tok.empty()) {
        return false;
    }
    const char c = tok.front();
    if (c == '"') {
        // The token takes in the whitespace after the string, so the byte after
        // it is the next structural character, and only a comma or a closing
        // bracket may follow a value. A string left unconsumed is skipped by the
        // iterator, whose skip takes a following colon as a key's and runs on to
        // the closing bracket, so {"s":"x":]} would ride columnar where the DOM
        // parse refuses the line.
        const char next = tok.data()[tok.size()];
        if (next != ',' && next != '}' && next != ']') {
            return false;
        }
        return !has_backslash(tok) || escaped_string_parses(v);
    }
    if (c == '-' || (c >= '0' && c <= '9')) {
        return number_parses(v, tok);
    }
    if (c == 't' || c == 'f') {
        bool b{};
        return v.get_bool().get(b) == simdjson::SUCCESS;
    }
    if (c == 'n') {
        bool is_null{};
        return v.is_null().get(is_null) == simdjson::SUCCESS && is_null;
    }
    if (c == '[' || c == '{') {
        return container_parses(v, c, depth);
    }
    return false;
}

// The nested half of value_parses: an array or object, walked element by element
// with every key unescaped as the DOM parse unescapes it. Out of line because it
// recurses.
[[gnu::noinline]] bool container_parses(simdjson::ondemand::value& v, char open, int depth) {
    if (depth >= kMaxWalkedNesting) {
        return false;
    }
    if (open == '[') {
        simdjson::ondemand::array arr;
        if (v.get_array().get(arr) != simdjson::SUCCESS) {
            return false;
        }
        for (auto element : arr) {
            simdjson::ondemand::value e;
            if (element.get(e) != simdjson::SUCCESS || !value_parses(e, depth + 1)) {
                return false;
            }
        }
        return true;
    }
    simdjson::ondemand::object obj;
    if (v.get_object().get(obj) != simdjson::SUCCESS) {
        return false;
    }
    for (auto field : obj) {
        std::string_view key;
        simdjson::ondemand::value e;
        if (field.unescaped_key().get(key) != simdjson::SUCCESS ||
            field.value().get(e) != simdjson::SUCCESS || !value_parses(e, depth + 1)) {
            return false;
        }
    }
    return true;
}

// Append one on-demand value to a typed builder, mirroring append_cell_'s
// acceptance rules exactly. Returns false to bail the batch.
//
// Takes the Arrow type id by value rather than the DataType, so the dispatch reads a
// cached byte instead of chasing a shared_ptr and calling a virtual id() per field.
//
// Anything this cannot decide is refused rather than guessed: a false return
// costs a fallback to the DOM path (and, failing that, to rows), which is always
// correct. That asymmetry is deliberate - the columnar carrier only ever fires
// where it provably matches the row decode.
bool append_ondemand(arrow::Type::type type_id,
                     arrow::ArrayBuilder* b,
                     simdjson::ondemand::value& v,
                     std::int32_t decimal_scale) {
    // Through the error code, never the result's conversion to bool, which
    // throws for a token that starts with n and is not null, such as nul or nan:
    // the DOM parse refuses that line, so the batch goes to it instead.
    bool is_null{};
    if (v.is_null().get(is_null) != simdjson::SUCCESS) {
        return false;
    }
    if (is_null) {
        return b->AppendNull().ok();
    }
    switch (type_id) {
        case arrow::Type::INT64: {
            // An integer token only, as the DOM arm takes it. The row decode
            // carries an integer token as an exact int64 and a numeral with a
            // decimal point or an exponent, such as 5.0 or 5e0, as a double, and
            // the evaluator's arithmetic follows the value's kind, so a numeral
            // is refused rather than carried as an integer. So is a token past
            // int64, which a double read would clamp: -9223372036854775809 reads
            // as exactly -2^63.
            std::int64_t out{};
            return v.get_int64().get(out) == simdjson::SUCCESS &&
                   static_cast<arrow::Int64Builder*>(b)->Append(out).ok();
        }
        case arrow::Type::TIMESTAMP: {
            // timestamp(ms[, tz]): the epoch milliseconds, from an integer token
            // only, as the DOM arm takes them. A numeral such as 5.0 and a string,
            // digit text included, are refused.
            std::int64_t out{};
            return v.get_int64().get(out) == simdjson::SUCCESS &&
                   static_cast<arrow::TimestampBuilder*>(b)->Append(out).ok();
        }
        case arrow::Type::INT32: {
            // An integer token in int32 range, for the reason INT64 gives.
            std::int64_t x{};
            if (v.get_int64().get(x) != simdjson::SUCCESS ||
                x < std::numeric_limits<std::int32_t>::min() ||
                x > std::numeric_limits<std::int32_t>::max()) {
                return false;
            }
            return static_cast<arrow::Int32Builder*>(b)->Append(static_cast<std::int32_t>(x)).ok();
        }
        case arrow::Type::DOUBLE: {
            // An integer token is read as an integer first, as the DOM arm reads
            // it, and one past 2^53, which has no exact double, is left to the
            // row decode. A failed get_int64 leaves the value unconsumed. An
            // unsigned token above INT64_MAX reads through get_double, which
            // rounds it to the double the row decode's widening gives.
            constexpr std::int64_t kDoubleExactInt = std::int64_t{1} << 53;
            std::int64_t i{};
            if (v.get_int64().get(i) == simdjson::SUCCESS) {
                if (i < -kDoubleExactInt || i > kDoubleExactInt) {
                    return false;
                }
                return static_cast<arrow::DoubleBuilder*>(b)->Append(static_cast<double>(i)).ok();
            }
            double d{};
            if (integer_token_past_64_bits(v) || v.get_double().get(d) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::DoubleBuilder*>(b)->Append(d).ok();
        }
        case arrow::Type::FLOAT: {
            // An integer token is read as an integer first, as the row decode
            // reads it before rounding it to float precision, so -0 lands as
            // +0.0 on both. A failed get_int64 leaves the value unconsumed.
            std::int64_t i{};
            if (v.get_int64().get(i) == simdjson::SUCCESS) {
                return static_cast<arrow::FloatBuilder*>(b)
                    ->Append(static_cast<float>(static_cast<double>(i)))
                    .ok();
            }
            double d{};
            if (integer_token_past_64_bits(v) || v.get_double().get(d) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::FloatBuilder*>(b)->Append(static_cast<float>(d)).ok();
        }
        case arrow::Type::BOOL: {
            bool x{};
            if (v.get_bool().get(x) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::BooleanBuilder*>(b)->Append(x).ok();
        }
        case arrow::Type::STRING: {
            std::string_view sv;
            if (v.get_string().get(sv) != simdjson::SUCCESS) {
                return false;
            }
            return static_cast<arrow::StringBuilder*>(b)
                ->Append(sv.data(), static_cast<std::int32_t>(sv.size()))
                .ok();
        }
        case arrow::Type::DECIMAL128: {
            // Exact digits from the raw token, read during this same walk - so
            // unlike the DOM path there is no second scan of the line for them.
            // The token must parse as the DOM parse would read it first: the
            // decimal parser alone takes the digits in front of whatever follows
            // them, so 0x1F would land as 0 and 01 as 1 in a line the row decode
            // refuses, and an integer past 64 bits as itself.
            clink::config::JsonValue jv;
            const std::string_view tok = v.raw_json_token();
            if (tok.empty()) {
                return false;
            }
            if (tok.front() != '"') {
                if (!number_parses(v, tok)) {
                    return false;
                }
                std::size_t n = 0;
                while (n < tok.size() &&
                       (std::isdigit(static_cast<unsigned char>(tok[n])) != 0 || tok[n] == '-' ||
                        tok[n] == '+' || tok[n] == '.' || tok[n] == 'e' || tok[n] == 'E')) {
                    ++n;
                }
                auto d = clink::config::dec_parse(tok.substr(0, n));
                if (!d) {
                    return false;
                }
                jv = clink::config::make_dec_value(*d);
            } else {
                std::string_view sv;
                if (v.get_string().get(sv) != simdjson::SUCCESS) {
                    return false;
                }
                jv = clink::config::JsonValue{std::string(sv)};
            }
            const auto q = row_decimal_for(jv, decimal_scale);
            if (!q) {
                return false;
            }
            return static_cast<arrow::Decimal128Builder*>(b)->Append(q->unscaled).ok();
        }
        default:
            return false;
    }
}

}  // namespace

std::optional<Batch<Row>> JsonStringToRowColumnarOperator::build_columnar_ondemand_(
    const Batch<std::string>& in) const {
    if (!schema_capable_) {
        return std::nullopt;
    }
    if (!od_) {
        od_ = std::make_unique<Ondemand>();
    }
    const auto n = static_cast<std::int64_t>(in.size());
    auto* pool = arrow::default_memory_pool();

    arrow::Int64Builder t_b(pool);
    if (!t_b.Reserve(n).ok()) {
        return std::nullopt;
    }
    // An unprojected column gets NO builder: nothing downstream reads it, so building it
    // is the whole cost this projection exists to avoid. Its slot stays null and the field
    // walk below skips the append for it.
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> col_b(resolved_.size());
    for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
        if (!resolved_[ci].projected) {
            continue;
        }
        if (!arrow::MakeBuilder(pool, resolved_[ci].eff, &col_b[ci]).ok()) {
            return std::nullopt;
        }
        (void)col_b[ci]->Reserve(n);
    }

    std::vector<std::optional<std::int32_t>> parts;
    parts.reserve(in.size());
    bool any_partition = false;
    std::vector<char> seen(resolved_.size(), 0);

    // Per-record iterate(), with the scratch buffer grown MONOTONICALLY and the line
    // copied in - not assign() + resize() per record, which changed the vector's size
    // twice and zero-filled SIMDJSON_PADDING bytes for every single line. simdjson
    // requires the padding to be ALLOCATED and readable, not zeroed; only the first
    // `len` bytes are parsed, so whatever a previous longer line left in the padding
    // is never part of a document.
    //
    // MEASURED ALTERNATIVE, REJECTED: concatenating the batch into one buffer and
    // parsing it with a single iterate_many(). That is the API designed for streams of
    // small documents and it amortises stage1 (simdjson's SIMD structural prescan,
    // 13% of decode self time) from once per record to once per batch - but it
    // measured 5.85M rec/s against 5.87M... in fact SLOWER than 6.26M for the
    // per-record path, on ~120-byte documents. Streaming mode carries its own
    // per-document bookkeeping (document_reference, boundary rescan, _streaming
    // disabling some fast paths) and on these document sizes that costs more than the
    // stage1 it saves. Tried with batch_size at both 1MB and the exact buffer length,
    // in case parser capacity was hurting cache locality; both lost. Do not re-try
    // without measuring.
    for (const auto& rec : in) {
        const std::string& line = rec.value();
        const std::size_t need = line.size() + simdjson::SIMDJSON_PADDING;
        if (od_->pad.size() < need) {
            od_->pad.resize(need);
        }
        std::memcpy(od_->pad.data(), line.data(), line.size());

        simdjson::ondemand::document doc;
        if (od_->parser.iterate(od_->pad.data(), line.size(), od_->pad.size()).get(doc) !=
            simdjson::SUCCESS) {
            return std::nullopt;
        }
        simdjson::ondemand::object obj;
        if (doc.get_object().get(obj) != simdjson::SUCCESS) {
            return std::nullopt;
        }

        std::fill(seen.begin(), seen.end(), 0);
        std::size_t filled = 0;
        for (auto field : obj) {
            // escaped_key(), not unescaped_key(): the raw key bytes as they sit in the
            // input buffer, with no unescaping pass and no copy into simdjson's string
            // buffer. Real column names contain no escapes, so for them the raw key IS
            // the unescaped key and the comparison is identical.
            //
            // A key that DOES carry an escape simply fails to match, which returns -1,
            // bails the batch and falls back to the DOM path where the key is properly
            // unescaped - the same refuse-rather-than-guess asymmetry the rest of this
            // decoder relies on. It cannot match the WRONG column either: raw bytes
            // equal to a column name can only unescape to something else if the name
            // itself contains a backslash, and such a schema is refused up front
            // (schema_capable_).
            std::string_view key;
            if (field.escaped_key().get(key) != simdjson::SUCCESS) {
                return std::nullopt;
            }
            const int ci = column_index_at_(key, filled);
            if (ci < 0) {
                return std::nullopt;  // undeclared field: the row decode would keep it
            }
            const auto uci = static_cast<std::size_t>(ci);
            if (seen[uci] != 0) {
                return std::nullopt;  // duplicate key
            }
            simdjson::ondemand::value val;
            if (field.value().get(val) != simdjson::SUCCESS) {
                return std::nullopt;
            }
            const auto& res = resolved_[uci];
            // Declared but unprojected: read the value as far as the row decode's parse
            // reads it, then drop it. That parse refuses the whole line over a value it
            // cannot read, wherever it stands, so skipping it unread would carry a record
            // the row decode empties. It still counts toward `filled`, so the "every
            // declared column present" gate below is unchanged - the projection must not
            // turn a faithfulness check into a pass.
            if (res.projected) {
                if (!append_ondemand(res.type_id, col_b[uci].get(), val, res.scale)) {
                    return std::nullopt;
                }
            } else if (!value_parses(val, 0)) {
                return std::nullopt;
            }
            seen[uci] = 1;
            ++filled;
        }
        if (filled != resolved_.size()) {
            return std::nullopt;  // missing declared column
        }
        // Content after the closing brace, such as a second object on the same line:
        // the field walk stops at the brace, and the DOM parse refuses the line.
        if (!doc.at_end()) {
            return std::nullopt;
        }
        if (!clink::detail::append_event_time(t_b, rec.event_time()).ok()) {
            return std::nullopt;
        }
        auto p = rec.source_partition();
        if (p.has_value()) {
            any_partition = true;
        }
        parts.push_back(p);
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(resolved_.size() + 1);
    std::shared_ptr<arrow::Array> t_arr;
    if (!t_b.Finish(&t_arr).ok()) {
        return std::nullopt;
    }
    arrays.push_back(std::move(t_arr));
    // Same order and count as data_fields_, which also skipped the unprojected columns.
    for (std::size_t ci = 0; ci < resolved_.size(); ++ci) {
        if (!resolved_[ci].projected) {
            continue;
        }
        std::shared_ptr<arrow::Array> a;
        if (!col_b[ci]->Finish(&a).ok()) {
            return std::nullopt;
        }
        arrays.push_back(std::move(a));
    }
    auto rb = arrow::RecordBatch::Make(arrow::schema(data_fields_), n, std::move(arrays));
    return wrap_columnar_(std::move(rb), parts, any_partition);
}

}  // namespace clink::sql
