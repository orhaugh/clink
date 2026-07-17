#include "clink/sql/preparse.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

#include "clink/sql/parser.hpp"  // TranslationError
#include "clink/sql/type.hpp"    // parse_sql_type_expression (scalar leaves)

namespace clink::sql::preparse {

namespace {

bool is_ident_char(char c) {
    return (std::isalnum(static_cast<unsigned char>(c)) != 0) || c == '_';
}

char lower(char c) {
    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
}

// s[pos..] begins with `word` (case-insensitive) at an identifier boundary on
// both sides. `pos` is assumed to be a token start (caller checks the left
// boundary); we re-check the right boundary here.
bool word_at(std::string_view s, std::size_t pos, std::string_view word) {
    if (pos + word.size() > s.size()) {
        return false;
    }
    for (std::size_t k = 0; k < word.size(); ++k) {
        if (lower(s[pos + k]) != lower(word[k])) {
            return false;
        }
    }
    const std::size_t after = pos + word.size();
    if (after < s.size() && is_ident_char(s[after])) {
        return false;
    }
    return true;
}

bool left_boundary(std::string_view s, std::size_t pos) {
    return pos == 0 || !is_ident_char(s[pos - 1]);
}

// If a string literal / quoted identifier / dollar-quote / comment starts at
// s[i], return the index just past it; else return i unchanged. PG block
// comments nest.
std::size_t skip_quote_or_comment(std::string_view s, std::size_t i) {
    if (i >= s.size()) {
        return i;
    }
    const char c = s[i];
    // line comment
    if (c == '-' && i + 1 < s.size() && s[i + 1] == '-') {
        std::size_t j = i + 2;
        while (j < s.size() && s[j] != '\n') {
            ++j;
        }
        return j;
    }
    // block comment (nesting)
    if (c == '/' && i + 1 < s.size() && s[i + 1] == '*') {
        std::size_t j = i + 2;
        int depth = 1;
        while (j + 1 < s.size() && depth > 0) {
            if (s[j] == '/' && s[j + 1] == '*') {
                depth++;
                j += 2;
            } else if (s[j] == '*' && s[j + 1] == '/') {
                depth--;
                j += 2;
            } else {
                ++j;
            }
        }
        return depth > 0 ? s.size() : j;
    }
    // single-quoted string / double-quoted identifier ('' / "" escapes)
    if (c == '\'' || c == '"') {
        std::size_t j = i + 1;
        while (j < s.size()) {
            if (s[j] == c) {
                if (j + 1 < s.size() && s[j + 1] == c) {
                    j += 2;  // escaped quote
                    continue;
                }
                return j + 1;
            }
            ++j;
        }
        return s.size();
    }
    // dollar-quoted string: $tag$ ... $tag$
    if (c == '$') {
        std::size_t tag_end = i + 1;
        while (tag_end < s.size() && (is_ident_char(s[tag_end]) && s[tag_end] != '$')) {
            ++tag_end;
        }
        if (tag_end < s.size() && s[tag_end] == '$') {
            const std::string_view tag = s.substr(i, tag_end - i + 1);  // "$tag$"
            std::size_t j = tag_end + 1;
            while (j + tag.size() <= s.size()) {
                if (s.substr(j, tag.size()) == tag) {
                    return j + tag.size();
                }
                ++j;
            }
            return s.size();
        }
    }
    return i;  // nothing consumed
}

bool is_composite_keyword(std::string_view s, std::size_t i) {
    return word_at(s, i, "map") || word_at(s, i, "row") || word_at(s, i, "multiset") ||
           word_at(s, i, "array");
}

// From the '<' at s[lt], return the index just past the matching '>'. Skips
// strings/comments inside; counts nested '<'/'>'. Throws on imbalance.
std::size_t match_angle(std::string_view s, std::size_t lt) {
    std::size_t j = lt + 1;
    int depth = 1;
    while (j < s.size()) {
        const std::size_t q = skip_quote_or_comment(s, j);
        if (q != j) {
            j = q;
            continue;
        }
        if (s[j] == '<') {
            ++depth;
        } else if (s[j] == '>') {
            if (--depth == 0) {
                return j + 1;
            }
        }
        ++j;
    }
    throw TranslationError("pre-parser: unbalanced '<' in composite type", 0);
}

// If s[i] starts a composite-type island (MAP/ROW/MULTISET/ARRAY + '<...>'),
// return the index just past the matching '>'; else 0. `lt_out` receives the
// '<' position so the caller can confirm the keyword is angle-bracketed (and
// not e.g. a ROW(...) constructor or a `map < 5` comparison).
std::size_t composite_island_end(std::string_view s, std::size_t i, std::size_t* lt_out) {
    if (!is_composite_keyword(s, i)) {
        return 0;
    }
    // skip keyword, then whitespace/comments, then require '<'
    std::size_t j = i;
    while (j < s.size() && is_ident_char(s[j])) {
        ++j;
    }
    while (j < s.size()) {
        const std::size_t q = skip_quote_or_comment(s, j);
        if (q != j) {
            j = q;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(s[j])) != 0) {
            ++j;
            continue;
        }
        break;
    }
    if (j >= s.size() || s[j] != '<') {
        return 0;  // not a composite type (e.g. ROW(...) constructor, `map`, `array[`)
    }
    *lt_out = j;
    return match_angle(s, j);
}

std::string_view trim(std::string_view s) {
    std::size_t a = 0;
    std::size_t b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a])) != 0) {
        ++a;
    }
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1])) != 0) {
        --b;
    }
    return s.substr(a, b - a);
}

// Split `body` at top-level commas (ignoring commas nested inside <>, (), []).
std::vector<std::string_view> split_top_level(std::string_view body) {
    std::vector<std::string_view> parts;
    int angle = 0;
    int paren = 0;
    int bracket = 0;
    std::size_t start = 0;
    std::size_t j = 0;
    while (j < body.size()) {
        const std::size_t q = skip_quote_or_comment(body, j);
        if (q != j) {
            j = q;
            continue;
        }
        const char c = body[j];
        if (c == '<') {
            ++angle;
        } else if (c == '>') {
            --angle;
        } else if (c == '(') {
            ++paren;
        } else if (c == ')') {
            --paren;
        } else if (c == '[') {
            ++bracket;
        } else if (c == ']') {
            --bracket;
        } else if (c == ',' && angle == 0 && paren == 0 && bracket == 0) {
            parts.push_back(body.substr(start, j - start));
            start = j + 1;
        }
        ++j;
    }
    parts.push_back(body.substr(start));
    return parts;
}

}  // namespace

ast::TypeName parse_composite_type(std::string_view type_expr) {
    const std::string_view e = trim(type_expr);
    std::size_t lt = 0;
    const std::size_t end = composite_island_end(e, 0, &lt);
    if (end == 0) {
        // Scalar or `T[]` leaf: delegate so it gets PG-canonical spelling.
        return parse_sql_type_expression(std::string(e));
    }
    if (end != e.size()) {
        // Trailing text after the '>' (e.g. "MAP<..>[]"): handle a trailing
        // array suffix; anything else is malformed.
        const std::string_view tail = trim(e.substr(end));
        int extra = 0;
        std::size_t k = 0;
        while (k + 1 < tail.size() && tail[k] == '[' && tail[k + 1] == ']') {
            ++extra;
            k += 2;
        }
        if (k != tail.size()) {
            throw TranslationError(
                "pre-parser: unexpected text after composite type: " + std::string(tail), 0);
        }
        ast::TypeName inner = parse_composite_type(e.substr(0, end));
        inner.array_ndims += extra;
        return inner;
    }

    const std::string_view kw = trim(e.substr(0, lt));
    const std::string_view body = trim(e.substr(lt + 1, (end - 1) - (lt + 1)));
    auto parts = split_top_level(body);

    ast::TypeName t;
    if (word_at(kw, 0, "map") && kw.size() == 3) {
        if (parts.size() != 2) {
            throw TranslationError("pre-parser: MAP<k,v> requires exactly two type arguments", 0);
        }
        t.name = "map";
        t.params.push_back(parse_composite_type(parts[0]));
        t.params.push_back(parse_composite_type(parts[1]));
    } else if (word_at(kw, 0, "multiset") && kw.size() == 8) {
        if (parts.size() != 1) {
            throw TranslationError("pre-parser: MULTISET<t> requires exactly one type argument", 0);
        }
        t.name = "multiset";
        t.params.push_back(parse_composite_type(parts[0]));
    } else if (word_at(kw, 0, "array") && kw.size() == 5) {
        if (parts.size() != 1) {
            throw TranslationError("pre-parser: ARRAY<t> requires exactly one type argument", 0);
        }
        t = parse_composite_type(parts[0]);
        t.array_ndims += 1;
    } else {  // row
        if (parts.empty()) {
            throw TranslationError("pre-parser: ROW<...> requires at least one field", 0);
        }
        t.name = "row";
        for (const auto& part : parts) {
            const std::string_view f = trim(part);
            std::size_t ns = 0;
            while (ns < f.size() && is_ident_char(f[ns])) {
                ++ns;
            }
            if (ns == 0 || ns == f.size()) {
                throw TranslationError(
                    "pre-parser: ROW field must be 'name TYPE': " + std::string(f), 0);
            }
            t.field_names.emplace_back(f.substr(0, ns));
            t.params.push_back(parse_composite_type(trim(f.substr(ns))));
        }
    }
    return t;
}

// --- MATCH_RECOGNIZE (#61 phase 2) ------------------------------------------

namespace {

constexpr std::uint32_t kQuantMax = std::numeric_limits<std::uint32_t>::max();

std::size_t skip_ws(std::string_view s, std::size_t i) {
    while (i < s.size()) {
        const std::size_t q = skip_quote_or_comment(s, i);
        if (q != i) {
            i = q;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(s[i])) != 0) {
            ++i;
            continue;
        }
        break;
    }
    return i;
}

// Top-level (paren/bracket depth 0, not in string/comment) search for a single
// keyword word, from `from`. npos if absent.
std::size_t find_top_word(std::string_view s, std::string_view kw, std::size_t from) {
    int paren = 0;
    int bracket = 0;
    std::size_t i = from;
    while (i < s.size()) {
        const std::size_t q = skip_quote_or_comment(s, i);
        if (q != i) {
            i = q;
            continue;
        }
        const char c = s[i];
        if (c == '(') {
            ++paren;
        } else if (c == ')') {
            --paren;
        } else if (c == '[') {
            ++bracket;
        } else if (c == ']') {
            --bracket;
        } else if (paren == 0 && bracket == 0 && left_boundary(s, i) && word_at(s, i, kw)) {
            return i;
        }
        ++i;
    }
    return std::string_view::npos;
}

// Top-level " AS " (case-insensitive); returns the 'A' position or npos.
std::size_t find_top_as(std::string_view s) {
    return find_top_word(s, "as", 0);
}

// Consume the given words in sequence (whitespace/comment-separated) starting at
// the keyword position `i`; throw if any word is missing.
std::size_t expect_words(std::string_view s,
                         std::size_t i,
                         std::initializer_list<std::string_view> words) {
    for (const auto& w : words) {
        i = skip_ws(s, i);
        if (!word_at(s, i, w)) {
            throw TranslationError("MATCH_RECOGNIZE: expected '" + std::string(w) + "'", 0);
        }
        i += w.size();
    }
    return i;
}

// Split at top-level commas tracking only () and [] (NOT <>, which are
// comparison operators in MR predicates/measures, not type brackets).
std::vector<std::string_view> split_mr_commas(std::string_view body) {
    std::vector<std::string_view> parts;
    int paren = 0;
    int bracket = 0;
    std::size_t start = 0;
    std::size_t j = 0;
    while (j < body.size()) {
        const std::size_t q = skip_quote_or_comment(body, j);
        if (q != j) {
            j = q;
            continue;
        }
        const char c = body[j];
        if (c == '(') {
            ++paren;
        } else if (c == ')') {
            --paren;
        } else if (c == '[') {
            ++bracket;
        } else if (c == ']') {
            --bracket;
        } else if (c == ',' && paren == 0 && bracket == 0) {
            parts.push_back(body.substr(start, j - start));
            start = j + 1;
        }
        ++j;
    }
    parts.push_back(body.substr(start));
    return parts;
}

std::vector<std::string_view> split_ws(std::string_view s) {
    std::vector<std::string_view> out;
    std::size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i])) != 0) {
            ++i;
        }
        if (i >= s.size()) {
            break;
        }
        const std::size_t st = i;
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i])) == 0) {
            ++i;
        }
        out.push_back(s.substr(st, i - st));
    }
    return out;
}

ast::PatternVar parse_pattern_token(std::string_view tok) {
    tok = trim(tok);
    if (tok.find('(') != std::string_view::npos) {
        throw TranslationError(
            "MATCH_RECOGNIZE: grouped patterns like (A B)+ are not supported in v1", 0);
    }
    std::size_t n = 0;
    while (n < tok.size() && is_ident_char(tok[n])) {
        ++n;
    }
    if (n == 0) {
        throw TranslationError("MATCH_RECOGNIZE: empty pattern variable", 0);
    }
    ast::PatternVar v;
    v.name = std::string(tok.substr(0, n));
    const std::string_view q = trim(tok.substr(n));
    if (q.empty()) {
        v.min_count = 1;
        v.max_count = 1;
    } else if (q == "+") {
        v.min_count = 1;
        v.max_count = kQuantMax;
    } else if (q == "*") {
        v.min_count = 0;
        v.max_count = kQuantMax;
    } else if (q == "?") {
        v.min_count = 0;
        v.max_count = 1;
    } else if (q.size() >= 2 && q.front() == '{' && q.back() == '}') {
        const std::string_view inner = trim(q.substr(1, q.size() - 2));
        const auto comma = inner.find(',');
        try {
            if (comma == std::string_view::npos) {
                const auto exact = static_cast<std::uint32_t>(std::stoul(std::string(inner)));
                v.min_count = exact;
                v.max_count = exact;
            } else {
                v.min_count = static_cast<std::uint32_t>(
                    std::stoul(std::string(trim(inner.substr(0, comma)))));
                const std::string_view hi = trim(inner.substr(comma + 1));
                v.max_count = hi.empty() ? kQuantMax
                                         : static_cast<std::uint32_t>(std::stoul(std::string(hi)));
            }
        } catch (const std::exception&) {
            throw TranslationError("MATCH_RECOGNIZE: bad quantifier '" + std::string(q) + "'", 0);
        }
    } else {
        throw TranslationError("MATCH_RECOGNIZE: unsupported quantifier '" + std::string(q) + "'",
                               0);
    }
    return v;
}

}  // namespace

ast::MatchRecognizeClause parse_match_recognize(std::string_view input_table,
                                                std::string_view body) {
    ast::MatchRecognizeClause c;
    c.input.name = std::string(trim(input_table));
    if (const auto dot = c.input.name.find('.'); dot != std::string::npos) {
        c.input.schema = c.input.name.substr(0, dot);
        c.input.name = c.input.name.substr(dot + 1);
    }

    const auto npos = std::string_view::npos;
    const std::size_t p_part = find_top_word(body, "partition", 0);
    const std::size_t p_order = find_top_word(body, "order", 0);
    const std::size_t p_meas = find_top_word(body, "measures", 0);
    const std::size_t p_one = find_top_word(body, "one", 0);
    const std::size_t p_all = find_top_word(body, "all", 0);
    const std::size_t p_after = find_top_word(body, "after", 0);
    const std::size_t p_pat = find_top_word(body, "pattern", 0);
    const std::size_t p_def = find_top_word(body, "define", 0);

    if (p_part == npos) {
        throw TranslationError("MATCH_RECOGNIZE: PARTITION BY is required in v1", 0);
    }
    if (p_order == npos) {
        throw TranslationError("MATCH_RECOGNIZE: ORDER BY is required", 0);
    }
    if (p_pat == npos) {
        throw TranslationError("MATCH_RECOGNIZE: PATTERN clause is required", 0);
    }
    if (p_def == npos) {
        throw TranslationError("MATCH_RECOGNIZE: DEFINE clause is required", 0);
    }
    if (p_all != npos) {
        (void)expect_words(body, p_all, {"all", "rows", "per", "match"});
        c.all_rows = true;
    }

    std::vector<std::size_t> markers;
    for (const auto m : {p_part, p_order, p_meas, p_one, p_all, p_after, p_pat, p_def}) {
        if (m != npos) {
            markers.push_back(m);
        }
    }
    std::sort(markers.begin(), markers.end());
    auto next_marker = [&](std::size_t after_pos) -> std::size_t {
        std::size_t best = body.size();
        for (const auto m : markers) {
            if (m > after_pos && m < best) {
                best = m;
            }
        }
        return best;
    };

    // PARTITION BY <cols>
    {
        const std::size_t s = expect_words(body, p_part, {"partition", "by"});
        const std::string_view content = trim(body.substr(s, next_marker(p_part) - s));
        for (const auto part : split_mr_commas(content)) {
            const std::string_view col = trim(part);
            if (!col.empty()) {
                c.partition_by.emplace_back(col);
            }
        }
    }
    // ORDER BY <col> (v1: single column)
    {
        const std::size_t s = expect_words(body, p_order, {"order", "by"});
        const std::string_view content = trim(body.substr(s, next_marker(p_order) - s));
        const auto parts = split_top_level(content);
        if (parts.size() != 1) {
            throw TranslationError("MATCH_RECOGNIZE: v1 supports a single ORDER BY column", 0);
        }
        const std::string_view oc = trim(parts[0]);
        std::size_t n = 0;
        while (n < oc.size() && (is_ident_char(oc[n]) || oc[n] == '.')) {
            ++n;
        }
        c.order_by = std::string(oc.substr(0, n));
    }
    // MEASURES <expr AS alias>, ...
    if (p_meas != npos) {
        const std::size_t s = p_meas + std::string_view("measures").size();
        const std::string_view content = trim(body.substr(s, next_marker(p_meas) - s));
        for (const auto part : split_mr_commas(content)) {
            const std::string_view m = trim(part);
            if (m.empty()) {
                continue;
            }
            const std::size_t as = find_top_as(m);
            if (as == npos) {
                throw TranslationError(
                    "MATCH_RECOGNIZE: MEASURES item requires 'AS alias': " + std::string(m), 0);
            }
            std::string_view expr = trim(m.substr(0, as));
            const std::string_view alias = trim(m.substr(as + 2));
            if (word_at(expr, 0, "final")) {
                expr = trim(expr.substr(5));
            } else if (word_at(expr, 0, "running")) {
                throw TranslationError("MATCH_RECOGNIZE: RUNNING measures are not supported in v1",
                                       0);
            }
            c.measures.push_back(ast::MrMeasure{std::string(expr), std::string(alias)});
        }
    }
    // ONE ROW PER MATCH (default) / AFTER MATCH SKIP PAST LAST ROW (only form) - validate.
    if (p_one != npos) {
        (void)expect_words(body, p_one, {"one", "row", "per", "match"});
    }
    if (p_after != npos) {
        (void)expect_words(body, p_after, {"after", "match", "skip", "past", "last", "row"});
    }
    // PATTERN ( ... )
    {
        const std::size_t s = skip_ws(body, p_pat + std::string_view("pattern").size());
        if (s >= body.size() || body[s] != '(') {
            throw TranslationError("MATCH_RECOGNIZE: PATTERN expects '('", 0);
        }
        int depth = 1;
        std::size_t k = s + 1;
        const std::size_t inner_start = k;
        while (k < body.size() && depth > 0) {
            const std::size_t q = skip_quote_or_comment(body, k);
            if (q != k) {
                k = q;
                continue;
            }
            if (body[k] == '(') {
                ++depth;
            } else if (body[k] == ')') {
                --depth;
            }
            ++k;
        }
        if (depth != 0) {
            throw TranslationError("MATCH_RECOGNIZE: unbalanced PATTERN parentheses", 0);
        }
        const std::string_view inner = body.substr(inner_start, (k - 1) - inner_start);
        for (const auto tok : split_ws(inner)) {
            c.pattern.push_back(parse_pattern_token(tok));
        }
        if (c.pattern.empty()) {
            throw TranslationError("MATCH_RECOGNIZE: PATTERN is empty", 0);
        }
    }
    // DEFINE <var AS predicate>, ...
    {
        const std::size_t s = p_def + std::string_view("define").size();
        const std::string_view content = trim(body.substr(s, next_marker(p_def) - s));
        for (const auto part : split_mr_commas(content)) {
            const std::string_view d = trim(part);
            if (d.empty()) {
                continue;
            }
            const std::size_t as = find_top_as(d);
            if (as == npos) {
                throw TranslationError(
                    "MATCH_RECOGNIZE: DEFINE item requires 'var AS predicate': " + std::string(d),
                    0);
            }
            const std::string_view var = trim(d.substr(0, as));
            const std::string_view pred = trim(d.substr(as + 2));
            c.define.push_back(ast::MrDefine{std::string(var), std::string(pred)});
        }
        if (c.define.empty()) {
            throw TranslationError("MATCH_RECOGNIZE: DEFINE is empty", 0);
        }
    }
    return c;
}

// Parse a process-table-function body `TABLE t [PARTITION BY cols] [ORDER BY
// col]` (the text inside the `name(...)` parens) into a structural clause. v1
// rejects scalar args (a comma after the TABLE argument).
ast::ProcessTableFunctionClause parse_process_table_function(std::string_view fn_name,
                                                             std::string_view body) {
    ast::ProcessTableFunctionClause c;
    c.fn_name = std::string(trim(fn_name));

    const auto npos = std::string_view::npos;
    const std::size_t p_table = find_top_word(body, "table", 0);
    if (p_table == npos) {
        throw TranslationError("process table function: expected a TABLE argument", 0);
    }
    const std::size_t p_part = find_top_word(body, "partition", 0);
    const std::size_t p_order = find_top_word(body, "order", 0);
    auto next_marker = [&](std::size_t after_pos) -> std::size_t {
        std::size_t best = body.size();
        for (const auto m : {p_part, p_order}) {
            if (m != npos && m > after_pos && m < best) {
                best = m;
            }
        }
        return best;
    };

    // TABLE <table_ref> - a single identifier up to the next clause marker.
    {
        const std::size_t s = p_table + std::string_view("table").size();
        const std::string_view region = trim(body.substr(s, next_marker(p_table) - s));
        if (region.find(',') != std::string_view::npos) {
            throw TranslationError(
                "process table function: scalar arguments are not supported "
                "in v1 (only TABLE t PARTITION BY ...)",
                0);
        }
        std::size_t n = 0;
        while (n < region.size() && (is_ident_char(region[n]) || region[n] == '.')) {
            ++n;
        }
        if (n == 0) {
            throw TranslationError("process table function: TABLE requires a table name", 0);
        }
        if (!trim(region.substr(n)).empty()) {
            throw TranslationError(
                "process table function: unexpected tokens after the TABLE argument", 0);
        }
        c.input.name = std::string(region.substr(0, n));
        if (const auto dot = c.input.name.find('.'); dot != std::string::npos) {
            c.input.schema = c.input.name.substr(0, dot);
            c.input.name = c.input.name.substr(dot + 1);
        }
    }
    // PARTITION BY <cols>
    if (p_part != npos) {
        const std::size_t s = expect_words(body, p_part, {"partition", "by"});
        const std::string_view content = trim(body.substr(s, next_marker(p_part) - s));
        for (const auto part : split_mr_commas(content)) {
            const std::string_view col = trim(part);
            if (!col.empty()) {
                c.partition_by.emplace_back(col);
            }
        }
    }
    // ORDER BY <col> (parsed but unused in v1)
    if (p_order != npos) {
        const std::size_t s = expect_words(body, p_order, {"order", "by"});
        const std::string_view content = trim(body.substr(s, body.size() - s));
        const auto parts = split_top_level(content);
        if (!parts.empty()) {
            const std::string_view oc = trim(parts[0]);
            std::size_t n = 0;
            while (n < oc.size() && (is_ident_char(oc[n]) || oc[n] == '.')) {
                ++n;
            }
            c.order_by = std::string(oc.substr(0, n));
        }
    }
    return c;
}

namespace {

// From an open-paren at text[open], return the index just past the matching ')',
// skipping strings/comments. Throws on imbalance.
std::size_t match_paren(std::string_view text, std::size_t open, std::string_view ctx) {
    int depth = 1;
    std::size_t k = open + 1;
    while (k < text.size() && depth > 0) {
        const std::size_t q = skip_quote_or_comment(text, k);
        if (q != k) {
            k = q;
            continue;
        }
        if (text[k] == '(') {
            ++depth;
        } else if (text[k] == ')') {
            --depth;
        }
        ++k;
    }
    if (depth != 0) {
        throw TranslationError(std::string(ctx) + ": unbalanced parentheses", 0);
    }
    return k;  // index just past ')'
}

// Extract a `TABLE <ref>` island argument into a TableRef (schema.name aware).
ast::TableRef parse_table_arg(std::string_view part, std::string_view ctx) {
    const std::string_view t = trim(part);
    if (!(left_boundary(t, 0) && word_at(t, 0, "table"))) {
        throw TranslationError(std::string(ctx) + ": first argument must be TABLE <name>", 0);
    }
    const std::string_view region = trim(t.substr(std::string_view("table").size()));
    std::size_t n = 0;
    while (n < region.size() && (is_ident_char(region[n]) || region[n] == '.')) {
        ++n;
    }
    if (n == 0) {
        throw TranslationError(std::string(ctx) + ": TABLE requires a table name", 0);
    }
    if (!trim(region.substr(n)).empty()) {
        throw TranslationError(std::string(ctx) + ": unexpected tokens after the TABLE argument",
                               0);
    }
    ast::TableRef ref;
    ref.name = std::string(region.substr(0, n));
    if (const auto dot = ref.name.find('.'); dot != std::string::npos) {
        ref.schema = ref.name.substr(0, dot);
        ref.name = ref.name.substr(dot + 1);
    }
    return ref;
}

// Read a bare identifier that fills the whole (trimmed) part.
std::string parse_bare_ident(std::string_view part, std::string_view ctx, std::string_view what) {
    const std::string_view t = trim(part);
    std::size_t n = 0;
    while (n < t.size() && is_ident_char(t[n])) {
        ++n;
    }
    if (n == 0 || !trim(t.substr(n)).empty()) {
        throw TranslationError(std::string(ctx) + ": expected " + std::string(what), 0);
    }
    return std::string(t.substr(0, n));
}

// Parse a `DESCRIPTOR(col, col, ...)` part into its column-name list.
std::vector<std::string> parse_descriptor(std::string_view part, std::string_view ctx) {
    const std::string_view t = trim(part);
    if (!(left_boundary(t, 0) && word_at(t, 0, "descriptor"))) {
        throw TranslationError(std::string(ctx) + ": expected DESCRIPTOR(columns)", 0);
    }
    const std::size_t p = skip_ws(t, std::string_view("descriptor").size());
    if (p >= t.size() || t[p] != '(') {
        throw TranslationError(std::string(ctx) + ": DESCRIPTOR must be followed by (columns)", 0);
    }
    const std::size_t close = match_paren(t, p, ctx);  // index past ')'
    if (!trim(t.substr(close)).empty()) {
        throw TranslationError(std::string(ctx) + ": unexpected tokens after DESCRIPTOR(...)", 0);
    }
    const std::string_view inner = t.substr(p + 1, (close - 1) - (p + 1));
    std::vector<std::string> cols;
    for (const auto c : split_top_level(inner)) {
        const std::string_view col = trim(c);
        if (col.empty()) {
            continue;
        }
        std::size_t n = 0;
        while (n < col.size() && is_ident_char(col[n])) {
            ++n;
        }
        if (n == 0 || n != col.size()) {
            throw TranslationError(
                std::string(ctx) + ": DESCRIPTOR columns must be simple identifiers", 0);
        }
        cols.emplace_back(col);
    }
    if (cols.empty()) {
        throw TranslationError(std::string(ctx) + ": DESCRIPTOR requires at least one column", 0);
    }
    return cols;
}

// Parse a single `key = 'value'` option part into a StorageOption.
ast::StorageOption parse_kv_option(std::string_view part, std::string_view ctx) {
    const std::string_view t = trim(part);
    const auto eq = t.find('=');
    if (eq == std::string_view::npos) {
        throw TranslationError(std::string(ctx) + ": option must be key='value'", 0);
    }
    ast::StorageOption opt;
    // The key may be a bare identifier (connector='...') or a quoted string
    // ('provider'='...'); accept both and store the unquoted key.
    std::string_view k = trim(t.substr(0, eq));
    if (k.size() >= 2 && k.front() == '\'' && k.back() == '\'') {
        k = k.substr(1, k.size() - 2);
    }
    opt.key = std::string(k);
    std::string_view v = trim(t.substr(eq + 1));
    if (v.size() < 2 || v.front() != '\'' || v.back() != '\'') {
        throw TranslationError(
            std::string(ctx) + ": option value must be a quoted string for key '" + opt.key + "'",
            0);
    }
    opt.value = std::string(v.substr(1, v.size() - 2));
    return opt;
}

// Parse `name TYPE[, name TYPE ...]` into a ColumnDef list (INPUT / OUTPUT lists).
std::vector<ast::ColumnDef> parse_column_defs(std::string_view inner, std::string_view ctx) {
    std::vector<ast::ColumnDef> cols;
    for (const auto part : split_top_level(inner)) {
        const std::string_view p = trim(part);
        if (p.empty()) {
            continue;
        }
        std::size_t n = 0;
        while (n < p.size() && is_ident_char(p[n])) {
            ++n;
        }
        if (n == 0) {
            throw TranslationError(std::string(ctx) + ": expected 'column type'", 0);
        }
        ast::ColumnDef col;
        col.name = std::string(p.substr(0, n));
        const std::string_view type_expr = trim(p.substr(n));
        if (type_expr.empty()) {
            throw TranslationError(std::string(ctx) + ": column '" + col.name + "' has no type", 0);
        }
        col.type = parse_composite_type(type_expr);  // handles scalar + MAP/ROW/ARRAY leaves
        cols.push_back(std::move(col));
    }
    if (cols.empty()) {
        throw TranslationError(std::string(ctx) + ": empty column list", 0);
    }
    return cols;
}

}  // namespace

ast::MlPredictClause parse_ml_predict(std::string_view body) {
    ast::MlPredictClause c;
    const auto parts = split_top_level(body);
    if (parts.size() != 3) {
        throw TranslationError(
            "ML_PREDICT: expected ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(cols))", 0);
    }
    c.input = parse_table_arg(parts[0], "ML_PREDICT");
    // MODEL <name>
    const std::string_view m = trim(parts[1]);
    if (!(left_boundary(m, 0) && word_at(m, 0, "model"))) {
        throw TranslationError("ML_PREDICT: second argument must be MODEL <name>", 0);
    }
    c.model_name = parse_bare_ident(
        m.substr(std::string_view("model").size()), "ML_PREDICT", "a model name after MODEL");
    c.feature_columns = parse_descriptor(parts[2], "ML_PREDICT");
    return c;
}

ast::VectorSearchClause parse_vector_search(std::string_view body) {
    ast::VectorSearchClause c;
    const auto parts = split_top_level(body);
    if (parts.size() < 5) {
        throw TranslationError(
            "VECTOR_SEARCH: expected VECTOR_SEARCH(TABLE t, query_col, vector_table, "
            "DESCRIPTOR(index_col), top_k [, k='v', ...])",
            0);
    }
    c.input = parse_table_arg(parts[0], "VECTOR_SEARCH");
    c.query_vector_column = parse_bare_ident(parts[1], "VECTOR_SEARCH", "the query vector column");
    c.vector_table = parse_bare_ident(parts[2], "VECTOR_SEARCH", "the vector table name");
    const auto index_cols = parse_descriptor(parts[3], "VECTOR_SEARCH");
    if (index_cols.size() != 1) {
        throw TranslationError("VECTOR_SEARCH: DESCRIPTOR must name exactly one index column", 0);
    }
    c.index_column = index_cols[0];
    // top_k integer literal
    const std::string_view topk = trim(parts[4]);
    std::int64_t k = 0;
    const auto* first = topk.data();
    const auto* last = topk.data() + topk.size();
    const auto res = std::from_chars(first, last, k);
    if (res.ec != std::errc{} || res.ptr != last) {
        throw TranslationError("VECTOR_SEARCH: top_k must be an integer literal", 0);
    }
    c.top_k = k;
    // Trailing k='v' options (e.g. metric='cosine').
    for (std::size_t p = 5; p < parts.size(); ++p) {
        if (trim(parts[p]).empty()) {
            continue;
        }
        c.options.push_back(parse_kv_option(parts[p], "VECTOR_SEARCH"));
    }
    return c;
}

ast::CreateModelStmt parse_create_model(std::string_view text) {
    ast::CreateModelStmt c;
    std::size_t i = skip_ws(text, 0);
    if (!word_at(text, i, "create")) {
        throw TranslationError("CREATE MODEL: expected CREATE", 0);
    }
    i = skip_ws(text, i + std::string_view("create").size());
    if (!word_at(text, i, "model")) {
        throw TranslationError("CREATE MODEL: expected MODEL", 0);
    }
    i = skip_ws(text, i + std::string_view("model").size());
    // IF NOT EXISTS
    if (word_at(text, i, "if")) {
        i = skip_ws(text, i + 2);
        if (!word_at(text, i, "not")) {
            throw TranslationError("CREATE MODEL: expected NOT after IF", 0);
        }
        i = skip_ws(text, i + 3);
        if (!word_at(text, i, "exists")) {
            throw TranslationError("CREATE MODEL: expected EXISTS after IF NOT", 0);
        }
        i = skip_ws(text, i + 6);
        c.if_not_exists = true;
    }
    // model name (identifier, optional schema.name)
    std::size_t n = 0;
    while (i + n < text.size() && (is_ident_char(text[i + n]) || text[i + n] == '.')) {
        ++n;
    }
    if (n == 0) {
        throw TranslationError("CREATE MODEL: expected a model name", 0);
    }
    c.model_name = std::string(text.substr(i, n));
    if (const auto dot = c.model_name.find('.'); dot != std::string::npos) {
        c.schema = c.model_name.substr(0, dot);
        c.model_name = c.model_name.substr(dot + 1);
    }
    i = skip_ws(text, i + n);
    // INPUT ( ... )
    if (!word_at(text, i, "input")) {
        throw TranslationError("CREATE MODEL '" + c.model_name + "': expected INPUT (columns)", 0);
    }
    i = skip_ws(text, i + std::string_view("input").size());
    if (i >= text.size() || text[i] != '(') {
        throw TranslationError(
            "CREATE MODEL '" + c.model_name + "': INPUT must be followed by (columns)", 0);
    }
    std::size_t close = match_paren(text, i, "CREATE MODEL INPUT");
    c.input_columns =
        parse_column_defs(text.substr(i + 1, (close - 1) - (i + 1)), "CREATE MODEL INPUT");
    i = skip_ws(text, close);
    // OUTPUT ( ... )
    if (!word_at(text, i, "output")) {
        throw TranslationError("CREATE MODEL '" + c.model_name + "': expected OUTPUT (columns)", 0);
    }
    i = skip_ws(text, i + std::string_view("output").size());
    if (i >= text.size() || text[i] != '(') {
        throw TranslationError(
            "CREATE MODEL '" + c.model_name + "': OUTPUT must be followed by (columns)", 0);
    }
    close = match_paren(text, i, "CREATE MODEL OUTPUT");
    c.output_columns =
        parse_column_defs(text.substr(i + 1, (close - 1) - (i + 1)), "CREATE MODEL OUTPUT");
    i = skip_ws(text, close);
    // WITH ( ... )  (optional)
    if (i < text.size() && word_at(text, i, "with")) {
        i = skip_ws(text, i + std::string_view("with").size());
        if (i >= text.size() || text[i] != '(') {
            throw TranslationError(
                "CREATE MODEL '" + c.model_name + "': WITH must be followed by (options)", 0);
        }
        close = match_paren(text, i, "CREATE MODEL WITH");
        for (const auto part : split_top_level(text.substr(i + 1, (close - 1) - (i + 1)))) {
            if (trim(part).empty()) {
                continue;
            }
            c.options.push_back(parse_kv_option(part, "CREATE MODEL WITH"));
        }
        i = skip_ws(text, close);
    }
    if (!trim(text.substr(i)).empty()) {
        throw TranslationError("CREATE MODEL '" + c.model_name + "': unexpected trailing tokens",
                               0);
    }
    return c;
}

namespace {

// Rewrite `name(TABLE t PARTITION BY ...)` FROM-clause islands to "__clink_ptf_N"
// placeholder table refs. Fires only when a '(' whose first significant token is
// the keyword TABLE is preceded by an identifier (the function name) in FROM
// position (the token before it is FROM / JOIN / ','), so a scalar call like
// count(table) outside FROM is left untouched. Any trailing `[AS] alias` is left
// in place so PG parses it onto the placeholder TableRef (reattached later).
// Also dispatches the SQL-native-AI FROM islands ML_PREDICT / VECTOR_SEARCH, which
// share the exact `name(TABLE ...)`-in-FROM detection: the function-name token
// selects which structural parser + placeholder prefix to use, so a single scanner
// covers all three (and there is no double-detection risk of the generic PTF path
// swallowing an ML_PREDICT / VECTOR_SEARCH first).
std::string rewrite_table_functions(std::string_view sql,
                                    std::vector<ast::ProcessTableFunctionClause>& sites,
                                    std::vector<ast::MlPredictClause>& ml_sites,
                                    std::vector<ast::VectorSearchClause>& vs_sites) {
    std::string out;
    out.reserve(sql.size());
    std::size_t i = 0;
    while (i < sql.size()) {
        const std::size_t q = skip_quote_or_comment(sql, i);
        if (q != i) {
            out.append(sql.substr(i, q - i));
            i = q;
            continue;
        }
        if (sql[i] == '(') {
            // First significant token after '(' must be the keyword TABLE.
            std::size_t j = i + 1;
            while (j < sql.size()) {
                const std::size_t cq = skip_quote_or_comment(sql, j);
                if (cq != j) {
                    j = cq;
                } else if (std::isspace(static_cast<unsigned char>(sql[j])) != 0) {
                    ++j;
                } else {
                    break;
                }
            }
            if (j < sql.size() && left_boundary(sql, j) && word_at(sql, j, "table")) {
                // The function name is the identifier already emitted into `out`
                // immediately before this '(' (skip trailing whitespace).
                std::size_t e = out.size();
                while (e > 0 && std::isspace(static_cast<unsigned char>(out[e - 1])) != 0) {
                    --e;
                }
                std::size_t ts = e;
                while (ts > 0 && is_ident_char(out[ts - 1])) {
                    --ts;
                }
                // FROM position: the token before the name is FROM / JOIN / ','.
                std::size_t b = ts;
                while (b > 0 && std::isspace(static_cast<unsigned char>(out[b - 1])) != 0) {
                    --b;
                }
                bool from_position = false;
                if (b > 0 && out[b - 1] == ',') {
                    from_position = true;
                } else {
                    std::size_t ws = b;
                    while (ws > 0 && is_ident_char(out[ws - 1])) {
                        --ws;
                    }
                    std::string lower_prev;
                    for (std::size_t x = ws; x < b; ++x) {
                        lower_prev.push_back(lower(out[x]));
                    }
                    from_position = (lower_prev == "from" || lower_prev == "join");
                }
                if (ts < e && from_position) {
                    const std::string fn_name = out.substr(ts, e - ts);
                    const std::size_t body_start = i + 1;
                    int depth = 1;
                    std::size_t k = body_start;
                    while (k < sql.size() && depth > 0) {
                        const std::size_t cq = skip_quote_or_comment(sql, k);
                        if (cq != k) {
                            k = cq;
                            continue;
                        }
                        if (sql[k] == '(') {
                            ++depth;
                        } else if (sql[k] == ')') {
                            --depth;
                        }
                        ++k;
                    }
                    if (depth != 0) {
                        throw TranslationError("process table function: unbalanced parentheses", 0);
                    }
                    const std::string_view body = sql.substr(body_start, (k - 1) - body_start);
                    std::string fn_lower;
                    fn_lower.reserve(fn_name.size());
                    for (const char ch : fn_name) {
                        fn_lower.push_back(lower(ch));
                    }
                    out.erase(ts);  // drop the function-name token + trailing ws
                    if (fn_lower == "ml_predict") {
                        ast::MlPredictClause clause = parse_ml_predict(body);
                        out.append(kMlPredictPrefix);
                        out.append(std::to_string(ml_sites.size()));
                        ml_sites.push_back(std::move(clause));
                    } else if (fn_lower == "vector_search") {
                        ast::VectorSearchClause clause = parse_vector_search(body);
                        out.append(kVectorSearchPrefix);
                        out.append(std::to_string(vs_sites.size()));
                        vs_sites.push_back(std::move(clause));
                    } else {
                        ast::ProcessTableFunctionClause clause =
                            parse_process_table_function(fn_name, body);
                        out.append(kProcessTableFunctionPrefix);
                        out.append(std::to_string(sites.size()));
                        sites.push_back(std::move(clause));
                    }
                    i = k;  // resume past the closing ')'; any trailing alias flows to PG
                    continue;
                }
            }
        }
        out.push_back(sql[i]);
        ++i;
    }
    return out;
}

// Rewrite `<table> MATCH_RECOGNIZE (...)` islands to "__clink_mr_N" placeholder
// table refs, recording the parsed clause. Only fires when MATCH_RECOGNIZE is
// (a) followed by '(' and (b) the table it follows is in FROM position (the
// token before the table is FROM / JOIN / ',') - so a column or function named
// match_recognize is left untouched.
std::string rewrite_match_recognize(std::string_view sql,
                                    std::vector<ast::MatchRecognizeClause>& sites) {
    std::string out;
    out.reserve(sql.size());
    std::size_t i = 0;
    while (i < sql.size()) {
        const std::size_t q = skip_quote_or_comment(sql, i);
        if (q != i) {
            out.append(sql.substr(i, q - i));
            i = q;
            continue;
        }
        if (left_boundary(sql, i) && word_at(sql, i, "match_recognize")) {
            // (a) require a '(' after the keyword.
            std::size_t j = i + std::string_view("match_recognize").size();
            while (j < sql.size()) {
                const std::size_t cq = skip_quote_or_comment(sql, j);
                if (cq != j) {
                    j = cq;
                } else if (std::isspace(static_cast<unsigned char>(sql[j])) != 0) {
                    ++j;
                } else {
                    break;
                }
            }
            if (j < sql.size() && sql[j] == '(') {
                // Extract the table token already emitted into `out`.
                std::size_t e = out.size();
                while (e > 0 && std::isspace(static_cast<unsigned char>(out[e - 1])) != 0) {
                    --e;
                }
                std::size_t ts = e;
                while (ts > 0 && (is_ident_char(out[ts - 1]) || out[ts - 1] == '.')) {
                    --ts;
                }
                // (b) confirm FROM position: the token before the table is FROM / JOIN / ','.
                std::size_t b = ts;
                while (b > 0 && std::isspace(static_cast<unsigned char>(out[b - 1])) != 0) {
                    --b;
                }
                bool from_position = false;
                if (b > 0 && out[b - 1] == ',') {
                    from_position = true;
                } else {
                    std::size_t ws = b;
                    while (ws > 0 && (is_ident_char(out[ws - 1]))) {
                        --ws;
                    }
                    const std::string prev = out.substr(ws, b - ws);
                    std::string lower_prev;
                    for (char ch : prev) {
                        lower_prev.push_back(lower(ch));
                    }
                    from_position = (lower_prev == "from" || lower_prev == "join");
                }
                if (ts < e && from_position) {
                    const std::string table = out.substr(ts, e - ts);
                    // match balanced '(...)' body
                    const std::size_t body_start = j + 1;
                    int depth = 1;
                    std::size_t k = body_start;
                    while (k < sql.size() && depth > 0) {
                        const std::size_t cq = skip_quote_or_comment(sql, k);
                        if (cq != k) {
                            k = cq;
                            continue;
                        }
                        if (sql[k] == '(') {
                            ++depth;
                        } else if (sql[k] == ')') {
                            --depth;
                        }
                        ++k;
                    }
                    if (depth != 0) {
                        throw TranslationError("MATCH_RECOGNIZE: unbalanced parentheses", 0);
                    }
                    const std::string_view body = sql.substr(body_start, (k - 1) - body_start);
                    ast::MatchRecognizeClause clause = parse_match_recognize(table, body);
                    out.erase(ts);  // drop the table token + trailing ws
                    out.append(kMatchRecognizePrefix);
                    out.append(std::to_string(sites.size()));
                    sites.push_back(std::move(clause));
                    i = k;  // resume past the closing ')'
                    continue;
                }
            }
            // Not an MR-in-FROM island: emit the keyword verbatim.
            out.append(sql.substr(i, std::string_view("match_recognize").size()));
            i += std::string_view("match_recognize").size();
            continue;
        }
        out.push_back(sql[i]);
        ++i;
    }
    return out;
}

}  // namespace

namespace {

// Rewrite composite-type islands (MAP/ROW/MULTISET) inside CREATE TABLE column
// lists to "__clink_ctype_N" placeholders, recording the parsed types.
std::string rewrite_composite_types(std::string_view sql,
                                    std::vector<ast::TypeName>& composite_types) {
    std::string out;
    out.reserve(sql.size());

    std::size_t i = 0;
    while (i < sql.size()) {
        const std::size_t q = skip_quote_or_comment(sql, i);
        if (q != i) {
            out.append(sql.substr(i, q - i));
            i = q;
            continue;
        }
        // Detect "CREATE TABLE" at a word boundary; composite types are only
        // rewritten inside its column list (so expressions like `a < b` are
        // never mistaken for type brackets).
        if (left_boundary(sql, i) && word_at(sql, i, "create")) {
            std::size_t j = i + 6;
            std::size_t after_ws = j;
            while (after_ws < sql.size()) {
                const std::size_t cq = skip_quote_or_comment(sql, after_ws);
                if (cq != after_ws) {
                    after_ws = cq;
                } else if (std::isspace(static_cast<unsigned char>(sql[after_ws])) != 0) {
                    ++after_ws;
                } else {
                    break;
                }
            }
            if (word_at(sql, after_ws, "table")) {
                // Copy verbatim up to and including the column-list '(', then
                // process the body, then resume normal scanning after the ')'.
                std::size_t k = i;
                // copy through to the first top-level '(' (string/comment-aware)
                bool found_paren = false;
                while (k < sql.size()) {
                    const std::size_t cq = skip_quote_or_comment(sql, k);
                    if (cq != k) {
                        out.append(sql.substr(k, cq - k));
                        k = cq;
                        continue;
                    }
                    out.push_back(sql[k]);
                    if (sql[k] == '(') {
                        ++k;
                        found_paren = true;
                        break;
                    }
                    ++k;
                }
                if (!found_paren) {
                    i = k;
                    continue;
                }
                // Inside the column list: depth starts at 1.
                int depth = 1;
                while (k < sql.size() && depth > 0) {
                    const std::size_t cq = skip_quote_or_comment(sql, k);
                    if (cq != k) {
                        out.append(sql.substr(k, cq - k));
                        k = cq;
                        continue;
                    }
                    if (left_boundary(sql, k)) {
                        std::size_t lt = 0;
                        const std::size_t isl_end = composite_island_end(sql, k, &lt);
                        if (isl_end != 0) {
                            ast::TypeName t = parse_composite_type(sql.substr(k, isl_end - k));
                            out.append(kCompositeTypePrefix);
                            out.append(std::to_string(composite_types.size()));
                            composite_types.push_back(std::move(t));
                            k = isl_end;
                            continue;
                        }
                    }
                    if (sql[k] == '(') {
                        ++depth;
                    } else if (sql[k] == ')') {
                        --depth;
                    }
                    out.push_back(sql[k]);
                    ++k;
                }
                i = k;
                continue;
            }
        }
        out.push_back(sql[i]);
        ++i;
    }
    return out;
}

}  // namespace

// The `ANALYZE TABLE <name>` spelling used by other dialects is not PG-grammatical (PG is
// `ANALYZE <name>`), so drop the optional `TABLE` keyword right after a
// statement-leading `ANALYZE`. Statement-boundary-aware (only at input start or
// after a top-level ';') and string/comment-safe, so a literal containing
// "ANALYZE TABLE" is untouched.
std::string strip_analyze_table_keyword(std::string_view sql) {
    std::string out;
    out.reserve(sql.size());
    std::size_t i = 0;
    bool stmt_start = true;
    while (i < sql.size()) {
        if (stmt_start) {
            const std::size_t k = skip_ws(sql, i);  // leading ws + comments
            out.append(sql.substr(i, k - i));
            i = k;
            if (i >= sql.size()) {
                break;
            }
            if (word_at(sql, i, "analyze")) {
                out.append(sql.substr(i, 7));  // copy "analyze" verbatim (any case)
                i += 7;
                const std::size_t a = skip_ws(sql, i);
                if (word_at(sql, a, "table")) {
                    out.append(sql.substr(i, a - i));  // ws/comments between the keywords
                    out.push_back(' ');                // keep a name separator
                    i = a + 5;                         // drop "table"
                }
            }
            stmt_start = false;
            continue;
        }
        const std::size_t q = skip_quote_or_comment(sql, i);
        if (q != i) {  // a string/comment: copy verbatim
            out.append(sql.substr(i, q - i));
            i = q;
            continue;
        }
        const char c = sql[i];
        out.push_back(c);
        ++i;
        if (c == ';') {
            stmt_start = true;
        }
    }
    return out;
}

// Rewrite each leading `CREATE MODEL ...` statement to a placeholder
// `CREATE TABLE __clink_model_N (...)` statement (which libpg_query parses), recording
// the structurally-parsed CreateModelStmt for reattach. A statement-boundary walk like
// strip_analyze_table_keyword: at each top-level statement start, if it is CREATE MODEL,
// replace the whole statement text; otherwise copy verbatim (so CREATE TABLE / VIEW /
// MATERIALIZED VIEW fall through to libpg_query untouched).
std::string rewrite_create_model(std::string_view sql, std::vector<ast::CreateModelStmt>& sites) {
    std::string out;
    out.reserve(sql.size());
    std::size_t i = 0;
    bool stmt_start = true;
    while (i < sql.size()) {
        if (stmt_start) {
            const std::size_t k = skip_ws(sql, i);  // leading ws + comments
            out.append(sql.substr(i, k - i));
            i = k;
            stmt_start = false;
            if (i >= sql.size()) {
                break;
            }
            if (word_at(sql, i, "create")) {
                const std::size_t a = skip_ws(sql, i + std::string_view("create").size());
                if (word_at(sql, a, "model")) {
                    // Statement extent: up to the next top-level ';' or end of input.
                    std::size_t j = i;
                    while (j < sql.size()) {
                        const std::size_t q = skip_quote_or_comment(sql, j);
                        if (q != j) {
                            j = q;
                            continue;
                        }
                        if (sql[j] == ';') {
                            break;
                        }
                        ++j;
                    }
                    sites.push_back(parse_create_model(sql.substr(i, j - i)));
                    out.append("CREATE TABLE ");
                    out.append(kCreateModelPrefix);
                    out.append(std::to_string(sites.size() - 1));
                    out.append(" (__clink_model_marker BIGINT)");
                    i = j;  // resume at ';' or end; the ';' (if any) copied below
                }
            }
            continue;
        }
        const std::size_t q = skip_quote_or_comment(sql, i);
        if (q != i) {  // a string/comment: copy verbatim
            out.append(sql.substr(i, q - i));
            i = q;
            continue;
        }
        const char c = sql[i];
        out.push_back(c);
        ++i;
        if (c == ';') {
            stmt_start = true;
        }
    }
    return out;
}

PreparseResult preparse(std::string_view sql) {
    PreparseResult res;
    // Rewrite the FROM-clause islands PG cannot grammar-parse to placeholder
    // table refs (MATCH_RECOGNIZE, then the table-function family: process-table
    // functions plus the SQL-native-AI ML_PREDICT / VECTOR_SEARCH), then rewrite
    // composite-type islands on the result. The passes are independent. The
    // leading-statement rewrites (ANALYZE-TABLE keyword strip, then CREATE MODEL)
    // run first.
    const std::string an_sql = strip_analyze_table_keyword(sql);
    const std::string cm_sql = rewrite_create_model(an_sql, res.models);
    const std::string mr_sql = rewrite_match_recognize(cm_sql, res.match_recognize);
    const std::string ptf_sql =
        rewrite_table_functions(mr_sql, res.table_functions, res.ml_predicts, res.vector_searches);
    res.rewritten_sql = rewrite_composite_types(ptf_sql, res.composite_types);
    return res;
}

void reattach_composite_types(ast::Script& script,
                              const std::vector<ast::TypeName>& composite_types) {
    if (composite_types.empty()) {
        return;
    }
    const std::string prefix = kCompositeTypePrefix;
    auto fix = [&](ast::TypeName& type) {
        if (type.name.rfind(prefix, 0) != 0) {
            return;
        }
        const std::string suffix = type.name.substr(prefix.size());
        std::size_t idx = 0;
        try {
            idx = static_cast<std::size_t>(std::stoul(suffix));
        } catch (...) {
            return;
        }
        if (idx >= composite_types.size()) {
            return;
        }
        const int carried = type.array_ndims;
        type = composite_types[idx];
        type.array_ndims += carried;
    };
    for (auto& stmt : script.statements) {
        if (auto* create = std::get_if<ast::CreateTableStmt>(&stmt)) {
            for (auto& col : create->columns) {
                fix(col.type);
            }
        }
    }
}

namespace {

// Replace any from_items TableRef whose name is a "__clink_mr_N" placeholder
// with match_recognize[N]; drop the mirror copy from from_clause. Recurses into
// subqueries / CTEs / set-op branches.
void reattach_select_mr(ast::SelectStmt& sel, std::vector<ast::MatchRecognizeClause>& sites) {
    const std::string prefix = kMatchRecognizePrefix;
    for (auto& item : sel.from_items) {
        if (auto* tr = std::get_if<ast::TableRef>(&item)) {
            if (tr->name.rfind(prefix, 0) != 0) {
                continue;
            }
            std::size_t idx = 0;
            try {
                idx = static_cast<std::size_t>(std::stoul(tr->name.substr(prefix.size())));
            } catch (...) {
                continue;
            }
            if (idx >= sites.size()) {
                continue;
            }
            if (tr->alias.has_value() && !sites[idx].alias.has_value()) {
                sites[idx].alias = tr->alias;
            }
            const std::string placeholder = tr->name;
            item = std::make_unique<ast::MatchRecognizeClause>(std::move(sites[idx]));
            sel.from_clause.erase(
                std::remove_if(sel.from_clause.begin(),
                               sel.from_clause.end(),
                               [&](const ast::TableRef& t) { return t.name == placeholder; }),
                sel.from_clause.end());
        } else if (auto* sub = std::get_if<std::unique_ptr<ast::SubqueryItem>>(&item)) {
            if (*sub && (*sub)->body) {
                reattach_select_mr(*(*sub)->body, sites);
            }
        }
    }
    if (sel.larg) {
        reattach_select_mr(*sel.larg, sites);
    }
    if (sel.rarg) {
        reattach_select_mr(*sel.rarg, sites);
    }
    for (auto& cte : sel.with_clause) {
        if (cte.body) {
            reattach_select_mr(*cte.body, sites);
        }
    }
}

void reattach_stmt_mr(ast::Statement& stmt, std::vector<ast::MatchRecognizeClause>& sites) {
    if (auto* sel = std::get_if<ast::SelectStmt>(&stmt)) {
        reattach_select_mr(*sel, sites);
    } else if (auto* ins = std::get_if<ast::InsertStmt>(&stmt)) {
        reattach_select_mr(ins->select, sites);
    } else if (auto* ex = std::get_if<std::unique_ptr<ast::ExplainStmt>>(&stmt)) {
        if (*ex) {
            reattach_stmt_mr((*ex)->query, sites);
        }
    }
}

// Replace any from_items TableRef whose name is a "__clink_ptf_N" placeholder
// with table_functions[N] (carrying the placeholder's alias); drop the mirror
// copy from from_clause. Recurses into subqueries / CTEs / set-op branches.
void reattach_select_ptf(ast::SelectStmt& sel,
                         std::vector<ast::ProcessTableFunctionClause>& sites) {
    const std::string prefix = kProcessTableFunctionPrefix;
    for (auto& item : sel.from_items) {
        if (auto* tr = std::get_if<ast::TableRef>(&item)) {
            if (tr->name.rfind(prefix, 0) != 0) {
                continue;
            }
            std::size_t idx = 0;
            try {
                idx = static_cast<std::size_t>(std::stoul(tr->name.substr(prefix.size())));
            } catch (...) {
                continue;
            }
            if (idx >= sites.size()) {
                continue;
            }
            if (tr->alias.has_value() && !sites[idx].alias.has_value()) {
                sites[idx].alias = tr->alias;
            }
            const std::string placeholder = tr->name;
            item = std::make_unique<ast::ProcessTableFunctionClause>(std::move(sites[idx]));
            sel.from_clause.erase(
                std::remove_if(sel.from_clause.begin(),
                               sel.from_clause.end(),
                               [&](const ast::TableRef& t) { return t.name == placeholder; }),
                sel.from_clause.end());
        } else if (auto* sub = std::get_if<std::unique_ptr<ast::SubqueryItem>>(&item)) {
            if (*sub && (*sub)->body) {
                reattach_select_ptf(*(*sub)->body, sites);
            }
        }
    }
    if (sel.larg) {
        reattach_select_ptf(*sel.larg, sites);
    }
    if (sel.rarg) {
        reattach_select_ptf(*sel.rarg, sites);
    }
    for (auto& cte : sel.with_clause) {
        if (cte.body) {
            reattach_select_ptf(*cte.body, sites);
        }
    }
}

void reattach_stmt_ptf(ast::Statement& stmt, std::vector<ast::ProcessTableFunctionClause>& sites) {
    if (auto* sel = std::get_if<ast::SelectStmt>(&stmt)) {
        reattach_select_ptf(*sel, sites);
    } else if (auto* ins = std::get_if<ast::InsertStmt>(&stmt)) {
        reattach_select_ptf(ins->select, sites);
    } else if (auto* ex = std::get_if<std::unique_ptr<ast::ExplainStmt>>(&stmt)) {
        if (*ex) {
            reattach_stmt_ptf((*ex)->query, sites);
        }
    }
}

// Generic FROM-island reattach for the SQL-native-AI clauses (ML_PREDICT /
// VECTOR_SEARCH): swap a "<prefix>N" placeholder TableRef for sites[N] (carrying the
// alias), drop the mirror from from_clause, and recurse into subqueries / set-op
// branches / CTEs. Clause must be a FromItem variant alternative carrying an `alias`.
template <class Clause>
void reattach_select_island(ast::SelectStmt& sel,
                            std::vector<Clause>& sites,
                            const std::string& prefix) {
    for (auto& item : sel.from_items) {
        if (auto* tr = std::get_if<ast::TableRef>(&item)) {
            if (tr->name.rfind(prefix, 0) != 0) {
                continue;
            }
            std::size_t idx = 0;
            try {
                idx = static_cast<std::size_t>(std::stoul(tr->name.substr(prefix.size())));
            } catch (...) {
                continue;
            }
            if (idx >= sites.size()) {
                continue;
            }
            if (tr->alias.has_value() && !sites[idx].alias.has_value()) {
                sites[idx].alias = tr->alias;
            }
            const std::string placeholder = tr->name;
            item = std::make_unique<Clause>(std::move(sites[idx]));
            sel.from_clause.erase(
                std::remove_if(sel.from_clause.begin(),
                               sel.from_clause.end(),
                               [&](const ast::TableRef& t) { return t.name == placeholder; }),
                sel.from_clause.end());
        } else if (auto* sub = std::get_if<std::unique_ptr<ast::SubqueryItem>>(&item)) {
            if (*sub && (*sub)->body) {
                reattach_select_island(*(*sub)->body, sites, prefix);
            }
        }
    }
    if (sel.larg) {
        reattach_select_island(*sel.larg, sites, prefix);
    }
    if (sel.rarg) {
        reattach_select_island(*sel.rarg, sites, prefix);
    }
    for (auto& cte : sel.with_clause) {
        if (cte.body) {
            reattach_select_island(*cte.body, sites, prefix);
        }
    }
}

template <class Clause>
void reattach_stmt_island(ast::Statement& stmt,
                          std::vector<Clause>& sites,
                          const std::string& prefix) {
    if (auto* sel = std::get_if<ast::SelectStmt>(&stmt)) {
        reattach_select_island(*sel, sites, prefix);
    } else if (auto* ins = std::get_if<ast::InsertStmt>(&stmt)) {
        reattach_select_island(ins->select, sites, prefix);
    } else if (auto* ex = std::get_if<std::unique_ptr<ast::ExplainStmt>>(&stmt)) {
        if (*ex) {
            reattach_stmt_island((*ex)->query, sites, prefix);
        }
    }
}

}  // namespace

void reattach_match_recognize(ast::Script& script,
                              std::vector<ast::MatchRecognizeClause>& match_recognize) {
    if (match_recognize.empty()) {
        return;
    }
    for (auto& stmt : script.statements) {
        reattach_stmt_mr(stmt, match_recognize);
    }
}

void reattach_process_table_functions(
    ast::Script& script, std::vector<ast::ProcessTableFunctionClause>& table_functions) {
    if (table_functions.empty()) {
        return;
    }
    for (auto& stmt : script.statements) {
        reattach_stmt_ptf(stmt, table_functions);
    }
}

void reattach_create_models(ast::Script& script, std::vector<ast::CreateModelStmt>& models) {
    if (models.empty()) {
        return;
    }
    const std::string prefix = kCreateModelPrefix;
    for (auto& stmt : script.statements) {
        auto* create = std::get_if<ast::CreateTableStmt>(&stmt);
        if (create == nullptr || create->table_name.rfind(prefix, 0) != 0) {
            continue;
        }
        std::size_t idx = 0;
        try {
            idx = static_cast<std::size_t>(std::stoul(create->table_name.substr(prefix.size())));
        } catch (...) {
            continue;
        }
        if (idx >= models.size()) {
            continue;
        }
        // Swap the whole Statement for the recorded CreateModelStmt. emplace<T> is
        // used rather than converting assignment so the alternative is selected
        // unambiguously (the variant holds move-only alternatives).
        stmt.emplace<ast::CreateModelStmt>(std::move(models[idx]));
    }
}

void reattach_ml_predicts(ast::Script& script, std::vector<ast::MlPredictClause>& ml_predicts) {
    if (ml_predicts.empty()) {
        return;
    }
    const std::string prefix = kMlPredictPrefix;
    for (auto& stmt : script.statements) {
        reattach_stmt_island(stmt, ml_predicts, prefix);
    }
}

void reattach_vector_searches(ast::Script& script,
                              std::vector<ast::VectorSearchClause>& vector_searches) {
    if (vector_searches.empty()) {
        return;
    }
    const std::string prefix = kVectorSearchPrefix;
    for (auto& stmt : script.statements) {
        reattach_stmt_island(stmt, vector_searches, prefix);
    }
}

}  // namespace clink::sql::preparse
