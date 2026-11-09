#pragma once

// Pure rules of the Redis keyed sink, testable without a server: the Redis key
// a changelog record names. The record reader, the changelog marker and the
// removal of the marker from the stored value are the SQL sinks' shared rules
// (clink/connectors/sink_record_rules.hpp).

#include <string>
#include <vector>

#include "clink/connectors/sink_record_rules.hpp"

namespace clink::redis {

using sinkrules::change_kind_of;
using sinkrules::ChangeKind;
using sinkrules::JsonMember;
using sinkrules::JsonMembers;
using sinkrules::kRowKindMember;
using sinkrules::read_json_object;
using sinkrules::RecordRefused;
using sinkrules::remove_member;

// The key a changelog record names: `prefix`, then each key member as text, a
// string as its characters, a number as its exact plain decimal, true or false,
// the members of a composite key joined by the unit separator (U+001F). Redis
// compares keys byte for byte, so this text is the row's identity. Refused: a
// key member missing or null, an object or array, a number too long to write
// plainly, and, in a composite key, a string holding U+001F, which would let
// two rows name one key.
[[nodiscard]] std::string redis_key(const JsonMembers& m,
                                    const std::vector<std::string>& key_columns,
                                    const std::string& prefix);

}  // namespace clink::redis
