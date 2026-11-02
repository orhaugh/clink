#pragma once

namespace clink::cluster::detail {

// While one is alive on a thread, a factory registration on that thread that
// replaces a factory already registered is not reported. clink's built-ins
// register the same factories twice, through OperatorRegistry's own set and
// then through the plugin API, and that replacement says nothing; a plugin's
// override, registered outside this scope, is still reported.
class QuietBuiltInReplacements {
public:
    QuietBuiltInReplacements() noexcept;
    ~QuietBuiltInReplacements();
    QuietBuiltInReplacements(const QuietBuiltInReplacements&) = delete;
    QuietBuiltInReplacements& operator=(const QuietBuiltInReplacements&) = delete;

private:
    bool previous_;
};

}  // namespace clink::cluster::detail
