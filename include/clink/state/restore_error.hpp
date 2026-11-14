#pragma once

// Restore failures that a retry cannot cure.
//
// A subtask that fails is normally restarted: a worker lost, a broker down, a
// peer gone are all transient. A restore that is refused is not. The restart
// restores the same point (a job that has taken no checkpoint of its own reuses
// the one it was submitted with), meets the same state and is refused again,
// until the job's restart budget is spent and it fails with the cause buried
// under a restart count. The Worker therefore marks a subtask that throws one of
// these fatal (Worker::run_task_), and the job fails once, with this message.
//
// Header-only on purpose. An exception thrown by code compiled into a job's
// plugin .so (which links its own clink_core) is caught by the host by type, and
// that works under RTLD_LOCAL only because both standard libraries compare the
// typeinfo of a default-visibility class without a key function by name
// (Cluster.PluginThrownRestoreRefusalIsFatalAcrossTheDlopenSeam pins it).

#include <stdexcept>

namespace clink::state {

// Base of every restore failure that fails the same way however often it is
// retried. CheckpointIntegrityError (a damaged restore point) derives from it.
class NonRetryableRestoreError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The engine declines a restore that would lose or misread state: keyed state
// of an operator the subtask does not run, a participant's snapshot missing, a
// schema with no migration path, a snapshot format newer than this build, a
// checkpoint the state backend cannot reach.
class RestoreRefused : public NonRetryableRestoreError {
public:
    using NonRetryableRestoreError::NonRetryableRestoreError;
};

}  // namespace clink::state
