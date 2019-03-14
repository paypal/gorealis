1.22.0 (unreleased)

* Changing compose environment to Aurora snapshot in order to support staggered update.
* Adding staggered updates API.

1.21.1

* CreateService and StartJobUpdate do not continue retrying if a timeout has been encountered
by the HTTP client. Instead they now return an error that conforms to the Timedout interface.
Users can check for a Timedout error by using `realis.IsTimeout(err)`.

1.21.0

* Version numbering change. Future versions will be labled X.Y.Z where X is the major version, Y is the Aurora version the library has been tested against (e.g. 21 -> 0.21.0), and X is the minor revision.
* Moved to Thrift 0.12.0 code generator and go library.
* `aurora.ACTIVE_STATES`, `aurora.SLAVE_ASSIGNED_STATES`, `aurora.LIVE_STATES`, `aurora.TERMINAL_STATES`, `aurora.ACTIVE_JOB_UPDATE_STATES`, `aurora.AWAITNG_PULSE_JOB_UPDATE_STATES` are all now generated as a slices.
* Please use `realis.ActiveStates`, `realis.SlaveAssignedStates`,`realis.LiveStates`, `realis.TerminalStates`, `realis.ActiveJobUpdateStates`, `realis.AwaitingPulseJobUpdateStates` in their places when map representations are needed.
* `GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error)` has changed signature to ` GetInstanceIds(key *aurora.JobKey, states []aurora.ScheduleStatus) ([]int32, error)`
* Adding support for GPU as resource.
