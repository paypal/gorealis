1.22.5

* Upgrading to thrift 0.14.0

1.22.4

* Updates which result in a no-op now return a response value so that the caller may analyze it to determine what happened

1.22.3

* Contains a monitor timeout fix. Previously an error was being left unchecked which made a specific monitor timining out not be handled properly.

1.22.2

* Bug fix: Change in retry mechanism created a deadlock. This release reverts that particular change.

1.22.1

* Adding safeguards against setting multiple constraints with the same name for a single task.

1.22.0

* CreateService and StartJobUpdate do not continue retrying if a timeout has been encountered
by the HTTP client. Instead they now return an error that conforms to the Timedout interface.
Users can check for a Timedout error by using `realis.IsTimeout(err)`.
* New API function VariableBatchStep has been added which returns the current batch at which
a Variable Batch Update configured Update is currently in.
* Added new PauseUpdateMonitor which monitors an update until it is an `ROLL_FORWARD_PAUSED` state.
* Added variableBatchStep command to sample client to be used for testing new VariableBatchStep api.
* JobUpdateStatus has changed function signature from:
`JobUpdateStatus(updateKey aurora.JobUpdateKey, desiredStatuses map[aurora.JobUpdateStatus]bool, interval, timeout time.Duration) (aurora.JobUpdateStatus, error)`
to
`JobUpdateStatus(updateKey aurora.JobUpdateKey, desiredStatuses []aurora.JobUpdateStatus, interval, timeout time.Duration) (aurora.JobUpdateStatus, error)`
* Added TerminalUpdateStates function which returns an slice containing all UpdateStates which are considered terminal states.

1.21.0

* Version numbering change. Future versions will be labled X.Y.Z where X is the major version, Y is the Aurora version the library has been tested against (e.g. 21 -> 0.21.0), and X is the minor revision.
* Moved to Thrift 0.12.0 code generator and go library.
* `aurora.ACTIVE_STATES`, `aurora.SLAVE_ASSIGNED_STATES`, `aurora.LIVE_STATES`, `aurora.TERMINAL_STATES`, `aurora.ACTIVE_JOB_UPDATE_STATES`, `aurora.AWAITNG_PULSE_JOB_UPDATE_STATES` are all now generated as a slices.
* Please use `realis.ActiveStates`, `realis.SlaveAssignedStates`,`realis.LiveStates`, `realis.TerminalStates`, `realis.ActiveJobUpdateStates`, `realis.AwaitingPulseJobUpdateStates` in their places when map representations are needed.
* `GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error)` has changed signature to ` GetInstanceIds(key *aurora.JobKey, states []aurora.ScheduleStatus) ([]int32, error)`
* Adding support for GPU as resource.
* Changing compose environment to Aurora snapshot in order to support staggered update.
* Adding staggered updates API.
