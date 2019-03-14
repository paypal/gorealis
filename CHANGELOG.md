1.22.0 (unreleased)

* Moved to Thrift 0.12.0 code generator and go library.
* `aurora.ACTIVE_STATES`, `aurora.SLAVE_ASSIGNED_STATES`, `aurora.LIVE_STATES`, `aurora.TERMINAL_STATES`, `aurora.ACTIVE_JOB_UPDATE_STATES`, `aurora.AWAITNG_PULSE_JOB_UPDATE_STATES` are all now generated as a slices.
* Please use `realis.ActiveStates`, `realis.SlaveAssignedStates`,`realis.LiveStates`, `realis.TerminalStates`, `realis.ActiveJobUpdateStates`, `realis.AwaitingPulseJobUpdateStates` in their places when map representations are needed.
* `GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error)` has changed signature to ` GetInstanceIds(key *aurora.JobKey, states []aurora.ScheduleStatus) ([]int32, error)`
* Changing compose environment to Aurora snapshot in order to support staggered update.
* Adding staggered updates API.
