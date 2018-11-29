package realis

import (
	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

var ActiveStates = make(map[aurora.ScheduleStatus]bool)
var SlaveAssignedStates = make(map[aurora.ScheduleStatus]bool)
var LiveStates = make(map[aurora.ScheduleStatus]bool)
var TerminalStates = make(map[aurora.ScheduleStatus]bool)
var ActiveJobUpdateStates = make(map[aurora.JobUpdateStatus]bool)
var AwaitingPulseJobUpdateStates = make(map[aurora.JobUpdateStatus]bool)

func init() {
	for _, status := range aurora.ACTIVE_STATES {
		ActiveStates[status] = true
	}

	for _, status := range aurora.SLAVE_ASSIGNED_STATES {
		SlaveAssignedStates[status] = true
	}

	for _, status := range aurora.LIVE_STATES {
		LiveStates[status] = true
	}

	for _, status := range aurora.TERMINAL_STATES {
		TerminalStates[status] = true
	}

	for _, status := range aurora.ACTIVE_JOB_UPDATE_STATES {
		ActiveJobUpdateStates[status] = true
	}
	for _, status := range aurora.AWAITNG_PULSE_JOB_UPDATE_STATES {
		AwaitingPulseJobUpdateStates[status] = true
	}
}
