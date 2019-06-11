package realis

import (
	"net/url"
	"strings"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

const apiPath = "/api"

// ActiveStates - States a task may be in when active.
var ActiveStates = make(map[aurora.ScheduleStatus]bool)

// SlaveAssignedStates - States a task may be in when it has already been assigned to a Mesos agent.
var SlaveAssignedStates = make(map[aurora.ScheduleStatus]bool)

// LiveStates - States a task may be in when it is live (e.g. able to take traffic)
var LiveStates = make(map[aurora.ScheduleStatus]bool)

// TerminalStates - Set of states a task may not transition away from.
var TerminalStates = make(map[aurora.ScheduleStatus]bool)

// ActiveJobUpdateStates - States a Job Update may be in where it is considered active.
var ActiveJobUpdateStates = make(map[aurora.JobUpdateStatus]bool)

// AwaitingPulseJobUpdateStates - States a job update may be in where it is waiting for a pulse.
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

func validateAuroraURL(location string) (string, error) {

	// If no protocol defined, assume http
	if !strings.Contains(location, "://") {
		location = "http://" + location
	}

	u, err := url.Parse(location)

	if err != nil {
		return "", errors.Wrap(err, "error parsing url")
	}

	// If no path provided assume /api
	if u.Path == "" {
		u.Path = "/api"
	}

	// If no port provided, assume default 8081
	if u.Port() == "" {
		u.Host = u.Host + ":8081"
	}

	if !(u.Scheme == "http" || u.Scheme == "https") {
		return "", errors.Errorf("only protocols http and https are supported %v\n", u.Scheme)
	}

	// This could theoretically be elsewhwere but we'll be strict for the sake of simplicty
	if u.Path != apiPath {
		return "", errors.Errorf("expected /api path %v\n", u.Path)
	}

	return u.String(), nil
}
