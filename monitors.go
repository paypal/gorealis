/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package realis

import (
	"time"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

// Monitor is a wrapper for the Realis client which allows us to have functions
// with the same name for Monitoring purposes.
// TODO(rdelvalle): Deprecate monitors and instead add prefix Monitor to
// all functions in this file like it is done in V2.
type Monitor struct {
	Client Realis
}

// JobUpdate polls the scheduler every certain amount of time to see if the update has entered a terminal state.
func (m *Monitor) JobUpdate(
	updateKey aurora.JobUpdateKey,
	interval int,
	timeout int) (bool, error) {

	updateQ := aurora.JobUpdateQuery{
		Key:            &updateKey,
		Limit:          1,
		UpdateStatuses: TerminalUpdateStates(),
	}
	updateSummaries, err := m.JobUpdateQuery(
		updateQ,
		time.Duration(interval)*time.Second,
		time.Duration(timeout)*time.Second)

	status := updateSummaries[0].State.Status

	if err != nil {
		return false, err
	}

	m.Client.RealisConfig().logger.Printf("job update status: %v\n", status)

	// Rolled forward is the only state in which an update has been successfully updated
	// if we encounter an inactive state and it is not at rolled forward, update failed
	switch status {
	case aurora.JobUpdateStatus_ROLLED_FORWARD:
		return true, nil
	case aurora.JobUpdateStatus_ROLLED_BACK,
		aurora.JobUpdateStatus_ABORTED,
		aurora.JobUpdateStatus_ERROR,
		aurora.JobUpdateStatus_FAILED:
		return false, errors.Errorf("bad terminal state for update: %v", status)
	default:
		return false, errors.Errorf("unexpected update state: %v", status)
	}
}

// JobUpdateStatus polls the scheduler every certain amount of time to see if the update has entered a specified state.
func (m *Monitor) JobUpdateStatus(updateKey aurora.JobUpdateKey,
	desiredStatuses []aurora.JobUpdateStatus,
	interval, timeout time.Duration) (aurora.JobUpdateStatus, error) {
	updateQ := aurora.JobUpdateQuery{
		Key:            &updateKey,
		Limit:          1,
		UpdateStatuses: desiredStatuses,
	}
	summary, err := m.JobUpdateQuery(updateQ, interval, timeout)

	return summary[0].State.Status, err
}

// JobUpdateQuery polls the scheduler every certain amount of time to see if the query call returns any results.
func (m *Monitor) JobUpdateQuery(
	updateQuery aurora.JobUpdateQuery,
	interval time.Duration,
	timeout time.Duration) ([]*aurora.JobUpdateSummary, error) {

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var cliErr error
	var respDetail *aurora.Response
	for {
		select {
		case <-ticker.C:
			respDetail, cliErr = m.Client.GetJobUpdateSummaries(&updateQuery)
			if cliErr != nil {
				return nil, cliErr
			}

			updateSummaries := respDetail.Result_.GetJobUpdateSummariesResult_.UpdateSummaries
			if len(updateSummaries) >= 1 {
				return updateSummaries, nil
			}

		case <-timer.C:
			return nil, newTimedoutError(errors.New("job update monitor timed out"))
		}
	}
}

// PausedUpdateMonitor polls Aurora for information about a job update until the job update has entered into
// a ROLL_FORWARD_PAUSED state.
func (m *Monitor) PausedUpdateMonitor(key aurora.JobUpdateKey, interval, timeout time.Duration) (bool, error) {
	status, err := m.JobUpdateStatus(
		key,
		append(TerminalUpdateStates(), aurora.JobUpdateStatus_ROLL_FORWARD_PAUSED),
		interval,
		timeout)

	if err != nil {
		return false, err
	}

	if status != aurora.JobUpdateStatus_ROLL_FORWARD_PAUSED {
		return false, errors.Errorf("update is in a terminal state %v", status)
	}

	return true, nil
}

// Monitor a Job until all instances enter one of the LIVE_STATES
func (m *Monitor) Instances(key *aurora.JobKey, instances int32, interval, timeout int) (bool, error) {
	return m.ScheduleStatus(key, instances, LiveStates, interval, timeout)
}

// ScheduleStatus will monitor a Job until all instances enter a desired status.
// Defaults sets of desired statuses provided by the thrift API include:
// ACTIVE_STATES, SLAVE_ASSIGNED_STATES, LIVE_STATES, and TERMINAL_STATES
func (m *Monitor) ScheduleStatus(
	key *aurora.JobKey,
	instanceCount int32,
	desiredStatuses map[aurora.ScheduleStatus]bool,
	interval int,
	timeout int) (bool, error) {

	ticker := time.NewTicker(time.Second * time.Duration(interval))
	defer ticker.Stop()
	timer := time.NewTimer(time.Second * time.Duration(timeout))
	defer timer.Stop()

	wantedStatuses := make([]aurora.ScheduleStatus, 0)

	for status := range desiredStatuses {
		wantedStatuses = append(wantedStatuses, status)
	}

	for {
		select {
		case <-ticker.C:

			// Query Aurora for the state of the job key ever interval
			instCount, cliErr := m.Client.GetInstanceIds(key, wantedStatuses)
			if cliErr != nil {
				return false, errors.Wrap(cliErr, "Unable to communicate with Aurora")
			}
			if len(instCount) == int(instanceCount) {
				return true, nil
			}
		case <-timer.C:

			// If the timer runs out, return a timeout error to user
			return false, newTimedoutError(errors.New("schedule status monitor timed out"))
		}
	}
}

// HostMaintenance will monitor host status until all hosts match the status provided.
// Returns a map where the value is true if the host
// is in one of the desired mode(s) or false if it is not as of the time when the monitor exited.
func (m *Monitor) HostMaintenance(
	hosts []string,
	modes []aurora.MaintenanceMode,
	interval, timeout int) (map[string]bool, error) {

	//  Transform modes to monitor for into a set for easy lookup
	desiredMode := make(map[aurora.MaintenanceMode]struct{})
	for _, mode := range modes {
		desiredMode[mode] = struct{}{}
	}

	// Turn slice into a host set to eliminate duplicates.
	// We also can't use a simple count because multiple modes means
	// we can have multiple matches for a single host.
	// I.e. host A transitions from ACTIVE to DRAINING to DRAINED while monitored
	remainingHosts := make(map[string]struct{})
	for _, host := range hosts {
		remainingHosts[host] = struct{}{}
	}

	hostResult := make(map[string]bool)

	ticker := time.NewTicker(time.Second * time.Duration(interval))
	defer ticker.Stop()
	timer := time.NewTimer(time.Second * time.Duration(timeout))
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			// Client call has multiple retries internally
			_, result, err := m.Client.MaintenanceStatus(hosts...)
			if err != nil {
				// Error is either a payload error or a severe connection error
				for host := range remainingHosts {
					hostResult[host] = false
				}
				return hostResult, errors.Wrap(err, "client error in monitor")
			}

			for _, status := range result.GetStatuses() {

				if _, ok := desiredMode[status.GetMode()]; ok {
					hostResult[status.GetHost()] = true
					delete(remainingHosts, status.GetHost())

					if len(remainingHosts) == 0 {
						return hostResult, nil
					}
				}
			}

		case <-timer.C:
			for host := range remainingHosts {
				hostResult[host] = false
			}

			return hostResult, newTimedoutError(errors.New("host maintenance monitor timed out"))
		}
	}
}
