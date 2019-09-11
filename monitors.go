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

// Collection of monitors to create synchronicity
package realis

import (
	"time"

	"github.com/paypal/gorealis/v2/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

// MonitorJobUpdate polls the scheduler every certain amount of time to see if the update has succeeded.
// If the update entered a terminal update state but it is not ROLLED_FORWARD, this function will return an error.
func (c *Client) MonitorJobUpdate(updateKey aurora.JobUpdateKey, interval, timeout time.Duration) (bool, error) {
	if interval < 1*time.Second {
		interval = interval * time.Second
	}

	if timeout < 1*time.Second {
		timeout = timeout * time.Second
	}
	updateSummaries, err := c.MonitorJobUpdateQuery(
		aurora.JobUpdateQuery{
			Key:   &updateKey,
			Limit: 1,
			UpdateStatuses: []aurora.JobUpdateStatus{
				aurora.JobUpdateStatus_ROLLED_FORWARD,
				aurora.JobUpdateStatus_ROLLED_BACK,
				aurora.JobUpdateStatus_ABORTED,
				aurora.JobUpdateStatus_ERROR,
				aurora.JobUpdateStatus_FAILED,
			},
		},
		interval,
		timeout)

	if err != nil {
		return false, err
	}

	status := updateSummaries[0].State.Status

	c.RealisConfig().logger.Printf("job update status: %v\n", status)

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

// MonitorJobUpdateStatus polls the scheduler for information about an update until the update enters one of the
// desired states or until the function times out.
func (c *Client) MonitorJobUpdateStatus(updateKey aurora.JobUpdateKey,
	desiredStatuses []aurora.JobUpdateStatus,
	interval, timeout time.Duration) (aurora.JobUpdateStatus, error) {

	if len(desiredStatuses) == 0 {
		return aurora.JobUpdateStatus(-1), errors.New("no desired statuses provided")
	}

	// Make deep local copy to avoid side effects from job key being manipulated externally.
	updateKeyLocal := &aurora.JobUpdateKey{
		Job: &aurora.JobKey{
			Role:        updateKey.Job.GetRole(),
			Environment: updateKey.Job.GetEnvironment(),
			Name:        updateKey.Job.GetName(),
		},
		ID: updateKey.GetID(),
	}

	updateQ := aurora.JobUpdateQuery{
		Key:            updateKeyLocal,
		Limit:          1,
		UpdateStatuses: desiredStatuses,
	}

	summary, err := c.MonitorJobUpdateQuery(updateQ, interval, timeout)
	if len(summary) > 0 {
		return summary[0].State.Status, err
	}

	return aurora.JobUpdateStatus(-1), err
}

func (c *Client) MonitorJobUpdateQuery(
	updateQuery aurora.JobUpdateQuery,
	interval time.Duration,
	timeout time.Duration) ([]*aurora.JobUpdateSummary, error) {

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			updateSummaryResults, cliErr := c.GetJobUpdateSummaries(&updateQuery)
			if cliErr != nil {
				return nil, cliErr
			}

			if len(updateSummaryResults.GetUpdateSummaries()) >= 1 {
				return updateSummaryResults.GetUpdateSummaries(), nil
			}

		case <-timer.C:
			return nil, newTimedoutError(errors.New("job update monitor timed out"))
		}
	}
}

// Monitor a AuroraJob until all instances enter one of the LiveStates
func (c *Client) MonitorInstances(key aurora.JobKey, instances int32, interval, timeout time.Duration) (bool, error) {
	return c.MonitorScheduleStatus(key, instances, aurora.LIVE_STATES, interval, timeout)
}

// Monitor a AuroraJob until all instances enter a desired status.
// Defaults sets of desired statuses provided by the thrift API include:
// ActiveStates, SlaveAssignedStates, LiveStates, and TerminalStates
func (c *Client) MonitorScheduleStatus(key aurora.JobKey,
	instanceCount int32,
	desiredStatuses []aurora.ScheduleStatus,
	interval, timeout time.Duration) (bool, error) {
	if interval < 1*time.Second {
		interval = interval * time.Second
	}

	if timeout < 1*time.Second {
		timeout = timeout * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:

			// Query Aurora for the state of the job key ever interval
			instCount, cliErr := c.GetInstanceIds(key, desiredStatuses)
			if cliErr != nil {
				return false, errors.Wrap(cliErr, "Unable to communicate with Aurora")
			}
			if len(instCount) == int(instanceCount) {
				return true, nil
			}
		case <-timer.C:

			// If the timer runs out, return a timeout error to user
			return false, newTimedoutError(errors.New("schedule status monitor timedout"))
		}
	}
}

// Monitor host status until all hosts match the status provided. Returns a map where the value is true if the host
// is in one of the desired mode(s) or false if it is not as of the time when the monitor exited.
func (c *Client) MonitorHostMaintenance(hosts []string,
	modes []aurora.MaintenanceMode,
	interval, timeout time.Duration) (map[string]bool, error) {
	if interval < 1*time.Second {
		interval = interval * time.Second
	}

	if timeout < 1*time.Second {
		timeout = timeout * time.Second
	}

	//  Transform modes to monitor for into a set for easy lookup
	desiredMode := make(map[aurora.MaintenanceMode]struct{})
	for _, mode := range modes {
		desiredMode[mode] = struct{}{}
	}

	// Turn slice into a host set to eliminate duplicates.
	// We also can't use a simple count because multiple modes means we can have multiple matches for a single host.
	// I.e. host A transitions from ACTIVE to DRAINING to DRAINED while monitored
	remainingHosts := make(map[string]struct{})
	for _, host := range hosts {
		remainingHosts[host] = struct{}{}
	}

	hostResult := make(map[string]bool)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			// Client call has multiple retries internally
			result, err := c.MaintenanceStatus(hosts...)
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

			return hostResult, newTimedoutError(errors.New("host maintenance monitor timedout"))
		}
	}
}
