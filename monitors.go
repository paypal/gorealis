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

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

const (
	UpdateFailed = "update failed"
	RolledBack   = "update rolled back"
	Timedout     = "timeout"
)

// Polls the scheduler every certain amount of time to see if the update has succeeded
func (c *Client) JobUpdateMonitor(updateKey aurora.JobUpdateKey, interval, timeout time.Duration) (bool, error) {
	if interval < 1*time.Second {
		interval = interval * time.Second
	}

	if timeout < 1*time.Second {
		timeout = timeout * time.Second
	}

	updateQ := aurora.JobUpdateQuery{
		Key:   &updateKey,
		Limit: 1,
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			updateDetail, cliErr := c.JobUpdateDetails(updateQ)
			if cliErr != nil {
				return false, cliErr
			}

			if len(updateDetail) == 0 {
				c.RealisConfig().logger.Println("No update found")
				return false, errors.New("No update found for " + updateKey.String())
			}

			status := updateDetail[0].Update.Summary.State.Status

			// Convert Thrift Set to Golang map for quick lookup
			if _, ok := ActiveJobUpdateStates[status]; !ok {

				// Rolled forward is the only state in which an update has been successfully updated
				// if we encounter an inactive state and it is not at rolled forward, update failed
				switch status {
				case aurora.JobUpdateStatus_ROLLED_FORWARD:
					c.RealisConfig().logger.Println("Update succeeded")
					return true, nil
				case aurora.JobUpdateStatus_FAILED:
					c.RealisConfig().logger.Println("Update failed")
					return false, errors.New(UpdateFailed)
				case aurora.JobUpdateStatus_ROLLED_BACK:
					c.RealisConfig().logger.Println("rolled back")
					return false, errors.New(RolledBack)
				default:
					return false, nil
				}
			}
		case <-timer.C:
			return false, errors.New(Timedout)
		}
	}
}

// Monitor a AuroraJob until all instances enter one of the LiveStates
func (c *Client) InstancesMonitor(key aurora.JobKey, instances int32, interval, timeout time.Duration) (bool, error) {
	return c.ScheduleStatusMonitor(key, instances, aurora.LIVE_STATES, interval, timeout)
}

// Monitor a AuroraJob until all instances enter a desired status.
// Defaults sets of desired statuses provided by the thrift API include:
// ActiveStates, SlaveAssignedStates, LiveStates, and TerminalStates
func (c *Client) ScheduleStatusMonitor(key aurora.JobKey, instanceCount int32, desiredStatuses []aurora.ScheduleStatus, interval, timeout time.Duration) (bool, error) {
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
			return false, errors.New(Timedout)
		}
	}
}

// Monitor host status until all hosts match the status provided. Returns a map where the value is true if the host
// is in one of the desired mode(s) or false if it is not as of the time when the monitor exited.
func (c *Client) HostMaintenanceMonitor(hosts []string, modes []aurora.MaintenanceMode, interval, timeout time.Duration) (map[string]bool, error) {
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

			return hostResult, errors.New(Timedout)
		}
	}
}
