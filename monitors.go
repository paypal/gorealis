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
	"fmt"
	"strings"
	"time"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/pkg/errors"
)

const (
	UpdateFailed = "update failed"
	RolledBack   = "update rolled back"
)

type Monitor struct {
	Client Realis
}

// Polls the scheduler every certain amount of time to see if the update has succeeded
func (m *Monitor) JobUpdate(updateKey aurora.JobUpdateKey, interval int, timeout int) (bool, error) {

	updateQ := aurora.JobUpdateQuery{
		Key:   &updateKey,
		Limit: 1,
	}

	var cliErr error
	var respDetail *aurora.Response

	retryErr := ExponentialBackoff(*m.Client.RealisConfig().backoff, func() (bool, error) {
		respDetail, cliErr = CheckAndRetryConn(m.Client, func() (*aurora.Response, error) {
			return m.Client.JobUpdateDetails(updateQ)
		})
		if cliErr == RetryConnErr {
			return false, nil
		} else {
			return false, cliErr
		}
		updateDetail := response.JobUpdateDetails(respDetail)

		if len(updateDetail) == 0 {
			fmt.Println("No update found")
			return false, errors.New("No update found for " + updateKey.String())
		}
		status := updateDetail[0].Update.Summary.State.Status

		if _, ok := aurora.ACTIVE_JOB_UPDATE_STATES[status]; !ok {

			// Rolled forward is the only state in which an update has been successfully updated
			// if we encounter an inactive state and it is not at rolled forward, update failed
			switch status {
			case aurora.JobUpdateStatus_ROLLED_FORWARD:
				fmt.Println("Update succeded")
				return true, nil
			case aurora.JobUpdateStatus_FAILED:
				fmt.Println("Update failed")
				return false, errors.New(UpdateFailed)
			case aurora.JobUpdateStatus_ROLLED_BACK:
				fmt.Println("rolled back")
				return false, errors.New(RolledBack)
			default:
				return false, nil
			}
		}
		return false, nil
	})
	if retryErr != nil {
		return false, errors.Wrap(cliErr, retryErr.Error())
	}
	return true, nil
}

func (m *Monitor) Instances(key *aurora.JobKey, instances int32, interval int, timeout int) (bool, error) {

	var cliErr error
	var live map[int32]bool

	retryErr := ExponentialBackoff(*m.Client.RealisConfig().backoff, func() (bool, error) {
		live, cliErr = m.Client.GetInstanceIds(key, aurora.LIVE_STATES)
		if strings.Contains(cliErr.Error(), ConnRefusedErr) || strings.Contains(cliErr.Error(), NoLeaderFoundErr) {
			// TODO try this condition only if the error is connection related
			conErr := m.Client.ReestablishConn()
			if conErr != nil {
				// TODO: identify specific type of connection errors
				return false, nil
			}
			return false, nil
		}
		if cliErr != nil {
			return false, errors.Wrap(cliErr, "Unable to communicate with Aurora")
		}
		if len(live) == int(instances) {
			return true, nil
		}
		return false, nil
	})
	if cliErr != nil {
		return false, cliErr
	}
	if retryErr != nil {
		return false, retryErr
	}
	return true, nil
}

// Monitor host status until all hosts match the status provided. Returns a map where the value is true if the host
// is in one of the desired mode(s) or false if it is not as of the time when the monitor exited.
func (m *Monitor) HostMaintenance(hosts []string, modes []aurora.MaintenanceMode, sleepTime, steps int) (map[string]bool, error) {

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

	for step := 0; step < steps; step++ {
		if step != 0 {
			time.Sleep(time.Duration(sleepTime) * time.Second)
		}

		// Client call has multiple retries internally
		_, result, err := m.Client.MaintenanceStatus(hosts...)
		if err != nil {
			// Error is either a payload error or a severe connection error
			for host := range remainingHosts {
				hostResult[host] = false
			}
			return hostResult, errors.Wrap(err, "client error in monitor")
		}

		for status := range result.GetStatuses() {

			if _, ok := desiredMode[status.GetMode()]; ok {
				hostResult[status.GetHost()] = true
				delete(remainingHosts, status.GetHost())

				if len(remainingHosts) == 0 {
					return hostResult, nil
				}
			}
		}

	}

	for host := range remainingHosts {
		hostResult[host] = false
	}

	return hostResult, errors.New("Timed out")
}
