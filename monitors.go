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
	"time"

	"github.com/pkg/errors"
	"github.com/rdelval/gorealis/gen-go/apache/aurora"
	"github.com/rdelval/gorealis/response"
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

	defaultBackoff := m.Client.RealisConfig().backoff
	duration := defaultBackoff.Duration //defaultBackoff.Duration
	var err error
	var respDetail *aurora.Response

	for i := 0; i*interval <= timeout; i++ {
		for step := 0; step < defaultBackoff.Steps; step++ {
			if step != 0 {
				adjusted := duration
				if defaultBackoff.Jitter > 0.0 {
					adjusted = Jitter(duration, defaultBackoff.Jitter)
				}
				fmt.Println(" sleeping for: ", adjusted)
				time.Sleep(adjusted)
				duration = time.Duration(float64(duration) * defaultBackoff.Factor)
			}
			if respDetail, err = m.Client.JobUpdateDetails(updateQ); err == nil {
				break
			}
			err1 := m.Client.ReestablishConn()
			if err1 != nil {
				fmt.Println("error in ReestablishConn: ", err1)
			}
		}
		// if error remains then return (false, err).
		if err != nil {
			return false, err
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

		fmt.Println("Polling, update still active...")
		time.Sleep(time.Duration(interval) * time.Second)
	}

	fmt.Println("Timed out")
	return false, nil
}

func (m *Monitor) Instances(key *aurora.JobKey, instances int32, interval int, timeout int) (bool, error) {

	defaultBackoff := m.Client.RealisConfig().backoff
	duration := defaultBackoff.Duration
	var err error
	var live map[int32]bool

	for i := 0; i*interval < timeout; i++ {
		for step := 0; step < defaultBackoff.Steps; step++ {
			if step != 0 {
				adjusted := duration
				if defaultBackoff.Jitter > 0.0 {
					adjusted = Jitter(duration, defaultBackoff.Jitter)
				}
				fmt.Println(" sleeping for: ", adjusted)
				time.Sleep(adjusted)
				fmt.Println(" sleeping done")
				duration = time.Duration(float64(duration) * defaultBackoff.Factor)
			}
			if live, err = m.Client.GetInstanceIds(key, aurora.LIVE_STATES); err == nil {
				fmt.Println(" live: ", live)
				break
			}

			if err != nil {
				err1 := m.Client.ReestablishConn()
				if err1 != nil {
					fmt.Println("error in ReestablishConn: ", err1)
				}
			}

		}

		//live, err := m.Client.GetInstanceIds(key, aurora.LIVE_STATES)
		if err != nil {
			return false, errors.Wrap(err, "Unable to communicate with Aurora")
		}
		if len(live) == int(instances) {
			return true, nil
		}
		fmt.Println("Polling, instances running: ", len(live))
		time.Sleep(time.Duration(interval) * time.Second)
	}

	fmt.Println("Timed out")
	return false, nil
}

// Monitor host status until all hosts match the status provided. May return an error along with a non nil map which contains
// the hosts that did not transition to the desired modes(s).
func (m *Monitor) HostMaintenance(hosts []string, modes []aurora.MaintenanceMode, sleepTime, steps int) (map[string]struct{}, error) {

	//  Transform modes to monitor for into a set for easy lookup
	desiredMode := make(map[aurora.MaintenanceMode]struct{})
	for _,mode := range modes {
		desiredMode[mode] = struct{}{}
	}

	// Turn slice into a host set to eliminate duplicates. Delete hosts that have entered the desired mode from
	// observed list. We are done when the number of observed hosts reaches zero.
	// This avoids having to go through and check the list one by one each cycle.
	observedHosts := make(map[string]struct{})
	for _,host := range hosts {
		observedHosts[host] = struct{}{}
	}

	for step := 0; step < steps; step++ {
		// Client may have multiple retries handle retries
		_, result, err := m.Client.MaintenanceStatus(hosts...)
		if err != nil {
			// Error is either a payload error or a severe connection error
			return observedHosts, errors.Wrap(err,"client error")
		}

		for status := range result.GetStatuses() {
			if  _, ok := desiredMode[status.GetMode()]; ok {
				fmt.Printf("host %s entered %s state\n", status.GetHost(), status.GetMode())
				delete(observedHosts, status.GetHost())
			}
		}

		if len(observedHosts) == 0{
			return observedHosts, nil
		} else {
			fmt.Printf("%d host(s) not in desired state\n", len(observedHosts))
		}

		time.Sleep(time.Duration(sleepTime) * time.Second)
	}

	return observedHosts, errors.New("Timed out")
}
