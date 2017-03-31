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
			if i != 0 {
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
			if status == aurora.JobUpdateStatus_ROLLED_FORWARD {
				fmt.Println("Update succeded")
				return true, nil
			} else {
				fmt.Println("Update failed")
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
			if i != 0 {
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
