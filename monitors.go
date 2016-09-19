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
	"github.com/rdelval/gorealis/gen-go/apache/aurora"
	"github.com/rdelval/gorealis/response"
	"os"
	"time"
)

type Monitor struct {
	Client Realis
}

// Polls the scheduler every certain amount of time to see if the update has succeeded
func (m *Monitor) JobUpdate(updateKey aurora.JobUpdateKey, interval int, timeout int) bool {

	for i := 0; i*interval <= timeout; i++ {
		respDetail, err := m.Client.JobUpdateDetails(updateKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		updateDetail := response.JobUpdateDetails(respDetail)

		status := updateDetail.Update.Summary.State.Status

		if _, ok := aurora.ACTIVE_JOB_UPDATE_STATES[status]; !ok {

			// Rolled forward is the only state in which an update has been successfully updated
			// if we encounter an inactive state and it is not at rolled forward, update failed
			if status == aurora.JobUpdateStatus_ROLLED_FORWARD {
				fmt.Println("Update succeded")
				return true
			} else {
				fmt.Println("Update failed")
				return false
			}
		}

		fmt.Println("Polling, update still active...")
		time.Sleep(time.Duration(interval) * time.Second)
	}

	fmt.Println("Timed out")
	return false
}

func (m *Monitor) Instances(key *aurora.JobKey, instances int32, interval int, timeout int) bool {

	for i := 0; i*interval < timeout; i++ {

		live, err := m.Client.GetInstanceIds(key, aurora.LIVE_STATES)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if len(live) == int(instances) {
			return true
		}

		fmt.Println("Polling, instances running: ", len(live))
		time.Sleep(time.Duration(interval) * time.Second)
	}

	fmt.Println("Timed out")
	return false
}
