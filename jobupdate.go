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
	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

// Structure to collect all information required to create job update
type JobUpdate struct {
	Job     *AuroraJob
	request *aurora.JobUpdateRequest
}

// Create a default JobUpdate object.
func NewDefaultJobUpdate(job *AuroraJob) *JobUpdate {

	req := aurora.JobUpdateRequest{}
	req.TaskConfig = job.jobConfig.TaskConfig
	req.Settings = NewUpdateSettings()

	// Rebuild resource map from TaskConfig
	for _, ptr := range job.jobConfig.TaskConfig.Resources {
		if ptr.NumCpus != nil {
			job.resources["cpu"].NumCpus = ptr.NumCpus
			continue // Guard against Union violations that Go won't enforce
		}

		if ptr.RamMb != nil {
			job.resources["ram"].RamMb = ptr.RamMb
			continue
		}

		if ptr.DiskMb != nil {
			job.resources["disk"].DiskMb = ptr.DiskMb
			continue
		}
	}

	// Mirrors defaults set by Pystachio
	req.Settings.UpdateOnlyTheseInstances = []*aurora.Range{}
	req.Settings.UpdateGroupSize = 1
	req.Settings.WaitForBatchCompletion = false
	req.Settings.MinWaitInInstanceRunningMs = 45000
	req.Settings.MaxPerInstanceFailures = 0
	req.Settings.MaxFailedInstances = 0
	req.Settings.RollbackOnFailure = true

	//TODO(rdelvalle): Deep copy job struct to avoid unexpected behavior
	return &JobUpdate{Job: job, request: &req}
}

func NewUpdateJob(config *aurora.TaskConfig, settings *aurora.JobUpdateSettings) *JobUpdate {

	req := aurora.NewJobUpdateRequest()
	req.TaskConfig = config
	req.Settings = settings

	job := NewJob()
	job.jobConfig.TaskConfig = config

	// Rebuild resource map from TaskConfig
	for _, ptr := range config.Resources {
		if ptr.NumCpus != nil {
			job.resources["cpu"].NumCpus = ptr.NumCpus
			continue // Guard against Union violations that Go won't enforce
		}

		if ptr.RamMb != nil {
			job.resources["ram"].RamMb = ptr.RamMb
			continue
		}

		if ptr.DiskMb != nil {
			job.resources["disk"].DiskMb = ptr.DiskMb
			continue
		}
	}

	//TODO(rdelvalle): Deep copy job struct to avoid unexpected behavior
	return &JobUpdate{Job: job, request: req}
}

// Set instance count the job will have after the update.
func (u *JobUpdate) InstanceCount(inst int32) *JobUpdate {
	u.request.InstanceCount = inst
	return u
}

// Max number of instances being updated at any given moment.
func (u *JobUpdate) BatchSize(size int32) *JobUpdate {
	u.request.Settings.UpdateGroupSize = size
	return u
}

// Minimum number of seconds a shard must remain in RUNNING state before considered a success.
func (u *JobUpdate) WatchTime(ms int32) *JobUpdate {
	u.request.Settings.MinWaitInInstanceRunningMs = ms
	return u
}

// Wait for all instances in a group to be done before moving on.
func (u *JobUpdate) WaitForBatchCompletion(batchWait bool) *JobUpdate {
	u.request.Settings.WaitForBatchCompletion = batchWait
	return u
}

// Max number of instance failures to tolerate before marking instance as FAILED.
func (u *JobUpdate) MaxPerInstanceFailures(inst int32) *JobUpdate {
	u.request.Settings.MaxPerInstanceFailures = inst
	return u
}

// Max number of FAILED instances to tolerate before terminating the update.
func (u *JobUpdate) MaxFailedInstances(inst int32) *JobUpdate {
	u.request.Settings.MaxFailedInstances = inst
	return u
}

// When False, prevents auto rollback of a failed update.
func (u *JobUpdate) RollbackOnFail(rollback bool) *JobUpdate {
	u.request.Settings.RollbackOnFailure = rollback
	return u
}

func NewUpdateSettings() *aurora.JobUpdateSettings {

	us := aurora.JobUpdateSettings{}
	// Mirrors defaults set by Pystachio
	us.UpdateOnlyTheseInstances = []*aurora.Range{}
	us.UpdateGroupSize = 1
	us.WaitForBatchCompletion = false
	us.MinWaitInInstanceRunningMs = 45000
	us.MaxPerInstanceFailures = 0
	us.MaxFailedInstances = 0
	us.RollbackOnFailure = true

	return &us
}
