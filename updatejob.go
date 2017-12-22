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
type UpdateJob struct {
	Job // SetInstanceCount for job is hidden, access via full qualifier
	req *aurora.JobUpdateRequest
}

// Create a default UpdateJob object.
func NewDefaultUpdateJob(config *aurora.TaskConfig) *UpdateJob {

	req := aurora.NewJobUpdateRequest()
	req.TaskConfig = config
	s := NewUpdateSettings().Settings()
	req.Settings = &s

	job := NewJob().(*AuroraJob)
	job.jobConfig.TaskConfig = config

	// Rebuild resource map from TaskConfig
	for ptr := range config.Resources {
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
	req.Settings.UpdateOnlyTheseInstances = make(map[*aurora.Range]bool)
	req.Settings.UpdateGroupSize = 1
	req.Settings.WaitForBatchCompletion = false
	req.Settings.MinWaitInInstanceRunningMs = 45000
	req.Settings.MaxPerInstanceFailures = 0
	req.Settings.MaxFailedInstances = 0
	req.Settings.RollbackOnFailure = true

	//TODO(rdelvalle): Deep copy job struct to avoid unexpected behavior
	return &UpdateJob{Job: job, req: req}
}

func NewUpdateJob(config *aurora.TaskConfig, settings *aurora.JobUpdateSettings) *UpdateJob {

	req := aurora.NewJobUpdateRequest()
	req.TaskConfig = config
	req.Settings = settings

	job := NewJob().(*AuroraJob)
	job.jobConfig.TaskConfig = config

	// Rebuild resource map from TaskConfig
	for ptr := range config.Resources {
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
	return &UpdateJob{Job: job, req: req}
}

// Set instance count the job will have after the update.
func (u *UpdateJob) InstanceCount(inst int32) *UpdateJob {
	u.req.InstanceCount = inst
	return u
}

// Max number of instances being updated at any given moment.
func (u *UpdateJob) BatchSize(size int32) *UpdateJob {
	u.req.Settings.UpdateGroupSize = size
	return u
}

// Minimum number of seconds a shard must remain in RUNNING state before considered a success.
func (u *UpdateJob) WatchTime(ms int32) *UpdateJob {
	u.req.Settings.MinWaitInInstanceRunningMs = ms
	return u
}

// Wait for all instances in a group to be done before moving on.
func (u *UpdateJob) WaitForBatchCompletion(batchWait bool) *UpdateJob {
	u.req.Settings.WaitForBatchCompletion = batchWait
	return u
}

// Max number of instance failures to tolerate before marking instance as FAILED.
func (u *UpdateJob) MaxPerInstanceFailures(inst int32) *UpdateJob {
	u.req.Settings.MaxPerInstanceFailures = inst
	return u
}

// Max number of FAILED instances to tolerate before terminating the update.
func (u *UpdateJob) MaxFailedInstances(inst int32) *UpdateJob {
	u.req.Settings.MaxFailedInstances = inst
	return u
}

// When False, prevents auto rollback of a failed update.
func (u *UpdateJob) RollbackOnFail(rollback bool) *UpdateJob {
	u.req.Settings.RollbackOnFailure = rollback
	return u
}

// TODO(rdelvalle): Integrate this struct with the JobUpdate struct so that we don't repeat code
type UpdateSettings struct {
	settings aurora.JobUpdateSettings
}

func NewUpdateSettings() *UpdateSettings {

	us := new(UpdateSettings)

	// Mirrors defaults set by Pystachio
	us.settings.UpdateOnlyTheseInstances = make(map[*aurora.Range]bool)
	us.settings.UpdateGroupSize = 1
	us.settings.WaitForBatchCompletion = false
	us.settings.MinWaitInInstanceRunningMs = 45000
	us.settings.MaxPerInstanceFailures = 0
	us.settings.MaxFailedInstances = 0
	us.settings.RollbackOnFailure = true

	return us
}

// Max number of instances being updated at any given moment.
func (u *UpdateSettings) BatchSize(size int32) *UpdateSettings {
	u.settings.UpdateGroupSize = size
	return u
}

// Minimum number of seconds a shard must remain in RUNNING state before considered a success.
func (u *UpdateSettings) WatchTime(ms int32) *UpdateSettings {
	u.settings.MinWaitInInstanceRunningMs = ms
	return u
}

// Wait for all instances in a group to be done before moving on.
func (u *UpdateSettings) WaitForBatchCompletion(batchWait bool) *UpdateSettings {
	u.settings.WaitForBatchCompletion = batchWait
	return u
}

// Max number of instance failures to tolerate before marking instance as FAILED.
func (u *UpdateSettings) MaxPerInstanceFailures(inst int32) *UpdateSettings {
	u.settings.MaxPerInstanceFailures = inst
	return u
}

// Max number of FAILED instances to tolerate before terminating the update.
func (u *UpdateSettings) MaxFailedInstances(inst int32) *UpdateSettings {
	u.settings.MaxFailedInstances = inst
	return u
}

// When False, prevents auto rollback of a failed update.
func (u *UpdateSettings) RollbackOnFail(rollback bool) *UpdateSettings {
	u.settings.RollbackOnFailure = rollback
	return u
}

// Return internal Thrift API structure
func (u UpdateSettings) Settings() aurora.JobUpdateSettings {
	return u.settings
}
