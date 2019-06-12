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

// UpdateJob is a structure to collect all information required to create job update.
type UpdateJob struct {
	Job // SetInstanceCount for job is hidden, access via full qualifier
	req *aurora.JobUpdateRequest
}

// NewDefaultUpdateJob creates an UpdateJob object with opinionated default settings.
func NewDefaultUpdateJob(config *aurora.TaskConfig) *UpdateJob {

	req := aurora.NewJobUpdateRequest()
	req.TaskConfig = config
	req.Settings = NewUpdateSettings()

	job, ok := NewJob().(*AuroraJob)
	if !ok {
		// This should never happen but it is here as a safeguard
		return nil
	}

	job.jobConfig.TaskConfig = config

	// Rebuild resource map from TaskConfig
	for _, ptr := range config.Resources {
		if ptr.NumCpus != nil {
			job.resources[CPU].NumCpus = ptr.NumCpus
			continue // Guard against Union violations that Go won't enforce
		}

		if ptr.RamMb != nil {
			job.resources[RAM].RamMb = ptr.RamMb
			continue
		}

		if ptr.DiskMb != nil {
			job.resources[DISK].DiskMb = ptr.DiskMb
			continue
		}

		if ptr.NumGpus != nil {
			job.resources[GPU] = &aurora.Resource{NumGpus: ptr.NumGpus}
			continue
		}
	}

	// Mirrors defaults set by Pystachio
	req.Settings.UpdateGroupSize = 1
	req.Settings.WaitForBatchCompletion = false
	req.Settings.MinWaitInInstanceRunningMs = 45000
	req.Settings.MaxPerInstanceFailures = 0
	req.Settings.MaxFailedInstances = 0
	req.Settings.RollbackOnFailure = true

	//TODO(rdelvalle): Deep copy job struct to avoid unexpected behavior
	return &UpdateJob{Job: job, req: req}
}

// NewUpdateJob creates an UpdateJob object wihtout default settings.
func NewUpdateJob(config *aurora.TaskConfig, settings *aurora.JobUpdateSettings) *UpdateJob {

	req := aurora.NewJobUpdateRequest()
	req.TaskConfig = config
	req.Settings = settings

	job, ok := NewJob().(*AuroraJob)
	if !ok {
		// This should never happen but it is here as a safeguard
		return nil
	}
	job.jobConfig.TaskConfig = config

	// Rebuild resource map from TaskConfig
	for _, ptr := range config.Resources {
		if ptr.NumCpus != nil {
			job.resources[CPU].NumCpus = ptr.NumCpus
			continue // Guard against Union violations that Go won't enforce
		}

		if ptr.RamMb != nil {
			job.resources[RAM].RamMb = ptr.RamMb
			continue
		}

		if ptr.DiskMb != nil {
			job.resources[DISK].DiskMb = ptr.DiskMb
			continue
		}

		if ptr.NumGpus != nil {
			job.resources[GPU] = &aurora.Resource{}
			job.resources[GPU].NumGpus = ptr.NumGpus
			continue // Guard against Union violations that Go won't enforce
		}
	}

	//TODO(rdelvalle): Deep copy job struct to avoid unexpected behavior
	return &UpdateJob{Job: job, req: req}
}

// InstanceCount sets instance count the job will have after the update.
func (u *UpdateJob) InstanceCount(inst int32) *UpdateJob {
	u.req.InstanceCount = inst
	return u
}

// BatchSize sets the max number of instances being updated at any given moment.
func (u *UpdateJob) BatchSize(size int32) *UpdateJob {
	u.req.Settings.UpdateGroupSize = size
	return u
}

// WatchTime sets the minimum number of seconds a shard must remain in RUNNING state before considered a success.
func (u *UpdateJob) WatchTime(ms int32) *UpdateJob {
	u.req.Settings.MinWaitInInstanceRunningMs = ms
	return u
}

// WaitForBatchCompletion configures the job update to wait for all instances in a group to be done before moving on.
func (u *UpdateJob) WaitForBatchCompletion(batchWait bool) *UpdateJob {
	u.req.Settings.WaitForBatchCompletion = batchWait
	return u
}

// MaxPerInstanceFailures sets the max number of instance failures to tolerate before marking instance as FAILED.
func (u *UpdateJob) MaxPerInstanceFailures(inst int32) *UpdateJob {
	u.req.Settings.MaxPerInstanceFailures = inst
	return u
}

// MaxFailedInstances sets the max number of FAILED instances to tolerate before terminating the update.
func (u *UpdateJob) MaxFailedInstances(inst int32) *UpdateJob {
	u.req.Settings.MaxFailedInstances = inst
	return u
}

// RollbackOnFail configure the job to rollback automatically after a job update fails.
func (u *UpdateJob) RollbackOnFail(rollback bool) *UpdateJob {
	u.req.Settings.RollbackOnFailure = rollback
	return u
}

// NewUpdateSettings return an opinionated set of job update settings.
func NewUpdateSettings() *aurora.JobUpdateSettings {
	us := new(aurora.JobUpdateSettings)
	// Mirrors defaults set by Pystachio
	us.UpdateGroupSize = 1
	us.WaitForBatchCompletion = false
	us.MinWaitInInstanceRunningMs = 45000
	us.MaxPerInstanceFailures = 0
	us.MaxFailedInstances = 0
	us.RollbackOnFailure = true

	return us
}
