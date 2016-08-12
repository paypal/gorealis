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

import "gen-go/apache/aurora"

// Structure to collect all information requrired to create job update
type UpdateJob struct {
	*Job // SetInstanceCount for job is hidden, access via full qualifier
	req  *aurora.JobUpdateRequest
}

// Create a default UpdateJob object.
func NewUpdateJob(job *Job) *UpdateJob {

	req := aurora.NewJobUpdateRequest()
	req.TaskConfig = job.jobConfig.TaskConfig
	req.Settings = aurora.NewJobUpdateSettings()

	// Mirrors defaults set by Pystachio
	req.Settings.UpdateOnlyTheseInstances = make(map[*aurora.Range]bool)
	req.Settings.UpdateGroupSize = 1
	req.Settings.WaitForBatchCompletion = false
	req.Settings.MinWaitInInstanceRunningMs = 45000 // Deprecated
	req.Settings.MaxPerInstanceFailures = 0
	req.Settings.MaxFailedInstances = 0
	req.Settings.RollbackOnFailure = true
	req.Settings.WaitForBatchCompletion = false

	//TODO(rdelvalle): Deep copy job struct to avoid unexpected behavior
	return &UpdateJob{job, req}
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
func (u *UpdateJob) WatchTime(milliseconds int32) *UpdateJob {
	u.req.Settings.MaxPerInstanceFailures = milliseconds
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
