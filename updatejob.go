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

type UpdateJob struct {
	*Job //Go Embedding, SetInstanceCount for job is hidden
	req  *aurora.JobUpdateRequest
}

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

	//TODO(rdelvalle): Deep copy job
	return &UpdateJob{job, req}
}

func (u *UpdateJob) SetInstanceCount(inst int32) *UpdateJob {
	u.req.InstanceCount = inst
	return u
}

// Max number of instances being updated at any given moment.
func (u *UpdateJob) SetBatchSize(size int32) *UpdateJob {
	u.req.Settings.UpdateGroupSize = size
	return u
}

// Minimum number of seconds a shard must remain in RUNNING state before considered a success
func (u *UpdateJob) SetWatchTime(milliseconds int32) *UpdateJob {
	u.req.Settings.MaxPerInstanceFailures = milliseconds
	return u
}

// Wait for all instances in a group to be done before moving on
func (u *UpdateJob) SetWaitForBatchCompletion(batchWait bool) *UpdateJob {
	u.req.Settings.WaitForBatchCompletion = batchWait
	return u
}

//	Max number of instance failures to tolerate before marking instance as FAILED.
func (u *UpdateJob) SetMaxPerInstanceFailures(inst int32) *UpdateJob {
	u.req.Settings.MaxPerInstanceFailures = inst
	return u
}

// Max number of FAILED instances to tolerate before terminating the update.
func (u *UpdateJob) SetMaxFailedInstances(inst int32) *UpdateJob {

	u.req.Settings.MaxFailedInstances = inst
	return u
}

// When False, prevents auto rollback of a failed update
func (u *UpdateJob) SetRollbackOnFail(rollback bool) *UpdateJob {

	u.req.Settings.RollbackOnFailure = rollback
	return u
}
