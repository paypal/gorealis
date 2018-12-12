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

// Structure to collect all information pertaining to an Aurora job.
type AuroraJob struct {
	jobConfig *aurora.JobConfiguration
	task      *AuroraTask
}

// Create a AuroraJob object with everything initialized.
func NewJob() *AuroraJob {

	jobKey := &aurora.JobKey{}

	// AuroraTask clientConfig
	task := NewTask()
	task.task.Job = jobKey

	// AuroraJob clientConfig
	jobConfig := &aurora.JobConfiguration{
		Key:        jobKey,
		TaskConfig: task.TaskConfig(),
	}

	return &AuroraJob{
		jobConfig: jobConfig,
		task:      task,
	}
}

// Set AuroraJob Key environment. Explicit changes to AuroraTask's job key are not needed
// because they share a pointer to the same JobKey.
func (j *AuroraJob) Environment(env string) *AuroraJob {
	j.jobConfig.Key.Environment = env
	return j
}

// Set AuroraJob Key Role.
func (j *AuroraJob) Role(role string) *AuroraJob {
	j.jobConfig.Key.Role = role

	// Will be deprecated
	identity := &aurora.Identity{User: role}
	j.jobConfig.Owner = identity
	j.jobConfig.TaskConfig.Owner = identity
	return j
}

// Set AuroraJob Key Name.
func (j *AuroraJob) Name(name string) *AuroraJob {
	j.jobConfig.Key.Name = name
	return j
}

// How many instances of the job to run
func (j *AuroraJob) InstanceCount(instCount int32) *AuroraJob {
	j.jobConfig.InstanceCount = instCount
	return j
}

func (j *AuroraJob) CronSchedule(cron string) *AuroraJob {
	j.jobConfig.CronSchedule = &cron
	return j
}

func (j *AuroraJob) CronCollisionPolicy(policy aurora.CronCollisionPolicy) *AuroraJob {
	j.jobConfig.CronCollisionPolicy = policy
	return j
}

// How many instances of the job to run
func (j *AuroraJob) GetInstanceCount() int32 {
	return j.jobConfig.InstanceCount
}

// Get the current job configurations key to use for some realis calls.
func (j *AuroraJob) JobKey() aurora.JobKey {
	return *j.jobConfig.Key
}

// Get the current job configurations key to use for some realis calls.
func (j *AuroraJob) JobConfig() *aurora.JobConfiguration {
	return j.jobConfig
}

/*
   AuroraTask specific API, see task.go for further documentation.
   These functions are provided for the convenience of chaining API calls.
*/

func (j *AuroraJob) ExecutorName(name string) *AuroraJob {
	j.task.ExecutorName(name)
	return j
}

func (j *AuroraJob) ExecutorData(data string) *AuroraJob {
	j.task.ExecutorData(data)
	return j
}

func (j *AuroraJob) CPU(cpus float64) *AuroraJob {
	j.task.CPU(cpus)
	return j
}

func (j *AuroraJob) RAM(ram int64) *AuroraJob {
	j.task.RAM(ram)
	return j
}

func (j *AuroraJob) Disk(disk int64) *AuroraJob {
	j.task.Disk(disk)
	return j
}

func (j *AuroraJob) Tier(tier string) *AuroraJob {
	j.task.Tier(tier)
	return j
}

func (j *AuroraJob) MaxFailure(maxFail int32) *AuroraJob {
	j.task.MaxFailure(maxFail)
	return j
}

func (j *AuroraJob) IsService(isService bool) *AuroraJob {
	j.task.IsService(isService)
	return j
}

func (j *AuroraJob) TaskConfig() *aurora.TaskConfig {
	return j.task.TaskConfig()
}

func (j *AuroraJob) AddURIs(extract bool, cache bool, values ...string) *AuroraJob {
	j.task.AddURIs(extract, cache, values...)
	return j
}

func (j *AuroraJob) AddLabel(key string, value string) *AuroraJob {
	j.task.AddLabel(key, value)
	return j
}

func (j *AuroraJob) AddNamedPorts(names ...string) *AuroraJob {
	j.task.AddNamedPorts(names...)
	return j
}

func (j *AuroraJob) AddPorts(num int) *AuroraJob {
	j.task.AddPorts(num)
	return j
}
func (j *AuroraJob) AddValueConstraint(name string, negated bool, values ...string) *AuroraJob {
	j.task.AddValueConstraint(name, negated, values...)
	return j
}

func (j *AuroraJob) AddLimitConstraint(name string, limit int32) *AuroraJob {
	j.task.AddLimitConstraint(name, limit)
	return j
}

func (j *AuroraJob) AddDedicatedConstraint(role, name string) *AuroraJob {
	j.task.AddDedicatedConstraint(role, name)
	return j
}

func (j *AuroraJob) Container(container Container) *AuroraJob {
	j.task.Container(container)
	return j
}
