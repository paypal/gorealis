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
	"gen-go/apache/aurora"
	"strconv"
)

type Job struct {
	jobConfig *aurora.JobConfiguration
	numCpus   *aurora.Resource
	ramMb     *aurora.Resource
	diskMb    *aurora.Resource
	portCount int
}

type CreateJobBuilder struct {
	jobConfig  *aurora.JobConfiguration
	jobKey     *aurora.JobKey
	taskConfig *aurora.TaskConfig
	numCpus    *aurora.Resource
	ramMb      *aurora.Resource
	diskMb     *aurora.Resource
}

func NewJob() *Job {
	jobConfig := aurora.NewJobConfiguration()
	taskConfig := aurora.NewTaskConfig()
	jobKey := aurora.NewJobKey()

	//Job Config
	jobConfig.Key = jobKey
	jobConfig.TaskConfig = taskConfig

	//Task Config
	taskConfig.Job = jobKey
	taskConfig.Container = aurora.NewContainer()
	taskConfig.Container.Mesos = aurora.NewMesosContainer()
	taskConfig.ExecutorConfig = aurora.NewExecutorConfig()
	taskConfig.MesosFetcherUris = make(map[*aurora.MesosFetcherURI]bool)
	taskConfig.Metadata = make(map[*aurora.Metadata]bool)
	taskConfig.Constraints = make(map[*aurora.Constraint]bool)

	//Resources
	numCpus := aurora.NewResource()
	ramMb := aurora.NewResource()
	diskMb := aurora.NewResource()

	taskConfig.Resources = make(map[*aurora.Resource]bool)
	taskConfig.Resources[numCpus] = true
	taskConfig.Resources[ramMb] = true
	taskConfig.Resources[diskMb] = true

	return &Job{jobConfig, numCpus, ramMb, diskMb, 0}
}

func (a *Job) SetEnvironment(env string) *Job {
	a.jobConfig.Key.Environment = env
	return a
}

func (a *Job) SetRole(role string) *Job {
	a.jobConfig.Key.Role = role

	//Will be deprecated
	identity := &aurora.Identity{role}
	a.jobConfig.Owner = identity
	a.jobConfig.TaskConfig.Owner = identity
	return a
}

func (a *Job) SetName(name string) *Job {
	a.jobConfig.Key.Name = name
	return a
}

func (a *Job) SetExecutorName(name string) *Job {
	a.jobConfig.TaskConfig.ExecutorConfig.Name = name
	return a
}

func (a *Job) SetExecutorData(data string) *Job {
	a.jobConfig.TaskConfig.ExecutorConfig.Data = data
	return a
}

func (a *Job) SetNumCpus(cpus float64) *Job {
	a.numCpus.NumCpus = &cpus
	a.jobConfig.TaskConfig.NumCpus = cpus //Will be deprecated soon

	return a
}

func (a *Job) SetRam(ram int64) *Job {
	a.ramMb.RamMb = &ram
	a.jobConfig.TaskConfig.RamMb = ram //Will be deprecated soon

	return a
}

func (a *Job) SetDisk(disk int64) *Job {
	a.diskMb.DiskMb = &disk
	a.jobConfig.TaskConfig.DiskMb = disk //Will be deprecated

	return a
}

func (a *Job) SetMaxFailure(maxFail int32) *Job {
	a.jobConfig.TaskConfig.MaxTaskFailures = maxFail
	return a
}

func (a *Job) SetInstanceCount(instCount int32) *Job {
	a.jobConfig.InstanceCount = instCount
	return a
}

func (a *Job) SetIsService(isService bool) *Job {
	a.jobConfig.TaskConfig.IsService = isService
	return a
}

func (a *Job) GetJobKey() *aurora.JobKey {
	return a.jobConfig.Key
}

func (a *Job) AddURI(value string, extract bool, cache bool) *Job {
	a.jobConfig.TaskConfig.MesosFetcherUris[&aurora.MesosFetcherURI{value, &extract, &cache}] = true
	return a
}

func (a *Job) AddURIs(extract bool, cache bool, values ...string) *Job {
	for _, value := range values {
		a.jobConfig.TaskConfig.MesosFetcherUris[&aurora.MesosFetcherURI{value, &extract, &cache}] = true
	}
	return a
}

// Note: By default Aurora will add the prefix "org.apache.aurora.metadata." to the beginning of each key
func (a *Job) AddLabel(key string, value string) *Job {
	a.jobConfig.TaskConfig.Metadata[&aurora.Metadata{key, value}] = true
	return a
}

//Each port is equivalent to Marathon's 0 port
func (a *Job) AddPorts(num int) *Job {
	start := a.portCount
	a.portCount += num
	for i := start; i < a.portCount; i++ {
		portName := "port" + strconv.Itoa(i)
		a.jobConfig.TaskConfig.Resources[&aurora.Resource{NamedPort: &portName}] = true
	}

	return a
}

// Add a Value constraint,
// name - Mesos slave attribute that the constraint is matched against.
// If negated = true , treat this as a 'not' - to avoid specific values.
func (a *Job) AddValueConstraint(name string,
	negated bool,
	values ...string) *Job {

	constraintValues := make(map[string]bool)
	for _, value := range values {
		constraintValues[value] = true
	}
	a.jobConfig.TaskConfig.Constraints[&aurora.Constraint{name,
		&aurora.TaskConstraint{&aurora.ValueConstraint{negated, constraintValues}, nil}}] = true

	return a
}

// From Aurora Docs: A constraint the specifies the maximum number of active tasks on a host with
// a matching attribute that may be scheduled simultaneously.
func (a *Job) AddLimitConstraint(name string, limit int32) *Job {

	a.jobConfig.TaskConfig.Constraints[&aurora.Constraint{name,
		&aurora.TaskConstraint{nil, &aurora.LimitConstraint{limit}}}] = true

	return a
}
