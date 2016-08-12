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

// Structure to collect all information pertaining to an Aurora job.
type Job struct {
	jobConfig *aurora.JobConfiguration
	numCpus   *aurora.Resource
	ramMb     *aurora.Resource
	diskMb    *aurora.Resource
	portCount int
}

// Create a Job object with everything initialized.
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

// Set Job Key environment.
func (a *Job) Environment(env string) *Job {
	a.jobConfig.Key.Environment = env
	return a
}

// Set Job Key Role.
func (a *Job) Role(role string) *Job {
	a.jobConfig.Key.Role = role

	//Will be deprecated
	identity := &aurora.Identity{role}
	a.jobConfig.Owner = identity
	a.jobConfig.TaskConfig.Owner = identity
	return a
}

// Set Job Key Name.
func (a *Job) Name(name string) *Job {
	a.jobConfig.Key.Name = name
	return a
}

// Set name of the executor that will the task will be configured to.
func (a *Job) ExecutorName(name string) *Job {
	a.jobConfig.TaskConfig.ExecutorConfig.Name = name
	return a
}

// Will be included as part of entire task inside the scheduler that will be serialized.
func (a *Job) ExecutorData(data string) *Job {
	a.jobConfig.TaskConfig.ExecutorConfig.Data = data
	return a
}

func (a *Job) CPU(cpus float64) *Job {
	a.numCpus.NumCpus = &cpus
	a.jobConfig.TaskConfig.NumCpus = cpus //Will be deprecated soon

	return a
}

func (a *Job) RAM(ram int64) *Job {
	a.ramMb.RamMb = &ram
	a.jobConfig.TaskConfig.RamMb = ram //Will be deprecated soon

	return a
}

func (a *Job) Disk(disk int64) *Job {
	a.diskMb.DiskMb = &disk
	a.jobConfig.TaskConfig.DiskMb = disk //Will be deprecated

	return a
}

// How many failures to tolerate before giving up.
func (a *Job) MaxFailure(maxFail int32) *Job {
	a.jobConfig.TaskConfig.MaxTaskFailures = maxFail
	return a
}

// How many instances of the job to run
func (a *Job) InstanceCount(instCount int32) *Job {
	a.jobConfig.InstanceCount = instCount
	return a
}

// Restart the job's tasks if they fail
func (a *Job) IsService(isService bool) *Job {
	a.jobConfig.TaskConfig.IsService = isService
	return a
}

// Get the current job configurations key to use for some realis calls.
func (a *Job) JobKey() *aurora.JobKey {
	return a.jobConfig.Key
}

// Add URI to fetch using the mesos fetcher. Scheduler must have --enable_mesos_fetcher flag
// enabled.
func (a *Job) AddURI(value string, extract bool, cache bool) *Job {
	a.jobConfig.TaskConfig.MesosFetcherUris[&aurora.MesosFetcherURI{value, &extract, &cache}] = true
	return a
}

// Add a list of URIs with the same extract and cache configuration.
func (a *Job) AddURIs(extract bool, cache bool, values ...string) *Job {
	for _, value := range values {
		a.jobConfig.TaskConfig.MesosFetcherUris[&aurora.MesosFetcherURI{value, &extract, &cache}] = true
	}
	return a
}

// Adds a Mesos label to the job. Note that as of Aurora 0.15.0, Aurora will add the
// prefix "org.apache.aurora.metadata." to the beginning of each key.
func (a *Job) AddLabel(key string, value string) *Job {
	a.jobConfig.TaskConfig.Metadata[&aurora.Metadata{key, value}] = true
	return a
}

// Add a named port to the job configuration  These are random ports as it's
// not currently possible to request specific ports using Aurora.
func (a *Job) AddNamedPorts(names ...string) *Job {
	a.portCount += len(names)
	for _, name := range names {
		a.jobConfig.TaskConfig.Resources[&aurora.Resource{NamedPort: &name}] = true
	}

	return a
}


// Adds a request for a number of ports to the job configuration. The names chosen for these ports
// will be org.apache.aurora.portX, where X is the current port count for the job configuration
// starting at 0. These are random ports as it's not currently possible to request
// specific ports using Aurora.
func (a *Job) AddPorts(num int) *Job {
	start := a.portCount
	a.portCount += num
	for i := start; i < a.portCount; i++ {
		portName := "gorealis.port" + strconv.Itoa(i)
		a.jobConfig.TaskConfig.Resources[&aurora.Resource{NamedPort: &portName}] = true
	}

	return a
}

// From Aurora Docs:
// Add a Value constraint
// name - Mesos slave attribute that the constraint is matched against.
// If negated = true , treat this as a 'not' - to avoid specific values.
// Values - list of values we look for in attribute name
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

// From Aurora Docs:
// A constraint that specifies the maximum number of active tasks on a host with
// a matching attribute that may be scheduled simultaneously.
func (a *Job) AddLimitConstraint(name string, limit int32) *Job {

	a.jobConfig.TaskConfig.Constraints[&aurora.Constraint{name,
		&aurora.TaskConstraint{nil, &aurora.LimitConstraint{limit}}}] = true

	return a
}
